# orchestrator.py

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

import asyncio
import json
import socket
import subprocess
import time
from datetime import datetime, timedelta, timezone
from collections import OrderedDict
from dotenv import load_dotenv
from config_loader import config
import logging
from logging_utils import setup_logger
import redis.asyncio as aredis

# --- Logging Setup ---
setup_logger("orchestrator.json")
log = logging.getLogger(__name__)
load_dotenv(override=True)

# --- Configuration ---
try:
    server_ports = config['application_settings']['server_ports']
    orchestrator_cfg = config.get('orchestrator_settings', {})
    MAX_RESTARTS = orchestrator_cfg.get('max_restarts', 5)
except KeyError as e:
    log.critical(f"Missing critical configuration key: {e}. Check your config.yaml structure.")
    sys.exit(1)

SCRIPT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
running_processes = []

# --- Deduping & Correction Globals ---
processed_alerts_cache = OrderedDict()
CACHE_TTL_SECONDS = 300 # 5 Minutes

# Known OCR misreads map. User can expand this.
OCR_CORRECTIONS = {
    "8MNR": "BMNR",
    "GME": "GME", # Example
}

def correct_ocr_symbol(raw_symbol):
    """Corrects common OCR misreads."""
    clean = raw_symbol.upper().strip()
    return OCR_CORRECTIONS.get(clean, clean)

def is_duplicate_signal(symbol, signal_time_str):
    """Checks if this exact signal was processed recently."""
    global processed_alerts_cache
    
    # Prune old cache entries
    now = time.time()
    keys_to_remove = [k for k, (ts, _) in processed_alerts_cache.items() if now - ts > CACHE_TTL_SECONDS]
    for k in keys_to_remove:
        del processed_alerts_cache[k]
        
    # Create a unique key for this event
    key = f"{symbol}_{signal_time_str}"
    
    if key in processed_alerts_cache:
        return True
    
    # Add to cache
    processed_alerts_cache[key] = (now, True)
    return False

async def forward_alert(alert_data):
    """Forwards alerts to the correct trading module based on TRADING_MODE."""
    # Local import to avoid circular dependency issues at global scope
    from market_data import get_current_quote
    
    trading_mode = os.getenv("TRADING_MODE", "PAPER").upper()
    host = '127.0.0.1'
    port = server_ports['paper_trade'] if trading_mode == "PAPER" else server_ports['stream_and_trade']
    
    try:
        if symbol := alert_data.get("symbol"):
            quote = await get_current_quote(symbol)
            alert_data['current_price'] = quote.get('lastPrice') if quote else None

        # Give the child server a moment to start up if needed
        await asyncio.sleep(0.5)
        
        _, writer = await asyncio.open_connection(host, port)
        message = json.dumps(alert_data) + '\n'
        writer.write(message.encode('utf-8'))
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        log.info(f"Alert forwarded successfully to {trading_mode} trader.", extra={"symbol": alert_data.get("symbol")})

    except (ConnectionRefusedError, OSError) as e:
        log.error(f"Failed to connect to {trading_mode} trader to forward alert. The service may be restarting.", extra={"error": str(e)})
    except Exception as e:
        log.error("Unexpected error forwarding alert.", exc_info=True)


async def read_and_forward_stream(stream, process_name):
    """Asynchronously read from a stream and forward its output."""
    while True:
        try:
            line_bytes = await stream.readline()
            if not line_bytes:
                break  # End of stream
            
            decoded_line = line_bytes.decode('utf-8', errors='replace').rstrip()
            if decoded_line:
                # Print to stdout so Dashboard can capture it via its own pipe
                print(decoded_line, flush=True)

                # Forward specific IPC messages
                if "[TOS_ALERT_NEW]" in decoded_line:
                    try:
                        # Parse the Raw JSON
                        raw_json_str = decoded_line.split("]", 1)[1].strip()
                        alert_data = json.loads(raw_json_str)
                        
                        # 1. Correct Symbol
                        raw_symbol = alert_data.get("symbol", "")
                        corrected_symbol = correct_ocr_symbol(raw_symbol)
                        alert_data["symbol"] = corrected_symbol

                        # 2. Fix Timestamp (Add Date)
                        # Assuming incoming format is HH:MM:SS or similar time-only
                        raw_time = alert_data.get("timestamp") or alert_data.get("time") or datetime.now().strftime("%H:%M:%S")
                        
                        # If raw_time is just time (length < 10), append today's date
                        if len(raw_time) < 12: 
                            full_timestamp = f"{datetime.now().strftime('%Y-%m-%d')}T{raw_time}"
                        else:
                            full_timestamp = raw_time
                        
                        alert_data["timestamp"] = full_timestamp

                        # 3. Tag Source
                        alert_data["source"] = "OCR"
                        
                        # 4. Deduping
                        if is_duplicate_signal(corrected_symbol, full_timestamp):
                            log.info(f"Dropped duplicate OCR signal: {corrected_symbol} @ {full_timestamp}")
                            continue

                        if raw_symbol != corrected_symbol:
                            log.info(f"OCR Correction: '{raw_symbol}' -> '{corrected_symbol}'")

                        asyncio.create_task(forward_alert(alert_data))
                        
                    except (json.JSONDecodeError, IndexError):
                        log.error("Could not parse TOS_ALERT_NEW IPC message.", extra={"line": decoded_line})
        except Exception as e:
            log.error(f"Error reading stream for {process_name}", exc_info=True)
            break

def is_port_in_use(port: int) -> bool:
    """Checks if a network port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0

def kill_process_on_port(port: int):
    """Finds and forcefully terminates a process using a specific port."""
    if not is_port_in_use(port):
        return
        
    log.warning(f"Port {port} is in use by a zombie process. Attempting to terminate it...")
    try:
        if os.name == 'nt': # For Windows
            result = subprocess.run(
                f"netstat -aon | findstr \"LISTENING\" | findstr \":{port}\"", 
                shell=True, capture_output=True, text=True
            )
            if result.stdout:
                line = result.stdout.strip().split('\n')[0]
                pid = line.strip().split()[-1]
                
                if pid.isdigit() and int(pid) > 0:
                    subprocess.run(f"taskkill /PID {pid} /F", shell=True, check=True)
                    log.info(f"Successfully terminated zombie process with PID {pid} on port {port}.")
                    time.sleep(1) 
                else:
                    log.error(f"Could not parse a valid PID for port {port}. Found '{pid}'.")
        else: # For macOS/Linux
            result = subprocess.run(f"lsof -ti:{port}", shell=True, capture_output=True, text=True)
            if result.stdout:
                pid = result.stdout.strip()
                subprocess.run(f"kill -9 {pid}", shell=True, check=True)
                log.info(f"Successfully terminated zombie process with PID {pid} on port {port}.")
                time.sleep(1)
    except Exception:
        log.error(f"Failed to automatically kill process on port {port}.", exc_info=True)

shutdown_event = asyncio.Event()

async def monitor_and_restart(name, script_path, args=None):
    """Starts, monitors, forwards output from, and restarts a child process."""
    global running_processes 
    
    if not os.path.exists(script_path):
        log.critical(f"Cannot start process '{name}'. File not found at path: {script_path}")
        return

    restart_count = 0
    while restart_count < MAX_RESTARTS:
        log.info(f"Starting process '{name}'", extra={"attempt": f"{restart_count + 1}/{MAX_RESTARTS}"})
        
        process_env = os.environ.copy()
        process_env["PYTHONPATH"] = SCRIPT_DIRECTORY
        command = [sys.executable, "-u", script_path] + (args if args else [])
        
        try: 
            proc = await asyncio.create_subprocess_exec(
                *command, 
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.STDOUT, 
                limit=1024 * 1024,
                cwd=SCRIPT_DIRECTORY,
                env=process_env,
                start_new_session=True 
            )
            running_processes.append(proc)

            stream_task = asyncio.create_task(read_and_forward_stream(proc.stdout, name))

            await proc.wait()
            stream_task.cancel()
            
            if proc in running_processes:
                running_processes.remove(proc)

            if proc.returncode == 0:
                log.info(f"Process '{name}' exited gracefully.", extra={"exit_code": proc.returncode})
                break
            else:
                log.warning(f"Process '{name}' exited with an error.", extra={"exit_code": proc.returncode})
                restart_count += 1
                if restart_count < MAX_RESTARTS:
                    await asyncio.sleep(5)
                else:
                    log.critical(f"Process '{name}' has failed {MAX_RESTARTS} times.")

        except Exception:
            log.critical(f"Unhandled error while supervising '{name}'.", exc_info=True)
            restart_count += 1
            await asyncio.sleep(5)
            
    log.error(f"Supervision for '{name}' terminated.")

# --- NEW: Self-Healing Heartbeat ---
async def report_orchestrator_heartbeat():
    """Reports the Orchestrator's own status to Redis independently."""
    service_name = "orchestrator"
    try:
        r = aredis.from_url("redis://localhost", decode_responses=True)
        log.info("Orchestrator heartbeat loop started.")
        while True:
            try:
                status_payload = {"status": "RUNNING", "timestamp": datetime.now(timezone.utc).isoformat()}
                await r.hset("process_statuses", service_name, json.dumps(status_payload))
                await asyncio.sleep(5)
            except Exception as e:
                log.warning(f"Heartbeat failed: {e}")
                await asyncio.sleep(15)
    except Exception as e:
        log.error(f"Failed to start orchestrator heartbeat: {e}")

async def main():
    log.info("Performing pre-flight check to clear occupied ports...")
    
    dashboard_port = server_ports.get('dashboard') 
    
    for service_name, service_port in server_ports.items():
        if service_port != dashboard_port:
            kill_process_on_port(service_port)
    
    scripts_to_run = {
        "market_data_service": (os.path.join(SCRIPT_DIRECTORY, "market_data_service.py"), None),
        "stream_and_trade": (os.path.join(SCRIPT_DIRECTORY, "stream_and_trade.py"), None),
        "paper_trade": (os.path.join(SCRIPT_DIRECTORY, "paper_trading.py"), None),
        "TOS_alerts": (os.path.join(SCRIPT_DIRECTORY, "TOS_message_center_alerts.py"), None),
        "exit_monitor": (os.path.join(SCRIPT_DIRECTORY, "exit_monitor.py"), None),
        "schwab_iv_driver": (os.path.join(SCRIPT_DIRECTORY, "schwab_iv_driver.py"), None),
        "signal_processor": (os.path.join(SCRIPT_DIRECTORY, "signal_processor.py"), None),
        "news_scraper": (os.path.join(SCRIPT_DIRECTORY, "news_engine", "news_scraper_service.py"), None),
        "charting_service": (os.path.join(SCRIPT_DIRECTORY, "charting_service.py"), None),
        "state_reconciler": (os.path.join(SCRIPT_DIRECTORY, "state_reconciler.py"), None),
    }

    # Start Heartbeat Task
    asyncio.create_task(report_orchestrator_heartbeat())

    tasks = []
    for name, (path, args) in scripts_to_run.items():
        task = asyncio.create_task(monitor_and_restart(name, path, args))
        tasks.append(task)
        await asyncio.sleep(2)

    log.info("All supervised tasks have been launched.")
    await shutdown_event.wait()
    
    log.info("Shutdown signal received. Cancelling all supervised tasks...")
    for task in tasks: task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    
    for proc in running_processes:
        if proc.returncode is None: proc.terminate()
            
    await asyncio.sleep(2)
    for proc in running_processes:
        if proc.returncode is None: proc.kill()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    main_task = None
    try:
        main_task = loop.create_task(main())
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        log.info("Orchestrator shutdown signal received via KeyboardInterrupt.")
    finally:
        if not shutdown_event.is_set():
            shutdown_event.set()
        
        if main_task and not main_task.done():
            try:
                main_task.cancel()
                loop.run_until_complete(main_task)
            except asyncio.CancelledError:
                pass 

        pending = asyncio.all_tasks(loop=loop)
        for task in pending:
            task.cancel()
        group = asyncio.gather(*pending, return_exceptions=True)
        loop.run_until_complete(group)
        loop.close()
        log.info("Orchestrator has shut down cleanly.")
