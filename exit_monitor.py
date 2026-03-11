# exit_monitor.py (Definitive Merged: EOD + Trailing Stops + Cooldown + Robust Recovery)

import asyncio
import json
import os
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import sys
import logging
import pandas_market_calendars as mcal
from zoneinfo import ZoneInfo

import redis.asyncio as aredis 
from redis import exceptions as RedisExceptions

# --- Custom Module Imports ---
from logging_utils import setup_logger
from config_loader import config
from auth_utils import get_valid_access_token
from market_data import get_current_quote
from ml_predictor import MLPredictor
from feature_engineer import get_live_features_for_prediction

# --- Logging & Config ---
setup_logger("exit_monitor.json")
log = logging.getLogger(__name__)
load_dotenv() 

cfg = config['utilities']['exit_monitor']
paths = cfg['paths']
check_interval = config['trading_logic']['exit_logic_defaults']['check_interval']
ml_cfg = config['machine_learning']
exit_strategy_defaults = config['trading_logic']['exit_logic_defaults']
SCRIPT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
ET = ZoneInfo("America/New_York")

# --- Global State ---
exit_predictor = None
trading_rules = {"is_market_open_today": False, "sit_out_today": False, "market_open_time_et": None, "eod_exit_time_et": None, "last_checked_date": None, "market_close_time_et": None}
eod_routine_executed_for_date = None
eod_routine_has_run = False

def get_market_hours(now_et: datetime):
    """
    Checks the NYSE calendar to see if the market is open today.
    Updates the global trading_rules dictionary for this process.
    """
    global trading_rules
    
    # Only check once per day
    if trading_rules["last_checked_date"] == now_et.date():
        return
        
    log.info("Exit Monitor checking market calendar for today...")
    trading_rules["last_checked_date"] = now_et.date()
    nyse = mcal.get_calendar('NYSE')
    
    try:
        schedule_df = nyse.schedule(start_date=now_et.date(), end_date=now_et.date())
        
        if not schedule_df.empty:
            trading_rules["is_market_open_today"] = True
            
            # --- CRITICAL FIX: Convert UTC times from mcal to America/New_York ---
            market_open_utc = schedule_df.iloc[0]['market_open']
            market_close_utc = schedule_df.iloc[0]['market_close']
            
            market_open_et = market_open_utc.astimezone(ET)
            market_close_et = market_close_utc.astimezone(ET)
            
            trading_rules["market_open_time_et"] = market_open_et
            trading_rules["market_close_time_et"] = market_close_et
            
            # Set EOD exit time from config
            exit_minutes = exit_strategy_defaults.get('force_exit_minutes_before_close', 10)
            trading_rules["eod_exit_time_et"] = market_close_et - timedelta(minutes=exit_minutes)
            
            log.info(f"Today is a trading day. Open: {market_open_et.strftime('%H:%M')}, Close: {market_close_et.strftime('%H:%M')}, EOD Exit: {trading_rules['eod_exit_time_et'].strftime('%H:%M')} ET.")
        else:
            trading_rules["is_market_open_today"] = False
            log.info("Today is not a trading day (weekend or holiday). Exit monitor will idle.")

    except Exception as e:
        log.error(f"Error getting market schedule: {e}", exc_info=True)
        trading_rules["is_market_open_today"] = False


async def load_active_trades_from_redis(redis_conn):
    """Loads the current state of both LIVE and PAPER trades from Redis."""
    active_trades_local_copy = {} 
    if not redis_conn: 
        log.error("Cannot load trades; no Redis connection.")
        return {}

    try:
        # Load LIVE trades
        live_trades_redis = await redis_conn.hgetall("active_trades")
        for symbol, trade_data_json in live_trades_redis.items():
            trade_data = json.loads(trade_data_json)
            trade_data['trading_mode'] = 'LIVE'
            active_trades_local_copy[symbol] = trade_data

        # Load PAPER trades
        paper_state_json = await redis_conn.get("paper_account_state")
        if paper_state_json:
            paper_state = json.loads(paper_state_json)
            for pos in paper_state.get("openPositions", []):
                pos['trading_mode'] = 'PAPER'
                # Use unique key for paper to allow simultaneous live/paper on same symbol
                paper_key = f"{pos['symbol']}_PAPER"
                active_trades_local_copy[paper_key] = pos
        return active_trades_local_copy

    except (RedisExceptions.RedisError, json.JSONDecodeError):
        log.error("A Redis error occurred while loading trade states.", exc_info=True)
        return {}

async def signal_exit_via_redis(redis_conn, trade_details, reason):
    symbol = trade_details.get('symbol')
    mode = trade_details.get('trading_mode', 'LIVE').upper()
    
    # --- CRITICAL FIX: ANTI-SPAM COOLDOWN ---
    # Set a 60-second cooldown key. Even if the trade is re-imported by the 
    # Reconciler, this key persists, preventing a spam loop.
    try:
        if redis_conn:
            await redis_conn.set(f"exit_cooldown:{symbol}", "pending", ex=60)
    except Exception as e:
        log.warning(f"Failed to set exit cooldown for {symbol}: {e}")
    # ----------------------------------------

    log.info(f"Signaling automatic exit for {symbol}", extra={"mode": mode, "reason": reason})
    
    channel = 'live_trade_commands' if mode == 'LIVE' else 'paper_trade_commands'
    exit_command = { "action": "EXIT_POSITION", "symbol": symbol, "reason": reason, "timestamp": datetime.now().isoformat() }
    
    if redis_conn:
        await redis_conn.publish(channel, json.dumps(exit_command))
    else:
        log.error("Cannot signal exit; no Redis connection.")

async def check_exit_conditions(redis_conn):
    """
    Monitors active trades for exit conditions like stop-loss, profit targets, and a mandatory EOD close.
    """
    log.info("Starting exit condition check loop...")
    global eod_routine_executed_for_date, eod_routine_has_run, trading_rules
    
    while True:
        # Move load_dotenv inside to pick up dynamic config changes from dashboard
        load_dotenv(override=True)
        try:
            now_et = datetime.now(ET)
            current_date_et = now_et.date()

            get_market_hours(now_et)

            market_open = trading_rules.get("market_open_time_et")
            market_close = trading_rules.get("market_close_time_et")

            if not trading_rules["is_market_open_today"]:
                await asyncio.sleep(60)
                continue

            # Ensure we have valid times before comparing
            if not (market_open and market_close):
                await asyncio.sleep(60)
                continue

            # Check if we are within valid operating hours (allow 15m post-close for cleanup)
            if not (market_open <= now_et < (market_close + timedelta(minutes=15))):
                await asyncio.sleep(60) 
                continue
            
            # --- Reset the EOD flag on a new day ---
            if eod_routine_executed_for_date != current_date_et:
                eod_routine_executed_for_date = current_date_et
                eod_routine_has_run = False
            
            exit_all_flag = await redis_conn.get("exit_all_trades_flag")
            active_trades_local_copy = await load_active_trades_from_redis(redis_conn)

            # --- MANUAL "EXIT ALL" OVERRIDE ---
            if exit_all_flag == 'true':
                log.critical("EXIT_ALL_TRADES_FLAG detected. Signaling all positions to close immediately.")
                if active_trades_local_copy:
                    for trade in active_trades_local_copy.values():
                        await signal_exit_via_redis(redis_conn, trade, "Manual Exit All Flag")
                await redis_conn.delete("exit_all_trades_flag")
                await asyncio.sleep(check_interval)
                continue

            # --- EOD (End of Day) EXIT LOGIC ---
            force_exit_time = trading_rules.get("eod_exit_time_et")
            is_eod_strategy = os.getenv("TRADE_HOLDING_STRATEGY", "EOD").upper() == "EOD"
            
            if (is_eod_strategy and force_exit_time and
                now_et >= force_exit_time and 
                not eod_routine_has_run):
                
                log.warning("EOD force-exit time reached. Initiating closure of all open EOD positions.")
                eod_routine_has_run = True 
                
                if active_trades_local_copy:
                    paper_trades_to_verify = []
                    for trade in active_trades_local_copy.values():
                        await signal_exit_via_redis(redis_conn, trade, "End-of-Day force exit")
                        if trade.get('trading_mode') == 'PAPER':
                            paper_trades_to_verify.append(trade['symbol'])
                    
                    if paper_trades_to_verify:
                        log.info(f"Waiting 15 seconds to verify closure of {len(paper_trades_to_verify)} paper trades...")
                        await asyncio.sleep(15)

                        # --- EOD FAILSAFE with LOCKING ---
                        # We must acquire the same lock as paper_trading.py to safely modify state
                        async with redis_conn.lock("paper_account_state_lock", timeout=10.0):
                            paper_state_json = await redis_conn.get("paper_account_state")
                            if paper_state_json:
                                paper_state = json.loads(paper_state_json)
                                open_positions_after_cmd = paper_state.get("openPositions", [])
                                
                                # Clean up stragglers directly in Redis if command failed
                                if open_positions_after_cmd:
                                    log.error(f"{len(open_positions_after_cmd)} paper positions failed to close via command. Manually closing state in Redis.")
                                    
                                    from paper_trading import archive_trade # Local import to avoid circular dep
                                    
                                    for pos in list(open_positions_after_cmd):
                                        log.warning(f"Force-closing stale paper position: {pos['symbol']}")
                                        quote = await get_current_quote(pos['symbol'])
                                        exit_price = quote.get('closePrice') or quote.get('lastPrice') or pos['entryPrice']
                                        
                                        is_option = pos.get('assetType') == 'OPTION'
                                        multiplier = 100 if is_option else 1
                                        proceeds = pos['quantity'] * exit_price * multiplier
                                        pnl = (proceeds - pos['costBasis']) if pos.get('side', 'Long').lower() == 'long' else (pos['costBasis'] - proceeds)
                                        
                                        paper_state['cashBalance'] += proceeds
                                        paper_state['performance']['realizedPnL'] += pnl
                                        paper_state['performance']['tradeCount'] += 1
                                        if pnl > 0: paper_state['performance']['winCount'] += 1
                                        
                                        archive_details = {
                                            "trade_id": pos.get('trade_id') or f"eod-force-{int(time.time())}",
                                            "entryTimestamp": pos['entryTimestamp'],
                                            "exit_time": datetime.now(timezone.utc).isoformat(),
                                            "symbol": pos['symbol'],
                                            "side": pos.get('side', 'Long'),
                                            "entryPrice": pos['entryPrice'],
                                            "exit_price": exit_price,
                                            "quantity": pos['quantity'],
                                            "assetType": pos.get('assetType'),
                                            "costBasis": pos['costBasis'],
                                            "pnl": pnl,
                                            "notes": "Force-closed by Exit Monitor EOD"
                                        }
                                        archive_trade(archive_details)
                                    
                                    paper_state['openPositions'] = [p for p in open_positions_after_cmd if p['symbol'] != pos['symbol']]
                                    
                                    # Save safely under lock
                                    await redis_conn.set("paper_account_state", json.dumps(paper_state))
                                    log.info(f"Successfully cleaned stale paper position for {pos['symbol']}.")

                continue # Skip regular exit checks after EOD has run

            if not active_trades_local_copy:
                await asyncio.sleep(check_interval)
                continue

            # --- STANDARD EXIT LOGIC (Prices, Stops, Targets) ---
            
            # Load dynamic settings
            EXIT_STRATEGY_TYPE = os.getenv("EXIT_STRATEGY_TYPE", exit_strategy_defaults['exit_strategy']['default_type'])
            PRICE_TARGET_PCT = float(os.getenv("PRICE_TARGET_PCT", exit_strategy_defaults['exit_strategy']['default_price_target_pct']))
            
            # Enhanced Settings for Trailing Stops
            STOP_LOSS_TYPE = os.getenv("STOP_LOSS_TYPE", "Trailing %")
            STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_VALUE", config['trading_logic']['default_strategies']['stop_loss']['value_percent']))
            USE_ADV_STOP = os.getenv("USE_ADVANCED_STOP_LOSS", "false").lower() == "true"
            
            live_iv_metrics = await redis_conn.hgetall("iv_liquidity_metrics")
            
            for key, trade in list(active_trades_local_copy.items()):
                symbol = trade.get('symbol')
                
                # --- CHECK COOLDOWN ---
                # If we signaled recently, skip to prevent spam loop
                if await redis_conn.get(f"exit_cooldown:{symbol}"):
                    continue
                # ----------------------

                # --- CRITICAL RECOVERY FIX: Handle alias keys for Entry Price ---
                # Recovered trades (from State Reconciler) often use 'averagePrice' or 'price'
                # instead of 'entryPrice'. We must check all possibilities.
                try:
                    raw_entry = (trade.get('entryPrice') or 
                                 trade.get('entry_price') or 
                                 trade.get('averagePrice') or 
                                 trade.get('price') or 
                                 0.0)
                    entry_price = float(raw_entry)
                except (ValueError, TypeError):
                    log.warning(f"Invalid entry price found for {symbol}. Skipping exit check.")
                    continue

                if entry_price <= 0:
                    continue
                # -------------------------------------------------------------

                quote = await get_current_quote(symbol)
                current_price = quote.get('lastPrice')
                if current_price is None:
                    continue

                exit_triggered = False
                exit_reason = ""
                
                # --- 1. TRAILING STOP LOGIC ---
                # Initialize 'best_price' if missing (High Water Mark)
                if 'best_price' not in trade:
                    trade['best_price'] = entry_price

                side = trade.get('side', 'Long').upper()
                is_short = side in ['SHORT', 'SELL', 'STO']
                
                # Update High/Low Watermark
                state_changed = False
                if is_short:
                    if current_price < trade['best_price']:
                        trade['best_price'] = current_price
                        state_changed = True
                else: # Long
                    if current_price > trade['best_price']:
                        trade['best_price'] = current_price
                        state_changed = True

                # Check Stop Condition
                if USE_ADV_STOP and STOP_LOSS_TYPE == "Trailing %":
                    if is_short:
                        # Short: Exit if price rises X% above lowest point found
                        trail_trigger = trade['best_price'] * (1 + (STOP_LOSS_PCT / 100.0))
                        if current_price >= trail_trigger:
                            exit_triggered = True
                            exit_reason = f"Trailing Stop hit. Curr: {current_price} >= Trigger: {trail_trigger:.2f}"
                    else:
                        # Long: Exit if price falls X% below highest point found
                        trail_trigger = trade['best_price'] * (1 - (STOP_LOSS_PCT / 100.0))
                        if current_price <= trail_trigger:
                            exit_triggered = True
                            exit_reason = f"Trailing Stop hit. Curr: {current_price} <= Trigger: {trail_trigger:.2f}"
                else:
                    # Fallback to Fixed Stop Loss
                    trade_cost = trade.get('costBasis') or (entry_price * trade.get('quantity', 0))
                    if trade_cost > 0:
                        quantity = trade.get('quantity', 0)
                        is_option = trade.get('assetType') == 'OPTION' or (symbol and len(symbol) > 6)
                        multiplier = 100 if is_option else 1
                        current_market_value = current_price * quantity * multiplier
                        
                        if is_short:
                            pnl = trade_cost - current_market_value
                        else:
                            pnl = current_market_value - trade_cost
                            
                        pnl_percent = (pnl / trade_cost) * 100
                        if pnl_percent <= -STOP_LOSS_PCT:
                            exit_triggered = True
                            exit_reason = f"Fixed Stop loss of {STOP_LOSS_PCT}% reached."

                # Save updated 'best_price' to Redis if changed (Vital for persistence)
                if state_changed and not exit_triggered:
                    if trade.get('trading_mode') == 'LIVE':
                        await redis_conn.hset("active_trades", symbol, json.dumps(trade))
                
                if exit_triggered:
                    log.info("Automatic exit trigger met.", extra={"symbol": symbol, "reason": exit_reason})
                    await signal_exit_via_redis(redis_conn, trade, exit_reason)
                    continue

                # --- 2. PROFIT TARGET LOGIC (With Aliases) ---
                if EXIT_STRATEGY_TYPE in ["Price Target", "% Increase"]:
                    if is_short:
                        target_price = entry_price * (1 - (PRICE_TARGET_PCT / 100.0))
                        if current_price <= target_price:
                            exit_triggered = True
                            exit_reason = f"Profit target of ${target_price:.2f} reached."
                    else:
                        target_price = entry_price * (1 + (PRICE_TARGET_PCT / 100.0))
                        if current_price >= target_price:
                            exit_triggered = True
                            exit_reason = f"Profit target of ${target_price:.2f} reached."

                elif EXIT_STRATEGY_TYPE == "IV Spike":
                    if symbol in live_iv_metrics:
                        current_metrics = json.loads(live_iv_metrics[symbol])
                        current_iv = current_metrics.get('IV_Rank', 0.0)
                        previous_iv = trade.get("last_iv", current_iv)
                        iv_spike_threshold = float(os.getenv("IV_SPIKE_PCT", "10.0"))
                        
                        if (current_iv - previous_iv) >= iv_spike_threshold:
                            exit_triggered = True
                            exit_reason = f"IV Rank Spike of {(current_iv - previous_iv):.2f}% detected."

                        trade['last_iv'] = current_iv
                        if trade.get('trading_mode') == 'LIVE':
                           await redis_conn.hset("active_trades", symbol, json.dumps(trade))

                if exit_triggered:
                    log.info("Automatic exit trigger met.", extra={"symbol": symbol, "reason": exit_reason})
                    await signal_exit_via_redis(redis_conn, trade, exit_reason)
                    
        except Exception:
            log.error("Unhandled error in main check_exit_conditions loop.", exc_info=True)
        
        await asyncio.sleep(check_interval)

async def report_status_heartbeat(redis_conn):
    """Periodically reports the service's status to Redis."""
    service_name = "exit_monitor"
    while True:
        try:
            await redis_conn.ping() # Health check before writing
            status_payload = { "status": "RUNNING", "timestamp": datetime.now(timezone.utc).isoformat() }
            await redis_conn.hset("process_statuses", service_name, json.dumps(status_payload))
            await asyncio.sleep(5)
        except RedisExceptions.RedisError:
            log.error(f"Error in {service_name} heartbeat. Connection may be lost.", exc_info=True)
            await asyncio.sleep(15) # Wait longer before retrying
        except Exception:
            log.error(f"An unexpected error occurred in {service_name} heartbeat.", exc_info=True)
            await asyncio.sleep(15)

async def main():
    global exit_predictor
    log.info("Initializing Exit Monitor...")
    
    EXIT_MODEL_PATH = os.path.join(SCRIPT_DIRECTORY, ml_cfg['ml_data_dir'], ml_cfg['models_dir'], ml_cfg['exit_model']['model_file'])
    EXIT_METRICS_PATH = os.path.join(SCRIPT_DIRECTORY, ml_cfg['ml_data_dir'], ml_cfg['models_dir'], ml_cfg['exit_model']['metrics_file'])

    try:
        redis_conn_main = aredis.from_url(
            "redis://localhost", decode_responses=True, health_check_interval=30
        )
        await redis_conn_main.ping()
        log.info("Exit Monitor successfully connected to Redis (async).")
    except RedisExceptions.ConnectionError as e:
        log.critical(f"Exit Monitor could not connect to async Redis. Exiting. Error: {e}")
        sys.exit(1)
    
    if os.path.exists(EXIT_MODEL_PATH) and os.path.exists(EXIT_METRICS_PATH):
        log.info("Loading ML exit model.")
        exit_predictor = MLPredictor(EXIT_MODEL_PATH, EXIT_METRICS_PATH)
    else:
        log.warning("Exit model or metrics file not found. ML exit predictions will be disabled.")
    
    tasks = [
        check_exit_conditions(redis_conn_main),
        report_status_heartbeat(redis_conn_main)
    ]
    
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Process stopped by user (KeyboardInterrupt).")
        sys.exit(0)
    except Exception:
        log.critical("A critical unhandled exception occurred in the main exit_monitor process.", exc_info=True)
        sys.exit(1)
