# QPAI-Sentinel Core (Linux Port)

![Status](https://img.shields.io/badge/Status-Hardening_Phase-orange)
![Environment](https://img.shields.io/badge/Environment-WSL2_/_Ubuntu_22.04-blue)
![Language](https://img.shields.io/badge/Language-C%2B%2B17_/_Python_3.x-green)

## Overview
**QPAI-Sentinel** is a high-reliability watchdog system designed for autonomous platforms. It serves as a deterministic safety layer that monitors process health and system state, ensuring that failures in non-critical AI modules do not compromise the physical integrity of the platform.

### Evolutionary Architecture
This repository demonstrates a full engineering lifecycle:
1. **Rapid Prototype (Python):** `orchestrator.py` and `exit_monitor.py` were used to validate the initial logic flow and state transitions.
2. **Hardened Kernel (C++):** The logic was ported to native C++ to achieve POSIX compliance, sub-millisecond latency, and memory safety for deployment on edge-compute hardware.

## Core Features
* **Deterministic Heartbeat Monitoring:** Implements low-latency signal checks.
* **POSIX Signal Integration:** Leverages native Linux `SIGALRM` and `SIGKILL`.
* **Hardened Logging:** Isolated build logs to track failure points.
* **Zero-Overhead Architecture:** Designed for embedded systems and RTOS.

## Technical Implementation
The sentinel operates on a **"Fail-Safe, Not Fail-Soft"** philosophy. 

* **Watchdog.cpp:** Manages the primary monitoring thread using `std::chrono`.
* **Signal Handling:** Implements custom handlers to maintain state awareness.
* **Cross-Language Validation:** Logic remains consistent between the Python orchestrator and the C++ sentinel.

## Getting Started

### Prerequisites
* GCC/G++ 11.0 or higher
* Python 3.x
* Linux Kernel 5.10+ (or WSL2)

### Building the Hardened Sentinel
```bash
g++ -o watchdog_linux main.cpp Watchdog.cpp -lpthread


## Future Roadmap
- [x] Logic Porting (Python to C++)
- [ ] Implementation of Shared Memory (SHM) for IPC
- [ ] Integration with Hardware Watchdog Timers via /dev/watchdog
- [ ] Formal verification of safety-loop timing logic

***

***

## Contact
**Daniel Fike** [View My GitHub Profile](https://github.com/orchestrator17)  
Direct Link: https://github.com/orchestrator17
