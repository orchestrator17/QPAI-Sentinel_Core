# QPAI-Sentinel Core (Linux Port)

![Status](https://img.shields.io/badge/Status-Hardening_Phase-orange)
![Environment](https://img.shields.io/badge/Environment-WSL2_/_Ubuntu_22.04-blue)
![Language](https://img.shields.io/badge/Language-C%2B%2B17-green)

## Overview
**QPAI-Sentinel** is a high-reliability watchdog system designed for autonomous platforms. It serves as a deterministic safety layer that monitors process health and system state, ensuring that failures in non-critical AI modules do not compromise the physical integrity of the platform.

Originally prototyped for specific hardware, this repository represents the **Hardening Phase**, focusing on a full port to a Linux-native environment for deployment on edge-compute hardware.



## Core Features
* **Deterministic Heartbeat Monitoring:** Implements low-latency signal checks to ensure the main controller loop is executing within defined temporal constraints.
* **POSIX Signal Integration:** Leverages native Linux `SIGALRM` and `SIGKILL` for robust process management.
* **Hardened Logging:** Isolated build logs to track failure points without risking primary memory corruption.
* **Zero-Overhead Architecture:** Minimal footprint designed for embedded systems and real-time operating environments (RTOS).

## Technical Implementation
The sentinel operates on a "Fail-Safe, Not Fail-Soft" philosophy. 
* **Watchdog.cpp:** Manages the primary monitoring thread using `std::chrono` for microsecond-precision timing.
* **Signal Handling:** Implements custom handlers to gracefully manage system interrupts while maintaining state awareness.



## Getting Started

### Prerequisites
* GCC/G++ 11.0 or higher
* Make / CMake
* Linux Kernel 5.10+ (or WSL2)

### Building the Sentinel
To compile the current hardening build:
```bash
g++ -o watchdog_linux main.cpp Watchdog.cpp -lpthread
