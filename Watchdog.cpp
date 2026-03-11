#include "Watchdog.hpp"
#include <iostream>

Watchdog::Watchdog(std::chrono::milliseconds timeout)
    : current_state(SystemState::INITIALIZING),
      is_active(false),
      timeout_threshold(timeout) {
    last_heartbeat = std::chrono::steady_clock::now();
}

Watchdog::~Watchdog() {
    stop();
}

void Watchdog::start() {
    is_active = true;
    current_state = SystemState::RUNNING;
    worker_thread = std::thread(&Watchdog::monitor_loop, this);
}

void Watchdog::stop() {
    is_active = false;
    if (worker_thread.joinable()) {
        worker_thread.join();
    }
    current_state = SystemState::TERMINATED;
}

void Watchdog::kick() {
    last_heartbeat = std::chrono::steady_clock::now();
}

void Watchdog::set_failsafe_callback(std::function<void()> callback) {
    failsafe_action = callback;
}

void Watchdog::monitor_loop() {
    while (is_active) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat.load());

        if (elapsed > timeout_threshold && current_state == SystemState::RUNNING) {
            current_state = SystemState::FAILSAFE;
            std::cerr << "[CRITICAL] Heartbeat Timeout: Entering Failsafe State!" << std::endl;
            
            if (failsafe_action) {
                failsafe_action(); // Trigger the Kill Switch / Safe State
            }
        }

        // Sleep for a fraction of the timeout to maintain deterministic checks
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}