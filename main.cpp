#include "Watchdog.hpp"
#include <iostream>
#include <thread>

void trigger_emergency_landing() {
    std::cout << "[SYSTEM] FAILSAFE ENGAGED: Executing emergency landing sequence..." << std::endl;
}

int main() {
    // Initialize Watchdog with a 250ms threshold
    Watchdog dog(std::chrono::milliseconds(250));
    dog.set_failsafe_callback(trigger_emergency_landing);

    std::cout << "[SYSTEM] Initializing Mission Logic..." << std::endl;
    dog.start();

    // Simulate 1 second of healthy operation (Kicking the dog)
    for (int i = 0; i < 5; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        std::cout << "[SYSTEM] Heartbeat sent..." << std::endl;
        dog.kick();
    }

    // SIMULATE A CRITICAL HANG (Stop kicking the dog)
    std::cout << "[SYSTEM] CRITICAL ERROR: Logic loop hung. Monitoring for timeout..." << std::endl;
    
    // We stop kicking here. After 250ms, the monitor thread will trigger the failsafe.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    dog.stop();
    return 0;
}