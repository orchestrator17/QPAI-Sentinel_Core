#ifndef WATCHDOG_HPP
#define WATCHDOG_HPP

#include <atomic>
#include <chrono>
#include <functional>
#include <thread>
#include <string>

enum class SystemState {
    INITIALIZING,
    RUNNING,
    FAILSAFE,
    TERMINATED
};

class Watchdog {
public:
    // 250ms heartbeat as defined in your QPAI dossier
    Watchdog(std::chrono::milliseconds timeout = std::chrono::milliseconds(250));
    ~Watchdog();

    void start();
    void stop();
    void kick(); // Equivalent to the heartbeat signal
    void set_failsafe_callback(std::function<void()> callback);

    SystemState get_state() const { return current_state.load(); }

private:
    void monitor_loop();
    
    std::atomic<SystemState> current_state;
    std::atomic<bool> is_active;
    std::atomic<std::chrono::steady_clock::time_point> last_heartbeat;
    
    std::chrono::milliseconds timeout_threshold;
    std::thread worker_thread;
    std::function<void()> failsafe_action;
};

#endif