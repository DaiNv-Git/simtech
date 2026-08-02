package app.simsmartgsm.config;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Quản lý reconnection với exponential backoff và circuit breaker
 * để tránh spam reconnect khi server down
 */
@Slf4j
public class ReconnectionManager {

    private final String name;
    private final Runnable reconnectAction;

    // Exponential backoff config
    private static final long INITIAL_DELAY_MS = 5_000; // 5 seconds
    private static final long MAX_DELAY_MS = 300_000; // 5 minutes
    private static final int MAX_RETRIES_BEFORE_CIRCUIT_BREAK = 10;

    // Circuit breaker config
    private static final long CIRCUIT_BREAKER_DURATION_MS = 15 * 60 * 1000; // 15 minutes

    // State
    private final AtomicInteger retryCount = new AtomicInteger(0);
    private final AtomicLong currentDelay = new AtomicLong(INITIAL_DELAY_MS);
    private final AtomicLong circuitBreakerUntil = new AtomicLong(0);
    private final ScheduledExecutorService scheduler;

    public ReconnectionManager(String name, Runnable reconnectAction) {
        this.name = name;
        this.reconnectAction = reconnectAction;

        // ✅ Tạo scheduler sau khi name đã được khởi tạo
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("Reconnect-" + this.name);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Gọi khi connection thành công - reset tất cả counters
     */
    public void onConnected() {
        int previousRetries = retryCount.getAndSet(0);
        currentDelay.set(INITIAL_DELAY_MS);
        circuitBreakerUntil.set(0);

        if (previousRetries > 0) {
            log.info("✅ [{}] Connection restored after {} retries", name, previousRetries);
        }
    }

    /**
     * Gọi khi connection fail - schedule retry với backoff
     */
    public void onConnectionFailed(String reason) {
        // Kiểm tra circuit breaker
        long breakerUntil = circuitBreakerUntil.get();
        if (breakerUntil > System.currentTimeMillis()) {
            long remainingSeconds = (breakerUntil - System.currentTimeMillis()) / 1000;
            if (retryCount.get() % 10 == 1) { // Log mỗi 10 lần để tránh spam
                log.warn("🔴 [{}] Circuit breaker active - will retry in {}s", name, remainingSeconds);
            }
            return;
        }

        int count = retryCount.incrementAndGet();

        // Activate circuit breaker nếu quá nhiều lần thất bại
        if (count >= MAX_RETRIES_BEFORE_CIRCUIT_BREAK) {
            activateCircuitBreaker();
            return;
        }

        // Exponential backoff
        long delay = currentDelay.get();
        long nextDelay = Math.min(delay * 2, MAX_DELAY_MS);
        currentDelay.set(nextDelay);

        // Log có chọn lọc để tránh spam
        if (count <= 3 || count % 5 == 0) {
            log.warn("⚠️ [{}] Connection failed ({}): retry #{} in {}s",
                    name, reason, count, delay / 1000);
        }

        // Schedule retry
        scheduler.schedule(() -> {
            try {
                reconnectAction.run();
            } catch (Exception e) {
                log.error("❌ [{}] Reconnect action failed: {}", name, e.getMessage());
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Kích hoạt circuit breaker - tạm dừng reconnect
     */
    private void activateCircuitBreaker() {
        long until = System.currentTimeMillis() + CIRCUIT_BREAKER_DURATION_MS;
        circuitBreakerUntil.set(until);

        log.error("🔴 [{}] Circuit breaker ACTIVATED after {} failed attempts", name, retryCount.get());
        log.error("🔴 [{}] Server appears DOWN - pausing reconnection for 15 minutes", name);
        log.error("🔴 [{}] Will resume at: {}", name, new java.util.Date(until));

        // Schedule circuit breaker reset
        scheduler.schedule(() -> {
            log.info("🟢 [{}] Circuit breaker RESET - resuming reconnection", name);
            retryCount.set(0);
            currentDelay.set(INITIAL_DELAY_MS);
            circuitBreakerUntil.set(0);

            try {
                reconnectAction.run();
            } catch (Exception e) {
                log.error("❌ [{}] Reconnect after circuit breaker failed: {}", name, e.getMessage());
            }
        }, CIRCUIT_BREAKER_DURATION_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Cleanup khi shutdown
     */
    public void shutdown() {
        log.info("🧹 [{}] Shutting down reconnection manager", name);
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Lấy số lần retry hiện tại (cho testing)
     */
    public int getRetryCount() {
        return retryCount.get();
    }

    /**
     * Kiểm tra circuit breaker có active không (cho testing)
     */
    public boolean isCircuitBreakerActive() {
        return circuitBreakerUntil.get() > System.currentTimeMillis();
    }
}
