package app.simsmartgsm.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test cho ReconnectionManager
 */
class ReconnectionManagerTest {

    private ReconnectionManager manager;
    private AtomicInteger reconnectCallCount;
    private CountDownLatch reconnectLatch;

    @BeforeEach
    void setUp() {
        reconnectCallCount = new AtomicInteger(0);
        reconnectLatch = new CountDownLatch(1);

        manager = new ReconnectionManager("TEST", () -> {
            reconnectCallCount.incrementAndGet();
            reconnectLatch.countDown();
        });
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.shutdown();
        }
    }

    @Test
    void testOnConnected_shouldResetRetryCount() {
        // Simulate failures
        manager.onConnectionFailed("test error 1");
        manager.onConnectionFailed("test error 2");

        assertEquals(2, manager.getRetryCount(), "Should have 2 retries");

        // Simulate successful connection
        manager.onConnected();

        assertEquals(0, manager.getRetryCount(), "Retry count should reset to 0");
    }

    @Test
    void testOnConnectionFailed_shouldScheduleReconnect() throws InterruptedException {
        // Trigger failure
        manager.onConnectionFailed("test error");

        // Wait for reconnect to be called (with timeout)
        boolean reconnected = reconnectLatch.await(10, TimeUnit.SECONDS);

        assertTrue(reconnected, "Reconnect should be called");
        assertTrue(reconnectCallCount.get() >= 1, "Reconnect action should be executed");
    }

    @Test
    void testExponentialBackoff_shouldIncreaseDelay() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        // First failure - should retry in ~5s
        manager.onConnectionFailed("error 1");

        // Wait for first retry
        Thread.sleep(6000);

        long firstRetryTime = System.currentTimeMillis() - startTime;

        // Should be around 5 seconds (5000ms ± 1000ms tolerance)
        assertTrue(firstRetryTime >= 4000 && firstRetryTime <= 7000,
                "First retry should be around 5s, was: " + firstRetryTime + "ms");
    }

    @Test
    void testCircuitBreaker_shouldActivateAfterMaxRetries() {
        // Trigger 10 failures
        for (int i = 0; i < 10; i++) {
            manager.onConnectionFailed("error " + i);
        }

        // Circuit breaker should be active
        assertTrue(manager.isCircuitBreakerActive(),
                "Circuit breaker should activate after 10 failures");

        assertEquals(10, manager.getRetryCount(),
                "Should have exactly 10 retries before circuit breaker");
    }

    @Test
    void testCircuitBreaker_shouldPreventReconnectWhenActive() throws InterruptedException {
        // Activate circuit breaker
        for (int i = 0; i < 10; i++) {
            manager.onConnectionFailed("error " + i);
        }

        assertTrue(manager.isCircuitBreakerActive(), "Circuit breaker should be active");

        int callsBefore = reconnectCallCount.get();

        // Try to trigger more failures
        manager.onConnectionFailed("should be ignored");

        // Wait a bit
        Thread.sleep(2000);

        int callsAfter = reconnectCallCount.get();

        // Should not trigger new reconnect attempts
        assertEquals(callsBefore, callsAfter,
                "Should not schedule new reconnects when circuit breaker is active");
    }

    @Test
    void testOnConnected_shouldDeactivateCircuitBreaker() {
        // Activate circuit breaker
        for (int i = 0; i < 10; i++) {
            manager.onConnectionFailed("error " + i);
        }

        assertTrue(manager.isCircuitBreakerActive(), "Circuit breaker should be active");

        // Simulate successful connection
        manager.onConnected();

        assertFalse(manager.isCircuitBreakerActive(),
                "Circuit breaker should deactivate on successful connection");
        assertEquals(0, manager.getRetryCount(), "Retry count should reset");
    }

    @Test
    void testShutdown_shouldStopScheduler() throws InterruptedException {
        manager.onConnectionFailed("test error");

        // Shutdown
        manager.shutdown();

        int callsBefore = reconnectCallCount.get();

        // Wait to see if any more reconnects happen
        Thread.sleep(7000);

        int callsAfter = reconnectCallCount.get();

        // Should not execute more reconnects after shutdown
        assertEquals(callsBefore, callsAfter,
                "Should not execute reconnects after shutdown");
    }

    @Test
    void testMultipleFailures_shouldIncrementRetryCount() {
        manager.onConnectionFailed("error 1");
        assertEquals(1, manager.getRetryCount());

        manager.onConnectionFailed("error 2");
        assertEquals(2, manager.getRetryCount());

        manager.onConnectionFailed("error 3");
        assertEquals(3, manager.getRetryCount());
    }

    @Test
    void testReconnectAction_shouldHandleExceptions() throws InterruptedException {
        // Create manager with failing action
        ReconnectionManager failingManager = new ReconnectionManager("FAIL_TEST", () -> {
            throw new RuntimeException("Simulated failure");
        });

        try {
            // Should not throw exception
            assertDoesNotThrow(() -> failingManager.onConnectionFailed("test"));

            // Wait a bit
            Thread.sleep(6000);

            // Manager should still be functional
            assertEquals(1, failingManager.getRetryCount());

        } finally {
            failingManager.shutdown();
        }
    }
}
