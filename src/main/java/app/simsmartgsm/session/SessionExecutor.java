package app.simsmartgsm.session;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ✅ Session Executor cho 1 SIM
 * - Xử lý tasks tuần tự (single thread queue)
 * - Mỗi task chạy trong 1 session độc lập
 * - Tự động cleanup khi xong
 */
@Slf4j
public class SessionExecutor {

    private final String comPort;
    private final ExecutorService taskQueue;
    private final AtomicInteger activeCount = new AtomicInteger(0);
    private final AtomicInteger pendingCount = new AtomicInteger(0);

    public SessionExecutor(String comPort, ExecutorService sessionPool) {
        this.comPort = comPort;
        // Single thread executor → tasks xử lý tuần tự
        this.taskQueue = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "SessionQueue-" + comPort);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * ✅ Submit task và trả về CompletableFuture
     */
    public CompletableFuture<SessionResult> submit(SessionTask task) {
        pendingCount.incrementAndGet();

        CompletableFuture<SessionResult> future = new CompletableFuture<>();

        // Submit vào queue (tuần tự)
        taskQueue.submit(() -> {
            pendingCount.decrementAndGet();
            activeCount.incrementAndGet();

            log.info("🔄 [{}] Starting session: {} | orderId={} | pending={} | active={}",
                    comPort, task.getTaskType(), task.getOrderId(),
                    pendingCount.get(), activeCount.get());

            SessionResult result = null;
            try {
                // Tạo session độc lập
                try (TaskSession session = task.createSession()) {
                    // Mở port
                    session.openPort();

                    // Thực thi task
                    result = session.execute();

                    // Port tự động đóng (try-with-resources)
                }

                log.info("✅ [{}] Session completed: {} | status={} | orderId={}",
                        comPort, task.getTaskType(), result.getStatus(), task.getOrderId());

                future.complete(result);

            } catch (Exception e) {
                log.error("❌ [{}] Session failed: {} | orderId={} | error={}",
                        comPort, task.getTaskType(), task.getOrderId(), e.getMessage(), e);

                result = SessionResult.failed(
                        task.getOrderId(),
                        task.getTaskType(),
                        "SYSTEM_ERROR: " + e.getMessage());

                future.complete(result);

            } finally {
                activeCount.decrementAndGet();

                log.info("📊 [{}] Session stats | pending={} | active={} | result={}",
                        comPort, pendingCount.get(), activeCount.get(),
                        result != null ? result.getStatus() : "UNKNOWN");
            }
        });

        return future;
    }

    public int getActiveCount() {
        return activeCount.get();
    }

    public int getPendingCount() {
        return pendingCount.get();
    }

    public void shutdown() {
        log.info("🛑 [{}] Shutting down SessionExecutor", comPort);
        taskQueue.shutdown();
        try {
            if (!taskQueue.awaitTermination(10, TimeUnit.SECONDS)) {
                taskQueue.shutdownNow();
            }
        } catch (InterruptedException e) {
            taskQueue.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
