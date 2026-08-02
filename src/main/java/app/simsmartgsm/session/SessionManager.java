package app.simsmartgsm.session;

import app.simsmartgsm.entity.Sim;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.*;

/**
 * ✅ Session-based Manager
 * - Quản lý sessions cho mỗi SIM
 * - Mỗi SIM có 1 SessionExecutor (single thread) → xử lý tuần tự
 * - Tự động cleanup sessions khi xong
 */
@Slf4j
@Component
public class SessionManager {

    /**
     * Map: COM port → SessionExecutor
     * SessionExecutor xử lý các tasks tuần tự cho 1 SIM
     */
    private final Map<String, SessionExecutor> executors = new ConcurrentHashMap<>();

    /**
     * ✅ Thread pool để chạy sessions
     * Fixed size = CPU cores * 2 để tránh OOM
     */
    private ExecutorService sessionPool;

    {
        // ✅ Calculate optimal thread pool size
        int poolSize = Runtime.getRuntime().availableProcessors() * 2;
        final java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger(0);

        sessionPool = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "Session-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        });

        log.info("📦 SessionManager initialized with {} threads", poolSize);
    }

    /**
     * ✅ Submit CALL_OUT task
     */
    public CompletableFuture<SessionResult> submitCallOut(
            Sim sim,
            String targetPhone,
            int durationSeconds,
            boolean record,
            String serviceCode,
            String orderId) {

        String comPort = sim.getComName();
        SessionExecutor executor = getOrCreateExecutor(comPort);

        return executor.submit(new CallOutTask(
                sim, targetPhone, durationSeconds, record, serviceCode, orderId));
    }

    /**
     * ✅ Submit CALL_WITH_AUDIO task
     * Gọi điện tới 1 số và phát file audio ghi sẵn qua uplink
     */
    public CompletableFuture<SessionResult> submitCallWithAudio(
            Sim sim,
            String targetPhone,
            String audioFileName,
            String localAudioPath,
            boolean repeatAudio,
            int waitAfterAudioSeconds,
            boolean record,
            String serviceCode,
            String orderId) {

        String comPort = sim.getComName();
        SessionExecutor executor = getOrCreateExecutor(comPort);

        return executor.submit(new CallWithAudioTask(
                sim, targetPhone, audioFileName, localAudioPath,
                repeatAudio, waitAfterAudioSeconds, record,
                serviceCode, orderId));
    }

    /**
     * ✅ Submit CALL_IN task
     */
    public CompletableFuture<SessionResult> submitCallIn(
            Sim sim,
            String expectedCaller,
            int durationSeconds,
            boolean record,
            boolean acceptHidden,
            int timeWindowSeconds,
            String serviceCode,
            String orderId) {

        String comPort = sim.getComName();
        SessionExecutor executor = getOrCreateExecutor(comPort);

        return executor.submit(new CallInTask(
                sim, expectedCaller, durationSeconds, record,
                acceptHidden, timeWindowSeconds, serviceCode, orderId));
    }

    /**
     * ✅ Submit SMS task
     */
    public CompletableFuture<SessionResult> submitSms(
            Sim sim,
            String targetPhone,
            String message,
            String serviceCode,
            String orderId) {

        String comPort = sim.getComName();
        SessionExecutor executor = getOrCreateExecutor(comPort);

        return executor.submit(new SmsTask(sim, targetPhone, message, serviceCode, orderId));
    }

    /**
     * ✅ Get or create executor for SIM
     */
    private SessionExecutor getOrCreateExecutor(String comPort) {
        return executors.computeIfAbsent(comPort, port -> {
            log.info("📦 [{}] Creating new SessionExecutor", port);
            return new SessionExecutor(port, sessionPool);
        });
    }

    /**
     * ✅ Stop executor for SIM (khi SIM bị thay thế)
     */
    public void stopExecutor(String comPort) {
        SessionExecutor executor = executors.remove(comPort);
        if (executor != null) {
            log.info("🛑 [{}] Stopping SessionExecutor", comPort);
            executor.shutdown();
        }
    }

    /**
     * ✅ Get active session count for monitoring
     */
    public int getActiveSessionCount(String comPort) {
        SessionExecutor executor = executors.get(comPort);
        return executor != null ? executor.getActiveCount() : 0;
    }

    /**
     * ✅ Get pending task count
     */
    public int getPendingTaskCount(String comPort) {
        SessionExecutor executor = executors.get(comPort);
        return executor != null ? executor.getPendingCount() : 0;
    }

    /**
     * ✅ Shutdown all executors
     */
    public void shutdownAll() {
        log.info("🧹 Shutting down SessionManager...");
        executors.values().forEach(SessionExecutor::shutdown);
        executors.clear();
        sessionPool.shutdown();
    }
}
