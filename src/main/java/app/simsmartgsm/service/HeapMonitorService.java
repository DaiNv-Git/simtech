package app.simsmartgsm.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 🧹 Heap Monitor Service - Tự động dọn dẹp khi heap gần đầy
 * 
 * Chức năng:
 * - Theo dõi heap usage mỗi 30 giây
 * - Tự động chạy GC khi heap > 80%
 * - Dọn dẹp cache/maps khi heap > 85%
 * - Cảnh báo khi heap > 90%
 */
@Service
@Slf4j
public class HeapMonitorService {

    private ScheduledExecutorService scheduler;
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

    // Ngưỡng cảnh báo
    private static final double GC_THRESHOLD = 0.80; // 80% - Chạy GC
    private static final double CLEANUP_THRESHOLD = 0.85; // 85% - Dọn cache
    private static final double WARNING_THRESHOLD = 0.90; // 90% - Cảnh báo nghiêm trọng

    // Tracking
    private long lastGcTime = 0;
    private long lastCleanupTime = 0;
    private static final long MIN_GC_INTERVAL_MS = 60_000; // Tối thiểu 1 phút giữa các lần GC

    @PostConstruct
    public void init() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("HeapMonitor");
            t.setDaemon(true);
            return t;
        });

        // Kiểm tra heap mỗi 30 giây
        scheduler.scheduleAtFixedRate(this::monitorHeap, 30, 30, TimeUnit.SECONDS);

        log.info("✅ HeapMonitor started - monitoring every 30s");
        log.info("   GC threshold: {}%", (int) (GC_THRESHOLD * 100));
        log.info("   Cleanup threshold: {}%", (int) (CLEANUP_THRESHOLD * 100));
        log.info("   Warning threshold: {}%", (int) (WARNING_THRESHOLD * 100));
    }

    private void monitorHeap() {
        try {
            MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
            long used = heapUsage.getUsed();
            long max = heapUsage.getMax();
            double usagePercent = (double) used / max;

            // ✅ FIX: Log mỗi 30 phút HOẶC khi heap > 85% để giảm spam
            long now = System.currentTimeMillis();
            boolean shouldLog = (now - lastCleanupTime > 1_800_000) || usagePercent > CLEANUP_THRESHOLD;

            if (shouldLog) {
                log.info("📊 Heap: {}/{} MB ({}%)",
                        used / 1_048_576,
                        max / 1_048_576,
                        (int) (usagePercent * 100));
            }

            // 🚨 Cảnh báo nghiêm trọng
            if (usagePercent > WARNING_THRESHOLD) {
                log.error("🚨 CRITICAL: Heap usage > 90%! Used: {} MB / {} MB",
                        used / 1_048_576, max / 1_048_576);
                performEmergencyCleanup();
            }
            // 🧹 Dọn dẹp cache
            else if (usagePercent > CLEANUP_THRESHOLD) {
                log.warn("⚠️ Heap usage > 85% - performing cleanup");
                performCleanup();
            }
            // ♻️ Chạy GC
            else if (usagePercent > GC_THRESHOLD) {
                if (now - lastGcTime > MIN_GC_INTERVAL_MS) {
                    log.info("♻️ Heap usage > 80% - suggesting GC");
                    suggestGC();
                }
            }

        } catch (Exception e) {
            log.error("❌ Error monitoring heap: {}", e.getMessage());
        }
    }

    /**
     * ♻️ Gợi ý JVM chạy garbage collection
     */
    private void suggestGC() {
        try {
            long before = memoryBean.getHeapMemoryUsage().getUsed();

            System.gc();
            lastGcTime = System.currentTimeMillis();

            // Chờ một chút để GC hoàn thành
            Thread.sleep(1000);

            long after = memoryBean.getHeapMemoryUsage().getUsed();
            long freed = before - after;

            if (freed > 0) {
                log.info("♻️ GC freed {} MB", freed / 1_048_576);
            }

        } catch (Exception e) {
            log.error("❌ Error running GC: {}", e.getMessage());
        }
    }

    /**
     * 🧹 Dọn dẹp cache và maps
     */
    private void performCleanup() {
        try {
            long now = System.currentTimeMillis();

            // Tránh cleanup quá thường xuyên
            if (now - lastCleanupTime < MIN_GC_INTERVAL_MS) {
                return;
            }

            log.info("🧹 Starting cleanup...");

            // Không còn cache WebSocket remote; chỉ gợi ý GC cho cache nội bộ.
            suggestGC();

            lastCleanupTime = now;

            log.info("✅ Cleanup completed");

        } catch (Exception e) {
            log.error("❌ Error during cleanup: {}", e.getMessage());
        }
    }

    /**
     * 🚨 Dọn dẹp khẩn cấp khi heap > 90%
     */
    private void performEmergencyCleanup() {
        try {
            log.error("🚨 EMERGENCY CLEANUP - Heap critically high!");

            // 1. Force cleanup tất cả
            performCleanup();

            // 2. Chạy GC nhiều lần
            for (int i = 0; i < 3; i++) {
                System.gc();
                Thread.sleep(500);
            }

            // 3. Kiểm tra lại
            MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
            double usagePercent = (double) heapUsage.getUsed() / heapUsage.getMax();

            if (usagePercent > WARNING_THRESHOLD) {
                log.error("🚨 CRITICAL: Heap still > 90% after emergency cleanup!");
                log.error("🚨 Consider increasing -Xmx or investigating memory leak");
            } else {
                log.info("✅ Emergency cleanup successful - heap now at {}%",
                        (int) (usagePercent * 100));
            }

        } catch (Exception e) {
            log.error("❌ Error during emergency cleanup: {}", e.getMessage());
        }
    }

    /**
     * 📊 Lấy thông tin heap hiện tại (cho API)
     */
    public HeapInfo getHeapInfo() {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        long used = heapUsage.getUsed();
        long max = heapUsage.getMax();
        double usagePercent = (double) used / max;

        return new HeapInfo(
                used / 1_048_576,
                max / 1_048_576,
                usagePercent,
                getStatus(usagePercent));
    }

    private String getStatus(double usagePercent) {
        if (usagePercent > WARNING_THRESHOLD)
            return "CRITICAL";
        if (usagePercent > CLEANUP_THRESHOLD)
            return "HIGH";
        if (usagePercent > GC_THRESHOLD)
            return "ELEVATED";
        return "NORMAL";
    }

    @PreDestroy
    public void cleanup() {
        log.info("🧹 Shutting down HeapMonitor...");
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("✅ HeapMonitor shutdown complete");
    }

    /**
     * Heap info DTO
     */
    public record HeapInfo(
            long usedMB,
            long maxMB,
            double usagePercent,
            String status) {
    }
}
