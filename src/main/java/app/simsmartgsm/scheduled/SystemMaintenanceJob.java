package app.simsmartgsm.scheduled;

import app.simsmartgsm.config.ComManager;
import app.simsmartgsm.uitils.PortWorker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ✅ Dọn dẹp hệ thống định kỳ:
 * - Xóa file log/temp cũ
 * - Giải phóng PortWorker idle
 * - Clear cache / queue / GC
 * - Theo dõi bộ nhớ tránh OOM
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SystemMaintenanceJob {

    private final ComManager comManager;

    /** 🕑 Mỗi 2 giờ dọn dẹp hệ thống */
    @Scheduled(fixedDelay = 2 * 60 * 60 * 1000)
    public void cleanupSystem() {
        log.info("🧹 Bắt đầu dọn dẹp hệ thống (RAM + Logs + Idle Ports + Temp)");

        try {
            cleanupLogsAndTemp();
            cleanupIdlePorts();
            forceGarbageCollect();
            logMemoryUsage();
        } catch (Exception e) {
            log.error("❌ Lỗi khi dọn dẹp hệ thống: {}", e.getMessage(), e);
        }

        log.info("✅ Dọn dẹp hệ thống hoàn tất.");
    }

    /** 🧾 Xóa log/temp cũ hơn 1 ngày */
    private void cleanupLogsAndTemp() {
        cleanupDirectory("logs", 1);
        cleanupDirectory("tmp", 1);
        cleanupDirectory("temp", 1);
    }

    private void cleanupDirectory(String folder, int days) {
        File dir = new File(folder);
        if (!dir.exists()) return;
        Instant cutoff = Instant.now().minus(days, ChronoUnit.DAYS);

        File[] files = dir.listFiles();
        if (files == null) return;

        int deleted = 0;
        for (File f : files) {
            try {
                if (Files.getLastModifiedTime(f.toPath()).toInstant().isBefore(cutoff)) {
                    if (f.delete()) deleted++;
                }
            } catch (Exception ignored) {}
        }

        if (deleted > 0)
            log.info("🗑 Dọn thư mục '{}' — đã xóa {} file cũ hơn {} ngày.", folder, deleted, days);
    }

    /** 📴 Giải phóng các PortWorker đã không hoạt động > 10 phút */
    private void cleanupIdlePorts() {
        ConcurrentHashMap<String, PortWorker> workers = comManager.getWorkers();
        if (workers == null || workers.isEmpty()) return;

        Instant cutoff = Instant.now().minus(10, ChronoUnit.MINUTES);
        Iterator<Map.Entry<String, PortWorker>> it = workers.entrySet().iterator();
        int closed = 0;

        while (it.hasNext()) {
            Map.Entry<String, PortWorker> entry = it.next();
            PortWorker worker = entry.getValue();

            Instant lastActive = worker.getLastActiveTime();
            if (lastActive != null && lastActive.isBefore(cutoff)) {
                try {
                    log.info("⏹ Đóng port {} (idle từ {})", entry.getKey(), lastActive);
                    worker.close();
                    it.remove();
                    closed++;
                } catch (Exception e) {
                    log.warn("⚠️ Lỗi khi đóng port {}: {}", entry.getKey(), e.getMessage());
                }
            }
        }

        if (closed > 0)
            log.info("🔌 Đã giải phóng {} PortWorker không hoạt động.", closed);
    }

    /** ♻️ Ép GC và log usage */
    private void forceGarbageCollect() {
        log.debug("♻️ Đang ép JVM GC...");
        System.gc();
    }

    /** 📊 Ghi log dung lượng bộ nhớ hiện tại */
    private void logMemoryUsage() {
        MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = memBean.getHeapMemoryUsage();
        long usedMB = heap.getUsed() / (1024 * 1024);
        long maxMB = heap.getMax() / (1024 * 1024);
        double percent = (usedMB * 100.0) / maxMB;
        log.info("💾 JVM Memory: {}MB / {}MB ({:.1f}%)", usedMB, maxMB, percent);
    }
}
