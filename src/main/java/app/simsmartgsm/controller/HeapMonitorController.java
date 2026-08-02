package app.simsmartgsm.controller;

import app.simsmartgsm.service.HeapMonitorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 📊 Heap Monitor Controller
 * API để kiểm tra heap status và trigger cleanup thủ công
 */
@RestController
@RequestMapping("/api/system/heap")
@RequiredArgsConstructor
@Slf4j
public class HeapMonitorController {

    private final HeapMonitorService heapMonitorService;

    /**
     * GET /api/system/heap/status
     * Lấy thông tin heap hiện tại
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getHeapStatus() {
        try {
            HeapMonitorService.HeapInfo info = heapMonitorService.getHeapInfo();

            Map<String, Object> response = new HashMap<>();
            response.put("usedMB", info.usedMB());
            response.put("maxMB", info.maxMB());
            response.put("usagePercent", String.format("%.2f%%", info.usagePercent() * 100));
            response.put("status", info.status());
            response.put("timestamp", System.currentTimeMillis());

            // Thêm màu cho frontend
            String color = switch (info.status()) {
                case "CRITICAL" -> "red";
                case "HIGH" -> "orange";
                case "ELEVATED" -> "yellow";
                default -> "green";
            };
            response.put("color", color);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("❌ Error getting heap status: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * POST /api/system/heap/gc
     * Trigger garbage collection thủ công
     */
    @PostMapping("/gc")
    public ResponseEntity<Map<String, Object>> triggerGC() {
        try {
            log.info("🔧 Manual GC triggered via API");

            HeapMonitorService.HeapInfo before = heapMonitorService.getHeapInfo();

            System.gc();
            Thread.sleep(1000); // Chờ GC hoàn thành

            HeapMonitorService.HeapInfo after = heapMonitorService.getHeapInfo();

            Map<String, Object> response = new HashMap<>();
            response.put("before", Map.of(
                    "usedMB", before.usedMB(),
                    "usagePercent", String.format("%.2f%%", before.usagePercent() * 100)));
            response.put("after", Map.of(
                    "usedMB", after.usedMB(),
                    "usagePercent", String.format("%.2f%%", after.usagePercent() * 100)));
            response.put("freedMB", before.usedMB() - after.usedMB());
            response.put("message", "GC completed successfully");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("❌ Error triggering GC: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * GET /api/system/heap/info
     * Lấy thông tin chi tiết về heap và system
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getDetailedInfo() {
        try {
            Runtime runtime = Runtime.getRuntime();

            Map<String, Object> response = new HashMap<>();

            // Heap info
            HeapMonitorService.HeapInfo heapInfo = heapMonitorService.getHeapInfo();
            response.put("heap", Map.of(
                    "usedMB", heapInfo.usedMB(),
                    "maxMB", heapInfo.maxMB(),
                    "usagePercent", heapInfo.usagePercent() * 100,
                    "status", heapInfo.status()));

            // Runtime info
            response.put("runtime", Map.of(
                    "totalMemoryMB", runtime.totalMemory() / 1_048_576,
                    "freeMemoryMB", runtime.freeMemory() / 1_048_576,
                    "maxMemoryMB", runtime.maxMemory() / 1_048_576,
                    "availableProcessors", runtime.availableProcessors()));

            // Thread info
            response.put("threads", Map.of(
                    "activeCount", Thread.activeCount(),
                    "currentThread", Thread.currentThread().getName()));

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("❌ Error getting detailed info: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }
}
