package app.simsmartgsm.controller;

import app.simsmartgsm.dto.request.ProxyStartRequest;
import app.simsmartgsm.dto.response.ApiResponse;
import app.simsmartgsm.dto.response.ProxyStatusResponse;
import app.simsmartgsm.service.ProxyDataService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * ✅ Proxy Controller
 * API quản lý proxy cho từng SIM.
 *
 * Mỗi SIM (COM port) = 1 proxy riêng biệt (IP khác nhau từ nhà mạng).
 */
@RestController
@RequestMapping("/api/proxy")
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "proxy", name = "enabled", havingValue = "true")
@Slf4j
@Tag(name = "Proxy API", description = "API quản lý Mobile Proxy - mỗi SIM 1 proxy riêng")
public class ProxyController {

    private final ProxyDataService proxyDataService;

    // ==================== PROXY MANAGEMENT ====================

    @GetMapping("/list")
    @Operation(summary = "Danh sách proxy",
            description = "Lấy danh sách tất cả proxy (đang chạy + available SIM)")
    public ResponseEntity<ApiResponse<List<ProxyStatusResponse>>> listProxies() {
        try {
            List<ProxyStatusResponse> proxies = proxyDataService.getAllProxies();
            return ResponseEntity.ok(ApiResponse.success(
                    "Tìm thấy " + proxies.size() + " proxy/SIM", proxies));
        } catch (Exception e) {
            log.error("❌ Error listing proxies: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/status/{comPort}")
    @Operation(summary = "Trạng thái proxy",
            description = "Xem chi tiết trạng thái 1 proxy theo COM port")
    public ResponseEntity<ApiResponse<ProxyStatusResponse>> getStatus(@PathVariable String comPort) {
        try {
            ProxyStatusResponse status = proxyDataService.getProxyStatus(comPort);
            return ResponseEntity.ok(ApiResponse.success(status));
        } catch (Exception e) {
            log.error("❌ Error getting proxy status: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/start")
    @Operation(summary = "Start proxy cho 1 SIM",
            description = "Kết nối data qua modem EC25 và start HTTP proxy. " +
                    "Proxy sẽ có IP riêng từ nhà mạng. " +
                    "Có thể cấu hình APN, port và authentication.")
    public ResponseEntity<ApiResponse<ProxyStatusResponse>> startProxy(
            @RequestBody ProxyStartRequest request) {
        try {
            log.info("🚀 API: Start proxy for {} with APN: {}", request.getComPort(), request.getApn());
            ProxyStatusResponse result = proxyDataService.startProxy(request);
            return ResponseEntity.ok(ApiResponse.success("Proxy started thành công", result));
        } catch (Exception e) {
            log.error("❌ Error starting proxy: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/stop/{comPort}")
    @Operation(summary = "Stop proxy",
            description = "Dừng proxy và ngắt kết nối data cho COM port cụ thể")
    public ResponseEntity<ApiResponse<Void>> stopProxy(@PathVariable String comPort) {
        try {
            log.info("🛑 API: Stop proxy for {}", comPort);
            proxyDataService.stopProxy(comPort);
            return ResponseEntity.ok(ApiResponse.success("Proxy stopped", null));
        } catch (Exception e) {
            log.error("❌ Error stopping proxy: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/stop-all")
    @Operation(summary = "Stop tất cả proxy",
            description = "Dừng toàn bộ proxy đang chạy")
    public ResponseEntity<ApiResponse<Void>> stopAllProxies() {
        try {
            log.info("🛑 API: Stop ALL proxies");
            proxyDataService.stopAllProxies();
            return ResponseEntity.ok(ApiResponse.success("Tất cả proxy đã dừng", null));
        } catch (Exception e) {
            log.error("❌ Error stopping all proxies: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    // ==================== IP ROTATION ====================

    @PostMapping("/rotate/{comPort}")
    @Operation(summary = "Rotate IP (đổi IP)",
            description = "Ngắt kết nối data rồi kết nối lại để nhận IP mới từ nhà mạng. " +
                    "Proxy sẽ giữ nguyên port, chỉ đổi IP.")
    public ResponseEntity<ApiResponse<ProxyStatusResponse>> rotateIp(@PathVariable String comPort) {
        try {
            log.info("🔄 API: Rotate IP for {}", comPort);
            ProxyStatusResponse result = proxyDataService.rotateIp(comPort);
            return ResponseEntity.ok(ApiResponse.success("IP đã rotate thành công", result));
        } catch (Exception e) {
            log.error("❌ Error rotating IP: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    // ==================== BATCH OPERATIONS ====================

    @PostMapping("/start-all")
    @Operation(summary = "Start proxy cho tất cả SIM",
            description = "Tự động start proxy cho tất cả SIM đang online. " +
                    "Mỗi SIM sẽ có 1 proxy riêng với port tự động.")
    public ResponseEntity<ApiResponse<List<ProxyStatusResponse>>> startAllProxies(
            @RequestParam(defaultValue = "internet") String apn) {
        try {
            log.info("🚀 API: Start ALL proxies with APN: {}", apn);
            List<ProxyStatusResponse> results = proxyDataService.startAllProxies(apn);
            return ResponseEntity.ok(ApiResponse.success(
                    "Started " + results.size() + " proxies", results));
        } catch (Exception e) {
            log.error("❌ Error starting all proxies: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    // ==================== OVERVIEW ====================

    @GetMapping("/overview")
    @Operation(summary = "Tổng quan proxy",
            description = "Thống kê nhanh: tổng proxy, đang chạy, tổng requests, etc.")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getOverview() {
        try {
            List<ProxyStatusResponse> all = proxyDataService.getAllProxies();

            long running = all.stream().filter(p -> "CONNECTED".equals(p.getStatus())).count();
            long available = all.stream().filter(p -> "AVAILABLE".equals(p.getStatus())).count();
            long error = all.stream().filter(p -> "ERROR".equals(p.getStatus())).count();
            long totalRequests = all.stream().mapToLong(ProxyStatusResponse::getTotalRequests).sum();
            long totalBytes = all.stream().mapToLong(ProxyStatusResponse::getTotalBytes).sum();

            Map<String, Object> overview = Map.of(
                    "total", all.size(),
                    "running", running,
                    "available", available,
                    "error", error,
                    "totalRequests", totalRequests,
                    "totalBytes", totalBytes,
                    "totalBytesFormatted", formatBytes(totalBytes)
            );

            return ResponseEntity.ok(ApiResponse.success(overview));
        } catch (Exception e) {
            log.error("❌ Error getting overview: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    // ==================== EXPORT ====================

    @GetMapping("/export")
    @Operation(summary = "Export danh sách proxy",
            description = "Export danh sách proxy đang chạy dạng text (host:port:user:pass)")
    public ResponseEntity<ApiResponse<String>> exportProxies() {
        try {
            List<ProxyStatusResponse> all = proxyDataService.getAllProxies();
            StringBuilder sb = new StringBuilder();

            String hostname;
            try {
                hostname = java.net.InetAddress.getLocalHost().getHostAddress();
            } catch (Exception e) {
                hostname = "localhost";
            }

            for (ProxyStatusResponse proxy : all) {
                if ("CONNECTED".equals(proxy.getStatus()) && proxy.getProxyPort() != null) {
                    sb.append(hostname).append(":").append(proxy.getProxyPort());
                    if (proxy.isAuthRequired()) {
                        sb.append(":").append(proxy.getUsername())
                                .append(":").append("***"); // Don't expose password
                    }
                    sb.append(" # SIM: ").append(proxy.getPhoneNumber())
                            .append(" | IP: ").append(proxy.getPublicIp())
                            .append("\n");
                }
            }

            return ResponseEntity.ok(ApiResponse.success(
                    "Export thành công", sb.toString()));
        } catch (Exception e) {
            log.error("❌ Error exporting proxies: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    // ==================== HELPERS ====================

    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
