package app.simsmartgsm.controller;

import app.simsmartgsm.service.SimHealthCheckService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * 🏥 SIM Health Check API
 * Endpoints cho dashboard monitoring sức khỏe SIM
 */
@RestController
@RequestMapping("/api/sim-health")
@RequiredArgsConstructor
@Slf4j
public class SimHealthController {

    private final SimHealthCheckService healthCheckService;

    /**
     * GET /api/sim-health — Lấy kết quả health check gần nhất của tất cả SIM
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getAllHealthResults() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("results", healthCheckService.getAllHealthResults());
        response.put("deadSims", healthCheckService.getDeadSims());
        response.put("totalDead", healthCheckService.getDeadSims().size());
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/sim-health/dead — Lấy danh sách SIM đang DEAD
     */
    @GetMapping("/dead")
    public ResponseEntity<Map<String, String>> getDeadSims() {
        return ResponseEntity.ok(healthCheckService.getDeadSims());
    }

    /**
     * GET /api/sim-health/{comName} — Health check 1 SIM cụ thể (realtime)
     */
    @GetMapping("/{comName}")
    public ResponseEntity<SimHealthCheckService.HealthResult> checkSingleSim(
            @PathVariable String comName) {
        SimHealthCheckService.HealthResult result = healthCheckService.checkSingleSim(comName);
        return ResponseEntity.ok(result);
    }

    /**
     * POST /api/sim-health/{comName}/recover — Force recovery SIM (admin)
     */
    @PostMapping("/{comName}/recover")
    public ResponseEntity<Map<String, Object>> forceRecoverSim(
            @PathVariable String comName) {
        healthCheckService.forceRecoverSim(comName);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", true);
        response.put("comName", comName);
        response.put("message", "SIM " + comName + " đã được force recover");
        return ResponseEntity.ok(response);
    }
}
