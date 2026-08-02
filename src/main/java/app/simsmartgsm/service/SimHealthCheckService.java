package app.simsmartgsm.service;

import app.simsmartgsm.config.ComManager;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.uitils.PortWorker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 🏥 SIM Health Check Service
 *
 * Phát hiện SIM "chết" (bị nhà mạng suspend/block) dù modem vẫn đọc được CCID/SĐT.
 *
 * Cách phát hiện SIM chết (không cần gửi SMS thật):
 * 1. AT+CREG? — Network registration: SIM bị suspend → không đăng ký được mạng
 * 2. AT+CSQ   — Signal quality: SIM chết vẫn có thể hiện tín hiệu yếu hoặc 99
 * 3. AT+COPS? — Operator: SIM bị block → không attach được nhà mạng
 * 4. AT+CPIN? — SIM PIN status: SIM hỏng → trả ERROR hoặc SIM FAILURE
 * 5. SMS fail rate — Tracking từ SmsDailyLimitService: SIM fail liên tục → đánh dead
 *
 * SIM được đánh dấu DEAD khi:
 * - CREG NOT REGISTERED (0 hoặc 3) liên tục >= 3 lần check
 * - HOẶC Signal = 99 (unknown) liên tục >= 3 lần
 * - HOẶC CPIN trả ERROR/SIM FAILURE
 * - HOẶC fail gửi SMS >= FAIL_THRESHOLD liên tiếp (kết hợp SmsDailyLimitService)
 *
 * SIM được phục hồi (UNDEAD) khi:
 * - CREG trả 1 (home) hoặc 5 (roaming) 
 * - VÀ signal > 0 và != 99
 * - VÀ CPIN = READY
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SimHealthCheckService {

    private final ComManager comManager;
    private final SimRepository simRepository;
    private final SmsDailyLimitService smsDailyLimitService;
    private final SimpMessagingTemplate messagingTemplate;

    // ======================================================================
    // CONFIGURATION
    // ======================================================================

    /** Số lần check CREG fail liên tiếp trước khi đánh SIM DEAD */
    private static final int CREG_FAIL_THRESHOLD = 3;

    /** Số lần check signal = 99 liên tiếp trước khi đánh SIM DEAD */
    private static final int SIGNAL_FAIL_THRESHOLD = 3;

    /** Số lần SMS fail liên tiếp (từ SmsDailyLimitService) để coi SIM DEAD */
    private static final int SMS_FAIL_DEAD_THRESHOLD = 5;

    // ======================================================================
    // STATE TRACKING
    // ======================================================================

    /** Track số lần CREG fail liên tiếp: comName → count */
    private final ConcurrentHashMap<String, AtomicInteger> cregFailCounts = new ConcurrentHashMap<>();

    /** Track số lần signal fail liên tiếp: comName → count */
    private final ConcurrentHashMap<String, AtomicInteger> signalFailCounts = new ConcurrentHashMap<>();

    /** Danh sách SIM đã bị đánh dấu DEAD: comName → reason */
    private final ConcurrentHashMap<String, String> deadSims = new ConcurrentHashMap<>();

    /** Lần check cuối cùng: comName → Instant */
    private final ConcurrentHashMap<String, Instant> lastHealthCheck = new ConcurrentHashMap<>();

    /** Kết quả health check chi tiết: comName → HealthResult */
    private final ConcurrentHashMap<String, HealthResult> healthResults = new ConcurrentHashMap<>();

    // ======================================================================
    // HEALTH RESULT DTO
    // ======================================================================

    public static class HealthResult {
        public String comName;
        public String phoneNumber;
        public boolean cregOk;         // Network registered
        public int cregStatus;         // Raw CREG status (0-5)
        public int signalLevel;        // CSQ value (0-31, 99=unknown)
        public boolean cpinOk;         // SIM PIN ready
        public String cpinStatus;      // Raw CPIN status
        public String operatorName;    // Carrier name
        public boolean isDead;         // Final verdict
        public String deadReason;      // Why dead
        public int consecutiveCregFails;
        public int consecutiveSignalFails;
        public int consecutiveSmsFails;
        public int smsSuccessCount;    // Tổng SMS thành công
        public int smsFailedTotal;     // Tổng SMS fail
        public int smsSentTotal;       // Tổng SMS đã gửi
        public Instant checkedAt;

        @Override
        public String toString() {
            return String.format("%-8s %-15s CREG=%s(%d) CSQ=%-3d CPIN=%s OP=%s %s",
                    comName,
                    phoneNumber != null ? phoneNumber : "N/A",
                    cregOk ? "OK" : "FAIL", cregStatus,
                    signalLevel,
                    cpinOk ? "READY" : cpinStatus,
                    operatorName != null ? operatorName : "N/A",
                    isDead ? "🔴 DEAD: " + deadReason : "🟢 ALIVE");
        }
    }

    // ======================================================================
    // SCHEDULED HEALTH CHECK — Chạy mỗi 3 phút
    // ======================================================================

    @Scheduled(fixedRate = 180_000) // 3 phút
    public void scheduledHealthCheck() {
        try {
            var workers = comManager.getWorkers();
            if (workers.isEmpty()) {
                return;
            }

            log.info("🏥 [HEALTH] Starting SIM health check for {} workers...", workers.size());

            int aliveCount = 0;
            int deadCount = 0;
            int recoveredCount = 0;
            List<HealthResult> results = new ArrayList<>();

            for (var entry : workers.entrySet()) {
                String comName = entry.getKey();
                PortWorker worker = entry.getValue();

                if (!worker.isRunning() || !worker.isOpen()) {
                    continue;
                }

                try {
                    HealthResult result = checkSimHealth(comName, worker);
                    results.add(result);
                    healthResults.put(comName, result);

                    if (result.isDead) {
                        deadCount++;
                        handleDeadSim(comName, worker, result);
                    } else {
                        aliveCount++;
                    }
                } catch (Exception e) {
                    log.debug("⚠️ [HEALTH] Error checking {}: {}", comName, e.getMessage());
                }
            }

            // Log summary
            log.info("🏥 [HEALTH] Check complete: {} alive, {} dead, {} recovered",
                    aliveCount, deadCount, recoveredCount);

            // Push results qua WebSocket
            pushHealthResultsToWebSocket(results);

        } catch (Exception e) {
            log.error("❌ [HEALTH] Scheduled check failed: {}", e.getMessage());
        }
    }

    // ======================================================================
    // CORE HEALTH CHECK LOGIC
    // ======================================================================

    /**
     * 🏥 Kiểm tra sức khỏe 1 SIM bằng AT commands
     * ✅ OPTIMIZED: Gộp 4 AT commands thành 1 batch task duy nhất trên worker queue
     * → Không chen ngang 4 CRITICAL tasks vào queue gửi SMS
     */
    private HealthResult checkSimHealth(String comName, PortWorker worker) {
        HealthResult result = new HealthResult();
        result.comName = comName;
        result.phoneNumber = worker.getSim() != null ? worker.getSim().getPhoneNumber() : null;
        result.checkedAt = Instant.now();

        // 0️⃣ Rule 0: SIM đã bị khoá (Blacklisted) thì KHÔNG TỰ ĐỘNG MỞ KHOÁ
        if (deadSims.containsKey(comName)) {
            result.isDead = true;
            result.deadReason = deadSims.get(comName); // Keep the old reason
            
            // Lấy stats cũ nếu có
            Sim simEntity = worker.getSim();
            if (simEntity != null) {
                result.smsSuccessCount = simEntity.getSmsSuccessCount();
                result.smsFailedTotal = simEntity.getSmsFailedCount();
                result.smsSentTotal = simEntity.getSmsSentTotal();
            }
            if (smsDailyLimitService != null) {
                result.consecutiveSmsFails = getConsecutiveSmsFails(comName);
            }
            lastHealthCheck.put(comName, Instant.now());
            return result;
        }

        // ✅ Batch AT commands — CHỈ 1 task trên worker queue thay vì 4
        Map<String, String> responses;
        try {
            List<String[]> commands = List.of(
                    new String[]{"AT+CPIN?", "2000"},    // SIM status
                    new String[]{"AT+CREG?", "2000"},    // Network registration
                    new String[]{"AT+CSQ", "2000"},      // Signal quality
                    new String[]{"AT+COPS?", "2000"}     // Operator
            );
            responses = worker.executeAtCommandsSync(commands);
        } catch (Exception e) {
            log.warn("⚠️ [HEALTH] [{}] Batch AT commands failed: {}", comName, e.getMessage());
            // Fallback: đánh dấu tạm thời không biết
            result.cregStatus = -1;
            result.signalLevel = -1;
            result.cpinStatus = "UNKNOWN";
            result.cpinOk = true; // Benefit of doubt
            result.cregOk = true;
            lastHealthCheck.put(comName, Instant.now());
            return result;
        }

        // 1️⃣ PARSE CPIN (SIM PIN Status) — Check đầu tiên vì nếu SIM hỏng thì CREG/CSQ vô nghĩa
        String cpinResp = responses.getOrDefault("AT+CPIN?", "");
        result.cpinStatus = parseCpinStatus(cpinResp);
        result.cpinOk = "READY".equalsIgnoreCase(result.cpinStatus);

        // 2️⃣ PARSE CREG (Network Registration) — Quan trọng nhất cho SIM chết
        String cregResp = responses.getOrDefault("AT+CREG?", "");
        result.cregStatus = parseCregStatus(cregResp);
        result.cregOk = (result.cregStatus == 1 || result.cregStatus == 5);

        if (!result.cregOk) {
            int fails = cregFailCounts.computeIfAbsent(comName, k -> new AtomicInteger(0))
                    .incrementAndGet();
            result.consecutiveCregFails = fails;
            log.debug("⚠️ [HEALTH] [{}] CREG NOT REGISTERED (status={}), fail #{}/{}",
                    comName, result.cregStatus, fails, CREG_FAIL_THRESHOLD);
        } else {
            cregFailCounts.computeIfAbsent(comName, k -> new AtomicInteger(0)).set(0);
            result.consecutiveCregFails = 0;
        }

        // 3️⃣ PARSE CSQ (Signal Quality)
        String csqResp = responses.getOrDefault("AT+CSQ", "");
        result.signalLevel = parseCsqLevel(csqResp);

        if (result.signalLevel == 99 || result.signalLevel == 0) {
            int fails = signalFailCounts.computeIfAbsent(comName, k -> new AtomicInteger(0))
                    .incrementAndGet();
            result.consecutiveSignalFails = fails;
        } else {
            signalFailCounts.computeIfAbsent(comName, k -> new AtomicInteger(0)).set(0);
            result.consecutiveSignalFails = 0;
        }

        // 4️⃣ PARSE COPS (Operator) — Thông tin bổ sung
        String copsResp = responses.getOrDefault("AT+COPS?", "");
        result.operatorName = parseCopsOperator(copsResp);

        // 5️⃣ CHECK SMS FAIL RATE (từ SmsDailyLimitService)
        if (smsDailyLimitService != null) {
            result.consecutiveSmsFails = getConsecutiveSmsFails(comName);
        }

        // 6️⃣ SMS STATS (từ Sim entity — persisted in MongoDB)
        Sim simEntity = worker.getSim();
        if (simEntity != null) {
            result.smsSuccessCount = simEntity.getSmsSuccessCount();
            result.smsFailedTotal = simEntity.getSmsFailedCount();
            result.smsSentTotal = simEntity.getSmsSentTotal();
        }

        // ======================================================================
        // VERDICT: SIM DEAD hay ALIVE?
        // ======================================================================
        result.isDead = false;
        result.deadReason = null;

        // Rule 1: CPIN failure = SIM hỏng vật lý → DEAD ngay lập tức (không cần đợi threshold)
        if (!result.cpinOk && result.cpinStatus != null &&
                (result.cpinStatus.contains("FAILURE") || result.cpinStatus.contains("ERROR") ||
                 result.cpinStatus.contains("NOT INSERTED"))) {
            result.isDead = true;
            result.deadReason = "SIM_FAILURE: " + result.cpinStatus;
        }

        // ✅ LƯU Ý: Đã tạm bỏ các Rule kiểm tra CREG (Network) và CSQ (Signal) vì chúng báo false-positive (SIM vẫn gửi SMS bình thường).

        // Rule 4: SMS fail quá nhiều liên tiếp = SIM bị block gửi
        if (!result.isDead && result.consecutiveSmsFails >= SMS_FAIL_DEAD_THRESHOLD) {
            result.isDead = true;
            result.deadReason = "SMS_BLOCKED: " + result.consecutiveSmsFails + " SMS fail liên tiếp";
        }

        lastHealthCheck.put(comName, Instant.now());
        return result;
    }

    // ======================================================================
    // HANDLE DEAD / RECOVERED SIM
    // ======================================================================

    private void handleDeadSim(String comName, PortWorker worker, HealthResult result) {
        String previousReason = deadSims.get(comName);

        if (previousReason == null) {
            // Lần đầu phát hiện DEAD
            deadSims.put(comName, result.deadReason);
            log.error("🔴🔴 [HEALTH] SIM DEAD DETECTED: {} ({}) — Reason: {}",
                    comName, result.phoneNumber, result.deadReason);

            // Update SIM status trong DB
            try {
                Sim sim = worker.getSim();
                if (sim != null) {
                    sim.setAllowSms(false);
                    sim.setLastUpdated(Instant.now());
                    simRepository.save(sim);
                    log.info("💾 [HEALTH] SIM {} allowSms=false, status kept as-is", comName);
                }
            } catch (Exception e) {
                log.error("❌ [HEALTH] Error updating dead SIM {}: {}", comName, e.getMessage());
            }

            // Notify via WebSocket
            pushDeadSimNotification(comName, result);

        } else {
            // Đã biết SIM chết rồi — chỉ log nếu reason thay đổi
            if (!previousReason.equals(result.deadReason)) {
                deadSims.put(comName, result.deadReason);
                log.warn("🔴 [HEALTH] SIM {} still DEAD, reason changed: {} → {}",
                        comName, previousReason, result.deadReason);
            }
        }
    }

    // ======================================================================
    // DAILY RESET (Lúc 00:00)
    // ======================================================================
    
    @Scheduled(cron = "0 0 0 * * *")
    public void resetAllDeadSimsAtMidnight() {
        log.info("⏰ [HEALTH] Midnight Reset: Khôi phục tất cả SIM bị khoá...");
        
        int unlockedCount = 0;
        
        // 1. Reset in-memory maps
        deadSims.clear();
        cregFailCounts.clear();
        signalFailCounts.clear();
        
        // 2. Reset in MongoDB
        try {
            List<Sim> sims = simRepository.findByAllowSmsFalse();
            for (Sim sim : sims) {
                sim.setAllowSms(true);
                sim.setSmsFailedCount(0); // Reset consecutive fails
                sim.setLastUpdated(Instant.now());
                unlockedCount++;
            }
            if (!sims.isEmpty()) {
                simRepository.saveAll(sims);
                log.info("✅ [HEALTH] Đã mở khoá cho {} SIM bị DEAD hôm qua.", unlockedCount);
            }
        } catch (Exception e) {
            log.error("❌ [HEALTH] Lỗi khi reset DEAD SIMs lúc 00:00: {}", e.getMessage());
        }
    }

    // ======================================================================
    // PARSING AT RESPONSES
    // ======================================================================

    /**
     * Parse AT+CREG? response → registration status
     * +CREG: n,stat   where stat:
     *   0 = not registered, not searching
     *   1 = registered, home network ✅
     *   2 = not registered, searching
     *   3 = registration denied ❌
     *   4 = unknown
     *   5 = registered, roaming ✅
     */
    private int parseCregStatus(String response) {
        if (response == null || response.isEmpty()) return -1;

        try {
            // Pattern: +CREG: n,stat hoặc +CREG: n,stat,"lac","ci",act
            // n = URC mode (0-2), stat = registration status (0-5)
            // Ưu tiên match dạng có 2 số: +CREG: n,stat
            java.util.regex.Matcher m2 = java.util.regex.Pattern.compile(
                    "\\+CREG:\\s*(\\d),\\s*(\\d)").matcher(response);
            if (m2.find()) {
                return Integer.parseInt(m2.group(2)); // stat là số thứ 2
            }

            // Fallback: dạng chỉ có 1 số +CREG: stat (một số modem đơn giản)
            java.util.regex.Matcher m1 = java.util.regex.Pattern.compile(
                    "\\+CREG:\\s*(\\d)\\s*$").matcher(response);
            if (m1.find()) {
                return Integer.parseInt(m1.group(1));
            }
        } catch (Exception e) {
            log.debug("⚠️ CREG parse error: {}", e.getMessage());
        }
        return -1;
    }

    /**
     * Parse AT+CSQ response → signal level (0-31, 99=unknown)
     */
    private int parseCsqLevel(String response) {
        if (response == null || response.isEmpty()) return -1;

        try {
            java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                    "\\+CSQ:\\s*(\\d+)").matcher(response);
            if (m.find()) {
                return Integer.parseInt(m.group(1));
            }
        } catch (Exception e) {
            log.debug("⚠️ CSQ parse error: {}", e.getMessage());
        }
        return -1;
    }

    /**
     * Parse AT+CPIN? response → SIM status
     * +CPIN: READY = OK
     * +CPIN: SIM PIN = cần nhập PIN
     * +CME ERROR: 10 = SIM not inserted
     * +CME ERROR: 13 = SIM failure
     */
    private String parseCpinStatus(String response) {
        if (response == null || response.isEmpty()) return "UNKNOWN";

        if (response.contains("+CPIN: READY")) return "READY";
        if (response.contains("+CPIN: SIM PIN")) return "SIM_PIN_REQUIRED";
        if (response.contains("+CPIN: SIM PUK")) return "SIM_PUK_REQUIRED";
        if (response.contains("CME ERROR: 10")) return "NOT INSERTED";
        if (response.contains("CME ERROR: 13")) return "SIM FAILURE";
        if (response.contains("CME ERROR: 14")) return "SIM BUSY";
        if (response.contains("ERROR")) return "ERROR";

        // Try to extract status
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                "\\+CPIN:\\s*(.+?)(?:\\r|\\n|$)").matcher(response);
        if (m.find()) {
            return m.group(1).trim();
        }

        return "UNKNOWN";
    }

    /**
     * Parse AT+COPS? response → operator name
     */
    private String parseCopsOperator(String response) {
        if (response == null || response.isEmpty()) return "UNKNOWN";

        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                "\\+COPS:\\s*\\d+,\\d+,\"([^\"]+)\"").matcher(response);
        if (m.find()) {
            return m.group(1);
        }

        // Check nếu không attach operator nào
        if (response.contains("+COPS: 0")) {
            return "NO_OPERATOR";
        }

        return "UNKNOWN";
    }

    /**
     * Lấy số lần SMS fail liên tiếp từ SmsDailyLimitService
     */
    private int getConsecutiveSmsFails(String comName) {
        try {
            Map<String, Map<String, Object>> failStats = smsDailyLimitService.getFailStats();
            Map<String, Object> simStats = failStats.get(comName);
            if (simStats != null) {
                Object consecutive = simStats.get("consecutiveFails");
                if (consecutive instanceof Number) {
                    return ((Number) consecutive).intValue();
                }
            }
        } catch (Exception e) {
            log.debug("⚠️ Error getting fail stats for {}: {}", comName, e.getMessage());
        }
        return 0;
    }

    // ======================================================================
    // WEBSOCKET PUSH
    // ======================================================================

    private void pushHealthResultsToWebSocket(List<HealthResult> results) {
        try {
            List<Map<String, Object>> payload = new ArrayList<>();
            for (HealthResult r : results) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("comPort", r.comName);
                item.put("phoneNumber", r.phoneNumber);
                item.put("cregOk", r.cregOk);
                item.put("cregStatus", r.cregStatus);
                item.put("signalLevel", r.signalLevel);
                item.put("cpinOk", r.cpinOk);
                item.put("cpinStatus", r.cpinStatus);
                item.put("operator", r.operatorName);
                item.put("isDead", r.isDead);
                item.put("deadReason", r.deadReason);
                item.put("consecutiveSmsFails", r.consecutiveSmsFails);
                item.put("smsSuccessCount", r.smsSuccessCount);
                item.put("smsFailedTotal", r.smsFailedTotal);
                item.put("smsSentTotal", r.smsSentTotal);
                item.put("checkedAt", r.checkedAt != null ? r.checkedAt.toString() : null);
                payload.add(item);
            }

            messagingTemplate.convertAndSend("/topic/sim-health", payload);
        } catch (Exception e) {
            log.debug("⚠️ Error pushing health results: {}", e.getMessage());
        }
    }

    private void pushDeadSimNotification(String comName, HealthResult result) {
        try {
            Map<String, Object> notification = new LinkedHashMap<>();
            notification.put("type", "SIM_DEAD");
            notification.put("comPort", comName);
            notification.put("phoneNumber", result.phoneNumber);
            notification.put("reason", result.deadReason);
            notification.put("signalLevel", result.signalLevel);
            notification.put("cregStatus", result.cregStatus);
            notification.put("cpinStatus", result.cpinStatus);
            notification.put("timestamp", Instant.now().toString());

            messagingTemplate.convertAndSend("/topic/sim-health/alert", notification);
            log.info("📡 [HEALTH] Dead SIM alert pushed: {} ({})", comName, result.deadReason);
        } catch (Exception e) {
            log.debug("⚠️ Error pushing dead SIM notification: {}", e.getMessage());
        }
    }

    // ======================================================================
    // PUBLIC API (cho Controller/Dashboard)
    // ======================================================================

    /** Lấy tất cả kết quả health check gần nhất */
    public Map<String, HealthResult> getAllHealthResults() {
        return Collections.unmodifiableMap(healthResults);
    }

    /** Lấy danh sách SIM đang DEAD */
    public Map<String, String> getDeadSims() {
        return Collections.unmodifiableMap(deadSims);
    }

    /** Kiểm tra SIM cụ thể có đang DEAD không */
    public boolean isSimDead(String comName) {
        return deadSims.containsKey(comName);
    }

    /** Manual recovery: Force đánh dấu SIM là ALIVE */
    public void forceRecoverSim(String comName) {
        deadSims.remove(comName);
        cregFailCounts.computeIfAbsent(comName, k -> new AtomicInteger(0)).set(0);
        signalFailCounts.computeIfAbsent(comName, k -> new AtomicInteger(0)).set(0);

        if (smsDailyLimitService != null && smsDailyLimitService.isBlacklisted(comName)) {
            smsDailyLimitService.unblacklist(comName);
        }

        // Cập nhật DB
        try {
            var workers = comManager.getWorkers();
            PortWorker worker = workers.get(comName);
            if (worker != null) {
                Sim sim = worker.getSim();
                if (sim != null) {
                    sim.setAllowSms(true);
                    sim.setSmsFailedCount(0);
                    sim.setLastUpdated(Instant.now());
                    simRepository.save(sim);
                }
            }
        } catch (Exception e) {
            log.error("❌ Error force recovering SIM {}: {}", comName, e.getMessage());
        }

        log.info("🔓 [HEALTH] SIM {} force recovered by admin", comName);
    }

    /** Trigger manual health check cho 1 SIM */
    public HealthResult checkSingleSim(String comName) {
        var workers = comManager.getWorkers();
        PortWorker worker = workers.get(comName);

        if (worker == null || !worker.isRunning() || !worker.isOpen()) {
            HealthResult result = new HealthResult();
            result.comName = comName;
            result.isDead = true;
            result.deadReason = "WORKER_NOT_AVAILABLE";
            result.checkedAt = Instant.now();
            return result;
        }

        return checkSimHealth(comName, worker);
    }
}
