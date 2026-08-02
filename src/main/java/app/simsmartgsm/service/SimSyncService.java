package app.simsmartgsm.service;

import app.simsmartgsm.config.AgentStompSubscriber;
import app.simsmartgsm.dto.response.SimResponse;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.uitils.AtCommandHelper;

import app.simsmartgsm.uitils.DeviceIdProvider;
import app.simsmartgsm.uitils.PortWorker;
import app.simsmartgsm.uitils.SimStatus;
import app.simsmartgsm.uitils.SmsDecoder;
import com.fazecast.jSerialComm.SerialPort;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class SimSyncService {

    private final SimRepository simRepository;
    private final PortManager portManager;
    private final SimpMessagingTemplate messagingTemplate;
    private final app.simsmartgsm.config.ComManager comManager;
    private final MongoTemplate mongoTemplate;

    // ✅ OPTIMIZED: Sử dụng 2 thread để quét tuần tự tránh gây nghẽn và sụt áp USB Hub, đảm bảo quét chính xác 100% các SIM
    private static final int THREAD_POOL_SIZE = 2;
    private static final long SCAN_TIMEOUT_MIN = 5; // 5 phút đủ để scan 96 ports
    private static final int MISS_THRESHOLD = 2; // ✅ Giảm từ 10→2: chuyển sang REPLACED nhanh hơn khi thay SIM
    private static final int INACTIVE_THRESHOLD = 1; // ✅ Giảm từ 5→1: chuyển sang INACTIVE ngay lập tức khi mất SIM

    // 🆕 CCID fuzzy match: chỉ cần trùng 18 số liên tục là coi như match
    private static final int CCID_FUZZY_MATCH_LENGTH = 18;

    // Regex patterns
    private static final Pattern CSQ_PATTERN = Pattern.compile("\\+CSQ: (\\d+),");
    private static final Pattern PHONE_NUMBER_PATTERN = Pattern.compile("\"(\\+?\\d{8,15})\"");
    private static final Pattern USSD_NUMBER_PATTERN = Pattern.compile("(\\+?\\d{8,15})");

    // USSD codes for number detection
    private static final String[] USSD_CODES = { "*#100#", "*100#", "*888#", "*99#", "*123#", "*161#" };

    // Phonebook stores to check
    // ✅ FIX: CHỈ dùng "SM" (SIM card memory). KHÔNG dùng "ME"/"ON" (modem memory)
    // vì chúng lưu trên modem, không di chuyển theo SIM → gây hiện cùng số khi đổi
    // SIM
    private static final String[] PHONEBOOK_STORES = { "SM" };

    // ⚙️ Configuration: Enable/Disable writing phone number to SIM phonebook
    // Set to false if experiencing SIM disconnection issues
    private static final boolean ENABLE_WRITE_TO_SIM_PHONEBOOK = true;

    @Value("${gsm.auto-scan-on-startup:true}")
    private boolean autoScanOnStartup;

    private static volatile boolean scanInProgress = false;

    // ================== SCHEDULED TASKS ==================

    /**
     * Scan SIM đúng 1 lần khi app vừa khởi động. Sau khi scan hoàn tất,
     * scanSimsOnly() mới bật PortWorker cho SIM ACTIVE để tránh làm đứt URC.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void scanSimsOnceOnStartup() {
        if (!autoScanOnStartup) {
            log.info("⏭️ Startup SIM scan disabled by gsm.auto-scan-on-startup=false");
            return;
        }

        if (scanInProgress) {
            log.debug("⏭️ Startup scan skipped because another scan is already running");
            return;
        }

        CompletableFuture.runAsync(() -> {
            try {
                scanInProgress = true;
                log.info("🔍 Startup SIM scan started; PortWorker will start after scan completes");
                scanSimsOnly();
            } catch (Exception e) {
                log.error("❌ Startup SIM scan failed: {}", e.getMessage(), e);
            } finally {
                scanInProgress = false;
            }
        });
    }

    /**
     * 🔄 JOB 2: Recovery INACTIVE SIMs - Chạy mỗi 2 phút
     * Tự động restart worker cho những SIM có số điện thoại nhưng bị INACTIVE
     * (do connection lost hoặc port error)
     */
    @Scheduled(initialDelay = 180_000, fixedRate = 120_000) // Chờ startup scan xong rồi mới recovery
    public void scheduledRecoveryInactiveSims() {
        try {
            String deviceName = getDeviceName();

            // Tìm tất cả SIM INACTIVE có số điện thoại (nên được khôi phục)
            List<Sim> inactiveSims = simRepository.findAll().stream()
                    .filter(s -> deviceName.equals(s.getDeviceName()))
                    .filter(s -> "INACTIVE".equals(s.getStatus()))
                    .filter(s -> notBlank(s.getPhoneNumber()))
                    .filter(s -> notBlank(s.getComName()))
                    .collect(java.util.stream.Collectors.toList());

            if (inactiveSims.isEmpty()) {
                return; // Không có SIM cần recovery
            }

            log.info("🔄 [RECOVERY] Tìm thấy {} SIM INACTIVE cần khôi phục", inactiveSims.size());

            int recoveredCount = 0;
            for (Sim sim : inactiveSims) {
                try {
                    // ✅ FIX: Skip nếu port đang bị lock cho direct call
                    if (comManager.isPortLocked(sim.getComName())) {
                        log.debug("⏭️ [RECOVERY] {} đang bị lock cho direct call, skip", sim.getComName());
                        continue;
                    }

                    // Kiểm tra nếu worker đã chạy (đang trong quá trình khôi phục)
                    if (comManager.isWorkerRunning(sim.getComName())) {
                        log.debug("⏭️ [RECOVERY] {} đã có worker đang chạy, skip", sim.getComName());
                        continue;
                    }

                    // Thử khởi động lại worker
                    comManager.startWorker(sim);

                    // Nếu worker khởi động thành công, đánh dấu ACTIVE
                    if (comManager.isWorkerRunning(sim.getComName())) {
                        sim.setStatus("ACTIVE");
                        sim.setLastUpdated(java.time.Instant.now());
                        simRepository.save(sim);
                        recoveredCount++;
                        log.info("✅ [RECOVERY] Đã khôi phục SIM {} ({})", sim.getPhoneNumber(), sim.getComName());
                    }

                } catch (Exception e) {
                    log.debug("⚠️ [RECOVERY] Không thể khôi phục {}: {}", sim.getComName(), e.getMessage());
                }
            }

            if (recoveredCount > 0) {
                log.info("✅ [RECOVERY] Đã khôi phục {}/{} SIM INACTIVE", recoveredCount, inactiveSims.size());
            }

        } catch (Exception e) {
            log.error("❌ Lỗi scheduledRecoveryInactiveSims: {}", e.getMessage());
        }
    }

    /**
     * 📞 JOB 3: USSD Number Detection - Chạy mỗi 10 phút
     * 
     * Giải quyết bài toán con gà - quả trứng:
     * - Fast scan chỉ dùng AT+CNUM/phonebook (nhanh ~2s) → nhiều SIM không trả về số
     * - DB chưa có số → không thể dùng DATABASE_FALLBACK → không ghi được vào thân SIM
     * - USSD chậm (~15s/code) nhưng detect được số trên hầu hết nhà mạng
     * 
     * Job này chạy riêng biệt, xử lý từng SIM một (tránh overload):
     * 1. Tìm SIM INACTIVE có CCID nhưng chưa có phoneNumber
     * 2. Gọi USSD để detect số
     * 3. Ghi số vào DB + thân SIM (phonebook SM)
     * → Lần scan sau: AT command đọc được từ phonebook SM → không cần USSD nữa
     */
    @Scheduled(initialDelay = 120_000, fixedRate = 600_000) // Chờ 2 phút sau startup, chạy mỗi 10 phút
    public void scheduledUssdDetectMissingNumbers() {
        if (scanInProgress) {
            log.debug("⏭️ Scan đang chạy, bỏ qua USSD detect...");
            return;
        }

        try {
            String deviceName = getDeviceName();

            // Tìm SIM có CCID nhưng chưa có số điện thoại (trên device hiện tại)
            List<Sim> simsWithoutNumber = simRepository.findAll().stream()
                    .filter(s -> deviceName.equals(s.getDeviceName()))
                    .filter(s -> notBlank(s.getCcid()))
                    .filter(s -> isBlank(s.getPhoneNumber()))
                    .filter(s -> notBlank(s.getComName()))
                    .filter(s -> !"REPLACED".equals(s.getStatus()))
                    .collect(java.util.stream.Collectors.toList());

            if (simsWithoutNumber.isEmpty()) {
                return; // Tất cả SIM đã có số
            }

            log.info("📞 [USSD-DETECT] Tìm thấy {} SIM chưa có số điện thoại, bắt đầu detect qua USSD...",
                    simsWithoutNumber.size());

            int detectedCount = 0;

            for (Sim sim : simsWithoutNumber) {
                try {
                    // Skip nếu port đang bận
                    if (comManager.isPortLocked(sim.getComName()) || portManager.isLocked(sim.getComName())) {
                        log.debug("⏭️ [USSD-DETECT] {} đang bận, skip", sim.getComName());
                        continue;
                    }

                    // ✅ Stop worker tạm thời nếu đang chạy (cần exclusive port access cho USSD)
                    boolean workerWasRunning = comManager.isWorkerRunning(sim.getComName());
                    if (workerWasRunning) {
                        comManager.stopWorker(sim.getComName());
                        Thread.sleep(1000); // Đợi port giải phóng
                    }

                    // Mở port và chạy USSD
                    String detectedPhone = portManager.withPort(sim.getComName(), helper -> {
                        try {
                            // Thử AT+CNUM trước (có thể SIM mới hỗ trợ rồi)
                            String phone = helper.getCnum();
                            if (notBlank(phone)) {
                                log.info("📱 [USSD-DETECT] {} → Detect từ AT+CNUM: {}", sim.getComName(), phone);
                                return normalizeNumber(phone);
                            }

                            // Thử đọc phonebook SM
                            phone = readOwnNumberFromPhonebook(helper, "SM");
                            if (notBlank(phone)) {
                                log.info("📱 [USSD-DETECT] {} → Detect từ phonebook SM: {}", sim.getComName(), phone);
                                return normalizeNumber(phone);
                            }

                            // Chạy USSD (chậm nhưng chính xác)
                            phone = queryOwnNumberViaUssd(helper);
                            if (notBlank(phone)) {
                                phone = normalizeNumber(phone);
                                log.info("📱 [USSD-DETECT] {} → Detect từ USSD: {}", sim.getComName(), phone);

                                // ✅ GHI VÀO THÂN SIM ngay khi detect được
                                if (ENABLE_WRITE_TO_SIM_PHONEBOOK) {
                                    boolean writeOk = writeNumberToSimPhonebook(helper, phone);
                                    if (writeOk) {
                                        log.info("✅ [USSD-DETECT] {} → Đã ghi số {} vào thân SIM (phonebook SM)",
                                                sim.getComName(), phone);
                                    } else {
                                        log.warn("⚠️ [USSD-DETECT] {} → Không ghi được số vào thân SIM",
                                                sim.getComName());
                                    }
                                }

                                return phone;
                            }

                            log.debug("📱 [USSD-DETECT] {} → Không detect được số từ USSD", sim.getComName());
                            return null;
                        } catch (Exception e) {
                            log.warn("⚠️ [USSD-DETECT] {} lỗi: {}", sim.getComName(), e.getMessage());
                            return null;
                        }
                    }, 90_000L); // Timeout 90s (USSD codes cần thời gian)

                    // Cập nhật DB nếu detect được số
                    if (notBlank(detectedPhone)) {
                        sim.setPhoneNumber(detectedPhone);
                        sim.setStatus("ACTIVE");
                        sim.setLastUpdated(Instant.now());
                        simRepository.save(sim);
                        detectedCount++;

                        log.info("✅ [USSD-DETECT] {} → Đã lưu số {} vào DB (CCID: {})",
                                sim.getComName(), detectedPhone, sim.getCcid());

                        // Push cập nhật lên WebSocket
                        pushSimStatusToUI(sim, "ONLINE", "Đã phát hiện số qua USSD");
                    }

                    // ✅ Restart worker nếu trước đó đang chạy
                    if (workerWasRunning && notBlank(sim.getPhoneNumber())) {
                        comManager.startWorker(sim);
                    }

                    // Delay giữa các SIM để không overload USB hub
                    Thread.sleep(2000);

                } catch (Exception e) {
                    log.warn("⚠️ [USSD-DETECT] Lỗi khi detect {} ({}): {}",
                            sim.getComName(), sim.getCcid(), e.getMessage());
                }
            }

            if (detectedCount > 0) {
                log.info("✅ [USSD-DETECT] Hoàn tất: detect được {}/{} SIM mới",
                        detectedCount, simsWithoutNumber.size());

                // Push lại toàn bộ danh sách SIM
                try {
                    List<Sim> allSims = simRepository.findByDeviceName(deviceName);
                    pushFinalSimListToWebSocket(allSims);
                } catch (Exception e) {
                    log.warn("⚠️ Không push được danh sách SIM: {}", e.getMessage());
                }
            } else {
                log.info("📞 [USSD-DETECT] Không detect được số mới từ {} SIM", simsWithoutNumber.size());
            }

        } catch (Exception e) {
            log.error("❌ Lỗi scheduledUssdDetectMissingNumbers: {}", e.getMessage(), e);
        }
    }

    // ================== MAIN FLOW ==================

    /**
     * 🔍 Scan SIM only - Không detect số
     * Gọi từ API hoặc scheduled job
     * ✅ PURE AT MODE: Không check database, scan 100% bằng AT commands
     */
    public List<Sim> scanSimsOnly() throws Exception {
        String deviceName = getDeviceName();

        // ✅ PURE AT SCAN: Không check database, scan 100% bằng AT commands
        log.debug("=== 🔍 BẮT ĐẦU SCAN SIM cho deviceName={} (PURE AT MODE - no database lookup) ===",
                deviceName);

        // ✅ OPTIMIZED: KHÔNG stop PortWorkers khi scan định kỳ
        // Worker đang chạy → lấy SIM info từ DB/worker entity (không mở port lại)
        // Chỉ AT-scan port chưa có worker (port mới) → giảm tải USB hub đáng kể
        // Lợi ích: không mất SMS/call, không gây USB hub overload, scan nhanh hơn

        ScanBundle bundle = scanAllPorts();
        logScanResult(deviceName, bundle.scanned);

        log.info("📊 Scan summary: {} SIMs scanned from ports, {} busy ports skipped",
                bundle.scanned.size(), bundle.busyPorts.size());

        List<Sim> sims = syncScannedToDb(deviceName, bundle.scanned, bundle.busyPorts);
        log.info("=== ✅ SCAN HOÀN TẤT: {} SIMs returned (scanned: {}) ===",
                sims.size(), bundle.scanned.size());

        // ✅ FIX: Push lại TOÀN BỘ danh sách SIM đã normalize từ DB qua WebSocket
        // Đảm bảo frontend dropdown luôn đồng bộ với DB (số điện thoại đúng)
        pushFinalSimListToWebSocket(sims);

        startWorkersForActiveSims(sims);

        return sims;
    }

    /**
     * 🔄 Full sync (scan only) - Dùng cho API manual trigger
     *
     * @removed SMS detection logic - chỉ dùng AT commands để detect số
     */
    public void syncAndResolve() throws Exception {
        String deviceName = getDeviceName();
        log.info("=== 🔍 BẮT ĐẦU SCAN SIM cho deviceName={} ===", deviceName);

        // ✅ FIX: KHÔNG stop toàn bộ PortWorkers để tránh gián đoạn gửi/nhận SMS
        // stopAllWorkersForScan();

        ScanBundle bundle = scanAllPorts();
        logScanResult(deviceName, bundle.scanned);

        List<Sim> sims = syncScannedToDb(deviceName, bundle.scanned, bundle.busyPorts);

        log.info("=== ✅ SCAN HOÀN TẤT: {} SIMs scanned ===", sims.size());

        pushFinalSimListToWebSocket(sims);
        startWorkersForActiveSims(sims);
    }

    /**
     * Khởi động PortWorker cho các SIM ACTIVE để tiếp tục nhận SMS URC/polling sau
     * mọi luồng scan, kể cả luồng có stop worker trước khi scan.
     */
    private void startWorkersForActiveSims(List<Sim> sims) {
        for (Sim sim : sims) {
            if (!"ACTIVE".equals(sim.getStatus())) {
                continue;
            }

            try {
                if (comManager.isPortLocked(sim.getComName())) {
                    log.debug("⏭️ Skip auto-start listener cho {} (port locked)", sim.getComName());
                    continue;
                }

                comManager.startWorker(sim);
            } catch (Exception e) {
                log.warn("⚠️ Failed to auto-start listener for {}: {}", sim.getComName(), e.getMessage());
            }
        }
    }

    private String getDeviceName() throws Exception {
        return DeviceIdProvider.getDeviceId();
    }

    /**
     * ✅ Stop tất cả PortWorkers trước khi scan lại
     * Nếu không stop, lần scan 2+ sẽ thấy port busy → skip AT scan → lấy data cũ từ
     * DB
     * PortWorkers sẽ được restart sau khi scan xong (trong scanSimsOnly)
     */
    private void stopAllWorkersForScan() {
        try {
            var workers = comManager.getWorkers();
            if (workers.isEmpty()) {
                log.debug("⏭️ Không có PortWorker nào đang chạy");
                return;
            }

            log.info("🛑 Stopping {} PortWorkers trước khi scan...", workers.size());
            List<String> comNames = new ArrayList<>(workers.keySet());
            for (String com : comNames) {
                comManager.stopWorker(com);
            }

            // ✅ FIX: Đợi port được giải phóng hoàn toàn (2s thay vì 500ms)
            // 500ms không đủ khi có 96+ modem trên USB hub
            Thread.sleep(2000);
            log.info("✅ Đã stop tất cả PortWorkers, port sẵn sàng cho scan");
        } catch (Exception e) {
            log.warn("⚠️ Lỗi khi stop PortWorkers: {}", e.getMessage());
        }
    }

    // ================== SCAN LOGGING ==================

    private void logScanResult(String deviceName, List<ScannedSim> scanned) {
        if (scanned == null || scanned.isEmpty()) {
            log.warn("⚠️ Không phát hiện SIM nào trong lần scan cho {}", deviceName);
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\n================== 📡 SCAN RESULT for ")
                .append(deviceName)
                .append(" ==================\n");
        sb.append(String.format("%-8s %-10s %-18s %-18s %-8s %-22s %-20s\n",
                "COM", "STATUS", "PROVIDER", "PHONE", "CSQ", "CCID", "UPDATED"));
        sb.append("-------------------------------------------------------------------------------\n");

        for (ScannedSim s : scanned) {
            String status = s.ok ? "ACTIVE" : "NO_NUMBER";
            String color = s.ok ? "\u001B[32m" : "\u001B[33m";
            String reset = "\u001B[0m";
            String csqDisplay = s.signalLevel >= 0 ? s.signalLevel + "/31" : "-";

            sb.append(String.format("%-8s %s%-10s%s %-18s %-18s %-8s %-22s %-20s\n",
                    s.comName,
                    color, status, reset,
                    Optional.ofNullable(s.simProvider).orElse(""),
                    Optional.ofNullable(s.phoneNumber).orElse("(chưa có)"),
                    csqDisplay,
                    Optional.ofNullable(s.ccid).orElse(""),
                    Instant.now().toString().substring(0, 19)));
        }

        sb.append("==========================================================================\n");
        log.info(sb.toString());
    }

    private static class ScanBundle {
        final List<ScannedSim> scanned;
        final Set<String> busyPorts;

        ScanBundle(List<ScannedSim> scanned, Set<String> busyPorts) {
            this.scanned = scanned;
            this.busyPorts = busyPorts;
        }
    }

    private ScanBundle scanAllPorts() throws InterruptedException {
        SerialPort[] ports = SerialPort.getCommPorts();
        log.debug("🔍 Phát hiện {} cổng COM, sử dụng {} threads", ports.length, THREAD_POOL_SIZE);

        // ✅ PURE AT MODE: Không load SIM from database
        // Tất cả dữ liệu được lấy 100% từ AT commands
        log.debug("📋 PURE AT SCAN MODE: Scanning all ports without database lookup");

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<Future<ScannedSim>> futures = new ArrayList<>();
        Set<String> busyPorts = ConcurrentHashMap.newKeySet();

        // 🆕 Thread-safe list để collect kết quả realtime
        List<ScannedSim> scanned = Collections.synchronizedList(new ArrayList<>());

        for (SerialPort port : ports) {
            String com = port.getSystemPortName();

            if (comManager.isWorkerRunning(com)) {
                Optional<ScannedSim> workerSnapshot = snapshotKnownSim(com);
                if (workerSnapshot.isPresent()) {
                    busyPorts.add(com);
                    ScannedSim result = workerSnapshot.get();
                    scanned.add(result);
                    pushSimToWebSocket(result);
                    log.debug("⏭️ [{}] Port đang có worker, dùng snapshot SIM thay vì mở COM lại", com);
                } else {
                    log.warn("⚠️ [{}] Port có worker nhưng KHÔNG có snapshot hợp lệ -> Stop worker để scan lại", com);
                    comManager.stopWorkerAndWait(com, 3000L);
                    // Không add vào busyPorts để nó được retry ở second pass
                }
                continue;
            }

            // ✅ FIX: Skip nếu port đang bị lock cho direct call
            if (comManager.isPortLocked(com)) {
                busyPorts.add(com);
                snapshotKnownSim(com).ifPresent(result -> {
                    scanned.add(result);
                    pushSimToWebSocket(result);
                });
                log.debug("⏭️ [{}] Port đang bị lock cho direct call, skip scan", com);
                continue;
            }



            if (portManager.isLocked(com)) {
                busyPorts.add(com);
                snapshotKnownSim(com).ifPresent(result -> {
                    scanned.add(result);
                    pushSimToWebSocket(result);
                });
                log.debug("⏭️ Bỏ qua {} vì đang bận (locked)", com);
                continue;
            }

            // 🚀 Submit task
            futures.add(pool.submit(() -> {
                ScannedSim result = scanOnePort(com);

                if (result == null) {
                    result = new ScannedSim(com, "COM_ERROR", "", "", "Unknown", false, 0);
                } else if (isBlank(result.ccid)) {
                    result = new ScannedSim(com, "NO_SIM", "", "", "Unknown", false, 0);
                } else if (isBlank(result.imsi) || result.imsi.length() < 5) {
                    result = new ScannedSim(com, "SIM_ERROR", "", "", "Unknown", false, 0);
                }

                scanned.add(result);
                pushSimToWebSocket(result);
                return result;
            }));
        }

        pool.shutdown();
        boolean terminated = pool.awaitTermination(SCAN_TIMEOUT_MIN, TimeUnit.MINUTES);
        if (!terminated) {
            log.warn("⚠️ Scan timeout sau {} phút", SCAN_TIMEOUT_MIN);
            pool.shutdownNow();
        }

        // ✅ PURE AT MODE: Không retry busy ports với database lookup
        // Busy ports sẽ được scan ở lần tiếp theo
        if (!busyPorts.isEmpty()) {
            log.debug("⏭️ {} port đang bận, bỏ qua scan lần này", busyPorts.size());
        }

        // 🆕 SECOND PASS: Retry các port KHÔNG có trong scanned VÀ KHÔNG bận
        Set<String> scannedComNames = scanned.stream()
                .map(s -> s.comName)
                .collect(Collectors.toSet());

        List<String> portsToRetry = new ArrayList<>();
        for (SerialPort port : ports) {
            String com = port.getSystemPortName();
            if (!scannedComNames.contains(com) && !busyPorts.contains(com)) {
                portsToRetry.add(com);
            }
        }

        if (!portsToRetry.isEmpty()) {
            log.debug("🔁 SECOND PASS: {} ports chưa scan được, retry với full init...", portsToRetry.size());

            for (String retryPort : portsToRetry) {
                ScannedSim result = portManager.withPort(retryPort, helper -> {
                    try {
                        String ccid = helper.getCcid();
                        if (isBlank(ccid)) {
                            log.debug("❌ [RETRY] {}: Không lấy được CCID", retryPort);
                            return new ScannedSim(retryPort, "NO_SIM", "", "", "Unknown", false, 0);
                        }

                        String imsi = helper.getImsi();

                        // ✅ SMART PHONE NUMBER DETECTION (same as main scan)
                        String phone = null;
                        String phoneSource = null;

                        // Check if SIM exists in DB (fuzzy match 18 ký tự)
                        Optional<Sim> existingSimOpt = Optional.empty();
                        try {
                            existingSimOpt = findSimByCcidFuzzy(ccid);
                        } catch (Exception e) {
                            log.debug("⚠️ [RETRY-{}] Lỗi query DB: {}", retryPort, e.getMessage());
                        }

                        // Try to get phone from AT command first
                        try {
                            String phoneFromAT = getPhoneNumberFast(helper);
                            if (notBlank(phoneFromAT)) {
                                phone = normalizeNumber(phoneFromAT);
                                phoneSource = "AT_COMMAND";
                                log.debug("📱 [RETRY-{}] Detect số từ AT command: {}", retryPort, phone);

                                // ✅ FIX: So sánh số AT với số trong DB, nếu khác thì ưu tiên AT
                                if (existingSimOpt.isPresent() && notBlank(existingSimOpt.get().getPhoneNumber())) {
                                    String dbPhone = normalizeNumber(existingSimOpt.get().getPhoneNumber());
                                    if (!phone.equals(dbPhone)) {
                                        log.warn("⚠️ [RETRY-{}] SỐ KHÔNG KHỚP! AT={} ≠ DB={}. Ưu tiên số AT.",
                                                retryPort, phone, dbPhone);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            log.debug("⚠️ [RETRY-{}] AT command failed: {}", retryPort, e.getMessage());
                        }

                        // If AT command failed but DB has phone number, use it as fallback
                        if (isBlank(phone) && existingSimOpt.isPresent()
                                && notBlank(existingSimOpt.get().getPhoneNumber())) {
                            phone = existingSimOpt.get().getPhoneNumber();
                            phoneSource = "DATABASE_FALLBACK";
                            log.debug("📱 [RETRY-{}] AT command failed, dùng số từ DB: {} (CCID: {})", retryPort, phone,
                                    ccid);

                            // ✅ GHI SỐ VÀO THÂN SIM để lần sau AT command đọc được
                            try {
                                boolean writeSuccess = writeNumberToSimPhonebook(helper, phone);
                                if (writeSuccess) {
                                    log.debug("✅ [RETRY-{}] Đã ghi số {} vào thân SIM (phonebook)", retryPort, phone);
                                } else {
                                    log.warn("⚠️ [RETRY-{}] Không ghi được số vào thân SIM", retryPort);
                                }
                            } catch (Exception e) {
                                log.warn("⚠️ [RETRY-{}] Lỗi khi ghi số vào SIM: {}", retryPort, e.getMessage());
                            }
                        }

                        int signalLevel = getSignalLevel(helper);
                        String provider = detectProvider(imsi);

                        log.debug("✅ [RETRY] {} -> ccid={} phone={} source={}",
                                retryPort, ccid, phone, phoneSource);

                        if (isBlank(imsi) || imsi.length() < 5) {
                            return new ScannedSim(retryPort, "SIM_ERROR", "", "", "Unknown", false, 0);
                        }

                        return new ScannedSim(retryPort, ccid, imsi, phone, provider,
                                notBlank(phone), signalLevel);

                    } catch (Exception e) {
                        log.debug("❌ [RETRY] Lỗi scan {}: {}", retryPort, e.getMessage());
                        return null;
                    }
                }, 15000L);

                if (result == null) {
                    result = new ScannedSim(retryPort, "COM_ERROR", "", "", "Unknown", false, 0);
                }

                scanned.add(result);
                pushSimToWebSocket(result);
            }

            log.debug("📊 SECOND PASS hoàn tất: scan được {}/{} ports retry",
                    scanned.size() - (scanned.size() - portsToRetry.size()), portsToRetry.size());
        }

        return new ScanBundle(scanned, busyPorts);
    }

    private Optional<ScannedSim> snapshotKnownSim(String com) {
        Sim sim = null;
        try {
            PortWorker worker = comManager.getWorker(com);
            if (worker != null) {
                sim = worker.getSim();
            }

            if (sim == null || isBlank(sim.getCcid())) {
                sim = simRepository.findFirstByComName(com).orElse(null);
            }

            if (sim == null || isBlank(sim.getCcid())) {
                return Optional.empty();
            }

            return Optional.of(new ScannedSim(
                    com,
                    sim.getCcid(),
                    Optional.ofNullable(sim.getImsi()).orElse(""),
                    sim.getPhoneNumber(),
                    Optional.ofNullable(sim.getSimProvider()).orElse("Unknown"),
                    String.valueOf(SimStatus.ACTIVE).equals(sim.getStatus()) || notBlank(sim.getPhoneNumber()),
                    -1));
        } catch (Exception e) {
            log.debug("⚠️ [{}] Không lấy được snapshot SIM từ worker/DB: {}", com, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * 📡 Push SIM ngay qua WebSocket khi scan được (Progressive Loading)
     */
    private void pushSimToWebSocket(ScannedSim ss) {
        try {
            String status = "INACTIVE";
            if ("COM_ERROR".equals(ss.ccid)) status = "COM_ERROR";
            else if ("NO_SIM".equals(ss.ccid)) status = "NO_SIM";
            else if ("SIM_ERROR".equals(ss.ccid)) status = "SIM_ERROR";
            else if (notBlank(ss.phoneNumber)) status = "ONLINE";
            SimResponse response = SimResponse.builder()
                    .comPort(ss.comName)
                    .status(status)
                    .carrier(ss.simProvider)
                    .phoneNumber(ss.phoneNumber != null ? ss.phoneNumber : "Đang phát hiện...")
                    .iccid(ss.ccid)
                    .message("OK")
                    .build();
            // 🔧 Unified: Use /topic/sims for all SIM updates (single SIM wrapped in array)
            messagingTemplate.convertAndSend("/topic/sims", java.util.List.of(response));
            log.debug("📡 [WS-PUSH] SIM scan: {} | phone={} | status={}",
                    ss.comName, ss.phoneNumber, status);
        } catch (Exception e) {
            log.error("❌ [WS-PUSH] Failed to push SIM {}: {}", ss.comName, e.getMessage(), e);
        }
    }

    /**
     * ✅ Push TOÀN BỘ danh sách SIM đã normalize từ DB qua WebSocket
     * Gọi sau khi syncScannedToDb() hoàn tất để frontend luôn đồng bộ với DB
     * Đảm bảo dropdown hiển thị đúng số điện thoại (đã normalize)
     */
    private void pushFinalSimListToWebSocket(List<Sim> sims) {
        try {
            List<SimResponse> responses = sims.stream()
                    .filter(sim -> notBlank(sim.getCcid()))
                    .map(sim -> {
                        String status = notBlank(sim.getPhoneNumber()) ? "ONLINE" : "INACTIVE";
                        return SimResponse.builder()
                                .comPort(sim.getComName())
                                .status(status)
                                .carrier(sim.getSimProvider())
                                .phoneNumber(sim.getPhoneNumber() != null ? sim.getPhoneNumber() : "N/A")
                                .iccid(sim.getCcid())
                                .message("OK")
                                .build();
                    })
                    .collect(Collectors.toList());

            if (!responses.isEmpty()) {
                messagingTemplate.convertAndSend("/topic/sims", responses);
                log.info("📡 [WS-PUSH] Final SIM list pushed: {} SIMs (from DB, normalized)", responses.size());
            }
        } catch (Exception e) {
            log.error("❌ [WS-PUSH] Failed to push final SIM list: {}", e.getMessage(), e);
        }
    }

    /**
     * 💾 PROGRESSIVE SAVE: Lưu SIM ngay vào MongoDB khi scan được
     * Không đợi batch cuối cùng - tránh mất dữ liệu nếu scan bị timeout
     */
    private void saveSimImmediately(ScannedSim ss, String deviceName) {
        try {
            if (isBlank(ss.ccid)) {
                return;
            }

            // Tìm SIM existing theo CCID (fuzzy match 18 ký tự)
            Sim sim = findSimByCcidFuzzy(ss.ccid).orElse(null);

            if (sim == null && notBlank(ss.imsi)) {
                // Thử tìm theo IMSI
                sim = simRepository.findByImsi(ss.imsi).orElse(null);
            }

            if (sim == null && notBlank(ss.phoneNumber)) {
                // Thử tìm theo phoneNumber (có thể từ SELLER import)
                sim = simRepository.findByPhoneNumber(ss.phoneNumber).orElse(null);
                if (sim != null) {
                    log.debug("🔀 [IMMEDIATE] Merge SIM từ SELLER: phone={}, thêm CCID={}",
                            ss.phoneNumber, ss.ccid);
                }
            }

            if (sim == null) {
                // Tạo mới
                sim = Sim.builder()
                        .ccid(ss.ccid)
                        .deviceName(deviceName)
                        .comName(ss.comName)
                        .status(notBlank(ss.phoneNumber) ? "ACTIVE" : "INACTIVE")
                        .missCount(0)
                        .build();
                log.debug("💾 [IMMEDIATE] Tạo SIM mới: ccid={}, com={}", ss.ccid, ss.comName);
            } else {
                log.debug("💾 [IMMEDIATE] Cập nhật SIM: ccid={}, com={}", ss.ccid, ss.comName);
            }

            // Cập nhật các field
            sim.setCcid(ss.ccid);
            sim.setImsi(ss.imsi);
            sim.setComName(ss.comName);
            sim.setDeviceName(deviceName);
            sim.setPhoneNumber(notBlank(ss.phoneNumber) ? normalizeNumber(ss.phoneNumber) : sim.getPhoneNumber());
            sim.setSimProvider(ss.simProvider);
            sim.setStatus(notBlank(ss.phoneNumber) ? "ACTIVE" : "INACTIVE");
            sim.setMissCount(0);
            sim.setLastUpdated(Instant.now());
            sim.setAgentId(AgentStompSubscriber.agentId);

            simRepository.save(sim);
            log.debug("✅ [IMMEDIATE] Đã lưu SIM: ccid={}, phone={}, com={}",
                    ss.ccid, ss.phoneNumber, ss.comName);

        } catch (Exception e) {
            log.error("❌ [IMMEDIATE] Lỗi lưu SIM {}: {}", ss.ccid, e.getMessage());
        }
    }

    private ScannedSim getFutureResult(Future<ScannedSim> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            log.debug("Future result error: {}", e.getMessage());
            return null;
        }
    }

    private ScannedSim scanOnePort(String com) {
        // 🚀 FAST SCAN MODE with PURE AT commands
        return portManager.withPortFastScan(com, helper -> {
            try {
                // 1️⃣ Lấy mã CCID (bắt buộc)
                String ccid = helper.getCcid();
                if (isBlank(ccid)) {
                    log.debug("❌ {}: Không lấy được CCID", com);
                    return null;
                }

                // 2️⃣ Lấy IMSI
                String imsi = helper.getImsi();

                // 3️⃣ ✅ SMART PHONE NUMBER DETECTION
                String phone = null;
                String phoneSource = null; // Track where phone number came from

                // Check if SIM exists in DB (fuzzy match 18 ký tự)
                Optional<Sim> existingSimOpt = Optional.empty();
                try {
                    existingSimOpt = findSimByCcidFuzzy(ccid);
                } catch (Exception e) {
                    log.debug("⚠️ [{}] Lỗi query DB: {}", com, e.getMessage());
                }

                // Try to get phone from AT command first
                try {
                    String phoneFromAT = getPhoneNumberFast(helper);
                    if (notBlank(phoneFromAT)) {
                        phone = normalizeNumber(phoneFromAT);
                        phoneSource = "AT_COMMAND";
                        log.debug("📱 [{}] Detect số từ AT command: {}", com, phone);

                        // ✅ FIX: So sánh số AT với số trong DB, nếu khác thì ưu tiên AT và cảnh báo
                        if (existingSimOpt.isPresent() && notBlank(existingSimOpt.get().getPhoneNumber())) {
                            String dbPhone = normalizeNumber(existingSimOpt.get().getPhoneNumber());
                            if (!phone.equals(dbPhone)) {
                                log.warn(
                                        "⚠️ [{}] SỐ ĐIỆN THOẠI KHÔNG KHỚP! AT={} ≠ DB={}. Ưu tiên số AT, sẽ cập nhật DB.",
                                        com, phone, dbPhone);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.debug("⚠️ [{}] AT command failed: {}", com, e.getMessage());
                }

                // If AT command failed but DB has phone number, use it as fallback
                if (isBlank(phone) && existingSimOpt.isPresent() && notBlank(existingSimOpt.get().getPhoneNumber())) {
                    phone = existingSimOpt.get().getPhoneNumber();
                    phoneSource = "DATABASE_FALLBACK";
                    log.debug("📱 [{}] AT command failed, dùng số từ DB: {} (CCID: {})", com, phone, ccid);

                    // ✅ GHI SỐ VÀO THÂN SIM để lần sau AT command đọc được (nếu enabled)
                    if (ENABLE_WRITE_TO_SIM_PHONEBOOK) {
                        try {
                            boolean writeSuccess = writeNumberToSimPhonebook(helper, phone);
                            if (writeSuccess) {
                                log.debug("✅ [{}] Đã ghi số {} vào thân SIM (phonebook)", com, phone);
                            } else {
                                log.warn("⚠️ [{}] Không ghi được số vào thân SIM", com);
                            }
                        } catch (Exception e) {
                            log.warn("⚠️ [{}] Lỗi khi ghi số vào SIM: {}", com, e.getMessage());
                        }
                    } else {
                        log.debug("⏭️ [{}] Skip ghi số vào SIM (ENABLE_WRITE_TO_SIM_PHONEBOOK=false)", com);
                    }
                }

                // 4️⃣ Đọc mức tín hiệu sóng
                int signalLevel = getSignalLevel(helper);

                // 5️⃣ Provider
                String provider = detectProvider(imsi);

                log.debug("✅ {} -> ccid={} phone={} source={} provider={} csq={}",
                        com, ccid, phone, phoneSource, provider, signalLevel);

                return new ScannedSim(com, ccid, imsi, phone, provider,
                        notBlank(phone), signalLevel);

            } catch (Exception e) {
                log.debug("❌ Lỗi scan {}: {}", com, e.getMessage());
                return null;
            }
        }, 10000L);
    }

    /**
     * 🚀 Fast version - chỉ thử CNUM, không thử USSD (tốn thời gian)
     */
    private String getPhoneNumberFast(AtCommandHelper helper) throws Exception {
        // Chỉ thử CNUM - nhanh nhất
        String phone = helper.getCnum();
        if (notBlank(phone)) {
            return normalizeNumber(phone);
        }

        // ✅ FIX: Chỉ đọc từ "SM" (SIM card memory), KHÔNG đọc "ON"/"ME" (modem memory)
        // "ON"/"ME" lưu trên modem → khi đổi SIM, số cũ vẫn còn → hiện sai số!
        phone = readOwnNumberFromPhonebook(helper, "SM");
        if (notBlank(phone)) {
            return normalizeNumber(phone);
        }

        // Không thử USSD trong fast scan vì quá chậm (15s mỗi code)
        return null;
    }

    private String getPhoneNumber(AtCommandHelper helper) throws Exception {
        // Thử CNUM trước
        String phone = helper.getCnum();
        if (notBlank(phone)) {
            phone = normalizeNumber(phone);
            log.debug("📞 Lấy số từ CNUM: {}", phone);
            return phone;
        }

        // Thử các phonebook stores
        for (String store : PHONEBOOK_STORES) {
            phone = readOwnNumberFromPhonebook(helper, store);
            if (notBlank(phone)) {
                phone = normalizeNumber(phone);
                log.debug("📞 Lấy số từ phonebook {}: {}", store, phone);
                return phone;
            }
        }

        // Thử USSD
        phone = queryOwnNumberViaUssd(helper);
        if (notBlank(phone)) {
            phone = normalizeNumber(phone);
            log.debug("📞 Lấy số từ USSD: {}", phone);

            // Tự động ghi số vào SIM sau khi detect được
            writeNumberToSimPhonebook(helper, phone);
        }

        return phone;
    }

    private int getSignalLevel(AtCommandHelper helper) throws Exception {
        String csqResp = helper.sendAndRead("AT+CSQ", 2000);
        if (csqResp != null) {
            Matcher m = CSQ_PATTERN.matcher(csqResp);
            if (m.find()) {
                return Integer.parseInt(m.group(1));
            }
        }
        return -1;
    }

    // ================== DATABASE SYNC - FIXED ==================

    private List<Sim> syncScannedToDb(String deviceName, List<ScannedSim> scanned, Set<String> busyPorts) {
        // ✅ Load TẤT CẢ SIM từ DB (không chỉ theo deviceName) để tránh duplicate
        List<Sim> allDbSims = simRepository.findAll();

        // Map theo CCID để tìm nhanh
        Map<String, Sim> dbMapByCcid = allDbSims.stream()
                .filter(s -> notBlank(s.getCcid()))
                .collect(Collectors.toMap(Sim::getCcid, s -> s, (a, b) -> {
                    // Nếu duplicate, giữ lại SIM mới nhất
                    Instant timeA = a.getLastUpdated() != null ? a.getLastUpdated() : Instant.MIN;
                    Instant timeB = b.getLastUpdated() != null ? b.getLastUpdated() : Instant.MIN;
                    return timeA.isAfter(timeB) ? a : b;
                }));

        // ✅ NEW: Map theo phoneNumber để check duplicate từ SELLER/INTEGRATION
        Map<String, Sim> dbMapByPhone = allDbSims.stream()
                .filter(s -> notBlank(s.getPhoneNumber()))
                .collect(Collectors.toMap(Sim::getPhoneNumber, s -> s, (a, b) -> {
                    // Nếu duplicate, ưu tiên SIM có CCID (từ local app)
                    boolean aHasCcid = notBlank(a.getCcid());
                    boolean bHasCcid = notBlank(b.getCcid());
                    if (aHasCcid && !bHasCcid)
                        return a;
                    if (!aHasCcid && bHasCcid)
                        return b;
                    // Nếu cùng có hoặc cùng không có CCID, giữ mới nhất
                    Instant timeA = a.getLastUpdated() != null ? a.getLastUpdated() : Instant.MIN;
                    Instant timeB = b.getLastUpdated() != null ? b.getLastUpdated() : Instant.MIN;
                    return timeA.isAfter(timeB) ? a : b;
                }));

        // Lấy SIM của device hiện tại để check missing
        List<Sim> deviceSims = allDbSims.stream()
                .filter(s -> deviceName.equals(s.getDeviceName()))
                .collect(Collectors.toList());

        Set<String> scannedCcids = new HashSet<>();
        List<Sim> toSave = new ArrayList<>();
        List<Sim> toDelete = new ArrayList<>(); // ✅ Track SIMs to delete (duplicate by phone)
        // ✅ FIX: Track TẤT CẢ SIM đã scan được để trả về đúng số lượng
        List<Sim> allScannedSims = new ArrayList<>();

        // Update existing or create new SIMs
        for (ScannedSim ss : scanned) {
            String scannedCcid = ss.ccid;
            if (isBlank(scannedCcid) || "COM_ERROR".equals(scannedCcid) || "NO_SIM".equals(scannedCcid) || "SIM_ERROR".equals(scannedCcid)) {
                continue;
            }

            scannedCcids.add(ss.ccid);

            // ✅ Đóng các SIM cũ đang ACTIVE/INACTIVE trên cùng cổng COM này bằng cách gán sang REPLACED ngay lập tức
            for (Sim dbSim : deviceSims) {
                if (ss.comName.equals(dbSim.getComName()) && dbSim.getCcid() != null && !isCcidFuzzyMatch(dbSim.getCcid(), ss.ccid)) {
                    if (String.valueOf(SimStatus.ACTIVE).equals(dbSim.getStatus()) || String.valueOf(SimStatus.INACTIVE).equals(dbSim.getStatus())) {
                        dbSim.setStatus(String.valueOf(SimStatus.REPLACED));
                        dbSim.setMissCount(MISS_THRESHOLD);
                        dbSim.setLastUpdated(Instant.now());
                        toSave.add(dbSim);
                    }
                }
            }

            // ✅ Tìm SIM theo CCID trong toàn bộ database (fuzzy match 18 ký tự)
            Sim sim = fuzzyMatchCcidInMap(ss.ccid, dbMapByCcid);

            if (sim != null) {
                // SIM đã tồn tại theo CCID - UPDATE
                log.info(
                        "🔄 Update SIM (by CCID): ccid={}, oldDevice={}, newDevice={}, oldStatus={}, oldCom={}, newCom={}",
                        ss.ccid, sim.getDeviceName(), deviceName, sim.getStatus(), sim.getComName(), ss.comName);

                // Cập nhật deviceName nếu SIM chuyển thiết bị
                if (!deviceName.equals(sim.getDeviceName())) {
                    log.info("📦 SIM {} chuyển từ device '{}' sang '{}'",
                            ss.ccid, sim.getDeviceName(), deviceName);
                    sim.setDeviceName(deviceName);
                }
            } else {
                // ✅ NEW: Check phoneNumber trước khi tạo SIM mới
                // Điều này xử lý trường hợp SIM đã được import từ SELLER (không có CCID)
                if (notBlank(ss.phoneNumber)) {
                    Sim existingByPhone = dbMapByPhone.get(ss.phoneNumber);

                    if (existingByPhone != null) {
                        // ✅ TÌM THẤY duplicate theo phoneNumber!
                        // Case: SIM được import từ SELLER (không có CCID), local scan được CCID
                        log.warn("🔀 MERGE SIM: phoneNumber={} đã tồn tại (từ {}), cập nhật CCID và device info",
                                ss.phoneNumber,
                                existingByPhone.getDeviceName() != null ? existingByPhone.getDeviceName() : "SELLER");

                        // Update SIM cũ với thông tin mới từ scan
                        existingByPhone.setCcid(ss.ccid);
                        existingByPhone.setDeviceName(deviceName);
                        existingByPhone.setComName(ss.comName);
                        existingByPhone.setImsi(ss.imsi);
                        existingByPhone.setSimProvider(ss.simProvider);
                        existingByPhone.setStatus("ACTIVE");
                        existingByPhone.setMissCount(0);
                        existingByPhone.setLastUpdated(Instant.now());
                        existingByPhone.setAgentId(AgentStompSubscriber.agentId);

                        sim = existingByPhone;

                        log.debug("✅ Merged SIM: id={}, ccid={}, phone={}, device={}, com={}",
                                sim.getId(), sim.getCcid(), sim.getPhoneNumber(),
                                sim.getDeviceName(), sim.getComName());

                        // Update map để các iteration sau biết CCID này đã có
                        dbMapByCcid.put(ss.ccid, sim);
                    } else {
                        // Không tìm thấy duplicate, tạo mới
                        sim = createNewSim(deviceName, ss);
                        log.debug("🆕 Tạo SIM mới: ccid={}, phone={}, com={}, device={}",
                                ss.ccid, ss.phoneNumber, ss.comName, deviceName);
                    }
                } else {
                    // Không có phoneNumber, tạo SIM mới
                    sim = createNewSim(deviceName, ss);
                    log.debug("🆕 Tạo SIM mới (chưa có số): ccid={}, com={}, device={}",
                            ss.ccid, ss.comName, deviceName);
                }
            }

            updateSimFromScan(sim, ss);
            toSave.add(sim);
            // ✅ FIX: Thêm vào danh sách TẤT CẢ SIM đã scan được
            allScannedSims.add(sim);
        }

        // Handle missing SIMs - chỉ check SIM của device hiện tại
        updateMissingSims(deviceSims, scannedCcids, busyPorts, toSave);

        if (!toSave.isEmpty()) {
            try {
                simRepository.saveAll(toSave);
                log.debug("💾 Đã lưu {} SIM vào database", toSave.size());
            } catch (org.springframework.dao.DuplicateKeyException e) {
                log.error("❌ Duplicate key error: {}", e.getMessage());
                // Retry từng SIM để xác định SIM nào bị duplicate
                for (Sim sim : toSave) {
                    try {
                        simRepository.save(sim);
                    } catch (org.springframework.dao.DuplicateKeyException ex) {
                        log.error("❌ SIM duplicate: ccid={}, imsi={}, phone={}, device={}",
                                sim.getCcid(), sim.getImsi(), sim.getPhoneNumber(), sim.getDeviceName());

                        // Thử xóa và tạo lại
                        try {
                            Optional<Sim> existingOpt = findSimByCcidFuzzy(sim.getCcid());
                            if (existingOpt.isPresent()) {
                                Sim existing = existingOpt.get();
                                log.warn("🗑️ Xóa SIM duplicate cũ: id={}, ccid={}", existing.getId(),
                                        existing.getCcid());
                                simRepository.delete(existing);

                                // Lưu lại SIM mới
                                sim.setId(null); // Reset ID để tạo mới
                                simRepository.save(sim);
                                log.info("✅ Đã tạo lại SIM: ccid={}", sim.getCcid());
                            }
                        } catch (Exception e2) {
                            log.error("❌ Không thể fix duplicate cho SIM {}: {}", sim.getCcid(), e2.getMessage());
                        }
                    }
                }
            }
        }

        // ✅ FIX: Trả về TẤT CẢ SIM đã scan được, không chỉ SIM được tạo/cập nhật
        // Điều này đảm bảo số lượng SIM trả về khớp với số lượng SIM đã scan
        // allScannedSims chứa tất cả SIM được scan thấy trong lần scan này
        log.info("📊 Sync result: {} SIMs scanned from ports → {} SIMs in database ({} to save)",
                scanned.size(), allScannedSims.size(), toSave.size());

        if (allScannedSims.size() != scanned.size()) {
            log.warn("⚠️ Số lượng SIM không khớp: scanned={}, returned={}. Có thể có SIM bị bỏ qua (không có CCID)",
                    scanned.size(), allScannedSims.size());
        }

        return allScannedSims;
    }

    private Map<String, Sim> createDbMap(List<Sim> dbSims) {
        return dbSims.stream()
                .filter(s -> notBlank(s.getCcid()))
                .collect(Collectors.toMap(Sim::getCcid, s -> s, (a, b) -> a));
    }

    private Sim createNewSim(String deviceName, ScannedSim ss) {
        Sim sim = Sim.builder()
                .ccid(ss.ccid)
                .deviceName(deviceName)
                .comName(ss.comName)
                .status(String.valueOf(ss.ok ? SimStatus.ACTIVE : SimStatus.INACTIVE))
                .missCount(0)
                .build();

        // Set các field khác ngay khi tạo mới
        updateSimFromScan(sim, ss);
        return sim;
    }

    private void updateSimFromScan(Sim sim, ScannedSim ss) {
        String oldStatus = sim.getStatus();
        int oldMissCount = sim.getMissCount();

        sim.setImsi(ss.imsi);
        sim.setComName(ss.comName);

        // ✅ FIX: Chỉ cập nhật phoneNumber nếu scan được số mới từ AT command
        // Nếu AT command không lấy được số (null), giữ nguyên số cũ trong DB
        String normalizedPhone;
        if (notBlank(ss.phoneNumber)) {
            normalizedPhone = normalizeNumber(ss.phoneNumber);
            sim.setPhoneNumber(normalizedPhone);
        } else {
            normalizedPhone = sim.getPhoneNumber();
        }

        sim.setSimProvider(ss.simProvider);
        sim.setLastUpdated(Instant.now());
        sim.setAgentId(AgentStompSubscriber.agentId);

        // ✅ QUAN TRỌNG: Reset miss count và status khi SIM được scan thấy lại
        if (sim.getMissCount() > 0) {
            log.debug("🔄 Reset missCount cho SIM {} (từ {} về 0)", sim.getCcid(), sim.getMissCount());
            sim.setMissCount(0);
        }

        // ✅ Cập nhật status dựa trên việc có số hay không
        String newStatus;
        if (notBlank(normalizedPhone)) {
            newStatus = String.valueOf(SimStatus.ACTIVE);
        } else {
            newStatus = String.valueOf(SimStatus.INACTIVE);
        }

        // Log status change nếu có thay đổi
        if (!newStatus.equals(oldStatus)) {
            log.debug("📊 SIM {} status: {} → {} | missCount: {} → 0 | phone: {}",
                    sim.getCcid(), oldStatus, newStatus, oldMissCount, normalizedPhone);

            // ✅ Push notification về UI nếu status thay đổi từ REPLACED/INACTIVE về ACTIVE
            if ((String.valueOf(SimStatus.REPLACED).equals(oldStatus) ||
                    String.valueOf(SimStatus.INACTIVE).equals(oldStatus)) &&
                    String.valueOf(SimStatus.ACTIVE).equals(newStatus)) {
                pushSimStatusToUI(sim, newStatus, "SIM đã được phát hiện lại");
            }
        }

        sim.setStatus(newStatus);
    }

    /**
     * ✅ Push thông báo status SIM về UI qua WebSocket
     */
    private void pushSimStatusToUI(Sim sim, String status, String message) {
        try {
            SimResponse response = SimResponse.builder()
                    .comPort(sim.getComName())
                    .status(status)
                    .carrier(sim.getSimProvider())
                    .phoneNumber(sim.getPhoneNumber())
                    .iccid(sim.getCcid())
                    .message(message)
                    .build();
            messagingTemplate.convertAndSend("/topic/sims/status", response);
            log.debug("📡 [WS-PUSH] Status update: {} → {} | {}", sim.getComName(), status, message);
        } catch (Exception e) {
            log.warn("⚠️ Failed to push status to UI for {}: {}", sim.getComName(), e.getMessage());
        }
    }

    private void updateMissingSims(List<Sim> dbSims, Set<String> scannedCcids,
            Set<String> busyPorts, List<Sim> toSave) {
        for (Sim db : dbSims) {
            // Chỉ xử lý SIM không được scan thấy VÀ không phải đang busy
            if (!scannedCcids.contains(db.getCcid()) && !busyPorts.contains(db.getComName())) {
                updateMissingSimStatus(db);
                toSave.add(db);
            }
        }
    }

    private void updateMissingSimStatus(Sim sim) {
        String oldStatus = sim.getStatus();
        int newMissCount = sim.getMissCount() + 1;
        sim.setMissCount(newMissCount);
        sim.setLastUpdated(Instant.now());

        String ccidDisplay = notBlank(sim.getCcid()) ? sim.getCcid() : sim.getPhoneNumber();
        log.debug("📊 SIM {} (COM: {}) missCount: {}/{} | Status: {}",
                ccidDisplay, sim.getComName(), newMissCount, MISS_THRESHOLD, oldStatus);

        String newStatus = oldStatus;
        if (newMissCount >= MISS_THRESHOLD) {
            newStatus = String.valueOf(SimStatus.REPLACED);
        } else if (newMissCount >= INACTIVE_THRESHOLD) {
            newStatus = String.valueOf(SimStatus.INACTIVE);
        }

        // ✅ Push notification về UI nếu status thay đổi
        if (!newStatus.equals(oldStatus)) {
            // ✅ FIX: Chỉ log khi status THỰC SỰ thay đổi (không log lại nếu đã là
            // REPLACED/INACTIVE)
            if (newStatus.equals(String.valueOf(SimStatus.REPLACED))) {
                log.debug("🔄 SIM {} (COM: {}) chuyển sang REPLACED (missCount >= {})",
                        ccidDisplay, sim.getComName(), MISS_THRESHOLD);
            } else if (newStatus.equals(String.valueOf(SimStatus.INACTIVE))) {
                log.debug("🔄 SIM {} (COM: {}) chuyển sang INACTIVE (missCount >= {})",
                        ccidDisplay, sim.getComName(), INACTIVE_THRESHOLD);
            }

            sim.setStatus(newStatus);

            String message = newStatus.equals(String.valueOf(SimStatus.REPLACED))
                    ? "SIM đã bị thay thế hoặc rút ra"
                    : "SIM tạm thời mất kết nối";
            pushSimStatusToUI(sim, newStatus, message);

            // ✅ Nếu SIM bị REPLACED, stop worker
            if (newStatus.equals(String.valueOf(SimStatus.REPLACED))) {
                try {
                    comManager.stopWorkerForReplacedSim(sim);
                } catch (Exception e) {
                    log.error("❌ Lỗi khi stop worker cho SIM REPLACED {}: {}",
                            sim.getCcid(), e.getMessage());
                }
            }
        }
    }

    // ================== IMPROVED NUMBER MANAGEMENT ==================

    private boolean writeNumberToSimPhonebook(String com, String number) {
        Boolean result = portManager.withPort(com, helper -> {
            return writeNumberToSimPhonebook(helper, number);
        }, 8000L);
        return result != null && result;
    }

    private boolean writeNumberToSimPhonebook(AtCommandHelper helper, String number) {
        boolean success = false;
        try {
            // Xác định type của số điện thoại (145 cho số quốc tế có dấu +, 129 cho số thường)
            int type = number.startsWith("+") ? 145 : 129;
            String writeCmd = "AT+CPBW=1,\"" + number + "\"," + type + ",\"my_number\"";

            // 1. Ghi vào "SM" (SIM card memory)
            try {
                helper.sendAndRead("AT+CPBS=\"SM\"", 2000);
                String resp = helper.sendAndRead(writeCmd, 3000);
                if (resp != null && resp.contains("OK")) {
                    log.debug("✅ Đã ghi số vào SIM phonebook (SM): {}", number);
                    success = true;
                }
            } catch (Exception e) {
                log.debug("❌ Không thể ghi vào SM store: {}", e.getMessage());
            }

            // 2. GHI VÀO "ON" (Own Numbers) ĐỂ LỆNH AT+CNUM ĐỌC ĐƯỢC CHO PHẦN MỀM KHÁC
            try {
                helper.sendAndRead("AT+CPBS=\"ON\"", 2000);
                String resp = helper.sendAndRead(writeCmd, 3000);
                if (resp != null && resp.contains("OK")) {
                    log.debug("✅ Đã ghi số vào Own Numbers (ON): {}", number);
                    success = true;
                }
            } catch (Exception e) {
                log.debug("❌ Không thể ghi vào ON store: {}", e.getMessage());
            }

            // Xóa bộ nhớ thiết bị ME để tránh rác
            try {
                helper.sendAndRead("AT+CPBS=\"ME\"", 1500);
                helper.sendAndRead("AT+CPBW=1", 2000);
            } catch (Exception e) {}

            return success;
        } catch (Exception e) {
            log.warn("⚠️ Lỗi khi ghi số vào SIM: {}", e.getMessage());
            return false;
        }
    }

    private String readOwnNumberFromPhonebook(AtCommandHelper helper, String store) {
        try {
            helper.sendAndRead("AT+CPBS=\"" + store + "\"", 1500);
            String resp = helper.sendAndRead("AT+CPBR=1,5", 3000);
            if (resp != null) {
                Matcher m = PHONE_NUMBER_PATTERN.matcher(resp);
                if (m.find())
                    return m.group(1);
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private String queryOwnNumberViaUssd(AtCommandHelper helper) {
        for (String code : USSD_CODES) {
            try {
                log.debug("🔍 Thử USSD code: {}", code);
                String resp = helper.sendAndRead("AT+CUSD=1,\"" + code + "\",15", 15_000);
                if (resp != null) {
                    Matcher m = USSD_NUMBER_PATTERN.matcher(resp);
                    if (m.find()) {
                        String number = m.group(1);
                        log.debug("✅ Tìm thấy số từ USSD {}: {}", code, number);
                        return number;
                    }
                }
            } catch (Exception e) {
                log.debug("❌ USSD {} lỗi: {}", code, e.getMessage());
            }
        }
        return null;
    }

    private boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private boolean notBlank(String s) {
        return !isBlank(s);
    }

    /**
     * Normalize số điện thoại Japan (Rakuten Mobile)
     * Format chuẩn: 0X0XXXXXXXX (11 số, bắt đầu bằng 0)
     * 
     * Ví dụ:
     * - +817092029077 → 07092029077
     * - 817092029077 → 07092029077
     * - 7092029077 → 07092029077
     * - 07092029077 → 07092029077 (giữ nguyên)
     */
    private String normalizeNumber(String num) {
        if (num == null)
            return null;

        String original = num;
        String s = num.trim();

        // Loại bỏ ký tự không phải số (trừ dấu +)
        s = s.replaceAll("[^0-9+]", "");

        // Bỏ prefix +81 (Japan country code)
        if (s.startsWith("+81")) {
            s = "0" + s.substring(3); // +817092029077 → 07092029077
        }
        // Bỏ prefix 81 (không có dấu +)
        else if (s.startsWith("81") && s.length() == 13) {
            s = "0" + s.substring(2); // 817092029077 → 07092029077
        }

        // Nếu số Japan thiếu số 0 đầu (70x, 80x, 90x) → thêm 0
        // Pattern: 10 số bắt đầu bằng 70, 80, 90
        if (s.matches("^[7-9]0\\d{8}$")) {
            s = "0" + s; // 7092029077 → 07092029077
            log.debug("📞 Normalize: {} → {} (thêm 0 đầu)", original, s);
        }

        if (!original.equals(s)) {
            log.info("📞 Normalized phone: '{}' → '{}'", original, s);
        }

        return s;
    }

    private String detectProvider(String imsi) {
        if (isBlank(imsi))
            return "Unknown";
        if (imsi.startsWith("45204") || imsi.startsWith("45205"))
            return "Viettel (VN)";
        if (imsi.startsWith("45201"))
            return "Mobifone (VN)";
        if (imsi.startsWith("45202"))
            return "Vinaphone (VN)";
        if (imsi.startsWith("44010"))
            return "NTT Docomo (JP)";
        if (imsi.startsWith("44011"))
            return "Rakuten Mobile (JP)";
        return "Unknown";
    }

    // ================== CCID FUZZY MATCHING ==================

    /**
     * 🆕 Tìm SIM trong DB bằng fuzzy CCID matching (18 ký tự liên tục)
     * 
     * Xử lý 2 case:
     * - AT command trả CCID có trailing F: 8981090040025215666F
     * - Import từ Excel có CCID không có F: 8981090040025215666
     * - Hoặc import có prefix: 226RKT8981090040025215666
     * 
     * Logic:
     * 1. Exact match trước (nhanh nhất)
     * 2. Nếu exact match CÓ phone → trả về luôn
     * 3. Nếu không có phone → dùng MongoTemplate raw query tìm fuzzy
     * (bypass _class filter để tìm cả record từ app khác)
     * 4. Nếu fuzzy tìm thấy record CÓ phone → merge phone, xóa duplicate
     */
    private Optional<Sim> findSimByCcidFuzzy(String scannedCcid) {
        if (isBlank(scannedCcid)) {
            return Optional.empty();
        }

        // 1️⃣ Exact match trước (nhanh nhất)
        Optional<Sim> exactMatch = simRepository.findByCcid(scannedCcid);

        // ✅ Nếu exact match CÓ phone number → trả về luôn, không cần fuzzy
        if (exactMatch.isPresent() && notBlank(exactMatch.get().getPhoneNumber())) {
            return exactMatch;
        }

        // 2️⃣ Fuzzy match bằng MongoTemplate (bypass _class filter)
        // Tạo các variant CCID để tìm kiếm
        String ccidDigitsOnly = scannedCcid.replaceAll("[^0-9]", ""); // Bỏ trailing F và ký tự đặc biệt
        Set<String> searchPatterns = new LinkedHashSet<>();
        searchPatterns.add(scannedCcid); // Original
        if (!ccidDigitsOnly.equals(scannedCcid)) {
            searchPatterns.add(ccidDigitsOnly); // Không có F
        }
        // Thêm variant bỏ trailing F
        if (scannedCcid.toUpperCase().endsWith("F")) {
            searchPatterns.add(scannedCcid.substring(0, scannedCcid.length() - 1));
        }

        log.debug("🔍 CCID fuzzy search patterns: {}", searchPatterns);

        try {
            // 2a. Thử exact match các variant (ví dụ: bỏ trailing F)
            for (String pattern : searchPatterns) {
                Document found = mongoTemplate.getDb().getCollection("sims")
                        .find(new Document("ccid", pattern)
                                .append("phoneNumber", new Document("$ne", null)))
                        .first();
                if (found != null && found.getString("phoneNumber") != null
                        && !found.getString("phoneNumber").isBlank()) {
                    String phone = found.getString("phoneNumber");
                    String foundCcid = found.getString("ccid");
                    log.info("🔗 CCID variant match! phone={} từ CCID={} → scan CCID={}",
                            phone, foundCcid, scannedCcid);

                    if (exactMatch.isPresent()) {
                        // MERGE: copy phone vào scan record
                        Sim scanRecord = exactMatch.get();
                        scanRecord.setPhoneNumber(phone);
                        // Xóa record import duplicate
                        try {
                            mongoTemplate.getDb().getCollection("sims")
                                    .deleteOne(new Document("_id", found.get("_id")));
                            log.info("🗑️ Đã xóa record import duplicate: ccid={}", foundCcid);
                        } catch (Exception e) {
                            log.warn("⚠️ Không xóa được duplicate: {}", e.getMessage());
                        }
                        return Optional.of(scanRecord);
                    } else {
                        // Tạo Sim object từ document
                        Sim sim = Sim.builder()
                                .id(found.getString("_id"))
                                .ccid(foundCcid)
                                .phoneNumber(phone)
                                .status(found.getString("status"))
                                .countryCode(found.getString("countryCode"))
                                .build();
                        return Optional.of(sim);
                    }
                }
            }

            // 2b. Regex fuzzy match (18 ký tự liên tục)
            if (ccidDigitsOnly.length() >= CCID_FUZZY_MATCH_LENGTH) {
                for (int i = 0; i <= ccidDigitsOnly.length() - CCID_FUZZY_MATCH_LENGTH; i++) {
                    String sub18 = ccidDigitsOnly.substring(i, i + CCID_FUZZY_MATCH_LENGTH);
                    Document found = mongoTemplate.getDb().getCollection("sims")
                            .find(new Document("ccid", new Document("$regex", sub18))
                                    .append("phoneNumber", new Document("$ne", null)))
                            .first();
                    if (found != null && found.getString("phoneNumber") != null
                            && !found.getString("phoneNumber").isBlank()) {
                        String phone = found.getString("phoneNumber");
                        String foundCcid = found.getString("ccid");

                        // Bỏ qua nếu tìm thấy chính nó
                        if (foundCcid.equals(scannedCcid))
                            continue;

                        log.info("🔗 CCID fuzzy MERGE! phone={} từ imported CCID={} → scan CCID={} (trùng 18 số: {})",
                                phone, foundCcid, scannedCcid, sub18);

                        if (exactMatch.isPresent()) {
                            Sim scanRecord = exactMatch.get();
                            scanRecord.setPhoneNumber(phone);
                            // Xóa record import duplicate
                            try {
                                mongoTemplate.getDb().getCollection("sims")
                                        .deleteOne(new Document("_id", found.get("_id")));
                                log.info("🗑️ Đã xóa record import duplicate: ccid={}", foundCcid);
                            } catch (Exception e) {
                                log.warn("⚠️ Không xóa được duplicate: {}", e.getMessage());
                            }
                            return Optional.of(scanRecord);
                        } else {
                            Sim sim = Sim.builder()
                                    .id(found.getString("_id"))
                                    .ccid(foundCcid)
                                    .phoneNumber(phone)
                                    .status(found.getString("status"))
                                    .countryCode(found.getString("countryCode"))
                                    .build();
                            return Optional.of(sim);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("⚠️ CCID fuzzy search error: {}", e.getMessage());
        }

        // Trả về exact match (dù không có phone) hoặc empty
        return exactMatch;
    }

    /**
     * 🆕 Fuzzy match CCID trong Map (cho syncScannedToDb)
     * Tìm SIM trong map bằng cách so sánh 18 ký tự liên tục
     * ✅ FIX: Nếu exact match không có phone → tiếp tục tìm fuzzy có phone
     */
    private Sim fuzzyMatchCcidInMap(String scannedCcid, Map<String, Sim> dbMapByCcid) {
        if (isBlank(scannedCcid)) {
            return null;
        }

        // 1️⃣ Exact match trước
        Sim exactMatch = dbMapByCcid.get(scannedCcid);

        // ✅ Nếu exact match CÓ phone → trả về luôn
        if (exactMatch != null && notBlank(exactMatch.getPhoneNumber())) {
            return exactMatch;
        }

        // 2️⃣ Fuzzy match: tìm record CÓ phone number
        if (scannedCcid.length() >= CCID_FUZZY_MATCH_LENGTH) {
            Set<String> scannedSubs = new HashSet<>();
            for (int i = 0; i <= scannedCcid.length() - CCID_FUZZY_MATCH_LENGTH; i++) {
                scannedSubs.add(scannedCcid.substring(i, i + CCID_FUZZY_MATCH_LENGTH));
            }

            // ✅ Ưu tiên tìm record có phone number trước
            Sim fuzzyWithPhone = null;
            Sim fuzzyAny = null;
            String matchedSub = null;

            for (Map.Entry<String, Sim> entry : dbMapByCcid.entrySet()) {
                String dbCcid = entry.getKey();
                if (dbCcid != null && dbCcid.length() >= CCID_FUZZY_MATCH_LENGTH
                        && !dbCcid.equals(scannedCcid)) { // Loại trừ chính nó
                    for (int i = 0; i <= dbCcid.length() - CCID_FUZZY_MATCH_LENGTH; i++) {
                        String dbSub = dbCcid.substring(i, i + CCID_FUZZY_MATCH_LENGTH);
                        if (scannedSubs.contains(dbSub)) {
                            Sim matched = entry.getValue();
                            if (notBlank(matched.getPhoneNumber())) {
                                fuzzyWithPhone = matched;
                                matchedSub = dbSub;
                                break; // Tìm thấy record có phone → đủ rồi
                            }
                            if (fuzzyAny == null) {
                                fuzzyAny = matched;
                                matchedSub = dbSub;
                            }
                        }
                    }
                    if (fuzzyWithPhone != null)
                        break;
                }
            }

            if (fuzzyWithPhone != null) {
                if (exactMatch != null) {
                    // ✅ MERGE: Copy phone từ imported → scan record
                    log.info("🔗 CCID fuzzy MERGE (map)! phone={} từ imported CCID={} → scan CCID={}",
                            fuzzyWithPhone.getPhoneNumber(), fuzzyWithPhone.getCcid(), scannedCcid);
                    exactMatch.setPhoneNumber(fuzzyWithPhone.getPhoneNumber());
                    // Xóa record import duplicate
                    try {
                        simRepository.delete(fuzzyWithPhone);
                        dbMapByCcid.remove(fuzzyWithPhone.getCcid());
                        log.info("🗑️ Đã xóa record import duplicate: ccid={}", fuzzyWithPhone.getCcid());
                    } catch (Exception e) {
                        log.warn("⚠️ Không xóa được duplicate: {}", e.getMessage());
                    }
                    return exactMatch;
                } else {
                    log.info("🔗 CCID fuzzy match (map)! scanned={} ↔ db={} (trùng 18 số: {})",
                            scannedCcid, fuzzyWithPhone.getCcid(), matchedSub);
                    return fuzzyWithPhone;
                }
            }

            // Không có fuzzy với phone → trả fuzzy bất kỳ nếu không có exact
            if (exactMatch == null && fuzzyAny != null) {
                log.info("🔗 CCID fuzzy match (map, no phone)! scanned={} ↔ db={} (trùng 18 số: {})",
                        scannedCcid, fuzzyAny.getCcid(), matchedSub);
                return fuzzyAny;
            }
        }

        return exactMatch; // Trả exact match (dù không phone) hoặc null
    }

    private boolean isCcidFuzzyMatch(String ccid1, String ccid2) {
        if (ccid1 == null || ccid2 == null) return false;
        if (ccid1.equals(ccid2)) return true;
        String d1 = ccid1.replaceAll("[^0-9]", "");
        String d2 = ccid2.replaceAll("[^0-9]", "");
        if (d1.length() >= CCID_FUZZY_MATCH_LENGTH && d2.length() >= CCID_FUZZY_MATCH_LENGTH) {
            String sub1 = d1.substring(0, CCID_FUZZY_MATCH_LENGTH);
            String sub2 = d2.substring(0, CCID_FUZZY_MATCH_LENGTH);
            return sub1.equals(sub2);
        }
        return d1.equals(d2);
    }

    // ================== DATA CLASSES ==================

    private static record ScannedSim(
            String comName,
            String ccid,
            String imsi,
            String phoneNumber,
            String simProvider,
            boolean ok,
            int signalLevel) {
    }
}
