
package app.simsmartgsm.uitils;

import app.simsmartgsm.baseGateway.CloudGateway;
import app.simsmartgsm.baseGateway.GsmProperties;
import app.simsmartgsm.config.SmsParser;
import app.simsmartgsm.dto.response.SmsMessageUser;
import app.simsmartgsm.config.RemoteWsClient;
import app.simsmartgsm.entity.CallMessage;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.CallMessageRepository;
import app.simsmartgsm.service.GsmListenerService;
import com.fazecast.jSerialComm.SerialPort;
import com.github.pemistahl.lingua.api.Language;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PortWorker
 * - Vòng đời 1 modem/SIM (mở cổng, nhận/gửi SMS, gọi điện, ghi âm, upload,
 * callback).
 * - Luồng nền: run() đọc queue, xen kẽ scan SMS định kỳ nếu rảnh.
 */
@Slf4j
public class PortWorker implements Runnable, AutoCloseable {

    // ======================================================================
    // CẤU HÌNH / HẰNG SỐ
    // ======================================================================
    private static final int BAUD_RATE = 115200;

    private static final Duration AT_TIMEOUT_SHORT = Duration.ofMillis(300);
    private static final Duration AT_TIMEOUT_MEDIUM = Duration.ofMillis(800);
    private static final Duration AT_TIMEOUT_LONG = Duration.ofSeconds(2);
    private static final Duration SCAN_SLEEP_ON_FAIL = Duration.ofSeconds(3);

    private static final Duration CLCC_POLL_INTERVAL = Duration.ofSeconds(2);
    private static final int MAX_DIAL_ATTEMPTS = 2;
    private static final Duration DIAL_RETRY_DELAY = Duration.ofSeconds(2);
    private static final Duration CALLBACK_DEBOUNCE = Duration.ofSeconds(10);
    private static final int MAX_CONNECTION_FAILURES = 10; // 🔧 FIX: Tăng từ 5 lên 10 để tránh SIM bị INACTIVE quá sớm

    private volatile Instant lastSmsSentAt = Instant.EPOCH;

    // Mặc định (có thể thay bằng cấu hình ứng dụng).
    private static final Path DEFAULT_RECORD_DIR = Paths.get("C:\\recordings");
    private static final String DEFAULT_UPLOAD_URL = "https://be-dashboard-sms-global.smsglobalhub.com/api/upload/record";
    private static final String MODEM_STORAGE_PREFIX = "UFS:";
    private static final int FILE_READ_CHUNK_BYTES = 2048;

    // ======================================================================
    // INNER CLASSES - Shared by CallSession and IncomingCallSession
    // ======================================================================
    private static final class RecordingProfile {
        private final String extension;
        private final int format;
        private final String label;

        private RecordingProfile(String extension, int format, String label) {
            this.extension = extension;
            this.format = format;
            this.label = label;
        }
    }

    private static final RecordingProfile[] RECORDING_PROFILES = new RecordingProfile[] {
            new RecordingProfile(".wav", 14, "WAV_ALAW"), // ✅ BEST for telephony (clearer voice)
            new RecordingProfile(".wav", 15, "WAV_ULAW"), // Good quality (US/Japan standard)
            new RecordingProfile(".wav", 16, "WAV_ADPCM"), // Good balance but may cause distortion
            new RecordingProfile(".wav", 13, "WAV_PCM16"), // Best quality but very large
            new RecordingProfile(".amr", 3, "AMR") // Smallest but lowest quality
    };

    private static class ModemFileInfo {
        final String rawPath;
        final String normalizedPath;
        final long size;

        ModemFileInfo(String rawPath, String normalizedPath, long size) {
            this.rawPath = rawPath;
            this.normalizedPath = normalizedPath;
            this.size = size;
        }
    }

    // ======================================================================
    // DEPENDENCIES
    // ======================================================================
    private final RemoteWsClient remoteWsClient;

    // ======================================================================
    // STATE & DEPENDENCIES
    // ======================================================================
    private final Sim sim;
    private final GsmListenerService listenerService;
    private final CloudGateway cloudGateway;
    private final CallMessageRepository callMessageRepository;
    private final app.simsmartgsm.service.CallService callService;
    private final app.simsmartgsm.service.SmsDailyLimitService smsDailyLimitService;
    private final GsmProperties gsmProperties;
    private final Duration minSmsInterval;
    private final Duration smsJitter;
    private final Duration backupSmsScanInterval;
    private app.simsmartgsm.config.ComManager comManager; // ✅ For auto-switch SIM khi hết quota

    /**
     * ✅ Bounded-by-policy Priority Queue - giới hạn 500 task/SIM.
     * PriorityBlockingQueue không có max constructor, nên enqueue tự check size.
     * Task implements Comparable để CALL/SEND không bị scan nền chặn.
     */
    private static final int MAX_QUEUE_CAPACITY = 500;
    private final BlockingQueue<Task> queue = new PriorityBlockingQueue<>();

    /**
     * ✅ Flag để interrupt low-priority tasks khi có urgent task
     * Set = true khi có HIGH/CRITICAL task vào queue
     * Check trong các long-running tasks (như SMS scan)
     */
    private volatile boolean urgentTaskPending = false;

    /** Executor định thời cho worker */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "PortWorker-Scheduler-" + UUID.randomUUID());
        t.setDaemon(true);
        return t;
    });

    /** Executor riêng cho call-session */
    private final ScheduledExecutorService callScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "PortWorker-CallScheduler-" + UUID.randomUUID());
        t.setDaemon(true);
        return t;
    });

    private volatile boolean running = true;
    private volatile Instant lastActiveTime = Instant.now();
    private volatile Instant lastSmsReceivedAt = Instant.now();
    private volatile Instant lastSmsScanAt = Instant.EPOCH;

    // 🔥 NEW: URC support detection - test on startup, fallback to polling if not
    // supported
    private volatile boolean urcSupported = false;
    private volatile boolean urcTested = false;

    // 🔥 NEW: Simulated URC - track SMS count to detect new messages
    // When URC is not supported, we poll SMS count and compare with previous value
    private volatile int lastSmsCount = -1; // -1 = chưa khởi tạo
    private volatile Instant lastSmsCountCheck = Instant.EPOCH;

    // ✅ Periodic CNMI re-apply to prevent modem from resetting to mode 0
    private volatile Instant lastCnmiApply = Instant.now();

    // ✅ NEW: Call state tracking to prevent concurrent calls
    private volatile TaskType currentCallType = null; // Type of call currently in progress (CALL/CALL_IN)
    private volatile String currentCallOrderId = null; // Order ID of current call
    private volatile Instant currentCallStartTime = null; // When current call started

    private final Map<String, Instant> lastCallbackTime = new ConcurrentHashMap<>();
    private final Map<String, Instant> processedSmsCache = new ConcurrentHashMap<>();

    private static final Duration SMS_CACHE_TTL = Duration.ofMinutes(2);

    private int connectionFailCount = 0;

    // ✅ NEW: Track port disconnect for rate-limited reconnection
    private volatile Instant lastPortDisconnectTime = null;
    private volatile int consecutiveDisconnects = 0;
    private static final Duration MIN_RECONNECT_DELAY = Duration.ofSeconds(2);
    private static final Duration MAX_RECONNECT_DELAY = Duration.ofSeconds(30);

    // ✅ IMPROVEMENT #3: Storage management tracking
    private int scanCount = 0; // Track scan iterations for periodic cleanup

    // ✅ CREG cache - tránh query AT+CREG? mỗi lần gửi SMS (thêm ~200-500ms/tin)
    // Chỉ re-check mỗi 30s hoặc khi gửi fail
    private volatile boolean lastCregOk = true; // Assume OK until proven otherwise
    private volatile Instant lastCregCheck = Instant.EPOCH;
    private static final Duration CREG_CACHE_TTL = Duration.ofSeconds(30);

    // Serial resources
    private SerialPort port;
    private AtCommandHelper helper;

    // ======================================================================
    // CTOR
    // ======================================================================
    public PortWorker(Sim sim,
            GsmListenerService listenerService,
            CloudGateway cloudGateway,
            RemoteWsClient remoteWsClient,
            CallMessageRepository callMessageRepository,
            app.simsmartgsm.service.CallService callService,
            app.simsmartgsm.service.SmsDailyLimitService smsDailyLimitService,
            GsmProperties gsmProperties) {
        this.remoteWsClient = remoteWsClient;
        this.sim = sim;
        this.listenerService = listenerService;
        this.cloudGateway = cloudGateway;
        this.callMessageRepository = callMessageRepository;
        this.callService = callService;
        this.smsDailyLimitService = smsDailyLimitService;
        this.gsmProperties = gsmProperties;
        long configuredMinIntervalMs = 500;
        long configuredJitterMs = 0;
        long configuredBackupScanMs = 5000;
        if (gsmProperties != null && gsmProperties.getSms() != null) {
            configuredMinIntervalMs = Math.max(0, gsmProperties.getSms().getMinIntervalMs());
            configuredJitterMs = Math.max(0, gsmProperties.getSms().getJitterMs());
            configuredBackupScanMs = Math.max(1000, gsmProperties.getSms().getBackupScanDelayMs());
        }
        this.minSmsInterval = Duration.ofMillis(configuredMinIntervalMs);
        this.smsJitter = Duration.ofMillis(configuredJitterMs);
        this.backupSmsScanInterval = Duration.ofMillis(configuredBackupScanMs);
    }

    /** ✅ Set ComManager reference (called after construction to avoid circular ref) */
    public void setComManager(app.simsmartgsm.config.ComManager comManager) {
        this.comManager = comManager;
    }

    // ======================================================================
    // PUBLIC API
    // ======================================================================
    public Sim getSim() {
        return sim;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isOpen() {
        return port != null && port.isOpen();
    }

    /** Cập nhật thời gian hoạt động cuối */
    public void touch() {
        lastActiveTime = Instant.now();
    }

    public Instant getLastActiveTime() {
        return lastActiveTime;
    }

    public void updateLastSmsTime() {
        lastSmsReceivedAt = Instant.now();
        touch();
    }

    /**
     * ✅ Đẩy task vào queue và set urgent flag nếu cần
     * Priority queue sẽ tự động sắp xếp theo priority
     * ✅ FIX #7: Nếu queue đầy → reject task + gửi callback FAIL
     */
    public boolean enqueue(Task task) {
        // ✅ Check queue capacity trước khi enqueue
        if (queue.size() >= MAX_QUEUE_CAPACITY) {
            log.error("❌ [{}] Queue FULL ({} tasks), REJECT task {} (orderId={})",
                    sim.getComName(), queue.size(), task.type, task.orderId);

            // Gửi callback FAIL để backend biết job bị reject
            try {
                listenerService.onSmsSentResult(sim, task.to != null ? task.to : "unknown",
                        task.content, false, task.orderId, task.serviceCode, task.localMsgId);
            } catch (Exception ignored) {
            }
            return false;
        }

        boolean offered = queue.offer(task);

        if (!offered) {
            log.error("❌ [{}] Queue offer failed unexpectedly", sim.getComName());
            return false;
        }

        // ✅ Set flag nếu task urgent → để interrupt current low-priority task
        if (task.isUrgent()) {
            urgentTaskPending = true;
            log.info("🚨 [{}] Urgent task queued: {} (priority={})",
                    sim.getComName(), task.type, task.priority);
        }
        return true;
    }

    /**
     * ✅ Tiện ích enqueue ngắn gọn
     * Deprecated: Nên dùng factory methods (Task.send(), Task.call(), etc.)
     */
    @Deprecated
    public void enqueue(TaskType type, String to, String content, String serviceCode, String orderId) {
        Task task = new Task(type, to, content, serviceCode, orderId);
        // Set default priorities based on type
        switch (type) {
            case CALL:
            case CALL_IN:
                task.priority = TaskPriority.HIGH;
                break;
            case SEND:
            case CMD:
                task.priority = TaskPriority.NORMAL;
                break;
            case SCAN:
                task.priority = TaskPriority.LOW;
                break;
        }
        enqueue(task);
    }

    /**
     * ✅ Yêu cầu scan nền.
     * Dùng cho backup scanner định kỳ nên vẫn LOW priority.
     */
    public void forceScan() {
        forceScan(TaskPriority.LOW);
    }

    /**
     * Yêu cầu scan sớm khi đã có tín hiệu SMS đến từ modem.
     */
    public void forceImmediateScan() {
        forceScan(TaskPriority.NORMAL);
    }

    private void forceScan(TaskPriority priority) {
        if (queue.stream().anyMatch(t -> t.type == TaskType.SCAN
                && t.priority.getValue() <= priority.getValue())) {
            log.debug("⏭️ [{}] SMS scan already queued, skip duplicate forceScan", sim.getComName());
            return;
        }
        enqueue(Task.scan(priority));
    }

    /**
     * ✅ Get current queue size for monitoring
     */
    public int getQueueSize() {
        return queue.size();
    }

    /**
     * ✅ Execute AT command synchronously using this worker's serial port.
     * Used by ProxyDataService to send data connection commands without
     * opening a separate serial connection (which would conflict with PortWorker).
     *
     * Thread-safe: Uses CompletableFuture + queue to run on worker thread.
     *
     * @param command AT command to send
     * @param timeoutMs timeout in milliseconds
     * @return response string, or null on failure
     */
    public String executeAtCommandSync(String command, int timeoutMs) {
        if (helper == null || !isOpen()) {
            log.warn("⚠️ [{}] Cannot execute AT command - port not open", sim.getComName());
            return null;
        }

        CompletableFuture<String> future = new CompletableFuture<>();

        // Enqueue a CRITICAL CMD task that stores result in future
        Task task = Task.criticalCmd(command, "PROXY", "proxy-cmd");
        // Use a custom callback mechanism via a response holder
        task.responseFuture = future;
        enqueue(task);

        try {
            return future.get(timeoutMs + 2000L, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("⚠️ [{}] AT command timeout/error: {} (cmd: {})",
                    sim.getComName(), e.getMessage(), command);
            return null;
        }
    }

    /**
     * ✅ Execute multiple AT commands synchronously for proxy data connection.
     * Commands are executed sequentially on the worker's serial port.
     *
     * @param commands list of {command, timeout_ms} pairs
     * @return map of command → response
     */
    public Map<String, String> executeAtCommandsSync(List<String[]> commands) {
        Map<String, String> results = new LinkedHashMap<>();
        for (String[] cmdPair : commands) {
            String cmd = cmdPair[0];
            int timeout = cmdPair.length > 1 ? Integer.parseInt(cmdPair[1]) : 2000;
            String resp = executeAtCommandSync(cmd, timeout);
            results.put(cmd, resp);
            if (resp == null) {
                log.warn("⚠️ [{}] Command failed, stopping sequence: {}", sim.getComName(), cmd);
                break;
            }
        }
        return results;
    }

    /** Ngừng worker */
    public void stop() {
        running = false;
        try {
            scheduler.shutdownNow();
        } catch (Exception ignored) {
        }
        try {
            callScheduler.shutdownNow();
        } catch (Exception ignored) {
        }
        closePort();
    }

    /** AutoCloseable */
    @Override
    public void close() {
        try {
            stop();
            log.info("🧹 [{}] Worker closed cleanly", sim.getComName());
        } catch (Exception e) {
            log.warn("⚠️ [{}] Error while closing worker: {}", sim.getComName(), e.getMessage());
        }
    }

    // ======================================================================
    // MAIN LOOP
    // ======================================================================

    /** ✅ Max consecutive SEND tasks to process before yielding for URC check */
    private static final int BURST_SEND_LIMIT = 1;
    private static final long SIMULATED_URC_COUNT_INTERVAL_MS = 1000;
    /** ✅ Cooldown after burst sending before doing SIMULATED URC poll (ms)
     *  Reduced from 5000 → 1000: Hardware URC (+CMTI) bypasses this cooldown entirely.
     *  This only throttles AT+CPMS polling to avoid spamming the modem right after send.
     */
    private static final long POST_SEND_SCAN_COOLDOWN_MS = 1000;
    /** Track last send completion time for intelligent scan throttling */
    private volatile Instant lastSendCompleteTime = Instant.EPOCH;

    @Override
    public void run() {
        log.debug("▶️ Start PortWorker for SIM {} (COM={})", sim.getPhoneNumber(), sim.getComName());

        // 🔥 OPTIMIZED: Reduce polling frequency to save power/CPU
        // 500ms is enough for URC detection while significantly reducing load
        final int URC_POLL_MS = 500;

        while (running) {
            try {
                if (!ensurePort()) {
                    safeSleep(SCAN_SLEEP_ON_FAIL.toMillis());
                    continue;
                }

                // 🔥 Test URC support on first run
                if (!urcTested) {
                    testUrcSupport();
                    urcTested = true;
                }

                // ✅ BURST SEND MODE: Khi queue có SEND tasks, xử lý liên tục
                // không xen kẽ URC/SMS scan giữa mỗi task → giảm latency đáng kể
                if (hasPendingSendTasks()) {
                    int burstCount = 0;
                    while (burstCount < BURST_SEND_LIMIT && running) {
                        Task next = queue.peek();
                        if (next == null || next.type != TaskType.SEND) {
                            break;
                        }

                        Task task = queue.poll(100, TimeUnit.MILLISECONDS);
                        if (task == null) break;

                        handleTask(task);
                        burstCount++;

                        // Yield nếu có CALL/CALL_IN task trong queue (ưu tiên cao hơn)
                        if (hasUrgentNonSendTask()) break;
                    }
                    if (burstCount == 0) {
                        continue;
                    }
                    if (burstCount > 1) {
                        log.debug("⚡ [{}] Burst sent {} SMS tasks", sim.getComName(), burstCount);
                    }
                    lastSendCompleteTime = Instant.now();

                    // ✅ FIX: Sau burst send, LUÔN force scan SMS
                    // Vì trong lúc gửi SMS, flushInput() có thể đã nuốt mất +CMTI URC
                    // Scan sẽ phát hiện SMS mới ngay cả khi URC bị mất
                    log.debug("📨 [{}] Force scan after burst send ({} tasks)", sim.getComName(), burstCount);
                    doScanSms();

                    continue; // Quay lại loop, check queue tiếp trước khi scan
                }

                // 🔥 ADAPTIVE MODE: URC vs POLLING (chỉ khi KHÔNG có SEND tasks)
                boolean shouldScan = false;
                
                if (currentCallType == null) {
                    // ✅ FIX: Hardware URC (+CMTI) luôn được check - KHÔNG bị block bởi cooldown
                    // CMTI là interrupt từ modem, cực kỳ đáng tin cậy và PHẢI được xử lý ngay
                    // Luôn đọc raw URC. urcSupported chỉ là trạng thái cấu hình CNMI,
                    // không được dùng để chặn input reader vì một lần test CNMI fail
                    // có thể làm app bỏ qua toàn bộ +CMTI đang được modem gửi.
                    if (checkForUrcNotification()) {
                        log.debug("📨 [{}] URC detected (+CMTI) - triggering immediate SMS scan", sim.getComName());
                        shouldScan = true;
                    } 
                    // Simulated URC (AT+CPMS polling) CHỈ bị throttle bởi cooldown
                    // Tránh spam AT command ngay sau khi vừa gửi xong
                    else if (!isInPostSendCooldown() && checkSimulatedUrc()) {
                        log.debug("📨 [{}] Simulated URC detected new SMS - triggering immediate scan", sim.getComName());
                        shouldScan = true;
                    }
                    // ✅ Hard fallback: while the app is running, scan periodically even if URC
                    // and CPMS count detection both fail. This catches messages that only appear
                    // after app restart because they stayed in modem storage.
                    else if (!isInPostSendCooldown() && shouldRunPeriodicSmsScan()) {
                        log.debug("🔄 [{}] Periodic SMS fallback scan", sim.getComName());
                        shouldScan = true;
                    }

                    // ✅ FIX: Re-apply CNMI mỗi 2 phút để tránh modem reset về mode 0
                    if (Duration.between(lastCnmiApply, Instant.now()).toMillis() > 120000) {
                        try {
                            applySmsUrcMode(false);
                        } catch (Exception ignored) {}
                    }
                }

                if (shouldScan) {
                    doScanSms();
                    continue; // Quay lại loop để check URC/Queue ngay lập tức
                }

                // ✅ ALWAYS check task queue - không được skip!
                Task task = queue.poll(URC_POLL_MS, TimeUnit.MILLISECONDS);
                if (task != null) {
                    handleTask(task);
                }

            } catch (Exception e) {
                log.error("❌ Worker {} error: {}", sim.getComName(), e.getMessage(), e);
                closePort();
                safeSleep(SCAN_SLEEP_ON_FAIL.toMillis());
            }
        }
        closePort();
        log.debug("⏹ Stopped PortWorker for {}", sim.getComName());
    }

    /** ✅ Check if queue has pending SEND tasks */
    private boolean hasPendingSendTasks() {
        Task peeked = queue.peek();
        return peeked != null && peeked.type == TaskType.SEND;
    }

    /** ✅ Check if queue has urgent non-SEND tasks (CALL, CALL_IN) */
    private boolean hasUrgentNonSendTask() {
        Task peeked = queue.peek();
        return peeked != null && (peeked.type == TaskType.CALL || peeked.type == TaskType.CALL_IN);
    }

    /** ✅ Check if within post-send cooldown (avoid SMS scan immediately after burst) */
    private boolean isInPostSendCooldown() {
        return Duration.between(lastSendCompleteTime, Instant.now()).toMillis() < POST_SEND_SCAN_COOLDOWN_MS;
    }

    private boolean shouldRunPeriodicSmsScan() {
        return Duration.between(lastSmsScanAt, Instant.now()).compareTo(backupSmsScanInterval) >= 0;
    }

    /**
     * Test if URC is supported by checking AT+CNMI response
     */
    private void testUrcSupport() {
        if (helper == null || port == null || !port.isOpen()) {
            urcSupported = false;
            return;
        }

        try {
            String response = helper.sendAndRead("AT+CNMI=2,1,0,0,0", 1000);
            lastCnmiApply = Instant.now();
            if (response != null && response.contains("OK")) {
                urcSupported = true;
                log.info("✅ [{}] URC support enabled (AT+CNMI=2,1,0,0,0)", sim.getComName());
            } else {
                response = helper.sendAndRead("AT+CNMI?", 1000);
                if (response != null && response.contains("+CNMI:") && !response.contains("+CNMI: 0,0")) {
                    urcSupported = true;
                    log.info("✅ [{}] URC support detected: {}", sim.getComName(),
                            response.replace("\r", " ").replace("\n", " ").trim());
                } else {
                    urcSupported = false;
                    log.info("⚠️ [{}] CNMI enable not confirmed; raw URC reader still active, fallback scan enabled",
                            sim.getComName());
                }
            }
        } catch (Exception e) {
            urcSupported = false;
            log.warn("⚠️ [{}] URC test failed; raw URC reader still active, fallback scan enabled: {}",
                    sim.getComName(), e.getMessage());
        }
    }

    /**
     * ✅ Check for URC (Unsolicited Result Codes) from EC25 modem
     * EC25 supports AT+CNMI=2,1,0,0,0 which sends "+CMTI: storage,index" when new
     * SMS arrives
     * This enables near real-time SMS detection instead of polling
     * 
     * @return true if URC notification detected, false otherwise
     */
    private final StringBuilder urcBuffer = new StringBuilder();

    private boolean checkForUrcNotification() {
        if (helper == null || port == null || !port.isOpen()) {
            return false;
        }

        // ✅ FIX: Check pending URC data rescued from flushInput() first
        // flushInput() có thể đã nuốt +CMTI trong lúc gửi AT command
        // nhưng consumePendingUrc() giữ lại thay vì discard
        try {
            String rescued = helper.consumePendingUrc();
            if (rescued != null) {
                log.info("📨 [{}] Rescued URC from flushInput(): {}", sim.getComName(),
                        rescued.replace("\r", "\\r").replace("\n", "\\n"));
                if (rescued.contains("+CMT:") && !hasCompleteDirectCmtUrc(rescued)) {
                    urcBuffer.append(rescued);
                    if (!rescued.contains("+CMTI:")) {
                        return false;
                    }
                }
                processDirectSmsUrc(rescued);
                scheduleSmsScanFollowUps("rescued URC");
                urcBuffer.setLength(0); // Clear old buffer
                return true; // Trigger scan immediately
            }
        } catch (Exception e) {
            log.debug("⚠️ [{}] Error checking rescued URC: {}", sim.getComName(), e.getMessage());
        }

        try {
            InputStream in = port.getInputStream();
            int available = in.available();

            if (available <= 0) {
                return false; 
            }

            byte[] buffer = new byte[Math.min(available, 1024)];
            int len = in.read(buffer);

            if (len <= 0) {
                return false;
            }

            String chunk = new String(buffer, 0, len, StandardCharsets.US_ASCII);
            urcBuffer.append(chunk);

            // Prevent memory leak if lots of unhandled data
            if (urcBuffer.length() > 4096) {
                urcBuffer.delete(0, urcBuffer.length() - 2048);
            }

            String data = urcBuffer.toString();

            if (!chunk.trim().isEmpty()) {
                log.debug("📡 [{}] URC buffer ({} bytes): {}", sim.getComName(), len,
                        chunk.replace("\r", "\\r").replace("\n", "\\n"));
            }

            // Check for +CMTI: (new SMS indicator)
            if (data.contains("+CMTI:")) {
                log.debug("📬 [{}] EC25 URC received: {}", sim.getComName(),
                        data.replace("\r", "").replace("\n", " ").trim());
                scheduleSmsScanFollowUps("+CMTI");
                urcBuffer.setLength(0);
                return true;
            }

            // Check for +CMT: (SMS delivered directly - mode 2)
            if (data.contains("+CMT:")) {
                if (!hasCompleteDirectCmtUrc(data)) {
                    log.debug("📬 [{}] Partial direct +CMT URC received, waiting for body", sim.getComName());
                    return false;
                }
                log.debug("📬 [{}] EC25 URC (direct SMS): {}", sim.getComName(),
                        data.replace("\r", "").replace("\n", " ").trim());
                processDirectSmsUrc(data);
                scheduleSmsScanFollowUps("+CMT");
                urcBuffer.setLength(0);
                return true;
            }

            // Check for RING (incoming call)
            if (data.contains("RING") || data.contains("+CLIP:")) {
                log.debug("📞 [{}] Incoming call URC: {}", sim.getComName(),
                        data.replace("\r", "").replace("\n", " ").trim());
                urcBuffer.setLength(0);
            }

        } catch (Exception e) {
            log.debug("⚠️ [{}] URC check error: {}", sim.getComName(), e.getMessage());
        }

        return false;
    }

    private boolean hasCompleteDirectCmtUrc(String raw) {
        for (String chunk : splitDirectCmtUrcs(raw)) {
            String[] lines = chunk.split("\\n");
            boolean afterHeader = false;
            for (String line : lines) {
                String trimmed = line.trim();
                if (trimmed.startsWith("+CMT:")) {
                    afterHeader = true;
                    continue;
                }
                if (afterHeader && !trimmed.isEmpty()
                        && !trimmed.equals("OK")
                        && !trimmed.startsWith("+CMTI:")
                        && !trimmed.startsWith("+CMT:")) {
                    return true;
                }
            }
        }
        return false;
    }

    private void scheduleSmsScanFollowUps(String reason) {
        long[] delaysMs = { 700L, 2000L, 5000L };
        for (long delayMs : delaysMs) {
            scheduler.schedule(() -> {
                try {
                    if (!running) {
                        return;
                    }
                    log.debug("🔁 [{}] Follow-up SMS scan after {} (delay={}ms)",
                            sim.getComName(), reason, delayMs);
                    forceImmediateScan();
                } catch (Exception e) {
                    log.debug("⚠️ [{}] Follow-up SMS scan enqueue failed: {}", sim.getComName(), e.getMessage());
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    private void processDirectSmsUrc(String raw) {
        if (raw == null || !raw.contains("+CMT:")) {
            return;
        }

        for (String chunk : splitDirectCmtUrcs(raw)) {
            try {
                SmsMessageUser parsed = SmsParser.parse(chunk);
                if (parsed == null || parsed.getBody() == null || parsed.getBody().isBlank()) {
                    continue;
                }

                String sender = AtCommandHelper.ultimateTextDecode(parsed.getSender());
                String body = AtCommandHelper.ultimateTextDecode(parsed.getBody());
                if (sender == null || sender.isBlank() || body == null || body.isBlank()) {
                    continue;
                }

                AtCommandHelper.SmsRecord synthetic = new AtCommandHelper.SmsRecord();
                synthetic.index = parsed.getIndex();
                synthetic.storage = "URC";
                synthetic.timestamp = parsed.getTimestamp();

                if (isDuplicateSms(sender, body, synthetic)) {
                    log.debug("⏭️ [{}] Skip duplicate direct URC SMS: {} - {}", sim.getComName(), sender, body);
                    continue;
                }

                log.info("📩 [{}] Direct URC SMS processed immediately: from={} body={}",
                        sim.getComName(), sender, body);
                updateLastSmsTime();
                listenerService.processSms(sim, sender, body);
                markSmsAsProcessed(sender, body, synthetic);
            } catch (Exception e) {
                log.warn("⚠️ [{}] Cannot process direct SMS URC: {}", sim.getComName(), e.getMessage());
            }
        }
    }

    private List<String> splitDirectCmtUrcs(String raw) {
        List<String> chunks = new ArrayList<>();
        if (raw == null || raw.isBlank()) {
            return chunks;
        }

        String normalized = raw.replace("\r\n", "\n").replace('\r', '\n');
        int start = normalized.indexOf("+CMT:");
        while (start >= 0) {
            int next = normalized.indexOf("\n+CMT:", start + 1);
            String chunk = next >= 0 ? normalized.substring(start, next) : normalized.substring(start);
            if (!chunk.isBlank()) {
                chunks.add(chunk.trim());
            }
            start = next >= 0 ? next + 1 : -1;
        }
        return chunks;
    }

    /**
     * 🔥 NEW: Simulated URC - Mô phỏng URC khi modem không hỗ trợ
     * Polling thông minh: Kiểm tra số lượng SMS trong storage và so sánh với lần
     * trước
     * 
     * @return true nếu phát hiện SMS mới, false nếu không
     */
    private boolean checkSimulatedUrc() {
        if (helper == null || port == null || !port.isOpen()) {
            return false;
        }

        try {
            // ✅ Throttle: check count mỗi 1 giây để bắt SMS mới gần real-time
            // mà vẫn tránh full CMGL scan nếu số lượng SMS không đổi.
            Instant now = Instant.now();
            if (Duration.between(lastSmsCountCheck, now).toMillis() < SIMULATED_URC_COUNT_INTERVAL_MS) {
                return false;
            }
            lastSmsCountCheck = now;

            // ✅ Đọc tổng số SMS từ cả ME và SM.
            // Một số modem tự lưu vào ME dù CPMS đang là SM, nên chỉ nhìn 1 storage sẽ miss.
            int currentSmsCount = getTotalSmsCountAcrossStores();

            if (currentSmsCount < 0) {
                return false; // Parse failed
            }

            // ✅ Lần đầu tiên: Chỉ lưu giá trị, không trigger scan
            if (lastSmsCount == -1) {
                lastSmsCount = currentSmsCount;
                log.debug("📊 [{}] Initial SMS count: {}", sim.getComName(), currentSmsCount);
                return false;
            }

            // ✅ So sánh với lần trước
            if (currentSmsCount > lastSmsCount) {
                int newSmsCount = currentSmsCount - lastSmsCount;
                log.info("📨 [{}] SIMULATED URC: Detected {} new SMS (count: {} → {})",
                        sim.getComName(), newSmsCount, lastSmsCount, currentSmsCount);
                lastSmsCount = currentSmsCount;
                return true; // Có SMS mới!
            }

            // ✅ Cập nhật count nếu giảm (do đã xóa SMS)
            if (currentSmsCount < lastSmsCount) {
                log.debug("📉 [{}] SMS count decreased: {} → {} (messages deleted)",
                        sim.getComName(), lastSmsCount, currentSmsCount);
                lastSmsCount = currentSmsCount;
            }

            return false; // Không có SMS mới

        } catch (Exception e) {
            log.debug("⚠️ [{}] Simulated URC check error: {}", sim.getComName(), e.getMessage());
            return false;
        }
    }

    /**
     * Parse SMS count from AT+CPMS? response
     * Format: +CPMS: "MT",used,total,"MT",used,total,"MT",used,total
     * Returns the "used" value from the first storage (MT)
     */
    private int parseSmsCount(String response) {
        try {
            // Extract the first storage info: "MT",used,total
            Pattern pattern = Pattern.compile("\\+CPMS:\\s*\"([^\"]+)\",(\\d+),(\\d+)");
            Matcher matcher = pattern.matcher(response);

            if (matcher.find()) {
                return Integer.parseInt(matcher.group(2)); // Return "used" count
            }
        } catch (Exception e) {
            log.debug("⚠️ [{}] Failed to parse SMS count: {}", sim.getComName(), e.getMessage());
        }
        return -1;
    }

    private int getTotalSmsCountAcrossStores() {
        int total = 0;
        boolean foundAny = false;

        for (String store : List.of("ME", "SM")) {
            try {
                String response = helper.sendAndRead(
                        "AT+CPMS=\"" + store + "\",\"" + store + "\",\"" + store + "\"", 1000);
                int count = parseSmsCount(response);
                if (count >= 0) {
                    total += count;
                    foundAny = true;
                }
            } catch (Exception e) {
                log.debug("⚠️ [{}] Could not count SMS in {}: {}", sim.getComName(), store, e.getMessage());
            }
        }

        return foundAny ? total : -1;
    }

    // ======================================================================
    // TASKS
    // ======================================================================
    private void handleTask(Task task) {
        try {
            switch (task.type) {
                case SEND -> doSendSms(task);
                case SCAN -> doScanSms();
                case CMD -> doCommand(task);
                case CALL -> doCall(task);
                case CALL_IN -> doCallIn(task); // ✅ NEW: Handle incoming call
            }
        } finally {
            touch();
            // ✅ FIX: Reset urgent flag if no more urgent tasks pending
            if (!hasUrgentNonSendTask()) {
                urgentTaskPending = false;
            }
        }
    }

    // ------------------------------------------------------------------
    // CALL FLOW - ✅ Use CallSession with CORRECTED AT commands
    // ------------------------------------------------------------------
    private void doCall(Task task) {
        final String com = sim.getComName();
        log.info("📞 [{}] CALL → {} | record={} | duration={}s",
                com, task.to, task.record, task.duration);

        // ✅ FIX: Mark call as active to prevent URC conflict
        currentCallType = TaskType.CALL;
        currentCallOrderId = task.orderId;
        currentCallStartTime = Instant.now();

        if (helper == null || !helper.isPortOpen()) {
            sendCallCallback(com, Instant.now(), Instant.now(), task, null, 0, false, "PORT_NOT_OPEN");
            notifyCallStatus("FAILED", task);
            currentCallType = null;
            currentCallOrderId = null;
            currentCallStartTime = null;
            return;
        }

        notifyCallStatus("DIALING", task);

        try (CallSession session = new CallSession(task)) { // ✅ AUTO-CLOSE
            // ✅ FIXED: Use CallSession but with CORRECTED AT commands from CallService
            // The issue was CallSession had wrong AT sequence - now we keep it
            // but ensure AT commands match CallService.directCall() logic

            log.info("🔵 [{}] Step 1/3: prepareModem()", com);
            session.prepareModem(); // cấu hình cơ bản
            log.info("✅ [{}] Step 1/3 completed", com);

            log.info("🔵 [{}] Step 2/3: dial()", com);
            session.dial(); // quay số với logic đúng
            log.info("✅ [{}] Step 2/3 completed", com);

            log.info("🔵 [{}] Step 3/3: monitorCallState()", com);
            session.monitorCallState(); // theo dõi CLCC
            log.info("✅ [{}] Step 3/3 completed - call session active", com);
            // ❌ REMOVED: session.scheduleStopByDuration() - đã move vào CONNECTED state

        } catch (Exception e) {
            log.error("❌ [{}] CALL ERROR: {}", com, e.getMessage(), e);

            // ✅ NOTE: callback đã được gửi bởi CallSession.close() → endCall()
            // trong try-with-resources, KHÔNG gửi lại ở đây để tránh duplicate callback.
            // endCall() sẽ tự handle stop recording + download + upload + callback.
            notifyCallStatus("FAILED", task);

            // RESET LẠI MODEM KHI CÓ LỖI
            resetModemAsync();
        } finally {
            // ✅ FIX: Clear call active flag when call ends
            currentCallType = null;
            currentCallOrderId = null;
            currentCallStartTime = null;
        }
    }

    // ------------------------------------------------------------------
    // ✅ NEW: INCOMING CALL FLOW (CALL_IN)
    // ------------------------------------------------------------------
    private void doCallIn(Task task) {
        final String com = sim.getComName();
        log.info("📞 [{}] CALL_IN (WAITING) | record={} | max_duration={}s",
                com, task.record, task.duration);

        // ✅ FIX: Mark call as active to prevent URC conflict
        currentCallType = TaskType.CALL_IN;
        currentCallOrderId = task.orderId;
        currentCallStartTime = Instant.now();

        if (helper == null || !helper.isPortOpen()) {
            sendCallCallback(com, Instant.now(), Instant.now(), task, null, 0, false, "PORT_NOT_OPEN");
            notifyCallStatus("FAILED", task);
            currentCallType = null;
            currentCallOrderId = null;
            currentCallStartTime = null;
            return;
        }

        notifyCallStatus("WAITING_INCOMING_CALL", task);

        try (IncomingCallSession session = new IncomingCallSession(task)) { // ✅ AUTO-CLOSE
            // ✅ Use IncomingCallSession - waits for RING, answers with ATA
            session.prepareModem(); // enable CLIP
            session.waitForIncomingCall(); // ✅ BLOCKS until call finishes or timeout
            // After call ends, session auto-closes

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("❌ [{}] CALL_IN interrupted: {}", com, e.getMessage());
            // ✅ NOTE: callback đã được gửi bởi IncomingCallSession.close() → endCall()
            notifyCallStatus("FAILED", task);
        } catch (Exception e) {
            log.error("❌ [{}] CALL_IN ERROR: {}", com, e.getMessage(), e);
            // ✅ NOTE: callback đã được gửi bởi IncomingCallSession.close() → endCall()
            // endCall() sẽ tự handle stop recording + download + upload + callback
            notifyCallStatus("FAILED", task);

            // RESET LẠI MODEM KHI CÓ LỖI
            resetModemAsync();
        } finally {
            // ✅ FIX: Clear call active flag when call ends
            currentCallType = null;
            currentCallOrderId = null;
            currentCallStartTime = null;
        }
    }

    private void doSendSms(Task task) {
        final String com = sim.getComName();
        boolean ok = false;

        // ✅ Ghi nhận SIM hiện tại vào danh sách đã thử
        if (!task.triedComs.contains(com)) {
            task.triedComs.add(com);
        }

        // ✅ CHECK BLACKLIST trước - SIM bị đánh dấu đỏ thì skip luôn
        if (smsDailyLimitService != null && smsDailyLimitService.isBlacklisted(com)) {
            log.warn("🔴 [{}] SIM đang bị BLACKLISTED (fail nhiều) - chuyển sang SIM khác!", com);
            retryOnDifferentSim(task, "BLACKLISTED");
            return;
        }

        // ✅ CHECK SIM DEAD (từ SimHealthCheckService) - SIM bị nhà mạng suspend/block
        // SIM chết vẫn đọc được SĐT nhưng không gửi/nhận SMS → skip ngay để tránh lãng phí
        if (!sim.isAllowSms()) {
            log.warn("🔴 [{}] SIM đã bị đánh dấu DEAD (allowSms=false) - chuyển sang SIM khác!", com);
            retryOnDifferentSim(task, "SIM_DEAD");
            return;
        }

        // ✅ CHECK NETWORK REGISTRATION trước khi gửi SMS (CACHED - không query mỗi lần)
        // SIM bị khóa/suspend vẫn có thể accept AT+CMGS nhưng carrier sẽ drop SMS
        // Cache CREG mỗi 30s → 99% SMS không bị thêm latency
        // Re-check ngay khi: cache hết hạn HOẶC lần trước bị NOT_REGISTERED
        boolean needCregCheck = !lastCregOk
                || Duration.between(lastCregCheck, Instant.now()).compareTo(CREG_CACHE_TTL) > 0;

        if (needCregCheck && helper != null && helper.isPortOpen()) {
            try {
                String creg = helper.sendAndRead("AT+CREG?", 1500);
                lastCregOk = creg != null && (creg.contains(",1") || creg.contains(",5"));
                lastCregCheck = Instant.now();

                if (!lastCregOk) {
                    log.warn("🚫 [{}] SIM KHÔNG ĐĂNG KÝ MẠNG (CREG: {}) - chuyển SIM khác!",
                            com, creg != null ? creg.replace("\r", "").replace("\n", " ").trim() : "null");

                    if (smsDailyLimitService != null) {
                        smsDailyLimitService.recordFail(com);
                    }

                    boolean retried = retryOnDifferentSim(task, "NOT_REGISTERED");
                    if (!retried) {
                        try {
                            listenerService.onSmsSentResult(sim, task.to, task.content, false,
                                    task.orderId, task.serviceCode, task.localMsgId);
                        } catch (Exception cbEx) {
                            log.warn("⚠️ [{}] onSmsSentResult error: {}", com, cbEx.getMessage());
                        }
                    }
                    return;
                }
            } catch (Exception e) {
                log.warn("⚠️ [{}] CREG check failed, using cached value (ok={}): {}",
                        com, lastCregOk, e.getMessage());
            }
        }

        // ✅ CHECK DAILY LIMIT trước khi gửi - ĐẾM THEO SEGMENTS
        if (smsDailyLimitService != null && !smsDailyLimitService.canSend(com, task.content)) {
            int sent = smsDailyLimitService.getSentToday(com);
            int segmentsNeeded = app.simsmartgsm.service.SmsDailyLimitService.countSegments(task.content);
            log.warn("🛑 [{}] Không đủ quota! Cần {} segments, đã dùng {}/{} - auto-switch! (retry {}/{})",
                    com, segmentsNeeded, sent, smsDailyLimitService.getDailyLimit(),
                    task.retryCount, task.maxRetries);

            // ✅ Auto-switch sang SIM khác có quota (không đếm là retry vì ko phải lỗi gửi)
            retryOnDifferentSim(task, "QUOTA_EXCEEDED");
            return;
        }

        // ✅ Rate limiter + jitter — chờ ngẫu nhiên để tránh pattern gửi đều như máy
        if (!lastSmsSentAt.equals(Instant.EPOCH)) {
            long jitterMs = smsJitter.isZero() ? 0 : ThreadLocalRandom.current().nextLong(smsJitter.toMillis() + 1);
            Duration targetInterval = minSmsInterval.plusMillis(jitterMs);
            Duration elapsed = Duration.between(lastSmsSentAt, Instant.now());
            if (elapsed.compareTo(targetInterval) < 0) {
                long waitMs = targetInterval.toMillis() - elapsed.toMillis();
                log.debug("⏳ [{}] SMS pacing: chờ {}ms (base={}ms, jitter={}ms) trước SMS tiếp",
                        com, waitMs, minSmsInterval.toMillis(), jitterMs);
                safeSleep(waitMs);
            }
        }

        if (comManager != null) {
            try {
                comManager.ensureLocalReceiverReady(task.to, com);
            } catch (Exception e) {
                log.debug("⚠️ [{}] Local receiver warmup skipped for {}: {}", com, task.to, e.getMessage());
            }
        }

        try {
            ok = helper.sendTextSms(task.to, task.content, Duration.ofSeconds(30));

            log.info("📤 [{}] [{}] [{}] -> {} : {} (retry {}/{})",
                    com, task.serviceCode, task.orderId, task.to,
                    ok ? "✅ OK" : "❌ FAIL", task.retryCount, task.maxRetries);

            if (ok) {
                scheduleLocalReceiverScans(task.to, com);
            }

            // ✅ Ghi nhận SMS đã gửi thành công vào daily counter - ĐẾM THEO SEGMENTS
            if (ok && smsDailyLimitService != null) {
                smsDailyLimitService.recordSuccess(com); // ✅ Reset fail counter
                int todayCount = smsDailyLimitService.recordSent(com, task.content);
                int remaining = smsDailyLimitService.getRemainingQuota(com);
                if (remaining <= 10) {
                    log.warn("⚠️ [{}] Chỉ còn {} segments quota hôm nay! ({}/{})",
                            com, remaining, todayCount, smsDailyLimitService.getDailyLimit());
                }
            }

        } catch (AtCommandHelper.PortDisconnectedException e) {
            ok = false;
            log.error("❌ [{}] Port disconnected while sending SMS: {}", com, e.getMessage());
            closePort();
        } catch (Exception e) {
            ok = false;
            log.error("❌ [{}] Error sending SMS: {}", com, e.getMessage(), e);
            closePort();
        } finally {
            // ✅ FIX #3: Ghi nhận thời gian gửi thành công để tính rate limit
            if (ok) {
                lastSmsSentAt = Instant.now();
            }
            // Callback kết quả gửi - ✅ FIX: Pass localMsgId for proper message matching
            try {
                if (helper != null) {
                    // ✅ FIX: Chỉ AT ping nhẹ để verify modem OK sau khi gửi
                    // KHÔNG gọi flushInput() riêng vì sendTextSms() đã tự cleanup
                    // flushInput() thừa có thể nuốt URC (+CMTI) → bỏ lỡ SMS đến
                    helper.sendAndRead("AT", 800);
                }
            } catch (Exception resetEx) {
                log.warn("⚠️ [{}] Post-send recovery failed, closing port: {}", com, resetEx.getMessage());
                closePort();
            }
            if (ok) {
                // ✅ GỬI THÀNH CÔNG → callback SUCCESS ngay
                sim.setSmsSuccessCount(sim.getSmsSuccessCount() + 1);
                sim.setSmsSentTotal(sim.getSmsSentTotal() + 1);
                try {
                    listenerService.onSmsSentResult(sim, task.to, task.content, true,
                            task.orderId, task.serviceCode, task.localMsgId);
                } catch (Exception cbEx) {
                    log.warn("⚠️ [{}] onSmsSentResult error: {}", com, cbEx.getMessage());
                }
            } else {
                // ✅ GHI NHẬN FAIL → có thể dẫn đến blacklist
                sim.setSmsFailedCount(sim.getSmsFailedCount() + 1);
                sim.setSmsSentTotal(sim.getSmsSentTotal() + 1);
                if (smsDailyLimitService != null) {
                    smsDailyLimitService.recordFail(com);
                }
                // ✅ Invalidate CREG cache → re-check network lần gửi tiếp theo
                lastCregOk = false;

                // ✅ GỬI THẤT BẠI → thử retry trên SIM khác (Silent Retry Pattern)
                // Chỉ callback FAIL khi đã hết retry
                boolean retried = retryOnDifferentSim(task, "SEND_FAILED");
                if (!retried) {
                    // Hết retry → gửi callback FAIL cuối cùng
                    log.error("❌ [{}] SMS FINAL FAIL sau {} lần thử trên {} SIM: {} → {}",
                            com, task.retryCount + 1, task.triedComs.size(), task.to, task.triedComs);
                    try {
                        listenerService.onSmsSentResult(sim, task.to, task.content, false,
                                task.orderId, task.serviceCode, task.localMsgId);
                    } catch (Exception cbEx) {
                        log.warn("⚠️ [{}] onSmsSentResult error: {}", com, cbEx.getMessage());
                    }
                }
            }

            updateLastSmsTime();
        }
    }

    /**
     * ✅ Retry gửi SMS trên SIM khác
     * - Clone task với retryCount + 1
     * - Tìm SIM có quota + chưa thử + queue ít nhất
     * - Nếu không tìm được thì return false (caller sẽ callback FAIL)
     *
     * @param task   Task hiện tại đã fail
     * @param reason Lý do retry (SEND_FAILED, QUOTA_EXCEEDED)
     * @return true nếu đã dispatch retry thành công, false nếu hết SIM
     */
    private boolean retryOnDifferentSim(Task task, String reason) {
        // Check còn retry không
        if (task.retryCount >= task.maxRetries) {
            log.warn("🚫 [{}] Đã hết {} lần retry cho order {} (tried: {})",
                    sim.getComName(), task.maxRetries, task.orderId, task.triedComs);
            return false;
        }

        if (comManager == null) {
            return false;
        }

        // ✅ Tìm SIM khác: có quota + đang chạy + chưa thử + queue ít nhất
        PortWorker altWorker = comManager.findWorkerForRetry(
                sim.getComName(), task.triedComs, task.content);

        if (altWorker == null) {
            log.warn("🚫 [{}] Không tìm được SIM thay thế cho retry (reason: {}, tried: {})",
                    sim.getComName(), reason, task.triedComs);
            return false;
        }

        // ✅ Clone task cho retry
        Task retryTask = task.cloneForRetry();
        String newCom = altWorker.getSim().getComName();

        log.info("🔄 [{}] RETRY #{} → {} (reason: {}, quota còn {}, queue: {}, tried: {})",
                sim.getComName(), retryTask.retryCount, newCom, reason,
                smsDailyLimitService != null ? smsDailyLimitService.getRemainingQuota(newCom) : "?",
                altWorker.getQueueSize(), retryTask.triedComs);

        altWorker.enqueue(retryTask);
        return true;
    }

    private void scheduleLocalReceiverScans(String phoneNumber, String senderComName) {
        if (comManager == null || phoneNumber == null || phoneNumber.isBlank()) {
            return;
        }

        long[] delaysMs = { 1500L, 5000L, 10000L, 20000L };
        for (long delayMs : delaysMs) {
            scheduler.schedule(() -> {
                try {
                    comManager.forceScanLocalReceiver(phoneNumber, senderComName);
                } catch (Exception e) {
                    log.debug("⚠️ [{}] Local receiver delayed scan failed for {}: {}",
                            senderComName, phoneNumber, e.getMessage());
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    // ------------------------------------------------------------------
    // RAW CMD
    // ------------------------------------------------------------------
    private void doCommand(Task task) {
        try {
            String resp = helper.sendAndRead(task.content, (int) AT_TIMEOUT_LONG.toMillis());
            log.info("➡️ [{}] CMD '{}'\n{}", sim.getComName(), task.content, resp.trim());
            updateLastSmsTime();

            // ✅ Complete future if present (for executeAtCommandSync)
            if (task.responseFuture != null) {
                task.responseFuture.complete(resp);
            }

            if (isOtpOnlyMode()) {
                applySmsUrcMode(false);
            }
        } catch (Exception e) {
            log.error("❌ [{}] CMD error: {}", sim.getComName(), e.getMessage());
            // ✅ Complete future with null on error
            if (task.responseFuture != null) {
                task.responseFuture.complete(null);
            }
        }
    }

    private void doScanSms() {
        final String com = sim.getComName();
        try {
            if (helper == null || port == null || !port.isOpen()) {
                log.debug("⚠️ [{}] Port chưa mở, bỏ qua SCAN", com);
                return;
            }

            lastSmsScanAt = Instant.now();

            // ✅ IMPROVEMENT #3: Increment scan counter
            scanCount++;

            // Prepare modem for reading
            prepareModemForReadSms();

            // ✅ FIX: Giảm tần suất cleanup để tránh xóa SMS chưa kịp xử lý
            // Chỉ cleanup mỗi 200 scans (trước đó 50 - quá aggressive)
            // Dùng AT+CMGD=1,1 (chỉ xóa READ) thay vì AT+CMGD=1,3 (xóa ALL READ + SENT + UNSENT)
            if (scanCount % 200 == 0) {
                log.debug("🧹 [{}] Periodic storage cleanup (scan #{})", com, scanCount);
                try {
                    helper.sendAndRead("AT+CMGD=1,1", 3000); // Delete only READ messages
                } catch (Exception e) {
                    log.warn("⚠️ [{}] Periodic cleanup failed: {}", com, e.getMessage());
                }
            }

            List<AtCommandHelper.SmsRecord> all = new ArrayList<>();

            // Đọc từ cả ME và SM storage
            for (String store : List.of("ME", "SM")) {
                // ✅ Check interrupt trước khi scan storage tiếp
                if (urgentTaskPending) {
                    log.debug("⏸️ [{}] Pausing SMS scan - urgent task pending (before scanning {})", com, store);
                    enqueue(Task.scan()); // Queue a scan to resume later
                    return; // Return immediately, urgent task sẽ được xử lý ngay
                }

                try {
                    String setStore = "AT+CPMS=\"" + store + "\",\"" + store + "\",\"" + store + "\"";
                    String resp = helper.sendAndRead(setStore, (int) AT_TIMEOUT_MEDIUM.toMillis());

                    // ✅ IMPROVEMENT #3: Check storage capacity BEFORE scanning
                    checkStorageCapacity(store, resp);

                    if (resp.contains("OK")) {
                        log.debug("📱 [{}] Scanning storage: {}", com, store);

                        // ✅ FIX: Luôn scan ALL SMS trước (không chỉ UNREAD)
                        // Nhiều modem tự động đánh dấu SMS là READ khi nhận
                        // → Nếu chỉ scan UNREAD sẽ bỏ lỡ tin nhắn
                        // isDuplicateSms() sẽ lọc trùng, safeDeleteIndex() xóa sau khi xử lý
                        List<AtCommandHelper.SmsRecord> smsInStore = helper.listAllSmsText(5000);
                        for (AtCommandHelper.SmsRecord record : smsInStore) {
                            record.storage = store;
                        }

                        all.addAll(smsInStore);
                        log.debug("📬 [{}] Found {} SMS in {} (total {} so far)",
                                com, smsInStore.size(), store, all.size());
                    } else {
                        log.warn("⚠️ [{}] Cannot set storage to {}: {}", com, store, resp);
                    }
                } catch (Exception e) {
                    log.warn("⚠️ [{}] Error scanning {}: {}", com, store, e.getMessage());
                }
            }

            if (all.isEmpty()) {
                log.debug("🔭 [{}] Không có SMS mới", com);
                return;
            }

            log.debug("📨 [{}] Tìm thấy {} SMS chưa đọc", com, all.size());

            int processedCount = 0;
            int skippedCount = 0;
            int errorCount = 0;

            for (var sms : all) {
                // ✅ Check interrupt sau mỗi SMS được xử lý
                if (urgentTaskPending) {
                    log.debug("⏸️ [{}] Pausing SMS scan - urgent task pending ({}/{} processed)",
                            com, processedCount, all.size());
                    enqueue(Task.scan()); // Queue a scan to resume later
                    return; // Return ngay, để urgent task được xử lý
                }

                try {
                    String sender = AtCommandHelper.ultimateTextDecode(sms.sender);
                    String body = AtCommandHelper.ultimateTextDecode(sms.body);

                    // Validate SMS data
                    if (sender == null || sender.trim().isEmpty()) {
                        log.warn("⚠️ [{}] SMS with empty sender, index={}", com, sms.index);
                        safeDeleteIndex(sms.index, sms.storage);
                        skippedCount++;
                        continue;
                    }

                    if (body == null || body.trim().isEmpty()) {
                        log.warn("⚠️ [{}] SMS with empty body from {}, index={}", com, sender, sms.index);
                        safeDeleteIndex(sms.index, sms.storage);
                        skippedCount++;
                        continue;
                    }

                    // Bỏ tin nhắn ĐI (outgoing)
                    if (isOutgoingSms(sender, body)) {
                        log.debug("⏩ [{}] Skip outgoing: From='{}' | Body='{}'", com, sender, body);
                        safeDeleteIndex(sms.index, sms.storage);
                        skippedCount++;
                        continue;
                    }

                    // Chống trùng
                    if (isDuplicateSms(sender, body, sms)) {
                        log.debug("⏭️ [{}] Skip duplicate: {} - {} (storage={}, index={}, timestamp={})",
                                com, sender, body, sms.storage, sms.index, sms.timestamp);
                        safeDeleteIndex(sms.index, sms.storage);
                        skippedCount++;
                        continue;
                    }

                    // Detect language
                    Language lang = null;
                    try {
                        lang = LanguageUtil.detect(body);
                    } catch (Exception e) {
                        log.debug("⚠️ [{}] Language detection failed: {}", com, e.getMessage());
                    }

                    log.debug("📩 [{}] SMS From: {} | Body: {} | Lang: {}", com, sender, body, lang);

                    updateLastSmsTime();

                    // Callback xử lý SMS
                    try {
                        listenerService.processSms(sim, sender, body);
                        processedCount++;
                    } catch (Exception cbEx) {
                        log.error("❌ [{}] processSms error: {}", com, cbEx.getMessage(), cbEx);
                        errorCount++;
                    }

                    // Đánh dấu đã xử lý + xóa trên modem
                    markSmsAsProcessed(sender, body, sms);
                    safeDeleteIndex(sms.index, sms.storage);

                } catch (Exception e) {
                    log.error("❌ [{}] Error processing SMS index {}: {}", com, sms.index, e.getMessage());
                    errorCount++;
                    // Vẫn cố xóa SMS để tránh bị stuck
                    safeDeleteIndex(sms.index, sms.storage);
                }
            }

            log.debug("📊 [{}] SMS scan complete: processed={}, skipped={}, errors={}",
                    com, processedCount, skippedCount, errorCount);

            // ✅ Reset urgent flag sau khi hoàn thành scan
            urgentTaskPending = false;

            // Cleanup cache cũ
            cleanupOldProcessedSms();

        } catch (AtCommandHelper.PortDisconnectedException e) {
            // ✅ FIX: Handle port disconnect specifically - trigger reconnection
            log.warn("🔌 [{}] Port disconnected during SMS scan - closing port for reconnection: {}", com,
                    e.getMessage());
            closePort(); // Force close so ensurePort() will reopen on next cycle

        } catch (com.fazecast.jSerialComm.SerialPortIOException e) {
            // ✅ FIX: Handle SerialPortIOException (not wrapped) - same treatment
            log.warn("🔌 [{}] Serial port I/O error during SMS scan - closing port for reconnection: {}", com,
                    e.getMessage());
            closePort();

        } catch (Exception e) {
            // ✅ Check if this is a port-related exception even if not specifically typed
            String errMsg = e.getMessage() != null ? e.getMessage() : "";
            if (errMsg.contains("shutdown") || errMsg.contains("disconnected") ||
                    errMsg.contains("No bytes written") || errMsg.contains("SerialPort")) {
                log.warn("🔌 [{}] Port error detected during SMS scan - closing port for reconnection: {}", com,
                        e.getMessage());
                closePort();
            } else {
                log.error("❌ [{}] Lỗi khi quét SMS: {}", com, e.getMessage(), e);
            }
        }
    }

    private void safeDeleteIndex(Integer index, String storage) {
        if (index == null)
            return;
        try {
            if (storage != null && !storage.isBlank()) {
                helper.sendAndRead("AT+CPMS=\"" + storage + "\",\"" + storage + "\",\"" + storage + "\"",
                        (int) AT_TIMEOUT_MEDIUM.toMillis());
            }
            helper.sendAndRead("AT+CMGD=" + index, (int) AT_TIMEOUT_MEDIUM.toMillis());
        } catch (Exception ignored) {
        }
    }

    // ======================================================================
    // PORT / MODEM
    // ======================================================================
    private boolean ensurePort() {
        if (port != null && port.isOpen()) {
            // Reset fail count if port is open and working
            if (connectionFailCount > 0) {
                connectionFailCount = 0;
                listenerService.onConnectionRestored(sim);
            }
            return true;
        }

        // ✅ IMPROVEMENT: Exponential backoff for reconnection
        // Avoid rapid reconnection attempts when port is repeatedly failing
        if (lastPortDisconnectTime != null && consecutiveDisconnects > 0) {
            long backoffMs = Math.min(
                    MIN_RECONNECT_DELAY.toMillis() * (1L << Math.min(consecutiveDisconnects - 1, 4)), // 2s, 4s, 8s,
                                                                                                      // 16s, 32s
                    MAX_RECONNECT_DELAY.toMillis());
            long elapsedMs = Duration.between(lastPortDisconnectTime, Instant.now()).toMillis();

            if (elapsedMs < backoffMs) {
                // Not enough time has passed, skip this reconnection attempt
                if (consecutiveDisconnects <= 3 || consecutiveDisconnects % 20 == 0) {
                    log.debug("⏳ [{}] Waiting for reconnect backoff ({}/{}ms, attempt #{})",
                            sim.getComName(), elapsedMs, backoffMs, consecutiveDisconnects);
                }
                return false;
            }
        }

        try {
            port = SerialPort.getCommPort(sim.getComName());
            port.setBaudRate(BAUD_RATE);
            port.setComPortTimeouts(SerialPort.TIMEOUT_READ_BLOCKING, 1000, 1000);

            if (!port.openPort()) {
                // ✅ FIX: Chỉ log warn ở lần đầu hoặc sau mỗi 20 lần fail để tránh spam
                if (consecutiveDisconnects == 0 || consecutiveDisconnects % 20 == 0) {
                    log.warn("⚠️ Cannot open {} (attempt #{})", sim.getComName(), consecutiveDisconnects + 1);
                } else {
                    log.debug("⚠️ Cannot open {} (attempt #{})", sim.getComName(), consecutiveDisconnects + 1);
                }
                handleConnectionFailure();
                return false;
            }

            helper = new AtCommandHelper(port);
            // Base init
            helper.sendAndRead("AT", (int) AT_TIMEOUT_MEDIUM.toMillis());
            helper.sendAndRead("ATE0", (int) AT_TIMEOUT_MEDIUM.toMillis());
            helper.sendAndRead("AT+CMEE=2", (int) AT_TIMEOUT_MEDIUM.toMillis());

            // SMS/OTP receive mode
            applySmsUrcMode(true);

            log.info("✅ [{}] Port opened successfully (after {} disconnects)",
                    sim.getComName(), consecutiveDisconnects);
            updateLastSmsTime();

            // ✅ Reset disconnect tracking on successful connection
            consecutiveDisconnects = 0;
            lastPortDisconnectTime = null;

            // Connection restored logic
            if (connectionFailCount > 0) {
                connectionFailCount = 0;
                listenerService.onConnectionRestored(sim);
            }

            return true;

        } catch (Exception e) {
            log.error("❌ Failed to open {}: {}", sim.getComName(), e.getMessage());
            closePort();
            handleConnectionFailure();
            return false;
        }
    }

    private void handleConnectionFailure() {
        connectionFailCount++;

        // ✅ FIX: Chỉ đánh dấu INACTIVE nếu fail liên tục
        // Không gọi ngay lập tức, đợi đủ MAX_CONNECTION_FAILURES lần
        if (connectionFailCount == MAX_CONNECTION_FAILURES) {
            log.warn("🔴 [{}] Không thể kết nối sau {} lần thử - đánh dấu INACTIVE",
                    sim.getComName(), MAX_CONNECTION_FAILURES);
            listenerService.onConnectionLost(sim);
        } else if (connectionFailCount % 5 == 0 && connectionFailCount < MAX_CONNECTION_FAILURES) {
            // Log tiến trình mỗi 5 lần fail
            log.debug("⏳ [{}] Đang thử kết nối lại ({}/{} fails trước khi INACTIVE)",
                    sim.getComName(), connectionFailCount, MAX_CONNECTION_FAILURES);
        }
    }

    private void closePort() {
        // ✅ Track disconnect for rate-limited reconnection
        if (port != null && port.isOpen()) {
            lastPortDisconnectTime = Instant.now();
            consecutiveDisconnects++;

            if (consecutiveDisconnects <= 3 || consecutiveDisconnects % 10 == 0) {
                log.info("🔌 [{}] Closing port (disconnect count: {})",
                        sim.getComName(), consecutiveDisconnects);
            }
        }

        try {
            if (helper != null)
                helper.close();
        } catch (Exception ignored) {
        }
        try {
            if (port != null && port.isOpen())
                port.closePort();
        } catch (Exception ignored) {
        }
        helper = null;
        port = null;
    }

    // ======================================================================
    // RESET MODEM
    // ======================================================================
    private void resetModem() {
        try {
            log.info("🔄 [{}] Resetting modem...", sim.getComName());
            helper.sendAndRead("AT+CFUN=1,1", 5000); // Reset modem
            Thread.sleep(3000); // Chờ modem khởi động lại

            // Khởi tạo lại cấu hình cơ bản
            helper.sendAndRead("AT", 1000);
            helper.sendAndRead("ATE0", 1000);
            helper.sendAndRead("AT+CMEE=2", 1000);

            log.info("✅ [{}] Modem reset completed", sim.getComName());
        } catch (Exception e) {
            log.error("❌ [{}] Reset modem failed: {}", sim.getComName(), e.getMessage());
        }
    }

    private void resetModemAsync() {
        scheduler.schedule(() -> {
            try {
                resetModem();
            } catch (Exception e) {
                log.error("❌ [{}] Async reset failed: {}", sim.getComName(), e.getMessage());
            }
        }, 2, TimeUnit.SECONDS);
    }

    /**
     * ✅ Parse exception message to extract specific error code
     */
    private String parseCallFailReason(String errorMessage) {
        if (errorMessage == null) {
            return "UNKNOWN_ERROR";
        }

        String upper = errorMessage.toUpperCase();

        // Network/carrier errors
        if (upper.contains("NO CARRIER"))
            return "NO_CARRIER";
        if (upper.contains("NO ANSWER"))
            return "NO_ANSWER";
        if (upper.contains("BUSY"))
            return "BUSY";
        if (upper.contains("NO DIALTONE"))
            return "NO_DIALTONE";

        // Modem errors
        if (upper.contains("CME ERROR")) {
            if (upper.contains("CME ERROR: 3"))
                return "OPERATION_NOT_ALLOWED";
            if (upper.contains("CME ERROR: 10"))
                return "SIM_NOT_INSERTED";
            if (upper.contains("CME ERROR: 13"))
                return "SIM_FAILURE";
            if (upper.contains("CME ERROR: 14"))
                return "SIM_BUSY";
            if (upper.contains("CME ERROR: 30"))
                return "NO_NETWORK";
            return "CME_ERROR";
        }

        if (upper.contains("CMS ERROR"))
            return "CMS_ERROR";
        if (upper.contains("TIMEOUT"))
            return "TIMEOUT";
        if (upper.contains("PORT_NOT_OPEN"))
            return "PORT_NOT_OPEN";

        // Generic
        return "SYSTEM_ERROR";
    }

    private void ensureUcs2Mode() throws Exception {
        helper.sendAndRead("AT+CMGF=1", (int) AT_TIMEOUT_MEDIUM.toMillis());
        helper.sendAndRead("AT+CSCS=\"UCS2\"", (int) AT_TIMEOUT_MEDIUM.toMillis());
        helper.sendAndRead("AT+CSMP=17,167,0,8", (int) AT_TIMEOUT_MEDIUM.toMillis());
    }

    private void prepareModemForReadSms() throws Exception {
        helper.sendAndRead("AT+CMGF=1", (int) AT_TIMEOUT_MEDIUM.toMillis());
        helper.sendAndRead("AT+CSCS=\"UCS2\"", (int) AT_TIMEOUT_MEDIUM.toMillis());
        helper.sendAndRead("AT+CSMP=17,167,0,8", (int) AT_TIMEOUT_MEDIUM.toMillis());
    }

    private boolean isOtpOnlyMode() {
        return gsmProperties == null || gsmProperties.isOtpOnly();
    }

    private boolean shouldDisableDataForOtp() {
        return isOtpOnlyMode() && (gsmProperties == null || gsmProperties.isDisableDataOnStartup());
    }

    private void applySmsUrcMode(boolean reconnect) throws Exception {
        ensureUcs2Mode();

        if (shouldDisableDataForOtp()) {
            sendAtQuietly("AT+QNETDEVCTL=0,1", 1000, "disable modem data");
            sendAtQuietly("AT+CGACT=0,1", 1500, "deactivate PDP");
        }

        helper.sendAndRead("AT+CPMS=\"SM\",\"SM\",\"SM\"", (int) AT_TIMEOUT_MEDIUM.toMillis());

        if (reconnect) {
            sendAtQuietly("AT+QCFG=\"urc/cache\",1", 800, "enable URC cache");
            sendAtQuietly("AT+QINDCFG=\"smsincoming\",1,1", 800, "enable incoming SMS URC");
        }

        String cnmi = helper.sendAndRead("AT+CNMI=2,1,0,0,0", (int) AT_TIMEOUT_MEDIUM.toMillis());
        lastCnmiApply = Instant.now();
        urcSupported = cnmi != null && cnmi.contains("OK");
    }

    private void sendAtQuietly(String command, int timeoutMs, String label) {
        try {
            String response = helper.sendAndRead(command, timeoutMs);
            log.debug("🔧 [{}] {} -> {}", sim.getComName(), label,
                    response != null ? response.replace("\r", " ").replace("\n", " ").trim() : "null");
        } catch (Exception e) {
            log.debug("⚠️ [{}] {} ignored: {}", sim.getComName(), label, e.getMessage());
        }
    }

    /**
     * ✅ IMPROVEMENT #3: Check storage capacity and auto-cleanup if near full
     * Prevents "storage full" errors that block new SMS reception
     * 
     * @param store        Storage name (ME/SM)
     * @param cpmsResponse Response from AT+CPMS command (contains used/total)
     */
    private void checkStorageCapacity(String store, String cpmsResponse) {
        try {
            // Parse AT+CPMS response: +CPMS: "ME",used,total,"ME",used,total,"ME",used,total
            // We only care about first pair (read storage)
            if (cpmsResponse != null && cpmsResponse.contains("+CPMS:")) {
                // ✅ FIX: Use regex to extract used,total after storage name
                // Old code split by "," which broke on quoted storage names like "ME"
                java.util.regex.Matcher m = Pattern.compile(
                        "\\+CPMS:\\s*\"[^\"]*\"\\s*,\\s*(\\d+)\\s*,\\s*(\\d+)").matcher(cpmsResponse);
                if (m.find()) {
                    int used = Integer.parseInt(m.group(1));
                    int total = Integer.parseInt(m.group(2));
                    int percentUsed = (used * 100) / Math.max(1, total);

                    if (percentUsed >= 90) {
                        log.warn("🔴 [{}] Storage {} is {}% full ({}/{}), cleaning up...",
                                sim.getComName(), store, percentUsed, used, total);
                        helper.sendAndRead("AT+CMGD=1,1", 3000); // Delete only READ messages (not SENT/UNSENT)
                        log.info("✅ [{}] Emergency cleanup completed for {}", sim.getComName(), store);
                    } else if (percentUsed >= 75) {
                        log.warn("⚠️ [{}] Storage {} is {}% full ({}/{})",
                                sim.getComName(), store, percentUsed, used, total);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Storage capacity check failed for {}: {}", store, e.getMessage());
        }
    }

    private void safeSleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }

    private class CallSession implements AutoCloseable {
        private final Task task;
        private final String com;
        private final AtomicBoolean finished = new AtomicBoolean(false);

        private final Instant callStart = Instant.now();
        private final AtomicReference<Boolean> connected = new AtomicReference<>(false);
        private final AtomicReference<Instant> connectedAt = new AtomicReference<>(null);
        private final AtomicBoolean recordStarted = new AtomicBoolean(false);

        private final AtomicReference<String> uploadedUrl = new AtomicReference<>(null);
        private final String recordBaseName;
        private volatile String modemRecordPath;
        private volatile String localRecordFilename;
        private RecordingProfile activeProfile;

        private ScheduledFuture<?> clccMonitorFuture;
        private ScheduledFuture<?> stopFuture;

        private int activeConfirmCount = 0;

        CallSession(Task task) {
            this.task = task;
            this.com = sim.getComName();
            this.recordBaseName = "call_" + System.currentTimeMillis();
        }

        private void waitForNetworkReady() throws Exception {
            final int MAX_ATTEMPTS = 10; // Giảm từ 20 xuống 10
            final int QUICK_CHECK_COUNT = 3; // Chỉ cần 3 lần check liên tiếp OK
            int consecutiveSuccess = 0;

            log.info("📡 [{}] Checking network registration...", com);

            // Flush buffer trước
            try {
                helper.flush();
                Thread.sleep(200);
            } catch (Exception ignore) {
            }

            for (int i = 0; i < MAX_ATTEMPTS; i++) {
                try {
                    // Send AT để "đánh thức" modem
                    helper.sendAndRead("AT", 500);
                    Thread.sleep(100);

                    // Query CREG với timeout dài hơn
                    String creg = helper.sendAndRead("AT+CREG?", 1500);

                    // Nếu response rỗng hoặc chỉ có "OK", retry
                    if (creg == null || creg.trim().isEmpty() || creg.trim().equals("OK")) {
                        log.warn("⚠️ [{}] CREG response empty, flushing buffer...", com);
                        helper.flush();
                        Thread.sleep(500);
                        continue;
                    }

                    Thread.sleep(100);

                    // Query COPS
                    String cops = helper.sendAndRead("AT+COPS?", 1500);

                    if (cops == null || cops.trim().isEmpty() || cops.trim().equals("OK")) {
                        log.warn("⚠️ [{}] COPS response empty, flushing buffer...", com);
                        helper.flush();
                        Thread.sleep(500);
                        continue;
                    }

                    // Parse CREG status
                    boolean registered = creg.contains(",1") || creg.contains(",5");

                    // Parse COPS - kiểm tra có operator name
                    boolean operatorOk = cops.contains(",\"") && !cops.contains("0,0");

                    if (registered && operatorOk) {
                        consecutiveSuccess++;
                        log.info("✅ [{}] Network OK ({}/{}) - CREG: {} | COPS: {}",
                                com, consecutiveSuccess, QUICK_CHECK_COUNT,
                                creg.replace("\r", "").replace("\n", " ").trim(),
                                cops.replace("\r", "").replace("\n", " ").trim());

                        // Chỉ cần 3 lần check liên tiếp thành công
                        if (consecutiveSuccess >= QUICK_CHECK_COUNT) {
                            log.info("🎯 [{}] Network FULLY READY!", com);
                            return;
                        }

                        Thread.sleep(500); // Đợi ngắn giữa các lần check thành công

                    } else {
                        consecutiveSuccess = 0; // Reset counter nếu fail

                        log.info("⏳ [{}] Waiting network ({}/{})... CREG={} | COPS={}",
                                com, i + 1, MAX_ATTEMPTS,
                                creg.replace("\r", "").replace("\n", " ").trim(),
                                cops.replace("\r", "").replace("\n", " ").trim());

                        Thread.sleep(1000);
                    }

                } catch (Exception e) {
                    log.warn("⚠️ [{}] Network check error (attempt {}): {}", com, i + 1, e.getMessage());

                    // Flush buffer và retry
                    try {
                        helper.flush();
                    } catch (Exception ignore) {
                    }

                    Thread.sleep(800);
                }
            }

            // Sau MAX_ATTEMPTS vẫn chưa OK
            log.warn("⚠️ [{}] Network not fully ready after {} attempts, proceeding anyway...",
                    com, MAX_ATTEMPTS);
        }

        void prepareModem() throws Exception {
            log.info("📞 [{}] Preparing modem for voice call...", com);

            try {
                // ✅ MATCHED WITH CallService.directCall() - EXACTLY THE SAME
                helper.sendAndRead("AT", 300);
                helper.sendAndRead("ATE0", 300);
                helper.sendAndRead("AT+CLIP=1", 300);
                helper.sendAndRead("AT+CRC=1", 300);

                // ✅ REMOVED: All complex audio settings, network wait, QSCLK, etc.
                // CallService.directCall() only uses these 4 AT commands and it WORKS
                // Keep it simple and proven

                log.info("✅ [{}] Modem ready for voice call (using CallService-proven AT sequence)", com);

            } catch (Exception e) {
                log.error("❌ [{}] prepareModem failed: {}", com, e.getMessage());
                throw e;
            }
        }

        void dial() throws Exception {
            log.info("🔵 [{}] dial() STARTED (max {} attempts)", com, MAX_DIAL_ATTEMPTS);

            // ✅ FIX: ĐỒNG BỘ với CallService.directCall()
            // KHÔNG gọi waitForNetworkReady() - dial trực tiếp như API
            // API gọi được → Nghĩa là network check không cần thiết và có thể gây lỗi

            RuntimeException lastError = null;

            for (int attempt = 1; attempt <= MAX_DIAL_ATTEMPTS; attempt++) {
                log.info("🔵 [{}] Dial attempt {}/{}", com, attempt, MAX_DIAL_ATTEMPTS);

                // ❌ REMOVED: Network check before dial
                // if (!isNetworkRegistered()) {
                // waitForNetworkReady(); // ← GÂY LỖI!
                // }

                try {
                    performDialOnce();
                    log.info("✅ [{}] dial() COMPLETED SUCCESSFULLY (attempt {})", com, attempt);
                    return;
                } catch (RuntimeException ex) {
                    lastError = ex;
                    log.warn("⚠️ [{}] Dial attempt {} failed: {}", com, attempt, ex.getMessage());

                    if (!shouldRetryDial(ex) || attempt == MAX_DIAL_ATTEMPTS) {
                        log.error("❌ [{}] dial() FAILED - No more retries", com);
                        throw ex;
                    }

                    log.warn("🔁 [{}] Dial attempt {} failed ({}). Retrying in {} ms",
                            com, attempt, ex.getMessage(), DIAL_RETRY_DELAY.toMillis());
                    Thread.sleep(DIAL_RETRY_DELAY.toMillis());
                }
            }

            if (lastError != null) {
                log.error("❌ [{}] dial() FAILED after {} attempts", com, MAX_DIAL_ATTEMPTS);
                throw lastError;
            }

            log.info("✅ [{}] dial() COMPLETED (no error)", com);
        }

        private void performDialOnce() throws Exception {
            log.info("🔵 [{}] performDialOnce() STARTED", com);

            try {
                log.info("🔵 [{}] Flushing buffer...", com);
                helper.flush();
                log.info("✅ [{}] Buffer flushed", com);
            } catch (Exception e) {
                log.warn("⚠️ [{}] Flush failed: {}", com, e.getMessage());
            }

            String cmd = "ATD" + task.to + ";";
            log.info("☎ [{}] DIAL CMD → {}", com, cmd);

            try {
                log.info("🔵 [{}] Sending ATD command...", com);
                String dialResp = helper.sendAndRead(cmd, 4000);
                log.info("☎ [{}] DIAL RESP → {}", com,
                        dialResp.replace("\r", " ").replace("\n", " "));

                // ✅ FIX: Add logging to track where it blocks
                log.info("⏳ [{}] Waiting 1.5s for modem to process dial...", com);

                try {
                    Thread.sleep(1500);
                    log.info("✅ [{}] Wait complete", com);
                } catch (InterruptedException ie) {
                    log.error("❌ [{}] Thread.sleep() INTERRUPTED!", com, ie);
                    Thread.currentThread().interrupt();
                    throw ie;
                }

                log.info("🔵 [{}] Reading extra buffer...", com);
                String buf = null;
                try {
                    buf = helper.readAll();
                    log.info("✅ [{}] Buffer read complete (size: {} bytes)", com,
                            buf != null ? buf.length() : 0);
                } catch (Exception e) {
                    log.error("❌ [{}] readAll() FAILED: {}", com, e.getMessage(), e);
                    throw e;
                }

                if (buf != null && !buf.isEmpty()) {
                    log.info("☎ [{}] EXTRA DIAL BUFFER → {}", com,
                            buf.replace("\r", " ").replace("\n", " "));
                    dialResp += "\n" + buf;
                } else {
                    log.info("ℹ️ [{}] No extra buffer after dial", com);
                }

                log.info("🔵 [{}] Checking for dial errors...", com);

                String errorReason = null;
                String merged = dialResp;

                if (merged.contains("NO CARRIER")) {
                    errorReason = "NO_CARRIER - Network unavailable or number unreachable";
                } else if (merged.contains("BUSY")) {
                    errorReason = "BUSY - Line is busy";
                } else if (merged.contains("NO ANSWER")) {
                    errorReason = "NO_ANSWER - Call not answered";
                } else if (merged.contains("NO DIALTONE")) {
                    errorReason = "NO_DIALTONE - No dial tone detected";
                } else if (merged.contains("ERROR")) {
                    errorReason = "ERROR - General dial error";
                } else if (merged.contains("CME ERROR")) {
                    errorReason = "CME_ERROR - " + merged.replace("\n", " ").trim();
                }

                if (errorReason != null) {
                    log.error("❌ [{}] Dial failed immediately: {}", com, errorReason);
                    throw new RuntimeException("Dial failed: " + errorReason);
                }

                if (!merged.contains("OK") && !merged.isEmpty()) {
                    log.warn("⚠️ [{}] Unexpected dial response (proceeding anyway): {}", com, merged);
                }

                log.info("✅ [{}] performDialOnce() COMPLETED SUCCESSFULLY", com);

            } catch (Exception e) {
                log.error("❌ [{}] performDialOnce() FAILED with exception: {} - {}",
                        com, e.getClass().getSimpleName(), e.getMessage(), e);
                throw e;
            }
        }

        void monitorCallState() {
            log.debug("🔵 [{}] monitorCallState() - scheduling CLCC polling every 1s", com);

            // ✅ FIX: Check if scheduler is alive
            if (callScheduler.isShutdown() || callScheduler.isTerminated()) {
                log.error("❌ [{}] callScheduler is SHUTDOWN/TERMINATED! Cannot schedule CLCC polling!", com);
                log.error("❌ [{}] This is a CRITICAL BUG - call monitoring will NOT work!", com);
                return;
            }

            log.info("✅ [{}] callScheduler is ALIVE (shutdown={}, terminated={})",
                    com, callScheduler.isShutdown(), callScheduler.isTerminated());

            try {
                clccMonitorFuture = callScheduler.scheduleAtFixedRate(() -> {
                    // ✅ FIX: Log FIRST THING to prove task is running
                    log.debug("🔵 [{}] CLCC polling tick (thread={}, finished={})",
                            com, Thread.currentThread().getName(), finished.get());

                    if (finished.get()) {
                        log.debug("ℹ️ [{}] CLCC polling stopped (call finished)", com);
                        return;
                    }

                    try {
                        log.debug("🔵 [{}] Sending AT+CLCC...", com);
                        String st = helper.sendAndRead("AT+CLCC", 800);

                        if (st == null) {
                            log.warn("⚠️ [{}] CLCC returned NULL - modem not responding", com);
                            return;
                        }

                        if (!st.contains("+CLCC")) {
                            // ✅ FIX: Use INFO not DEBUG so we can see it
                            log.debug("ℹ️ [{}] CLCC → No active call (response: {})", com,
                                    st.replace("\r", " ").replace("\n", " ").trim());
                            return;
                        }

                        log.debug("📡 [{}] CLCC RAW → {}", com, st.replace("\r", " ").replace("\n", " "));

                        Pattern p = Pattern.compile("\\+CLCC:.*?,.*?,(\\d+),");
                        Matcher m = p.matcher(st);

                        while (m.find()) {
                            int stat = Integer.parseInt(m.group(1));
                            log.debug("🔵 [{}] CLCC stat={}", com, stat);

                            switch (stat) {
                                case 3: // DIALING
                                    if (!connected.get()) {
                                        log.debug("📞 [{}] CLCC stat=3 (DIALING)", com);
                                        notifyCallStatus("DIALING", task);
                                    }
                                    break;

                                case 2: // ALERTING (RINGING)
                                    if (!connected.get()) {
                                        log.debug("📞 [{}] CLCC stat=2 (RINGING)", com);
                                        notifyCallStatus("RINGING", task);
                                    }
                                    break;

                                case 0: // ACTIVE (đang đàm thoại)
                                    activeConfirmCount++;
                                    log.debug("🔵 [{}] CLCC stat=0 (ACTIVE) - count={}", com, activeConfirmCount);

                                    if (!connected.get() && activeConfirmCount >= 2) {
                                        connected.set(true);
                                        // ✅ Set initial connectedAt for status tracking
                                        Instant initialConnectedAt = Instant.now();
                                        connectedAt.set(initialConnectedAt);
                                        notifyCallStatus("IN_CALL", task);
                                        log.info("📞 [{}] ✅ CALL CONNECTED (CLCC stat=0, confirm={})", com,
                                                activeConfirmCount);

                                        // ✅ FIX: KHÔNG gửi callback ở đây - chỉ gửi ở endCall() với full info
                                        // (recording URL + duration)
                                        // Xoá phần gửi SUCCESS callback immediately

                                        // ✅ CRITICAL FIX: Start recording FIRST (before timer)
                                        // This ensures full duration is available for recording
                                        if (task.record && recordStarted.compareAndSet(false, true)) {
                                            long recordingStartTime = System.currentTimeMillis();
                                            log.info("🎙️ [{}] Starting recording initialization...", com);
                                            startRecording();
                                            long recordingInitTime = System.currentTimeMillis() - recordingStartTime;
                                            log.info("✅ [{}] Recording initialized in {}ms", com, recordingInitTime);

                                            // ✅ CRITICAL: Update connectedAt to AFTER recording is ready
                                            // This ensures the full configured duration is used for recording
                                            connectedAt.set(Instant.now());
                                            log.info(
                                                    "⏱️ [{}] Timer start point updated to after recording init (delay: {}ms)",
                                                    com, recordingInitTime);
                                        }

                                        // ✅ FIX: Start auto-hangup timer AFTER recording is ready
                                        // This prevents losing recording time due to initialization delay
                                        scheduleStopByDuration();
                                    }
                                    break;

                                case 6: // DISCONNECTED
                                    log.debug("📞 [{}] CLCC stat=6 (DISCONNECTED) - ending call", com);
                                    // kết thúc cuộc gọi
                                    try {
                                        endCall();
                                    } catch (Exception ignore) {
                                    }
                                    break;

                                default:
                                    log.debug("ℹ️ [{}] Unknown CLCC stat={}", com, stat);
                            }
                        }

                    } catch (Exception ex) {
                        log.error("❌ [{}] CLCC polling exception: {} - {}",
                                com, ex.getClass().getSimpleName(), ex.getMessage());
                        // Don't terminate polling on exception, just log and continue
                    }

                }, 0, 1, TimeUnit.SECONDS);

                log.debug("✅ [{}] CLCC polling scheduled successfully (interval: 1s)", com);
                log.debug("✅ [{}] clccMonitorFuture = {}", com, clccMonitorFuture != null ? "NOT NULL" : "NULL");

            } catch (Exception e) {
                log.error("❌ [{}] FAILED to schedule CLCC polling: {} - {}",
                        com, e.getClass().getSimpleName(), e.getMessage(), e);
            }

            // ✅ CRITICAL FIX: BLOCK HERE until call finishes!
            // Nếu return ngay → try-with-resources close() → cancel clccMonitorFuture!
            log.info("🔵 [{}] Waiting for call to finish (blocking)...", com);
            try {
                while (!finished.get()) {
                    Thread.sleep(500); // Check every 500ms
                }
                log.info("✅ [{}] Call finished, exiting monitorCallState()", com);
            } catch (InterruptedException e) {
                log.error("❌ [{}] monitorCallState() interrupted!", com, e);
                Thread.currentThread().interrupt();
            }
        }

        private void startRecording() {
            try {
                boolean started = false;

                for (RecordingProfile profile : RECORDING_PROFILES) {
                    String candidateLocalFile = recordBaseName + profile.extension;
                    // ✅ Don't add UFS: prefix in command - modem adds it automatically
                    String modemPath = candidateLocalFile;

                    deleteModemFileSilently(MODEM_STORAGE_PREFIX + candidateLocalFile);

                    // ✅ FIX: According to Quectel_GSM_Recording_AT_Commands_Manual_V3.1.txt:
                    // AT+QAUDRD=<control>,"filename"[,<format>]
                    // GSM modules (M10, M66, M85, M80, M72, M95) do NOT support <channel>
                    // parameter!
                    // Only EC25/EC21 modules support 4 parameters with channel.
                    String recordCmd = String.format("AT+QAUDRD=1,\"%s\",%d", modemPath, profile.format);
                    log.info("🎙️ [{}] Trying {} codec → {}", com, profile.label, recordCmd);

                    try {
                        String resp = helper.sendAndRead(recordCmd, 2000);
                        log.info("📝 [{}] Response: {}", com, resp.replace("\r", "\\r").replace("\n", "\\n").trim());

                        if (resp.contains("OK") && !resp.contains("ERROR")) {
                            // ✅ Store with UFS: prefix for later file operations
                            modemRecordPath = MODEM_STORAGE_PREFIX + modemPath;
                            localRecordFilename = candidateLocalFile;
                            started = true;
                            log.info("✅ [{}] Recording STARTED ({}) → {}", com, profile.label, modemRecordPath);
                            break;
                        } else {
                            log.warn("⚠️ [{}] {} failed: {}", com, profile.label,
                                    resp.replace("\r", " ").replace("\n", " ").trim());
                        }
                    } catch (Exception e) {
                        log.warn("⚠️ [{}] {} exception: {}", com, profile.label, e.getMessage());
                    }
                }

                if (!started) {
                    log.error("❌ [{}] ALL recording formats FAILED!", com);
                    modemRecordPath = null;
                    localRecordFilename = null;
                }

            } catch (Exception ex) {
                log.error("❌ [{}] Cannot start recording: {}", com, ex.getMessage(), ex);
                modemRecordPath = null;
                localRecordFilename = null;
            }
        }

        private void deleteModemFileSilently(String modemPath) {
            if (modemPath == null)
                return;
            try {
                String normalized = normalizeModemPath(modemPath);
                helper.sendAndRead("AT+QFDEL=\"" + normalized + "\"", 500);
            } catch (Exception ignore) {
            }
        }

        void scheduleStopByDuration() {
            // ✅ FIX: Calculate EXACT remaining time from connection moment
            int durationSec = (task.duration > 0 ? task.duration : 30);

            Instant connectedMoment = connectedAt.get();
            if (connectedMoment == null) {
                log.warn("⚠️ [{}] connectedAt is null when scheduling hangup!", com);
                // Fallback to full duration
                stopFuture = callScheduler.schedule(this::endCall, durationSec, TimeUnit.SECONDS);
                return;
            }

            // Calculate elapsed time since connection
            long connectedAtMillis = connectedMoment.toEpochMilli();
            long elapsedSinceConnect = System.currentTimeMillis() - connectedAtMillis;
            long remainingMs = (durationSec * 1000L) - elapsedSinceConnect;

            if (remainingMs < 1000) {
                remainingMs = 1000; // Minimum 1 second
            }

            // Schedule with EXACT remaining time in milliseconds
            stopFuture = callScheduler.schedule(this::endCall, remainingMs, TimeUnit.MILLISECONDS);

            log.info("⏰ [{}] Scheduling hangup in {}ms (configured={}s, elapsed={}ms)",
                    com, remainingMs, durationSec, elapsedSinceConnect);
        }

        private void endCall() {
            if (!finished.compareAndSet(false, true))
                return;

            Instant end = Instant.now();
            long duration = 0;

            if (Boolean.TRUE.equals(connected.get()) && connectedAt.get() != null) {
                long actualDuration = Duration.between(connectedAt.get(), end).getSeconds();
                // ✅ FIX: Cap duration at configured value (in case of delays)
                int maxDuration = (task.duration > 0 ? task.duration : 30);
                duration = Math.min(actualDuration, maxDuration);
                log.info("⏱️ [{}] Duration calc: actual={}s, configured={}s, reported={}s",
                        com, actualDuration, maxDuration, duration);
            }

            try {
                log.info("🛑 [{}] Sending ATH to hangup...", com);
                helper.sendAndRead("ATH", 500);
                log.info("✅ [{}] ATH sent", com);
            } catch (Exception e) {
                log.warn("⚠️ [{}] ATH failed: {}", com, e.getMessage());
            }

            // ✅ STEP 1: Xử lý ghi âm - STOP + DOWNLOAD
            File wav = null;
            if (task.record && recordStarted.get()) {
                log.info("🎙️ [{}] STEP 1/3: Stopping recording...", com);
                stopRecording();
                log.info("✅ [{}] Recording stopped, waiting for modem to finalize file...", com);

                // ✅ CRITICAL: Cancel CLCC monitoring to free up serial port for download
                try {
                    if (clccMonitorFuture != null && !clccMonitorFuture.isCancelled()) {
                        clccMonitorFuture.cancel(true);
                        Thread.sleep(100); // Give it time to stop
                        log.debug("🛑 [{}] CLCC monitoring paused for download", com);
                    }
                } catch (Exception e) {
                    log.warn("⚠️ [{}] Failed to cancel CLCC monitor: {}", com, e.getMessage());
                }

                log.info("📥 [{}] STEP 2/3: Accessing local recording file...", com);
                wav = downloadModemRecord();

                if (wav != null && wav.exists()) {
                    log.info("✅ [{}] Recording file ready: {} ({} bytes)",
                            com, wav.getName(), wav.length());
                } else {
                    log.error("❌ [{}] Failed to access recording file!", com);
                }
            }

            // ✅ STEP 2: Upload file lên server
            String url = null;
            if (wav != null && wav.exists() && wav.length() > 1024) {
                log.info("☁️ [{}] STEP 3/3: Uploading recording to server...", com);
                url = uploadRecordFile(wav.getAbsolutePath(), task.orderId, task.serviceCode);

                if (url != null) {
                    log.info("✅ [{}] Upload SUCCESS → URL: {}", com, url);
                } else {
                    log.error("❌ [{}] Upload FAILED - callback will have recordFile=null!", com);
                }
            } else if (wav != null) {
                log.warn("⚠️ [{}] Recording file too small: {} bytes (min: 1024) - skipping upload",
                        com, wav.length());
            } else if (task.record) {
                log.warn("⚠️ [{}] Recording was enabled but no file was saved", com);
            }

            uploadedUrl.set(url);

            // ✅ STEP 3: Gửi callback SAU KHI hoàn tất upload
            log.info("📡 [{}] Sending callback (AFTER recording processing)...", com);
            sendCallCallback(
                    com,
                    callStart,
                    end,
                    task,
                    url, // ✅ Recording URL (null nếu upload failed)
                    duration, // ✅ Actual duration
                    Boolean.TRUE.equals(connected.get()), // ✅ Connected status
                    null);

            log.info("📊 [{}] ✅ Callback sent | connected={} | duration={}s | recording={}",
                    com, connected.get(), duration, url != null ? url : "NONE");

            // Xác định trạng thái cuối cùng
            String finalWsStatus;

            if (Boolean.TRUE.equals(connected.get())) {
                finalWsStatus = "ENDED_SUCCESS";
            } else {
                finalWsStatus = "ENDED_NO_ANSWER";
            }

            notifyCallStatus(finalWsStatus, task);

            // Cleanup scheduler
            try {
                if (clccMonitorFuture != null)
                    clccMonitorFuture.cancel(true);
            } catch (Exception ignored) {
            }
            try {
                if (stopFuture != null)
                    stopFuture.cancel(true);
            } catch (Exception ignored) {
            }
        }

        private void stopRecording() {
            try {
                log.info("🛑 [{}] Stopping recording...", com);

                if (modemRecordPath == null) {
                    log.info("ℹ️ [{}] Record flag set but no modem file path was captured", com);
                }

                // Check status trước khi stop
                try {
                    String status = helper.sendAndRead("AT+QAUDRD?", 500);
                    log.info("📊 [{}] Status before stop: {}", com, status);
                } catch (Exception ignore) {
                }

                String resp = helper.sendAndRead("AT+QAUDRD=0", 2000);

                if (resp.contains("OK")) {
                    log.info("✅ [{}] Recording stopped successfully", com);
                } else {
                    log.warn("⚠️ [{}] Stop recording response: {}", com, resp);
                }

                Thread.sleep(2000); // ✅ Wait 2s for modem to finalize file (reduced from 5s for better performance)

                // NOTE: File is on MODEM, not local yet
                if (modemRecordPath != null) {
                    log.info("ℹ️ [{}] Modem record file: {} (will download next)", com, modemRecordPath);
                }

            } catch (Exception ex) {
                log.error("❌ [{}] Cannot stop recording: {}", com, ex.getMessage());
            }
        }

        private File downloadModemRecord() {
            final String modemPath = this.modemRecordPath;
            final String localName = this.localRecordFilename;

            if (modemPath == null || localName == null) {
                log.info("ℹ️ [{}] Recording flag set but no modem file path available", com);
                return null;
            }

            try {
                Optional<ModemFileInfo> infoOpt = getModemFileInfo(modemPath);
                if (infoOpt.isEmpty()) {
                    log.error("❌ [{}] Cannot determine modem file size for {}", com, modemPath);
                    return null;
                }

                ModemFileInfo info = infoOpt.get();
                long expectedSize = info.size;
                // ✅ FIX: Normalize path to avoid UFS:UFS: duplication
                String actualModemPath = normalizeModemPath(info.rawPath);

                log.info("📥 [{}] Downloading {} bytes from modem: {}", com, expectedSize, actualModemPath);

                // ✅ FIX: Retry opening file up to 3 times
                Integer handle = null;
                for (int attempt = 1; attempt <= 3; attempt++) {
                    try {
                        handle = openModemFile(actualModemPath);
                        if (handle != null) {
                            log.info("✅ [{}] File opened successfully (handle={})", com, handle);
                            break;
                        }
                        log.warn("⚠️ [{}] Attempt {}/3: Cannot open file {}", com, attempt, actualModemPath);
                        Thread.sleep(500); // Wait before retry
                    } catch (Exception e) {
                        log.warn("⚠️ [{}] Attempt {}/3 failed: {}", com, attempt, e.getMessage());
                        if (attempt < 3)
                            Thread.sleep(500);
                    }
                }

                if (handle == null) {
                    log.error("❌ [{}] Cannot open modem file after 3 attempts: {}", com, actualModemPath);
                    return null;
                }

                byte[] audioData = readModemFile(handle, expectedSize);
                closeModemFile(handle);

                if (audioData == null || audioData.length == 0) {
                    log.error("❌ [{}] Downloaded 0 bytes from modem", com);
                    return null;
                }

                log.info("✅ [{}] Downloaded {} bytes from modem", com, audioData.length);

                // ✅ Add WAV header if needed
                byte[] finalData = audioData;
                if (localName.endsWith(".wav")) {
                    finalData = addWavHeader(audioData, 16); // Assume WAV_ADPCM format
                    log.info("✅ [{}] Added WAV header ({} → {} bytes)", com, audioData.length, finalData.length);
                }

                // ✅ Write to local file
                try {
                    Files.createDirectories(DEFAULT_RECORD_DIR);
                } catch (Exception e) {
                    log.warn("⚠️ [{}] Cannot create record dir: {}", com, e.getMessage());
                }

                File localFile = DEFAULT_RECORD_DIR.resolve(localName).toFile();
                Files.write(localFile.toPath(), finalData);

                log.info("✅ [{}] Saved local file: {} ({} bytes)", com, localFile.getAbsolutePath(),
                        localFile.length());
                return localFile;

            } catch (Exception e) {
                log.error("❌ [{}] Download failed: {}", com, e.getMessage(), e);
                return null;
            }
        }

        /**
         * ✅ NEW: Add RIFF/WAV header to raw audio data
         * This makes the file playable in standard media players
         */
        private byte[] addWavHeader(byte[] audioData, int format) {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();

                int dataSize = audioData.length;
                int sampleRate = 8000; // Standard for voice calls
                int channels = 1; // Mono
                int bitsPerSample = 16; // 16-bit PCM

                // Adjust based on format
                if (format == 16) { // WAV_ADPCM
                    bitsPerSample = 4; // ADPCM uses 4 bits per sample
                }

                int byteRate = sampleRate * channels * bitsPerSample / 8;
                int blockAlign = channels * bitsPerSample / 8;

                // RIFF header
                out.write("RIFF".getBytes());
                out.write(intToBytes(36 + dataSize)); // File size - 8
                out.write("WAVE".getBytes());

                // fmt chunk
                out.write("fmt ".getBytes());
                out.write(intToBytes(16)); // fmt chunk size
                out.write(shortToBytes((short) (format == 16 ? 2 : 1))); // Audio format (1=PCM, 2=ADPCM)
                out.write(shortToBytes((short) channels));
                out.write(intToBytes(sampleRate));
                out.write(intToBytes(byteRate));
                out.write(shortToBytes((short) blockAlign));
                out.write(shortToBytes((short) bitsPerSample));

                // data chunk
                out.write("data".getBytes());
                out.write(intToBytes(dataSize));
                out.write(audioData);

                return out.toByteArray();

            } catch (Exception e) {
                log.warn("⚠️ [{}] Failed to add WAV header: {}", com, e.getMessage());
                return audioData; // Return original data if header fails
            }
        }

        private byte[] intToBytes(int value) {
            return new byte[] {
                    (byte) (value & 0xFF),
                    (byte) ((value >> 8) & 0xFF),
                    (byte) ((value >> 16) & 0xFF),
                    (byte) ((value >> 24) & 0xFF)
            };
        }

        private byte[] shortToBytes(short value) {
            return new byte[] {
                    (byte) (value & 0xFF),
                    (byte) ((value >> 8) & 0xFF)
            };
        }

        private long queryModemFileSize(String modemPath) throws Exception {
            Optional<ModemFileInfo> info = getModemFileInfo(modemPath);
            return info.map(m -> m.size).orElse(-1L);
        }

        private boolean shouldRetryDial(RuntimeException ex) {
            String msg = ex.getMessage();
            if (msg == null)
                return false;
            String normalized = msg.toUpperCase(Locale.ROOT);
            return normalized.contains("GENERAL DIAL ERROR")
                    || normalized.contains("NO DIALTONE");
        }

        private boolean isNetworkRegistered() {
            try {
                String creg = helper.sendAndRead("AT+CREG?", 1500);
                return creg.contains(",1") || creg.contains(",5");
            } catch (Exception e) {
                log.debug("⚠️ [{}] Unable to query CREG before dial: {}", com, e.getMessage());
                return false;
            }
        }

        private Integer openModemFile(String modemPath) throws Exception {
            String normalized = normalizeModemPath(modemPath);
            String resp = helper.sendAndRead("AT+QFOPEN=\"" + normalized + "\",0", 2000);
            Matcher m = Pattern.compile("\\+QFOPEN:\\s*(\\d+)").matcher(resp);
            if (m.find()) {
                return Integer.parseInt(m.group(1));
            }
            return null;
        }

        private Optional<ModemFileInfo> getModemFileInfo(String modemPath) throws Exception {
            if (modemPath == null || modemPath.isBlank()) {
                return Optional.empty();
            }

            String resp = helper.sendAndRead("AT+QFLST", 2000);
            Matcher matcher = Pattern.compile("\\+QFLST:\\s*\"([^\"]+)\",(\\d+)").matcher(resp);
            String target = canonicalizeModemPath(modemPath);

            while (matcher.find()) {
                String rawPath = matcher.group(1);
                long size = Long.parseLong(matcher.group(2));
                String candidate = canonicalizeModemPath(rawPath);
                if (candidate.equals(target)) {
                    return Optional.of(new ModemFileInfo(rawPath, candidate, size));
                }
            }

            return Optional.empty();
        }

        private String canonicalizeModemPath(String modemPath) {
            return normalizeModemPath(modemPath).toUpperCase(Locale.ROOT);
        }

        private String normalizeModemPath(String modemPath) {
            if (modemPath == null)
                return "";
            String normalized = modemPath.trim().replace("\\", "/");
            String upper = normalized.toUpperCase(Locale.ROOT);
            while (upper.startsWith("UFS:UFS:")) {
                normalized = "UFS:" + normalized.substring(8);
                upper = normalized.toUpperCase(Locale.ROOT);
            }
            if (!upper.startsWith("UFS:") && !upper.startsWith("RAM:") && !upper.startsWith("SD:")) {
                normalized = "UFS:" + normalized;
            }
            return normalized;
        }

        private byte[] readModemFile(Integer handle, long expectedSize) throws Exception {
            log.info("📥 [{}] Reading {} bytes from modem (handle={})", com, expectedSize, handle);

            final int CHUNK_SIZE = 1024;
            ByteArrayOutputStream allData = new ByteArrayOutputStream();
            long bytesRead = 0;

            while (bytesRead < expectedSize) {
                int toRead = (int) Math.min(CHUNK_SIZE, expectedSize - bytesRead);
                byte[] chunk = readModemFileChunk(handle, toRead);
                if (chunk == null || chunk.length == 0) {
                    log.warn("⚠️ [{}] No more data from modem (expected {} more bytes)",
                            com, expectedSize - bytesRead);
                    break;
                }
                allData.write(chunk);
                bytesRead += chunk.length;

                if (bytesRead % 10240 == 0) { // Log every 10KB
                    log.debug("📥 [{}] Downloaded {}/{} bytes", com, bytesRead, expectedSize);
                }
            }

            log.info("✅ [{}] Read {} bytes from modem", com, bytesRead);
            return allData.toByteArray();
        }

        private void closeModemFile(Integer handle) {
            if (handle == null)
                return;
            try {
                helper.sendAndRead("AT+QFCLOSE=" + handle, 1000);
                log.debug("🔒 [{}] Closed file handle {}", com, handle);
            } catch (Exception e) {
                log.warn("⚠️ [{}] Failed to close handle {}: {}", com, handle, e.getMessage());
            }
        }

        private byte[] readModemFileChunk(int handle, int bytesToRead) throws Exception {
            if (bytesToRead <= 0)
                return new byte[0];

            // ✅ FIX: Flush input buffer before sending QFREAD
            try {
                InputStream in = port.getInputStream();
                while (in.available() > 0) {
                    in.read(); // Discard old data
                }
            } catch (Exception e) {
                log.debug("⚠️ [{}] Failed to flush input buffer: {}", com, e.getMessage());
            }

            helper.send("AT+QFREAD=" + handle + "," + bytesToRead);

            // ✅ FIX: Increased timeout from 3000ms to 5000ms
            if (!waitForConnectBanner(5000)) {
                log.error("❌ [{}] CONNECT banner timeout for QFREAD (handle={}, bytes={})",
                        com, handle, bytesToRead);
                return new byte[0];
            }

            byte[] payload = readExactBytes(bytesToRead, 10000);
            consumeOkResponse(2000);
            return payload;
        }

        private boolean waitForConnectBanner(int timeoutMs) throws Exception {
            InputStream in = port.getInputStream();
            StringBuilder buffer = new StringBuilder();
            long deadline = System.currentTimeMillis() + timeoutMs;

            while (System.currentTimeMillis() < deadline) {
                if (in.available() <= 0) {
                    Thread.sleep(5);
                    continue;
                }

                int val = in.read();
                if (val < 0)
                    continue;
                char c = (char) val;
                buffer.append(c);

                String text = buffer.toString();
                if (text.contains("CONNECT")) {
                    consumeLineEnding(in, timeoutMs);
                    return true;
                }

                if (text.contains("ERROR") || text.contains("+CME")) {
                    log.error("❌ [{}] QFREAD error: {}", com, text.replace("\r", " ").replace("\n", " "));
                    return false;
                }
            }

            log.warn("⚠️ [{}] Timeout waiting for CONNECT banner", com);
            return false;
        }

        private void consumeLineEnding(InputStream in, int timeoutMs) throws Exception {
            long deadline = System.currentTimeMillis() + timeoutMs;

            while (System.currentTimeMillis() < deadline) {
                if (in.available() <= 0) {
                    Thread.sleep(5);
                    continue;
                }

                int val = in.read();
                if (val < 0)
                    continue;

                if (val == '\n') {
                    return;
                }
            }
        }

        private byte[] readExactBytes(int expectedBytes, int timeoutMs) throws Exception {
            InputStream in = port.getInputStream();
            byte[] buffer = new byte[expectedBytes];
            int offset = 0;
            long deadline = System.currentTimeMillis() + timeoutMs;

            while (offset < expectedBytes && System.currentTimeMillis() < deadline) {
                if (in.available() <= 0) {
                    Thread.sleep(5);
                    continue;
                }

                int read = in.read(buffer, offset, expectedBytes - offset);
                if (read > 0) {
                    offset += read;
                }
            }

            if (offset == expectedBytes) {
                return buffer;
            }

            byte[] result = new byte[offset];
            System.arraycopy(buffer, 0, result, 0, offset);
            return result;
        }

        private void consumeOkResponse(int timeoutMs) throws Exception {
            InputStream in = port.getInputStream();
            StringBuilder buffer = new StringBuilder();
            long deadline = System.currentTimeMillis() + timeoutMs;

            while (System.currentTimeMillis() < deadline) {
                if (in.available() <= 0) {
                    Thread.sleep(5);
                    continue;
                }

                int val = in.read();
                if (val < 0)
                    continue;
                char c = (char) val;
                buffer.append(c);

                String text = buffer.toString();
                if (text.contains("OK")) {
                    consumeLineEnding(in, timeoutMs);
                    return;
                }

                if (text.contains("ERROR") || text.contains("+CME")) {
                    log.warn("⚠️ [{}] Tail while reading modem file: {}", com,
                            text.replace("\r", " ").replace("\n", " "));
                    return;
                }
            }

            log.warn("⚠️ [{}] Timeout waiting for OK after QFREAD", com);
        }

        @Override
        public void close() {
            // ✅ FIX: Nếu recording đã bắt đầu nhưng endCall() chưa được gọi (do lỗi giữa
            // chừng),
            // thì gọi endCall() để stop recording, download file, upload và gửi callback
            if (!finished.get() && recordStarted.get()) {
                log.warn("⚠️ [{}] Call session closing with unfinished recording! Attempting recovery...", com);
                try {
                    endCall(); // Sẽ stop recording, download, upload, và gửi callback
                    log.info("✅ [{}] Recording recovery completed during error cleanup", com);
                } catch (Exception e) {
                    log.error("❌ [{}] Recording recovery failed during close: {}", com, e.getMessage());
                }
            } else if (!finished.get()) {
                // Call chưa connected hoặc chưa record -> chỉ cần endCall() để gửi callback
                log.debug("🔄 [{}] Call session closing without recording, calling endCall() for cleanup", com);
                try {
                    endCall();
                } catch (Exception e) {
                    log.warn("⚠️ [{}] endCall() failed during close: {}", com, e.getMessage());
                }
            }

            try {
                if (clccMonitorFuture != null)
                    clccMonitorFuture.cancel(true);
            } catch (Exception ignored) {
            }
            try {
                if (stopFuture != null)
                    stopFuture.cancel(true);
            } catch (Exception ignored) {
            }

            // ✅ FIX: Soft-reset modem sau call để đảm bảo AT commands hoạt động đúng
            // Điều này fix lỗi "SIM not ready" khi scan sau call
            try {
                log.debug("🔄 [{}] Soft-reset modem sau call session...", com);
                helper.sendAndRead("ATH", 500); // Đảm bảo đã hangup
                helper.sendAndRead("AT", 300); // Ping modem
                helper.sendAndRead("ATE0", 300); // Tắt echo
                helper.sendAndRead("AT+CMEE=2", 300); // Enable error reporting
                helper.sendAndRead("AT+CMGF=1", 300); // SMS text mode
                helper.sendAndRead("AT+CSCS=\"UCS2\"", 300); // UCS2 encoding
                helper.sendAndRead("AT+CNMI=2,1,0,0,0", 300); // URC for SMS
                log.debug("✅ [{}] Modem soft-reset hoàn tất", com);
            } catch (Exception e) {
                log.warn("⚠️ [{}] Soft-reset modem sau call failed: {}", com, e.getMessage());
            }
        }
    }

    // ======================================================================
    // ✅ NEW: INCOMING CALL SESSION
    // ======================================================================
    /**
     * IncomingCallSession - Handles incoming calls (CALL_IN)
     * Waits for RING/CLIP, answers with ATA, records, uploads, callbacks
     */
    private class IncomingCallSession implements AutoCloseable {
        private final Task task;
        private final String com;
        private final AtomicBoolean finished = new AtomicBoolean(false);
        private final Instant callStart = Instant.now();
        private final AtomicReference<Boolean> connected = new AtomicReference<>(false);
        private final AtomicReference<Instant> connectedAt = new AtomicReference<>(null);
        private final AtomicBoolean recordStarted = new AtomicBoolean(false);

        private volatile String callerNumber = "UNKNOWN"; // ✅ Detect from +CLIP
        private volatile String modemRecordPath;
        private volatile String localRecordFilename;
        private final String recordBaseName;

        private ScheduledFuture<?> clccMonitorFuture;
        private ScheduledFuture<?> stopFuture;
        private ScheduledFuture<?> timeoutFuture; // ✅ Timeout if no call comes

        IncomingCallSession(Task task) {
            this.task = task;
            this.com = sim.getComName();
            this.recordBaseName = "callin_" + System.currentTimeMillis();
        }

        void prepareModem() throws Exception {
            log.info("📞 [{}] Preparing modem for INCOMING call...", com);

            try {
                // ✅ SAME AS CallSession - enable CLIP
                helper.sendAndRead("AT", 300);
                helper.sendAndRead("ATE0", 300);
                helper.sendAndRead("AT+CLIP=1", 300); // ✅ Enable caller ID
                helper.sendAndRead("AT+CRC=1", 300); // ✅ Enable extended RING

                log.info("✅ [{}] Modem ready for incoming call (CLIP enabled)", com);

            } catch (Exception e) {
                log.error("❌ [{}] prepareModem failed: {}", com, e.getMessage());
                throw e;
            }
        }

        void waitForIncomingCall() throws InterruptedException {
            log.info("⏳ [{}] Waiting for incoming call (max {}s)...", com, task.duration + 60);

            // ✅ Set timeout - nếu không có call đến trong task.duration + 60s thì timeout
            int timeoutSeconds = task.duration + 60; // thêm 60s buffer
            timeoutFuture = callScheduler.schedule(() -> {
                if (!connected.get()) {
                    log.warn("⏰ [{}] CALL_IN timeout - no call received in {}s", com, timeoutSeconds);
                    sendCallCallback(com, callStart, Instant.now(), task, null, 0, false, "TIMEOUT_NO_CALL");
                    notifyCallStatus("TIMEOUT", task);
                    finished.set(true);
                }
            }, timeoutSeconds, TimeUnit.SECONDS);

            // ✅ Monitor serial port for RING/CLIP
            ScheduledFuture<?> monitorFuture = callScheduler.scheduleAtFixedRate(() -> {
                if (finished.get())
                    return;

                try {
                    // Read any unsolicited messages
                    String buffer = helper.readAll();
                    if (buffer == null || buffer.isEmpty())
                        return;

                    log.debug("📡 [{}] Buffer: {}", com, buffer.replace("\r", " ").replace("\n", " "));

                    // ✅ FIX: Also check for SMS URC during call monitoring to avoid missing SMS
                    if (buffer.contains("+CMTI:") || buffer.contains("+CMT:")) {
                        log.debug("📨 [{}] SMS URC detected during call - will scan after call ends", com);
                        // Note: SMS scan will happen after call ends via normal polling
                    }

                    // ✅ Detect RING
                    if (buffer.contains("RING") || buffer.contains("+CRING")) {
                        log.info("🔔 [{}] RING detected!", com);
                    }

                    // ✅ Detect +CLIP (caller ID) - only answer once
                    if (buffer.contains("+CLIP:") && !connected.get()) {
                        Pattern clipPattern = Pattern.compile("\\+CLIP:\\s*\"([^\"]+)\"");
                        Matcher m = clipPattern.matcher(buffer);
                        if (m.find()) {
                            String rawCallerNumber = m.group(1);

                            // ✅ STEP 1: Giải mã nếu bị encode (UCS2, hex, etc)
                            callerNumber = decodeCallerNumber(rawCallerNumber);
                            log.info("📞 [{}] Incoming call from: {} (raw: {})", com, callerNumber, rawCallerNumber);

                            // ✅ STEP 2: Check if caller ID is hidden/private
                            boolean isHiddenNumber = isHiddenCallerID(callerNumber);

                            if (isHiddenNumber) {
                                log.warn("📵 [{}] Incoming call with HIDDEN/PRIVATE number: '{}'", com, callerNumber);
                                handleHiddenNumberCall();
                            } else {
                                // ✅ STEP 3: Handle normal call with smart matching
                                handleNormalCallWithQueueMatching(callerNumber);
                            }
                        }
                    }

                } catch (Exception ex) {
                    log.warn("⚠️ [{}] Error monitoring for RING: {}", com, ex.getMessage());
                }

            }, 0, 500, TimeUnit.MILLISECONDS);

            // ✅ CRITICAL FIX: BLOCK here until call finishes or timeout
            try {
                while (!finished.get()) {
                    Thread.sleep(500); // Check every 500ms
                }
            } finally {
                // Cleanup monitor
                if (monitorFuture != null) {
                    monitorFuture.cancel(true);
                }
            }

            log.info("✅ [{}] waitForIncomingCall() completed", com);
        }

        /**
         * ✅ Answer incoming call (ATA)
         */
        private void answerCall() {
            if (finished.get())
                return;

            try {
                log.info("📞 [{}] Answering call from {} with ATA", com, callerNumber);

                // ✅ Send ATA to answer
                String resp = helper.sendAndRead("ATA", 2000);
                log.info("✅ [{}] ATA response: {}", com, resp.replace("\r", " ").replace("\n", " "));

                // ✅ Mark as connected
                connected.set(true);
                connectedAt.set(Instant.now());
                notifyCallStatus("IN_CALL", task);

                // ✅ Cancel timeout
                if (timeoutFuture != null) {
                    timeoutFuture.cancel(false);
                }

                // ✅ Start recording if needed
                if (task.record && recordStarted.compareAndSet(false, true)) {
                    startRecording();
                }

                // ✅ Monitor call state
                monitorCallState();

                // ✅ Schedule auto hangup
                scheduleStopByDuration();

            } catch (Exception e) {
                log.error("❌ [{}] Error answering call: {}", com, e.getMessage(), e);
                sendCallCallback(com, callStart, Instant.now(), task, null, 0, false, "ANSWER_FAILED");
                notifyCallStatus("FAILED", task);
                finished.set(true);
            }
        }

        /**
         * ✅ Reject incoming call (ATH) - Wrong caller
         */
        private void rejectCall() {
            try {
                log.info("🚫 [{}] Rejecting call from {} (expected: {})",
                        com, callerNumber, task.expectedCaller);

                // ✅ Send ATH to reject/hangup
                String resp = helper.sendAndRead("ATH", 1000);
                log.info("✅ [{}] ATH response: {}", com, resp.replace("\r", " ").replace("\n", " "));

                // ✅ Send callback with WRONG_CALLER status
                sendCallCallback(com, callStart, Instant.now(), task, null, 0, false, "WRONG_CALLER");
                notifyCallStatus("REJECTED_WRONG_CALLER", task);

                // ✅ Mark as finished - đợi timeout hoặc call tiếp theo
                // KHÔNG set finished = true vì có thể còn đợi call đúng
                log.info("⏳ [{}] Continue waiting for correct caller: {}", com, task.expectedCaller);

            } catch (Exception e) {
                log.error("❌ [{}] Error rejecting call: {}", com, e.getMessage(), e);
            }
        }

        /**
         * ✅ Match phone numbers (normalize +84, 84, 0)
         */
        private boolean matchesPhoneNumber(String number1, String number2) {
            if (number1 == null || number2 == null)
                return false;

            String normalized1 = normalizePhoneNumber(number1);
            String normalized2 = normalizePhoneNumber(number2);

            return normalized1.equals(normalized2);
        }

        /**
         * ✅ Normalize phone number: +84901234567 → 84901234567
         */
        private String normalizePhoneNumber(String phone) {
            if (phone == null)
                return "";

            // Remove spaces, dashes, parentheses
            String cleaned = phone.replaceAll("[\\s\\-()]", "");

            // Remove leading +
            if (cleaned.startsWith("+")) {
                cleaned = cleaned.substring(1);
            }

            // Convert 0901234567 → 84901234567
            if (cleaned.startsWith("0") && cleaned.length() == 10) {
                cleaned = "84" + cleaned.substring(1);
            }

            return cleaned;
        }

        /**
         * ✅ Check if caller ID is hidden/private/blocked
         */
        private boolean isHiddenCallerID(String callerNumber) {
            if (callerNumber == null || callerNumber.trim().isEmpty()) {
                return true; // Empty = hidden
            }

            String normalized = callerNumber.trim().toUpperCase();

            // Common patterns for hidden numbers
            return normalized.equals("PRIVATE")
                    || normalized.equals("UNKNOWN")
                    || normalized.equals("RESTRICTED")
                    || normalized.equals("ANONYMOUS")
                    || normalized.equals("UNAVAILABLE")
                    || normalized.equals("BLOCKED")
                    || normalized.equals("WITHHELD")
                    || normalized.isEmpty();
        }

        /**
         * ✅ Handle call with hidden/private caller ID
         */
        private void handleHiddenNumberCall() {
            task.to = "HIDDEN"; // Store as HIDDEN

            if (task.acceptHiddenNumber) {
                // ✅ Strategy 1: Time-based matching
                if (isWithinTimeWindow()) {
                    log.info("✅ [{}] Hidden number ACCEPTED (within time window)", com);
                    answerCall();
                } else {
                    log.warn("❌ [{}] Hidden number REJECTED (outside time window)", com);
                    rejectCallHidden("OUTSIDE_TIME_WINDOW");
                }
            } else {
                // ✅ Strategy 2: Reject all hidden numbers
                log.warn("❌ [{}] Hidden number REJECTED (policy: reject hidden)", com);
                rejectCallHidden("HIDDEN_NUMBER_BLOCKED");
            }
        }

        /**
         * ✅ Handle normal call with SMART QUEUE MATCHING
         * Nếu không match task hiện tại → tìm trong queue order khác
         */
        private void handleNormalCallWithQueueMatching(String callerNumber) {
            task.to = callerNumber;

            // ✅ STEP 1: Check match với task HIỆN TẠI
            if (task.expectedCaller != null && !task.expectedCaller.isBlank()) {
                if (matchesPhoneNumber(callerNumber, task.expectedCaller)) {
                    log.info("✅ [{}] Caller {} MATCHES current task (expected: {})",
                            com, callerNumber, task.expectedCaller);
                    answerCall();
                    return; // Done!
                }
            }

            // ✅ STEP 2: Không match → TÌM TRONG QUEUE
            log.warn("⚠️ [{}] Caller {} DOES NOT MATCH current task (expected: {})",
                    com, callerNumber, task.expectedCaller);
            log.info("🔍 [{}] Searching queue for order waiting for {}...", com, callerNumber);

            Task matchedTask = findTaskInQueueForCaller(callerNumber);

            if (matchedTask != null) {
                // ✅ FOUND! Answer for that order
                log.info("✅ [{}] Found matching order in queue: orderId={} expects {}",
                        com, matchedTask.orderId, matchedTask.expectedCaller);

                // ✅ Swap task - use matched task instead
                Task originalTask = this.task;

                log.info("🔄 [{}] SWITCHING from orderId={} to orderId={} (caller match)",
                        com, originalTask.orderId, matchedTask.orderId);

                // Remove matched task from queue
                queue.remove(matchedTask);

                // Re-enqueue original task (for later)
                queue.offer(originalTask);

                // Answer with matched task
                answerCallForTask(matchedTask, callerNumber);

            } else {
                // ❌ No matching order in queue → Reject
                log.warn("❌ [{}] No order in queue waiting for {} - REJECTING", com, callerNumber);
                rejectCall();
            }
        }

        /**
         * ✅ Find task in queue that's waiting for this caller
         */
        private Task findTaskInQueueForCaller(String callerNumber) {
            for (Task t : queue) {
                if (t.type == TaskType.CALL_IN && t.expectedCaller != null) {
                    if (matchesPhoneNumber(callerNumber, t.expectedCaller)) {
                        return t;
                    }
                }
            }
            return null;
        }

        /**
         * ✅ Answer call for specific task (not current task)
         */
        private void answerCallForTask(Task targetTask, String callerNumber) {
            // Update references to use target task
            targetTask.to = callerNumber;

            try {
                log.info("📞 [{}] Answering call from {} for order {}", com, callerNumber, targetTask.orderId);

                // Send ATA
                String resp = helper.sendAndRead("ATA", 2000);
                log.info("✅ [{}] ATA response: {}", com, resp.replace("\r", " ").replace("\n", " "));

                connected.set(true);
                connectedAt.set(Instant.now());
                notifyCallStatus("IN_CALL", targetTask);

                if (timeoutFuture != null) {
                    timeoutFuture.cancel(false);
                }

                if (targetTask.record && recordStarted.compareAndSet(false, true)) {
                    startRecording();
                }

                monitorCallState();
                scheduleStopByDuration();

            } catch (Exception e) {
                log.error("❌ [{}] Error answering call: {}", com, e.getMessage(), e);
                sendCallCallback(com, callStart, Instant.now(), targetTask, null, 0, false, "ANSWER_FAILED");
                notifyCallStatus("FAILED", targetTask);
                finished.set(true);
            }
        }

        /**
         * ✅ Decode caller number if encoded (UCS2, hex, etc)
         */
        private String decodeCallerNumber(String raw) {
            if (raw == null || raw.isEmpty()) {
                return "UNKNOWN";
            }

            // ✅ Check if UCS2 encoded (all hex digits, length % 4 == 0)
            if (raw.matches("^[0-9A-Fa-f]+$") && raw.length() % 4 == 0 && raw.length() >= 8) {
                try {
                    // Decode UCS2
                    StringBuilder decoded = new StringBuilder();
                    for (int i = 0; i < raw.length(); i += 4) {
                        String hex = raw.substring(i, i + 4);
                        int codePoint = Integer.parseInt(hex, 16);
                        decoded.append((char) codePoint);
                    }
                    String result = decoded.toString();
                    log.info("🔓 [{}] Decoded UCS2: {} → {}", com, raw, result);
                    return result;
                } catch (Exception e) {
                    log.debug("⚠️ [{}] UCS2 decode failed, using raw: {}", com, e.getMessage());
                }
            }

            // ✅ Already normal text
            return raw;
        }

        /**
         * ✅ Check if call is within acceptable time window
         */
        private boolean isWithinTimeWindow() {
            long elapsedSeconds = Duration.between(callStart, Instant.now()).getSeconds();
            boolean withinWindow = elapsedSeconds <= task.timeWindowSeconds;

            log.info("⏱️ [{}] Time check: {}s elapsed / {}s window = {}",
                    com, elapsedSeconds, task.timeWindowSeconds,
                    withinWindow ? "WITHIN" : "OUTSIDE");

            return withinWindow;
        }

        /**
         * ✅ Reject hidden number call with specific reason
         */
        private void rejectCallHidden(String reason) {
            try {
                log.info("🚫 [{}] Rejecting HIDDEN call - reason: {}", com, reason);

                // ✅ Send ATH to reject/hangup
                String resp = helper.sendAndRead("ATH", 1000);
                log.info("✅ [{}] ATH response: {}", com, resp.replace("\r", " ").replace("\n", " "));

                // ✅ Send callback with specific status
                sendCallCallback(com, callStart, Instant.now(), task, null, 0, false, reason);
                notifyCallStatus("REJECTED_" + reason, task);

                log.info("⏳ [{}] Continue waiting for valid caller...", com);

            } catch (Exception e) {
                log.error("❌ [{}] Error rejecting hidden call: {}", com, e.getMessage(), e);
            }
        }

        private void startRecording() {
            try {
                // ✅ Identify modem type for specialized recording
                String modemInfo = helper.sendAndRead("ATI", 1000).toUpperCase();
                boolean isEC2x = modemInfo.contains("EC25") || modemInfo.contains("EC21");

                boolean started = false;

                if (isEC2x) {
                    log.info("🔊 [{}] EC25 detected for incoming call, using 4-parameter recording", com);
                    // Enable recording feature
                    helper.sendAndRead("AT+QAUDCFG=\"record\",1", 500);

                    int[] ec2xFormats = { 16, 13, 3 };
                    String[] ec2xExt = { ".wav", ".wav", ".amr" };

                    for (int i = 0; i < ec2xFormats.length; i++) {
                        String candidateLocalFile = recordBaseName + ec2xExt[i];
                        String candidateModemPath = MODEM_STORAGE_PREFIX + candidateLocalFile;

                        // ✅ Delete old file first
                        deleteModemFileSilently(candidateModemPath);

                        // ✅ USE 4 PARAMETERS (channel=2) for Mixed audio on EC25
                        String recordCmd = String.format("AT+QAUDRD=1,\"%s\",%d,2", candidateModemPath, ec2xFormats[i]);
                        log.info("🎙️ [{}] EC25 attempt: {}", com, recordCmd);

                        String resp = helper.sendAndRead(recordCmd, 2000);
                        if (resp.contains("OK")) {
                            log.info("✅ [{}] EC25 Call-In Recording STARTED (format={}, channel=2)", com,
                                    ec2xFormats[i]);
                            modemRecordPath = candidateModemPath;
                            localRecordFilename = candidateLocalFile;
                            started = true;
                            break;
                        }
                    }
                } else {
                    // Legacy GSM Recording logic
                    for (RecordingProfile profile : RECORDING_PROFILES) {
                        String candidateLocalFile = recordBaseName + profile.extension;
                        String candidateModemPath = MODEM_STORAGE_PREFIX + candidateLocalFile;

                        deleteModemFileSilently(candidateModemPath);

                        String recordCmd = String.format("AT+QAUDRD=1,\"%s\",%d", candidateModemPath, profile.format);
                        log.info("🎙️ [{}] Legacy attempt → {}", com, recordCmd);

                        String resp = helper.sendAndRead(recordCmd, 2000);
                        if (resp.contains("OK")) {
                            log.info("✅ [{}] Legacy Recording STARTED: {}", com, candidateModemPath);
                            modemRecordPath = candidateModemPath;
                            localRecordFilename = candidateLocalFile;
                            started = true;
                            break;
                        }
                    }
                }

                if (!started) {
                    log.error("❌ [{}] ALL recording formats FAILED for incoming call!", com);
                }

            } catch (Exception ex) {
                log.error("❌ [{}] Cannot start recording: {}", com, ex.getMessage(), ex);
            }
        }

        private void monitorCallState() {
            clccMonitorFuture = callScheduler.scheduleAtFixedRate(() -> {
                if (finished.get())
                    return;

                try {
                    String st = helper.sendAndRead("AT+CLCC", 800);
                    if (st == null || !st.contains("+CLCC")) {
                        // No active call - might be disconnected
                        if (connected.get()) {
                            log.info("📞 [{}] Call disconnected (no CLCC)", com);
                            endCall();
                        }
                    }
                } catch (Exception ex) {
                    log.warn("⚠️ [{}] CLCC error: {}", com, ex.getMessage());
                }

            }, 2, 2, TimeUnit.SECONDS);
        }

        private void scheduleStopByDuration() {
            int durationSec = (task.duration > 0 ? task.duration : 30);
            stopFuture = callScheduler.schedule(this::endCall, durationSec, TimeUnit.SECONDS);
            log.info("⏰ [{}] Call will auto-end in {}s", com, durationSec);
        }

        private void endCall() {
            if (!finished.compareAndSet(false, true))
                return;

            Instant end = Instant.now();
            long duration = 0;

            if (Boolean.TRUE.equals(connected.get()) && connectedAt.get() != null) {
                duration = Duration.between(connectedAt.get(), end).getSeconds();
            }

            try {
                log.info("🛑 [{}] Sending ATH to hangup...", com);
                helper.sendAndRead("ATH", 500);
                log.info("✅ [{}] Call ended (duration={}s)", com, duration);
            } catch (Exception e) {
                log.warn("⚠️ [{}] ATH failed: {}", com, e.getMessage());
            }

            // ✅ STEP 1: Stop recording and download
            File wavFile = null;
            if (task.record && recordStarted.get() && modemRecordPath != null) {
                log.info("🎙️ [{}] STEP 1/3: Stopping recording...", com);
                stopRecording();
                log.info("✅ [{}] Recording stopped", com);

                // ✅ Access local recording file
                log.info("📥 [{}] STEP 2/3: Accessing local recording file...", com);
                try {
                    wavFile = downloadRecordingSimplified();

                    if (wavFile != null && wavFile.exists()) {
                        log.info("✅ [{}] Recording file ready: {} ({} bytes)",
                                com, wavFile.getName(), wavFile.length());
                    } else {
                        log.error("❌ [{}] Failed to access recording file!", com);
                    }
                } catch (Exception e) {
                    log.error("❌ [{}] Failed to access recording: {}", com, e.getMessage(), e);
                }
            } else if (task.record) {
                log.warn("⚠️ [{}] Recording was enabled but not started or no file path", com);
            }

            // ✅ STEP 2: Upload file lên server
            String url = null;
            if (wavFile != null && wavFile.exists() && wavFile.length() > 1024) {
                log.info("☁️ [{}] STEP 3/3: Uploading recording to server...", com);
                url = uploadRecordFile(wavFile.getAbsolutePath(), task.orderId, task.serviceCode);

                if (url != null) {
                    log.info("✅ [{}] Upload SUCCESS → URL: {}", com, url);
                } else {
                    log.error("❌ [{}] Upload FAILED - callback will have recordFile=null!", com);
                }
            } else if (wavFile != null) {
                log.warn("⚠️ [{}] WAV file too small: {} bytes (min: 1024) - skipping upload",
                        com, wavFile.length());
            }

            // ✅ STEP 3: Send callback SAU KHI hoàn tất upload
            log.info("📡 [{}] Sending callback (AFTER recording processing)...", com);
            sendCallCallback(
                    com,
                    callStart,
                    end,
                    task,
                    url,
                    duration,
                    Boolean.TRUE.equals(connected.get()),
                    null);

            log.info("📊 [{}] ✅ Callback sent | connected={} | duration={}s | recording={}",
                    com, connected.get(), duration, url != null ? url : "NONE");

            // ✅ Notify final status
            notifyCallStatus("ENDED_SUCCESS", task);

            // ✅ Cleanup
            close();
        }

        private void stopRecording() {
            try {
                if (modemRecordPath == null) {
                    log.info("ℹ️ [{}] Record flag set but no modem file path was captured", com);
                }

                // Check status trước khi stop
                try {
                    String status = helper.sendAndRead("AT+QAUDRD?", 500);
                    log.info("📊 [{}] Status before stop: {}", com, status);
                } catch (Exception ignore) {
                }

                String resp = helper.sendAndRead("AT+QAUDRD=0", 2000);

                if (resp.contains("OK")) {
                    log.info("✅ [{}] Recording stopped successfully", com);
                } else {
                    log.warn("⚠️ [{}] Stop recording response: {}", com, resp);
                }

                Thread.sleep(2000); // Wait for modem to finalize file

                // NOTE: File is on MODEM, not local yet
                if (modemRecordPath != null) {
                    log.info("ℹ️ [{}] Modem record file: {} (will download next)", com, modemRecordPath);
                }

            } catch (Exception ex) {
                log.error("❌ [{}] Cannot stop recording: {}", com, ex.getMessage());
            }
        }

        /**
         * ✅ Download from MODEM - reuses CallSession logic
         */
        private File downloadRecordingSimplified() throws Exception {
            if (modemRecordPath == null || localRecordFilename == null) {
                log.warn("⚠️ [{}] No modem path to download", com);
                return null;
            }

            log.info("📥 [{}] Downloading recording from modem: {}", com, modemRecordPath);

            // Get file info from modem
            Optional<ModemFileInfo> infoOpt = getModemFileInfo(modemRecordPath);
            if (infoOpt.isEmpty()) {
                log.error("❌ [{}] Cannot determine modem file size for {}", com, modemRecordPath);
                return null;
            }

            ModemFileInfo info = infoOpt.get();
            long expectedSize = info.size;
            String actualModemPath = normalizeModemPath(info.rawPath);

            log.info("📥 [{}] Downloading {} bytes from modem: {}", com, expectedSize, actualModemPath);

            // Open and read file from modem
            Integer handle = openModemFile(actualModemPath);
            if (handle == null) {
                log.error("❌ [{}] Cannot open modem file: {}", com, actualModemPath);
                return null;
            }

            byte[] audioData = readModemFile(handle, expectedSize);
            closeModemFile(handle);

            if (audioData == null || audioData.length == 0) {
                log.error("❌ [{}] Downloaded 0 bytes from modem", com);
                return null;
            }

            log.info("✅ [{}] Downloaded {} bytes from modem", com, audioData.length);

            // Add WAV header if needed
            byte[] finalData = audioData;
            if (localRecordFilename.endsWith(".wav")) {
                finalData = addWavHeader(audioData, 16); // Assume WAV_ADPCM format
                log.info("✅ [{}] Added WAV header ({} → {} bytes)", com, audioData.length, finalData.length);
            }

            // Write to local file
            try {
                Files.createDirectories(DEFAULT_RECORD_DIR);
            } catch (Exception e) {
                log.warn("⚠️ [{}] Cannot create record dir: {}", com, e.getMessage());
            }

            File localFile = DEFAULT_RECORD_DIR.resolve(localRecordFilename).toFile();
            Files.write(localFile.toPath(), finalData);

            log.info("✅ [{}] Saved local file: {} ({} bytes)", com, localFile.getAbsolutePath(), localFile.length());
            return localFile;
        }

        // ====== Helper Methods (copied from CallSession) ======
        private void deleteModemFileSilently(String modemPath) {
            if (modemPath == null)
                return;
            try {
                String normalized = normalizeModemPath(modemPath);
                helper.sendAndRead("AT+QFDEL=\"" + normalized + "\"", 500);
            } catch (Exception ignore) {
            }
        }

        private Optional<ModemFileInfo> getModemFileInfo(String modemPath) throws Exception {
            if (modemPath == null || modemPath.isBlank()) {
                return Optional.empty();
            }

            String resp = helper.sendAndRead("AT+QFLST", 2000);
            Matcher matcher = Pattern.compile("\\+QFLST:\\s*\"([^\"]+)\",(\\d+)").matcher(resp);
            String target = canonicalizeModemPath(modemPath);

            while (matcher.find()) {
                String rawPath = matcher.group(1);
                long size = Long.parseLong(matcher.group(2));
                String candidate = canonicalizeModemPath(rawPath);
                if (candidate.equals(target)) {
                    return Optional.of(new ModemFileInfo(rawPath, candidate, size));
                }
            }

            return Optional.empty();
        }

        private String canonicalizeModemPath(String modemPath) {
            return normalizeModemPath(modemPath).toUpperCase(Locale.ROOT);
        }

        private String normalizeModemPath(String modemPath) {
            if (modemPath == null)
                return "";
            String normalized = modemPath.trim().replace("\\", "/");
            String upper = normalized.toUpperCase(Locale.ROOT);
            while (upper.startsWith("UFS:UFS:")) {
                normalized = "UFS:" + normalized.substring(8);
                upper = normalized.toUpperCase(Locale.ROOT);
            }
            if (!upper.startsWith("UFS:") && !upper.startsWith("RAM:") && !upper.startsWith("SD:")) {
                normalized = "UFS:" + normalized;
            }
            return normalized;
        }

        private Integer openModemFile(String modemPath) throws Exception {
            String normalized = normalizeModemPath(modemPath);
            String resp = helper.sendAndRead("AT+QFOPEN=\"" + normalized + "\",0", 2000);
            Matcher m = Pattern.compile("\\+QFOPEN:\\s*(\\d+)").matcher(resp);
            if (m.find()) {
                return Integer.parseInt(m.group(1));
            }
            return null;
        }

        private byte[] readModemFile(Integer handle, long expectedSize) throws Exception {
            log.info("📥 [{}] Reading {} bytes from modem (handle={})", com, expectedSize, handle);

            final int CHUNK_SIZE = 1024;
            ByteArrayOutputStream allData = new ByteArrayOutputStream();
            long bytesRead = 0;
            int lastLoggedPercent = -1;

            while (bytesRead < expectedSize) {
                int toRead = (int) Math.min(CHUNK_SIZE, expectedSize - bytesRead);
                byte[] chunk = readModemFileChunk(handle, toRead);
                if (chunk == null || chunk.length == 0) {
                    log.warn("⚠️ [{}] No more data from modem (expected {} more bytes)",
                            com, expectedSize - bytesRead);
                    break;
                }
                allData.write(chunk);
                bytesRead += chunk.length;

                // ✅ Send progress update every 10%
                int percent = (int) ((bytesRead * 100) / expectedSize);
                if (percent / 10 > lastLoggedPercent / 10) {
                    lastLoggedPercent = percent;
                    String progressMsg = String.format("Đang tải: %d%% (%d/%d KB)",
                            percent, bytesRead / 1024, expectedSize / 1024);
                    log.info("📥 [{}] Download progress: {}%", com, percent);

                    // Notify progress to remote server
                    notifyCallStatus("RECORDING_DOWNLOAD", task, percent, progressMsg);
                }
            }

            log.info("✅ [{}] Read {} bytes from modem", com, bytesRead);
            return allData.toByteArray();
        }

        private void closeModemFile(Integer handle) {
            if (handle == null)
                return;
            try {
                helper.sendAndRead("AT+QFCLOSE=" + handle, 1000);
                log.debug("🔒 [{}] Closed file handle {}", com, handle);
            } catch (Exception e) {
                log.warn("⚠️ [{}] Failed to close handle {}: {}", com, handle, e.getMessage());
            }
        }

        private byte[] readModemFileChunk(int handle, int bytesToRead) throws Exception {
            if (bytesToRead <= 0)
                return new byte[0];

            helper.send("AT+QFREAD=" + handle + "," + bytesToRead);
            if (!waitForConnectBanner(3000)) {
                return new byte[0];
            }

            byte[] payload = readExactBytes(bytesToRead, 10000);
            consumeOkResponse(2000);
            return payload;
        }

        private boolean waitForConnectBanner(int timeoutMs) throws Exception {
            InputStream in = port.getInputStream();
            StringBuilder buffer = new StringBuilder();
            long deadline = System.currentTimeMillis() + timeoutMs;

            while (System.currentTimeMillis() < deadline) {
                if (in.available() <= 0) {
                    Thread.sleep(5);
                    continue;
                }

                int val = in.read();
                if (val < 0)
                    continue;
                char c = (char) val;
                buffer.append(c);

                String text = buffer.toString();
                if (text.contains("CONNECT")) {
                    consumeLineEnding(in, timeoutMs);
                    return true;
                }

                if (text.contains("ERROR") || text.contains("+CME")) {
                    log.error("❌ [{}] QFREAD error: {}", com, text.replace("\r", " ").replace("\n", " "));
                    return false;
                }
            }

            log.warn("⚠️ [{}] Timeout waiting for CONNECT banner", com);
            return false;
        }

        private void consumeLineEnding(InputStream in, int timeoutMs) throws Exception {
            long deadline = System.currentTimeMillis() + timeoutMs;

            while (System.currentTimeMillis() < deadline) {
                if (in.available() <= 0) {
                    Thread.sleep(5);
                    continue;
                }

                int val = in.read();
                if (val < 0)
                    continue;

                if (val == '\n') {
                    return;
                }
            }
        }

        private byte[] readExactBytes(int count, int timeoutMs) throws Exception {
            InputStream in = port.getInputStream();
            byte[] buffer = new byte[count];
            int offset = 0;
            long deadline = System.currentTimeMillis() + timeoutMs;

            while (offset < count && System.currentTimeMillis() < deadline) {
                if (in.available() <= 0) {
                    Thread.sleep(5);
                    continue;
                }
                int read = in.read(buffer, offset, count - offset);
                if (read > 0) {
                    offset += read;
                }
            }

            if (offset < count) {
                throw new IOException("Timeout: read " + offset + "/" + count + " bytes");
            }

            return buffer;
        }

        private void consumeOkResponse(int timeoutMs) throws Exception {
            InputStream in = port.getInputStream();
            StringBuilder buffer = new StringBuilder();
            long deadline = System.currentTimeMillis() + timeoutMs;

            while (System.currentTimeMillis() < deadline) {
                if (in.available() <= 0) {
                    Thread.sleep(5);
                    continue;
                }

                int val = in.read();
                if (val < 0)
                    continue;
                char c = (char) val;
                buffer.append(c);

                String text = buffer.toString();
                if (text.contains("OK")) {
                    return;
                }
            }
        }

        private byte[] addWavHeader(byte[] audioData, int format) {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();

                int dataSize = audioData.length;
                int sampleRate = 8000; // Standard for voice calls
                int channels = 1; // Mono
                int bitsPerSample = 16; // 16-bit PCM

                // Adjust based on format
                if (format == 16) { // WAV_ADPCM
                    bitsPerSample = 4; // ADPCM uses 4 bits per sample
                }

                int byteRate = sampleRate * channels * bitsPerSample / 8;
                int blockAlign = channels * bitsPerSample / 8;

                // RIFF header
                out.write("RIFF".getBytes());
                out.write(intToBytes(36 + dataSize)); // File size - 8
                out.write("WAVE".getBytes());

                // fmt chunk
                out.write("fmt ".getBytes());
                out.write(intToBytes(16)); // fmt chunk size
                out.write(shortToBytes((short) (format == 16 ? 2 : 1))); // Audio format (1=PCM, 2=ADPCM)
                out.write(shortToBytes((short) channels));
                out.write(intToBytes(sampleRate));
                out.write(intToBytes(byteRate));
                out.write(shortToBytes((short) blockAlign));
                out.write(shortToBytes((short) bitsPerSample));

                // data chunk
                out.write("data".getBytes());
                out.write(intToBytes(dataSize));
                out.write(audioData);

                return out.toByteArray();

            } catch (Exception e) {
                log.warn("⚠️ [{}] Failed to add WAV header: {}", com, e.getMessage());
                return audioData; // Return original data if header fails
            }
        }

        private byte[] intToBytes(int value) {
            return new byte[] {
                    (byte) (value & 0xFF),
                    (byte) ((value >> 8) & 0xFF),
                    (byte) ((value >> 16) & 0xFF),
                    (byte) ((value >> 24) & 0xFF)
            };
        }

        private byte[] shortToBytes(short value) {
            return new byte[] {
                    (byte) (value & 0xFF),
                    (byte) ((value >> 8) & 0xFF)
            };
        }

        @Override
        public void close() {
            // ✅ FIX: Nếu recording đã bắt đầu nhưng endCall() chưa được gọi (do lỗi giữa
            // chừng),
            // thì gọi endCall() để stop recording, download file, upload và gửi callback
            if (!finished.get() && recordStarted.get()) {
                log.warn("⚠️ [{}] IncomingCallSession closing with unfinished recording! Attempting recovery...", com);
                try {
                    endCall(); // Sẽ stop recording, download, upload, và gửi callback
                    log.info("✅ [{}] Recording recovery completed during CALL_IN error cleanup", com);
                } catch (Exception e) {
                    log.error("❌ [{}] Recording recovery failed during CALL_IN close: {}", com, e.getMessage());
                }
            } else if (!finished.get()) {
                // Call chưa connected hoặc chưa record -> chỉ cần endCall() để gửi callback
                log.debug("🔄 [{}] IncomingCallSession closing without recording, calling endCall() for cleanup", com);
                try {
                    endCall();
                } catch (Exception e) {
                    log.warn("⚠️ [{}] endCall() failed during CALL_IN close: {}", com, e.getMessage());
                }
            }

            try {
                if (timeoutFuture != null)
                    timeoutFuture.cancel(true);
            } catch (Exception ignored) {
            }
            try {
                if (clccMonitorFuture != null)
                    clccMonitorFuture.cancel(true);
            } catch (Exception ignored) {
            }
            try {
                if (stopFuture != null)
                    stopFuture.cancel(true);
            } catch (Exception ignored) {
            }

            // ✅ FIX: Soft-reset modem sau call để đảm bảo AT commands hoạt động đúng
            // Điều này fix lỗi "SIM not ready" khi scan sau call
            try {
                log.debug("🔄 [{}] Soft-reset modem sau CALL_IN session...", com);
                helper.sendAndRead("ATH", 500); // Đảm bảo đã hangup
                helper.sendAndRead("AT", 300); // Ping modem
                helper.sendAndRead("ATE0", 300); // Tắt echo
                helper.sendAndRead("AT+CMEE=2", 300); // Enable error reporting
                helper.sendAndRead("AT+CMGF=1", 300); // SMS text mode
                helper.sendAndRead("AT+CSCS=\"UCS2\"", 300); // UCS2 encoding
                helper.sendAndRead("AT+CNMI=2,1,0,0,0", 300); // URC for SMS
                log.debug("✅ [{}] Modem soft-reset hoàn tất (CALL_IN)", com);
            } catch (Exception e) {
                log.warn("⚠️ [{}] Soft-reset modem sau CALL_IN failed: {}", com, e.getMessage());
            }
        }
    }

    // ======================================================================
    // UPLOAD FILE GHI ÂM LÊN SERVER
    // ======================================================================
    private String uploadRecordFile(String localPath, String orderId, String serviceCode) {
        final String com = sim.getComName();

        log.info("☁️ [{}] ═══════════════════════════════════════", com);
        log.info("☁️ [{}] STARTING UPLOAD PROCESS", com);
        log.info("☁️ [{}] ═══════════════════════════════════════", com);

        try {
            // Step 1: Verify file exists
            File file = new File(localPath);
            if (!file.exists()) {
                log.error("❌ [{}] UPLOAD FAILED: File not found: {}", com, localPath);
                return null;
            }
            if (file.length() == 0) {
                log.error("❌ [{}] UPLOAD FAILED: File is empty: {}", com, localPath);
                return null;
            }

            log.info("✅ [{}] File verified: {} ({} bytes)", com, file.getName(), file.length());

            // Step 2: Prepare upload
            String uploadUrl = getUploadUrl();
            log.info("☁️ [{}] Upload URL: {}", com, uploadUrl);
            log.info("☁️ [{}] orderId: {}", com, orderId);
            log.info("☁️ [{}] serviceCode: {}", com, serviceCode);

            MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
            body.add("file", new FileSystemResource(file));
            body.add("simNumber", sim.getPhoneNumber());
            if (orderId != null)
                body.add("orderId", orderId);
            if (serviceCode != null)
                body.add("serviceCode", serviceCode);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.MULTIPART_FORM_DATA);

            HttpEntity<MultiValueMap<String, Object>> request = new HttpEntity<>(body, headers);
            RestTemplate restTemplate = new RestTemplate();

            // Step 3: Upload to server (BLOCKING)
            log.info("☁️ [{}] Uploading {} bytes... (this may take a while)", com, file.length());
            long uploadStart = System.currentTimeMillis();

            ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                    uploadUrl,
                    HttpMethod.POST,
                    request,
                    new ParameterizedTypeReference<Map<String, Object>>() {
                    });

            long uploadDuration = System.currentTimeMillis() - uploadStart;
            log.info("☁️ [{}] Upload completed in {}ms", com, uploadDuration);

            // Step 4: Parse response
            if (response.getStatusCode().is2xxSuccessful()) {
                Map<String, Object> resp = response.getBody();
                String returnedUrl = null;
                if (resp != null && resp.get("url") != null) {
                    returnedUrl = String.valueOf(resp.get("url"));
                }

                if (returnedUrl != null) {
                    log.info("✅ [{}] ═══════════════════════════════════════", com);
                    log.info("✅ [{}] UPLOAD SUCCESS!", com);
                    log.info("✅ [{}] URL: {}", com, returnedUrl);
                    log.info("✅ [{}] ═══════════════════════════════════════", com);
                } else {
                    log.error("❌ [{}] Upload returned HTTP 200 but no URL in response!", com);
                    log.error("❌ [{}] Response body: {}", com, resp);
                }

                // Xóa file local sau khi upload thành công
                try {
                    Files.deleteIfExists(file.toPath());
                    log.info("🗑️ [{}] Local file deleted: {}", com, file.getName());
                } catch (Exception e) {
                    log.warn("⚠️ [{}] Could not delete local file: {}", com, e.getMessage());
                }

                return returnedUrl;
            } else {
                log.error("❌ [{}] ═══════════════════════════════════════", com);
                log.error("❌ [{}] UPLOAD FAILED!", com);
                log.error("❌ [{}] HTTP Status: {}", com, response.getStatusCode());
                log.error("❌ [{}] ═══════════════════════════════════════", com);
                return null;
            }

        } catch (Exception e) {
            log.error("❌ [{}] ═══════════════════════════════════════", com);
            log.error("❌ [{}] UPLOAD EXCEPTION!", com);
            log.error("❌ [{}] Error: {} - {}", com, e.getClass().getSimpleName(), e.getMessage());
            log.error("❌ [{}] ═══════════════════════════════════════", com);
            if (e.getCause() != null) {
                log.error("❌ [{}] Cause: {}", com, e.getCause().getMessage());
            }
            return null;
        }
    }

    // ======================================================================
    // CALLBACK & SOCKET STATUS
    // ======================================================================
    private void sendCallCallback(String com, Instant start, Instant end, Task task,
            String uploadedUrl, long connectedDuration,
            boolean connected, String failReason) {
        if (!shouldSendCallbackDebounced(task.to, (failReason == null ? "OK" : failReason))) {
            log.debug("⏳ [{}] Bỏ callback do debounce", com);
            return;
        }

        try {
            // ✅ EXISTING: Send to backend
            cloudGateway.sendCallRecord(sim, task.to, start, end, uploadedUrl,
                    task.orderId, connectedDuration, connected);

            // ✅ NEW: Save CallMessage to local database
            if (callMessageRepository != null) {
                String status;
                if (failReason != null) {
                    status = "FAILED";
                } else if (connected) {
                    status = "SUCCESS";
                } else {
                    status = "NO_ANSWER";
                }

                CallMessage callMessage = CallMessage.builder()
                        .orderId(task.orderId)
                        .accountId(null) // TODO: Get from task if available
                        .simPhone(sim.getPhoneNumber())
                        .fromNumber(sim.getPhoneNumber())
                        .toNumber(task.to)
                        .startTime(start)
                        .endTime(end)
                        .status(status)
                        .recordingPath(uploadedUrl)
                        .build();

                callMessageRepository.save(callMessage);
                log.info("✅ [{}] Saved CallMessage: orderId={}, status={}, duration={}s",
                        com, task.orderId, status, connectedDuration);
            }

            // EXISTING: Log messages
            if (failReason != null) {
                log.info("📊 [{}] Callback - THẤT BẠI: {}", com, failReason);
            } else if (connected) {
                log.info("📊 [{}] Callback - THÀNH CÔNG: {} giây đàm thoại", com, connectedDuration);
            } else {
                log.info("📊 [{}] Callback - KHÔNG BẮT MÁY", com);
            }
        } catch (Exception e) {
            log.error("❌ [{}] Gửi callback thất bại: {}", com, e.getMessage());
        }
    }

    private void notifyCallStatus(String status, Task task) {
        notifyCallStatus(status, task, null, null);
    }

    /**
     * Notify call status with optional progress and message
     * 
     * @param status   Call status
     * @param task     Task
     * @param progress Optional progress percentage (0-100)
     * @param message  Optional message
     */
    private void notifyCallStatus(String status, Task task, Integer progress, String message) {
        try {
            Map<String, Object> event = new HashMap<>();
            event.put("orderId", task.orderId);
            event.put("service", task.serviceCode);
            event.put("status", status);
            event.put("deviceName", sim.getDeviceName());
            event.put("phone", task.to);
            event.put("fromNumber", sim.getPhoneNumber());
            event.put("com", sim.getComName());
            event.put("timestamp", Instant.now().toEpochMilli());

            // Add progress if provided
            if (progress != null) {
                event.put("progress", progress);
            }

            // Add message if provided
            if (message != null) {
                event.put("message", message);
            }

            if (remoteWsClient != null && remoteWsClient.isConnected()) {
                remoteWsClient.send("/topic/receive-call", event);
                if (progress != null) {
                    log.info("📡 [{}] WS SENT REMOTE: status={} progress={}%", sim.getComName(), status, progress);
                } else {
                    log.info("📡 [{}] WS SENT REMOTE TOPIC: {}", sim.getComName(), event);
                }
            } else {
                log.warn("⚠️ [{}] RemoteWsClient unavailable, call status not sent remotely: {}",
                        sim.getComName(), status);
                log.debug("📋 [{}] Call event (not sent): {}", sim.getComName(), event);
            }

        } catch (Exception e) {
            log.warn("⚠️ [{}] notifyCallStatus error: {}", sim.getComName(), e.getMessage());
        }
    }

    private boolean shouldSendCallbackDebounced(String phoneNumber, String content) {
        String key = phoneNumber + "|" + content.hashCode();
        Instant lastTime = lastCallbackTime.get(key);
        Instant now = Instant.now();
        if (lastTime != null && Duration.between(lastTime, now).compareTo(CALLBACK_DEBOUNCE) < 0) {
            return false;
        }
        lastCallbackTime.put(key, now);
        return true;
    }

    private boolean isOutgoingSms(String sender, String body) {
        if (sender == null || sender.trim().isEmpty())
            return true;

        String cleanSender = sender.trim().toLowerCase(Locale.ROOT);
        String cleanBody = body != null ? body.toLowerCase(Locale.ROOT) : "";

        return cleanSender.isEmpty()
                || cleanSender.equals("outgoing")
                || cleanSender.equals("mt")
                || cleanSender.startsWith("+cmgs")
                || cleanBody.contains("sms submitted")
                || cleanBody.contains("message sent")
                || cleanBody.contains("send sms success")
                || cleanBody.contains("gửi tin nhắn thành công");
    }

    private boolean isDuplicateSms(String sender, String body, AtCommandHelper.SmsRecord sms) {
        if (sender == null || body == null)
            return false;
        return processedSmsCache.containsKey(buildSmsDedupKey(sender, body, sms));
    }

    private void markSmsAsProcessed(String sender, String body, AtCommandHelper.SmsRecord sms) {
        if (sender == null || body == null)
            return;
        processedSmsCache.put(buildSmsDedupKey(sender, body, sms), Instant.now());
    }

    private String buildSmsDedupKey(String sender, String body, AtCommandHelper.SmsRecord sms) {
        String bodyKey = body.length() > 100 ? body.substring(0, 100) : body;
        String timestampKey = sms != null && sms.timestamp != null && !sms.timestamp.isBlank()
                ? sms.timestamp.trim()
                : ((sms != null ? sms.storage : "") + ":" + (sms != null ? sms.index : ""));
        return sender + "|" + bodyKey + "|" + timestampKey;
    }

    private void cleanupOldProcessedSms() {
        Instant now = Instant.now();
        processedSmsCache.entrySet()
                .removeIf(entry -> Duration.between(entry.getValue(), now).compareTo(SMS_CACHE_TTL) > 0);
    }

    private Path getUploadDir() {
        try {
            Files.createDirectories(DEFAULT_RECORD_DIR);
        } catch (Exception e) {
            log.warn("⚠️ Cannot ensure recording directory {}: {}", DEFAULT_RECORD_DIR, e.getMessage());
        }
        return DEFAULT_RECORD_DIR;
    }

    private String getUploadUrl() {
        return DEFAULT_UPLOAD_URL;
    }

    // ======================================================================
    // TASK & ENUM
    // ======================================================================

    /**
     * ✅ Task Priority Levels
     * - CRITICAL: Emergency tasks (system commands)
     * - HIGH: Calls (both incoming and outgoing) - need immediate response
     * - NORMAL: SMS sending - important but can wait briefly
     * - LOW: SMS scanning for OTP - can be interrupted
     * - BACKGROUND: Maintenance tasks
     */
    public enum TaskPriority {
        CRITICAL(0), // Emergency calls, critical commands
        HIGH(1), // Regular calls, urgent SMS
        NORMAL(2), // Regular SMS, OTP receive
        LOW(3), // Scan SMS, maintenance tasks
        BACKGROUND(4); // Cleanup, monitoring

        private final int value;

        TaskPriority(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public enum TaskType {
        SEND, SCAN, CMD, CALL, CALL_IN // ✅ NEW: Incoming call (answer + record)
    }

    /**
     * ✅ Task với Priority Support
     * Implements Comparable để có thể dùng PriorityBlockingQueue
     */
    public static class Task implements Comparable<Task> {
        public TaskType type;
        public String to;
        public String content;
        public String serviceCode;
        public String orderId;
        public String localMsgId; // ✅ ADDED: Track original message ID for callback matching
        public boolean record;
        public int duration;
        public String expectedCaller; // ✅ NEW: For CALL_IN - số điện thoại dự kiến gọi đến
        public boolean acceptHiddenNumber; // ✅ NEW: Accept calls with hidden/private caller ID
        public int timeWindowSeconds; // ✅ NEW: Time window for accepting calls (time-based matching)
        public CompletableFuture<String> responseFuture; // ✅ NEW: For sync AT command execution (proxy)

        // ✅ RETRY: Tracking fields for auto-retry across SIMs
        public int retryCount = 0;
        public int maxRetries = 2; // Tối đa thử trên 2 SIM khác (tổng cộng 3 SIM)
        public java.util.List<String> triedComs = new java.util.ArrayList<>(); // Danh sách COM đã thử

        // ✅ NEW: Priority field
        public TaskPriority priority = TaskPriority.NORMAL;

        // ✅ NEW: Timestamp for FIFO ordering within same priority
        public final long createdAt = System.currentTimeMillis();

        public Task(TaskType type, String to, String content, String serviceCode, String orderId) {
            this.type = type;
            this.to = to;
            this.content = content;
            this.serviceCode = serviceCode;
            this.orderId = orderId;
            this.localMsgId = orderId; // Default: use orderId if no specific localMsgId
        }

        /**
         * ✅ Compare tasks by priority (lower value = higher priority)
         * If same priority, use FIFO (earlier timestamp first)
         */
        @Override
        public int compareTo(Task other) {
            int priorityCompare = Integer.compare(this.priority.getValue(), other.priority.getValue());
            if (priorityCompare != 0) {
                return priorityCompare;
            }
            // Same priority → FIFO
            return Long.compare(this.createdAt, other.createdAt);
        }

        /**
         * ✅ Check if task is urgent (requires immediate attention)
         */
        public boolean isUrgent() {
            return priority.getValue() <= TaskPriority.HIGH.getValue();
        }

        /**
         * ✅ Create CALL_OUT task with HIGH priority
         */
        public static Task call(String to, int duration, boolean record, String serviceCode, String orderId) {
            Task t = new Task(TaskType.CALL, to, null, serviceCode, orderId);
            t.duration = duration;
            t.record = record;
            t.priority = TaskPriority.HIGH; // ✅ Calls = HIGH priority
            return t;
        }

        /**
         * ✅ Create CALL_IN task with HIGH priority
         */
        public static Task callIn(String expectedCaller, int duration, boolean record, String serviceCode,
                String orderId) {
            Task t = new Task(TaskType.CALL_IN, null, null, serviceCode, orderId);
            t.expectedCaller = expectedCaller; // ✅ Số điện thoại dự kiến
            t.duration = duration;
            t.record = record;
            t.acceptHiddenNumber = false; // Default: reject hidden numbers
            t.timeWindowSeconds = 300; // Default: 5 minutes window
            t.priority = TaskPriority.HIGH; // ✅ Incoming calls = HIGH priority
            return t;
        }

        /**
         * ✅ Create CALL_IN with hidden number support and HIGH priority
         */
        public static Task callInWithHidden(String expectedCaller, int duration, boolean record,
                boolean acceptHidden, int timeWindowSeconds,
                String serviceCode, String orderId) {
            Task t = callIn(expectedCaller, duration, record, serviceCode, orderId);
            t.acceptHiddenNumber = acceptHidden;
            t.timeWindowSeconds = timeWindowSeconds;
            // Priority already set to HIGH by callIn()
            return t;
        }

        /**
         * ✅ Create SMS SEND task with NORMAL priority
         */
        public static Task send(String to, String content, String serviceCode, String orderId) {
            Task t = new Task(TaskType.SEND, to, content, serviceCode, orderId);
            t.priority = TaskPriority.NORMAL; // ✅ SMS sending = NORMAL priority
            return t;
        }

        /**
         * ✅ Create SMS SEND task with localMsgId for SMPP/API callback matching
         */
        public static Task sendWithMsgId(String to, String content, String serviceCode, String orderId,
                String localMsgId) {
            Task t = new Task(TaskType.SEND, to, content, serviceCode, orderId);
            t.localMsgId = localMsgId; // ✅ Use specific localMsgId for callback
            t.priority = TaskPriority.NORMAL;
            return t;
        }

        /**
         * ✅ Clone task for retry on different SIM - giữ nguyên orderId, localMsgId, nội dung
         */
        public Task cloneForRetry() {
            Task t = new Task(this.type, this.to, this.content, this.serviceCode, this.orderId);
            t.localMsgId = this.localMsgId;
            t.priority = this.priority;
            t.retryCount = this.retryCount + 1;
            t.maxRetries = this.maxRetries;
            t.triedComs = new java.util.ArrayList<>(this.triedComs); // Copy danh sách đã thử
            return t;
        }

        /**
         * ✅ Create SMS SCAN task with LOW priority (can be interrupted)
         */
        public static Task scan() {
            return scan(TaskPriority.LOW);
        }

        public static Task scan(TaskPriority priority) {
            Task t = new Task(TaskType.SCAN, null, null, null, null);
            t.priority = priority != null ? priority : TaskPriority.LOW;
            return t;
        }

        /**
         * ✅ Create CMD task with configurable priority (default NORMAL)
         */
        public static Task cmd(String command, String serviceCode, String orderId) {
            Task t = new Task(TaskType.CMD, null, command, serviceCode, orderId);
            t.priority = TaskPriority.NORMAL; // ✅ Commands = NORMAL priority
            return t;
        }

        /**
         * ✅ Create CRITICAL priority CMD task
         */
        public static Task criticalCmd(String command, String serviceCode, String orderId) {
            Task t = new Task(TaskType.CMD, null, command, serviceCode, orderId);
            t.priority = TaskPriority.CRITICAL; // ✅ Critical commands
            return t;
        }

    }
}
