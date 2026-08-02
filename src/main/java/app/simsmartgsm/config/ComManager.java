package app.simsmartgsm.config;

import app.simsmartgsm.baseGateway.CloudGateway;
import app.simsmartgsm.baseGateway.GsmProperties;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.CallMessageRepository;
import app.simsmartgsm.service.GsmListenerService;
import app.simsmartgsm.uitils.DeviceIdProvider;
import app.simsmartgsm.uitils.PortWorker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.uitils.SimStatus;
import java.time.Instant;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

/**
 * ✅ ComManager
 * Quản lý toàn bộ PortWorker (mỗi worker = 1 modem vật lý / COM port).
 * Hỗ trợ 1 SIM có thể phục vụ nhiều dịch vụ cùng lúc (đa thuê) thông qua hàng
 * đợi tuần tự.
 */
@Component
@Lazy(false)
@Slf4j
public class ComManager {

    private static final int DEFAULT_CALL_DURATION_SECONDS = 60; // Thời lượng gọi mặc định nếu không truyền từ ngoài

    private static ComManager INSTANCE;

    private final RemoteWsClient remoteWsClient;

    /** Danh sách các PortWorker đang hoạt động, key = COM name */
    private final ConcurrentHashMap<String, PortWorker> workers = new ConcurrentHashMap<>();

    /** ✅ Track worker threads for proper shutdown */
    private final ConcurrentHashMap<String, Thread> workerThreads = new ConcurrentHashMap<>();

    /**
     * ✅ Danh sách các port đang bị LOCK cho direct call
     * Khi port bị lock, PortWorker KHÔNG được phép mở port đó
     * Recovery job cũng KHÔNG được phép restart worker cho port đó
     */
    private final Set<String> lockedPorts = ConcurrentHashMap.newKeySet();

    private final GsmListenerService gsmListenerService;
    private final CloudGateway cloudGateway;
    private final SimRepository simRepository;
    private final CallMessageRepository callMessageRepository;
    private final app.simsmartgsm.service.CallService callService;
    private final app.simsmartgsm.service.SmsDailyLimitService smsDailyLimitService; // ✅ Daily SMS limit
    private final GsmProperties gsmProperties;

    public ComManager(RemoteWsClient remoteWsClient,
            @Lazy GsmListenerService gsmListenerService,
            CloudGateway cloudGateway,
            SimRepository simRepository,
            CallMessageRepository callMessageRepository,
            @Lazy app.simsmartgsm.service.CallService callService,
            app.simsmartgsm.service.SmsDailyLimitService smsDailyLimitService,
            GsmProperties gsmProperties) {
        this.remoteWsClient = remoteWsClient;
        this.gsmListenerService = gsmListenerService;
        this.cloudGateway = cloudGateway;
        this.simRepository = simRepository;
        this.callMessageRepository = callMessageRepository;
        this.callService = callService;
        this.smsDailyLimitService = smsDailyLimitService;
        this.gsmProperties = gsmProperties;
    }

    @PostConstruct
    private void init() {
        INSTANCE = this;

        // ✅ JVM Shutdown Hook - chạy ngay cả khi app crash (OutOfMemory, etc)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.warn("⚠️ JVM Shutdown Hook triggered - forcing SIM status updates");
            updateAllSimsToInactive();
        }, "ComManager-ShutdownHook"));

        log.info("✅ ComManager initialized (singleton instance + JVM shutdown hook)");
    }

    @PreDestroy
    private void onShutdown() {
        log.warn("🛑 Application shutting down (@PreDestroy) - updating all SIM statuses to INACTIVE");
        updateAllSimsToInactive();
    }

    private final java.util.concurrent.atomic.AtomicBoolean shutdownExecuted = new java.util.concurrent.atomic.AtomicBoolean(false);

    /**
     * ✅ Shared method: Update tất cả SIM thành INACTIVE
     * Được gọi từ cả @PreDestroy và JVM Shutdown Hook
     */
    private void updateAllSimsToInactive() {
        if (!shutdownExecuted.compareAndSet(false, true)) {
            return;
        }
        try {
            java.util.List<Sim> simsToUpdate = new java.util.ArrayList<>();
            for (PortWorker worker : workers.values()) {
                try {
                    Sim sim = worker.getSim();
                    if (sim != null) {
                        sim.setStatus(String.valueOf(SimStatus.INACTIVE));
                        sim.setLastUpdated(Instant.now());
                        simsToUpdate.add(sim);
                    }
                } catch (Exception e) {
                    log.error("❌ Error collecting SIM status on shutdown: {}", e.getMessage());
                }
            }
            if (!simsToUpdate.isEmpty()) {
                simRepository.saveAll(simsToUpdate);
                log.info("✅ Shutdown complete - updated {} SIM(s) to INACTIVE in bulk", simsToUpdate.size());
            } else {
                log.info("✅ Shutdown complete - no SIMs needed updating");
            }
        } catch (Exception e) {
            log.error("❌ Error during shutdown handler: {}", e.getMessage(), e);
        }
    }

    // =====================================================================
    // LẤY / TẠO WORKER
    // =====================================================================

    /**
     * 🔹 Lấy hoặc tạo worker nếu chưa tồn tại.
     * - Chỉ dựa vào isRunning(), KHÔNG check isOpen() vì PortWorker tự
     * ensurePort().
     */
    public synchronized PortWorker getOrCreateWorker(Sim sim) {
        if (sim == null || sim.getComName() == null || sim.getComName().isBlank()) {
            log.warn("⚠️ SIM không hợp lệ hoặc thiếu COM name");
            return null;
        }

        String com = sim.getComName();
        PortWorker existing = workers.get(com);

        // ⚙️ Nếu đã có worker còn chạy thì dùng lại
        if (existing != null && existing.isRunning()) {
            return existing;
        }

        // ⚙️ Nếu worker cũ đã dừng thì bỏ khỏi map
        if (existing != null && !existing.isRunning()) {
            log.warn("♻️ Worker cũ cho {} không còn hoạt động, khởi tạo lại.", com);
            workers.remove(com);
        }

        // ⚙️ Tạo mới worker (luôn truyền remoteWsClient đầy đủ)
        // ✅ OPTIMIZE: Aggressive polling - 1000ms (do URC không hoạt động trên nhiều
        // module)
        PortWorker w = new PortWorker(
                sim,
                gsmListenerService,
                cloudGateway,
                remoteWsClient,
                callMessageRepository,
                callService,
                smsDailyLimitService,
                gsmProperties);
        w.setComManager(this); // ✅ For auto-switch SIM khi hết quota
        workers.put(com, w);

        Thread t = new Thread(w, "PortWorker-" + com);
        t.setDaemon(true);
        t.start();
        workerThreads.put(com, t); // ✅ Track thread

        log.debug("🆕 Worker created and started for {}", com);
        return w;
    }

    /** 🔹 Lấy worker hiện có theo COM (không tự tạo) */
    public PortWorker getWorker(String comName) {
        if (comName == null)
            return null;
        return workers.get(comName);
    }

    /**
     * ✅ Check xem PortWorker có đang chạy cho port này không
     * Dùng để tránh xung đột khi scan SIM
     */
    public boolean isWorkerRunning(String comName) {
        if (comName == null || comName.isBlank()) {
            return false;
        }
        PortWorker worker = workers.get(comName);
        return worker != null && worker.isRunning();
    }

    /**
     * 🔹 Lấy hoặc khởi tạo worker theo SIM, cho phép override listener.
     * - CHỦ YẾU dùng nếu bạn muốn PortWorker dùng GsmListenerService khác tạm thời.
     */
    public synchronized PortWorker getWorker(Sim sim, GsmListenerService listener) {
        if (sim == null || sim.getComName() == null || sim.getComName().isBlank()) {
            log.warn("⚠️ SIM không hợp lệ hoặc thiếu COM name (sim={})",
                    sim != null ? sim.getPhoneNumber() : "null");
            return null;
        }

        String com = sim.getComName();
        PortWorker existing = workers.get(com);

        if (existing != null && existing.isRunning()) {
            return existing;
        }

        if (existing != null) {
            workers.remove(com);
            log.warn("♻️ Worker cũ cho {} đã được dọn dẹp (không còn hoạt động).", com);
        }

        log.debug("🆕 Khởi tạo PortWorker mới cho {}", com);
        // ✅ OPTIMIZE: Aggressive polling - 1000ms (do URC không hoạt động trên nhiều
        // module)
        PortWorker worker = new PortWorker(
                sim,
                listener != null ? listener : gsmListenerService,
                cloudGateway,
                remoteWsClient,
                callMessageRepository,
                callService,
                smsDailyLimitService,
                gsmProperties);
        worker.setComManager(this); // ✅ For auto-switch SIM khi hết quota
        workers.put(com, worker);

        Thread thread = new Thread(worker, "PortWorker-" + com);
        thread.setDaemon(true);
        thread.start();
        workerThreads.put(com, thread); // ✅ Track thread
        log.debug("🚀 Worker for {} started successfully", com);
        return worker;
    }

    public ConcurrentHashMap<String, PortWorker> getWorkers() {
        return workers;
    }

    /**
     * ✅ Tìm worker có quota SMS còn lại, bỏ qua SIM hiện tại
     * Dùng khi SIM hiện tại đạt daily limit → auto-switch sang SIM khác
     * 
     * @param excludeComName COM port cần bỏ qua (SIM hiện tại đã hết quota)
     * @return PortWorker có quota, hoặc null nếu tất cả đều hết
     */
    public PortWorker findWorkerWithQuota(String excludeComName) {
        return workers.entrySet().stream()
                .filter(e -> !e.getKey().equals(excludeComName))
                .filter(e -> e.getValue().isRunning() && e.getValue().isOpen())
                .filter(e -> {
                    Sim sim = e.getValue().getSim();
                    return sim == null || sim.isAllowSms();
                })
                .filter(e -> smsDailyLimitService.canSend(e.getKey()))
                .min((a, b) -> {
                    // Ưu tiên SIM có queue ít task nhất
                    return Integer.compare(a.getValue().getQueueSize(), b.getValue().getQueueSize());
                })
                .map(java.util.Map.Entry::getValue)
                .orElse(null);
    }

    /**
     * ✅ Tìm worker tốt nhất cho retry SMS
     * Chiến lược chọn SIM thông minh cho high-volume SMS:
     * 1. LOẠI: SIM đã thử (triedComs) → tránh gửi lại trên SIM đã fail
     * 2. LOẠI: SIM không đủ quota cho tin nhắn (tính theo segments)
     * 3. ƯU TIÊN: SIM có nhiều quota nhất (giải phóng áp lực)
     * 4. ƯU TIÊN: SIM có queue ít task nhất (gửi nhanh nhất)
     *
     * @param excludeComName COM port hiện tại (đã fail)
     * @param triedComs      Danh sách COM đã thử rồi
     * @param messageContent Nội dung tin nhắn (để tính segments cần thiết)
     * @return PortWorker tốt nhất, hoặc null nếu không còn SIM nào phù hợp
     */
    public PortWorker findWorkerForRetry(String excludeComName,
                                         java.util.List<String> triedComs,
                                         String messageContent) {
        return workers.entrySet().stream()
                // 1. Loại SIM hiện tại
                .filter(e -> !e.getKey().equals(excludeComName))
                // 2. Loại SIM đã thử rồi
                .filter(e -> triedComs == null || !triedComs.contains(e.getKey()))
                // 2.5 ✅ Loại SIM đã bị blacklist
                .filter(e -> smsDailyLimitService == null || !smsDailyLimitService.isBlacklisted(e.getKey()))
                // 2.6 ✅ Loại SIM đã bị đánh DEAD (allowSms=false) bởi SimHealthCheckService
                .filter(e -> {
                    Sim sim = e.getValue().getSim();
                    return sim == null || sim.isAllowSms();
                })
                // 3. SIM phải đang chạy và port mở
                .filter(e -> e.getValue().isRunning() && e.getValue().isOpen())
                // 4. SIM phải còn đủ quota cho tin nhắn này (segment-aware)
                .filter(e -> smsDailyLimitService.canSend(e.getKey(), messageContent))
                // 5. Sắp xếp: ưu tiên quota nhiều nhất → queue ít nhất
                .min((a, b) -> {
                    int quotaA = smsDailyLimitService.getRemainingQuota(a.getKey());
                    int quotaB = smsDailyLimitService.getRemainingQuota(b.getKey());

                    // Ưu tiên quota nhiều hơn (giảm nguy cơ SIM bị chết)
                    int quotaCompare = Integer.compare(quotaB, quotaA); // DESC
                    if (quotaCompare != 0) return quotaCompare;

                    // Cùng quota → ưu tiên queue ít hơn (gửi nhanh hơn)
                    return Integer.compare(
                            a.getValue().getQueueSize(),
                            b.getValue().getQueueSize()); // ASC
                })
                .map(java.util.Map.Entry::getValue)
                .orElse(null);
    }

    /**
     * ✅ Stop worker cho SIM cụ thể (dùng khi SIM bị REPLACED)
     */
    public synchronized void stopWorker(String comName) {
        if (comName == null || comName.isBlank()) {
            return;
        }

        PortWorker worker = workers.get(comName);
        if (worker != null) {
            log.info("🛑 Stopping worker for {}", comName);
            worker.stop();
            workers.remove(comName);
            workerThreads.remove(comName);
            log.info("✅ Worker {} stopped and removed", comName);
        }
    }

    /**
     * ✅ Stop worker VÀ ĐỢI thread kết thúc hoàn toàn
     * Dùng trước khi mở port trực tiếp (direct call) để tránh race condition
     */
    public synchronized boolean stopWorkerAndWait(String comName, long timeoutMs) {
        if (comName == null || comName.isBlank()) {
            return true;
        }

        PortWorker worker = workers.get(comName);
        Thread workerThread = workerThreads.get(comName);

        if (worker != null) {
            log.info("🛑 Stopping worker for {} and waiting for thread to exit...", comName);
            worker.stop();
            workers.remove(comName);
            workerThreads.remove(comName);
        }

        if (workerThread != null && workerThread.isAlive()) {
            try {
                workerThread.join(timeoutMs);
                if (workerThread.isAlive()) {
                    log.warn("⚠️ Worker thread {} vẫn chưa thoát sau {}ms", comName, timeoutMs);
                    workerThread.interrupt();
                    workerThread.join(1000);
                    return !workerThread.isAlive();
                }
                log.info("✅ Worker thread {} đã thoát hoàn toàn", comName);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("⚠️ Interrupted while waiting for worker thread {}", comName);
                return false;
            }
        }

        return true;
    }

    // =====================================================================
    // PORT LOCKING (cho direct call)
    // =====================================================================

    /** ✅ Lock port để ngăn PortWorker/recovery job mở lại port */
    public void lockPort(String comName) {
        if (comName != null) {
            lockedPorts.add(comName);
            log.info("🔒 Port {} LOCKED for direct call", comName);
        }
    }

    /** ✅ Unlock port để PortWorker có thể hoạt động lại */
    public void unlockPort(String comName) {
        if (comName != null) {
            lockedPorts.remove(comName);
            log.info("🔓 Port {} UNLOCKED", comName);
        }
    }

    /** ✅ Check xem port có đang bị lock cho direct call không */
    public boolean isPortLocked(String comName) {
        return comName != null && lockedPorts.contains(comName);
    }

    /**
     * ✅ Stop worker cho SIM bị REPLACED
     * ⚠️ CHÚ Ý: Chỉ stop worker nếu không có task đang xử lý
     */
    public synchronized void stopWorkerForReplacedSim(Sim sim) {
        if (sim == null || sim.getComName() == null) {
            return;
        }

        String comName = sim.getComName();
        PortWorker worker = workers.get(comName);

        // ✅ KIỂM TRA: Nếu worker đang có task trong queue, KHÔNG stop
        if (worker != null && worker.getQueueSize() > 0) {
            log.warn("⚠️ KHÔNG stop worker {} vì đang có {} task trong queue (SIM REPLACED sẽ xử lý sau)",
                    comName, worker.getQueueSize());
            return;
        }

        log.warn("🚨 SIM {} (COM: {}) đã bị REPLACED, stopping worker...",
                sim.getCcid(), comName);
        stopWorker(comName);
    }

    // =====================================================================
    // START / SCAN
    // =====================================================================

    /** 🔹 Khởi động worker cho 1 SIM (chỉ 1 lần / COM) */
    public synchronized void startWorker(Sim sim) {
        if (sim == null || sim.getComName() == null || sim.getComName().isBlank()) {
            log.warn("⚠️ SIM không hợp lệ hoặc thiếu COM name (sim={})",
                    sim != null ? sim.getPhoneNumber() : "null");
            return;
        }

        PortWorker worker = getOrCreateWorker(sim);
        if (worker != null) {
            log.debug("🚀 PortWorker for {} sẵn sàng (isRunning={})",
                    sim.getComName(), worker.isRunning());
        }
    }

    /** 🔹 Quét toàn bộ port đang hoạt động để đọc lại SMS chưa đọc */
    public void scanAllActivePorts() {
        if (workers.isEmpty()) {
            log.info("📭 Không có port nào đang hoạt động để quét.");
            return;
        }

        log.debug("🔄 Bắt đầu quét {} port đang hoạt động...", workers.size());
        workers.forEach((com, worker) -> {
            try {
                if (worker != null && worker.isRunning()) {
                    worker.forceScan();
                    log.debug("📡 Đã gửi yêu cầu SCAN cho {}", com);
                } else {
                    log.debug("⚠️ Bỏ qua {} vì worker không hoạt động.", com);
                }
            } catch (Exception e) {
                log.error("❌ Lỗi khi quét port {}: {}", com, e.getMessage());
            }
        });
    }

    /**
     * Nếu số đích là SIM local trên cùng thiết bị, đảm bảo worker nhận đã chạy trước
     * khi gửi. Case SIM nội bộ gửi qua lại rất dễ miss nếu COM đích chưa có listener.
     */
    public PortWorker ensureLocalReceiverReady(String phoneNumber, String senderComName) {
        Optional<Sim> targetOpt = findLocalSimByPhone(phoneNumber);
        if (targetOpt.isEmpty()) {
            return null;
        }

        Sim target = targetOpt.get();
        String targetCom = target.getComName();
        if (targetCom == null || targetCom.isBlank()) {
            return null;
        }
        if (senderComName != null && senderComName.equalsIgnoreCase(targetCom)) {
            return workers.get(targetCom);
        }
        if (isPortLocked(targetCom)) {
            log.debug("⏭️ Skip receiver warmup for {} because port is locked", targetCom);
            return workers.get(targetCom);
        }

        PortWorker receiver = getOrCreateWorker(target);
        if (receiver != null) {
            receiver.forceScan();
            log.info("📥 [{}] Local receiver warmed for phone {} (target COM={})",
                    senderComName, phoneNumber, targetCom);
        }
        return receiver;
    }

    /**
     * Force scan SIM đích nếu số nhận thuộc cùng thiết bị.
     */
    public boolean forceScanLocalReceiver(String phoneNumber, String senderComName) {
        Optional<Sim> targetOpt = findLocalSimByPhone(phoneNumber);
        if (targetOpt.isEmpty()) {
            return false;
        }

        Sim target = targetOpt.get();
        String targetCom = target.getComName();
        if (targetCom == null || targetCom.isBlank()) {
            return false;
        }
        if (senderComName != null && senderComName.equalsIgnoreCase(targetCom)) {
            return false;
        }

        PortWorker receiver = workers.get(targetCom);
        if (receiver == null || !receiver.isRunning()) {
            receiver = getOrCreateWorker(target);
        }
        if (receiver == null) {
            return false;
        }

        receiver.forceScan();
        log.info("🔎 [{}] Force scan local receiver {} for phone {}", senderComName, targetCom, phoneNumber);
        return true;
    }

    private Optional<Sim> findLocalSimByPhone(String phoneNumber) {
        Set<String> variants = phoneVariants(phoneNumber);
        if (variants.isEmpty()) {
            return Optional.empty();
        }

        String deviceName = DeviceIdProvider.getDeviceId();
        List<Sim> localSims = simRepository.findByDeviceName(deviceName);
        Optional<Sim> matched = localSims.stream()
                .filter(sim -> sim.getComName() != null && !sim.getComName().isBlank())
                .filter(sim -> !SimStatus.REPLACED.name().equalsIgnoreCase(sim.getStatus()))
                .filter(sim -> !java.util.Collections.disjoint(phoneVariants(sim.getPhoneNumber()), variants))
                .findFirst();

        if (matched.isPresent()) {
            return matched;
        }

        for (String variant : variants) {
            Optional<Sim> exact = simRepository.findFirstByPhoneNumber(variant)
                    .filter(sim -> sim.getComName() != null && !sim.getComName().isBlank())
                    .filter(sim -> deviceName.equals(sim.getDeviceName()));
            if (exact.isPresent()) {
                return exact;
            }
        }

        return Optional.empty();
    }

    private Set<String> phoneVariants(String phoneNumber) {
        Set<String> variants = new LinkedHashSet<>();
        if (phoneNumber == null) {
            return variants;
        }

        String cleaned = phoneNumber.replaceAll("[^0-9+]", "");
        if (cleaned.isBlank()) {
            return variants;
        }

        variants.add(cleaned);
        String digits = cleaned.startsWith("+") ? cleaned.substring(1) : cleaned;
        variants.add(digits);

        if (cleaned.startsWith("+81") && cleaned.length() > 3) {
            variants.add("0" + cleaned.substring(3));
        } else if (digits.startsWith("81") && digits.length() > 2) {
            variants.add("+81" + digits.substring(2));
            variants.add("0" + digits.substring(2));
        } else if (digits.startsWith("0") && digits.length() > 1) {
            variants.add("+81" + digits.substring(1));
            variants.add("81" + digits.substring(1));
        }

        return variants;
    }

    // =====================================================================
    // SMS
    // =====================================================================

    /** 🔹 Gửi SMS qua hàng đợi của PortWorker (tuần tự, không mở port trực tiếp) */
    public void sendSms(Sim sim, String toNumber, String content, String serviceCode, String orderId) {
        if (sim == null || sim.getComName() == null || sim.getComName().isBlank()) {
            log.warn("⚠️ SIM không hợp lệ khi gửi SMS");
            return;
        }

        try {
            // ✅ Lấy hoặc tạo worker
            PortWorker worker = getOrCreateWorker(sim);
            if (worker == null) {
                log.error("❌ Không thể khởi tạo worker cho {}", sim.getComName());
                return;
            }

            // ✅ Đưa task gửi SMS vào hàng đợi
            worker.enqueue(PortWorker.TaskType.SEND, toNumber, content, serviceCode, orderId);

            log.debug("📩 [QUEUE] Đã enqueue gửi SMS {} → {} | content='{}'",
                    sim.getComName(), toNumber, content);

        } catch (Exception e) {
            log.error("❌ Lỗi khi enqueue SMS {} → {}: {}", sim.getComName(), toNumber, e.getMessage());
        }
    }

    // =====================================================================
    // CALL
    // =====================================================================

    /**
     * 🔹 Thực hiện cuộc gọi ra qua PortWorker.
     *
     * @param sim          SIM dùng để gọi
     * @param targetNumber Số cần gọi
     * @param record       Có ghi âm hay không
     * @param serviceCode  Mã dịch vụ
     * @param orderId      Mã order
     */
    public void makeCall(Sim sim,
            String targetNumber,
            boolean record,
            int duration, // ✅ NEW: Duration parameter
            String serviceCode,
            String orderId) {
        if (sim == null || sim.getComName() == null || sim.getComName().isBlank()) {
            log.warn("⚠️ SIM không hợp lệ khi thực hiện call");
            return;
        }
        if (targetNumber == null || targetNumber.isBlank()) {
            log.warn("⚠️ Số gọi ra không hợp lệ");
            return;
        }

        try {
            PortWorker worker = getOrCreateWorker(sim);
            if (worker == null) {
                log.error("❌ Không thể khởi tạo worker cho {} khi CALL", sim.getComName());
                return;
            }

            // ✅ Use provided duration or default
            int callDuration = duration > 0 ? duration : DEFAULT_CALL_DURATION_SECONDS;

            PortWorker.Task callTask = PortWorker.Task.call(
                    targetNumber,
                    callDuration,
                    record,
                    serviceCode,
                    orderId);

            worker.enqueue(callTask);

            log.debug("📞 [QUEUE] Đã enqueue CALL {} → {} | record={} | duration={}s | serviceCode={} | orderId={}",
                    sim.getComName(), targetNumber, record, callDuration, serviceCode, orderId);

        } catch (Exception e) {
            log.error("❌ Lỗi enqueue call {} → {}: {}", sim.getComName(), targetNumber, e.getMessage());
        }
    }

    /**
     * ✅ NEW: Nhận cuộc gọi đến (CALL_IN) - SIMPLE VERSION
     * Worker sẽ đợi cuộc gọi đến, tự động trả lời, ghi âm và upload
     *
     * @param sim            SIM dùng để nhận cuộc gọi
     * @param expectedCaller Số điện thoại DỰ KIẾN gọi đến (bắt buộc để match order)
     * @param duration       Thời lượng tối đa cuộc gọi (giây)
     * @param record         Có ghi âm hay không
     * @param serviceCode    Mã dịch vụ
     * @param orderId        Mã order
     */
    public void waitForIncomingCall(Sim sim,
            String expectedCaller,
            int duration,
            boolean record,
            String serviceCode,
            String orderId) {
        waitForIncomingCallAdvanced(sim, expectedCaller, duration, record,
                false, 300, serviceCode, orderId);
    }

    /**
     * ✅ NEW: Nhận cuộc gọi đến (CALL_IN) - ADVANCED VERSION
     * Hỗ trợ hidden number và time-based matching
     *
     * @param sim               SIM dùng để nhận cuộc gọi
     * @param expectedCaller    Số điện thoại DỰ KIẾN gọi đến (để match order)
     * @param duration          Thời lượng tối đa cuộc gọi (giây)
     * @param record            Có ghi âm hay không
     * @param acceptHidden      Accept calls with hidden caller ID (true/false)
     * @param timeWindowSeconds Time window để accept calls (giây) - dùng cho hidden
     *                          numbers
     * @param serviceCode       Mã dịch vụ
     * @param orderId           Mã order
     */
    public void waitForIncomingCallAdvanced(Sim sim,
            String expectedCaller,
            int duration,
            boolean record,
            boolean acceptHidden,
            int timeWindowSeconds,
            String serviceCode,
            String orderId) {
        if (sim == null || sim.getComName() == null || sim.getComName().isBlank()) {
            log.warn("⚠️ SIM không hợp lệ khi đợi CALL_IN");
            return;
        }

        if (expectedCaller == null || expectedCaller.isBlank()) {
            log.warn("⚠️ expectedCaller is required for CALL_IN to match correct order!");
        }

        try {
            PortWorker worker = getOrCreateWorker(sim);
            if (worker == null) {
                log.error("❌ Không thể khởi tạo worker cho {} khi CALL_IN", sim.getComName());
                return;
            }

            PortWorker.Task callInTask = PortWorker.Task.callInWithHidden(
                    expectedCaller, // Số điện thoại dự kiến
                    duration > 0 ? duration : DEFAULT_CALL_DURATION_SECONDS,
                    record,
                    acceptHidden, // ✅ Accept hidden numbers?
                    timeWindowSeconds, // ✅ Time window for matching
                    serviceCode,
                    orderId);

            worker.enqueue(callInTask);

            log.debug(
                    "📞 [QUEUE] Đã enqueue CALL_IN {} | expectedCaller={} | record={} | acceptHidden={} | timeWindow={}s | max_duration={}s | orderId={}",
                    sim.getComName(), expectedCaller, record, acceptHidden, timeWindowSeconds, duration, orderId);

        } catch (Exception e) {
            log.error("❌ Lỗi enqueue CALL_IN {}: {}", sim.getComName(), e.getMessage());
        }
    }
}
