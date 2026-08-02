package app.simsmartgsm.config;

import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.entity.SmsSession;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.repository.SmsSessionRepository;
import app.simsmartgsm.entity.SmsMessageEntity;
import app.simsmartgsm.repository.SmsMessageJpaRepository;
import app.simsmartgsm.uitils.DeviceIdProvider;
import app.simsmartgsm.uitils.PortWorker;
import app.simsmartgsm.uitils.SmsDecoder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.*;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Component
@RequiredArgsConstructor
@Slf4j
public class RemoteSmsJobSubscriberConfig {

    private static final String REMOTE_WS_URL = "wss://be-user-sms-global.smsglobalhub.com/ws-native";
    private static final String SUB_TOPIC = "/topic/sms-job-topic";
    private static final String INBOUND_TOPIC = "/topic/sms-inbound";
    private static final String LOCAL_RESPONSE_TOPIC = "/topic/sms-response";

    // ✅ ReconnectionManager - replaces old wsFailCount logic
    private ReconnectionManager reconnectionManager;

    private final ConcurrentHashMap<String, Long> inboundGuard = new ConcurrentHashMap<>();
    private static final long INBOUND_TTL_MS = 5000;

    // ✅ Lock for synchronizing WebSocket send operations
    private final Object wsLock = new Object();

    // ✅ Job deduplication guard
    private final ConcurrentHashMap<String, Long> jobGuard = new ConcurrentHashMap<>();
    private static final long JOB_TTL_MS = 5000; // 5 seconds

    private static final long PING_INTERVAL_MS = 60_000;
    private static final long STALE_THRESHOLD_MS = 120_000;
    private static final long HEALTHCHECK_INTERVALMS = 30_000;
    private static final long FORCE_RECONNECT_MS = 3 * 60 * 60 * 1000;

    private final SimpMessagingTemplate messagingTemplate;
    private ComManager comManager;
    private final SimRepository simRepository;
    private final SmsSessionRepository smsSessionRepository;
    private final SmsMessageJpaRepository smsMessageJpaRepository;

    private final ConcurrentHashMap<String, SmsSession> sessions = new ConcurrentHashMap<>();
    private final AtomicReference<StompSession> stompSessionRef = new AtomicReference<>();
    private final AtomicBoolean reconnecting = new AtomicBoolean(false);
    private final AtomicLong lastWsActivity = new AtomicLong(0L);

    private volatile boolean subscribed = false;
    private String localDeviceName;

    private WebSocketStompClient stompClient;
    private ScheduledExecutorService scheduler;

    @Autowired
    public void setComManager(ComManager comManager) {
        this.comManager = comManager;
    }

    private app.simsmartgsm.service.SmsDailyLimitService smsDailyLimitService;

    @Autowired
    public void setSmsDailyLimitService(app.simsmartgsm.service.SmsDailyLimitService smsDailyLimitService) {
        this.smsDailyLimitService = smsDailyLimitService;
    }

    // ===========================================================
    // INIT
    // ===========================================================
    @PostConstruct
    public void init() {
        try {
            this.localDeviceName = DeviceIdProvider.getDeviceId();
        } catch (Exception e) {
            this.localDeviceName = "UNKNOWN_HOST";
        }
        log.info("💻 GSM Agent initialized: {}", localDeviceName);

        reloadActiveSessions();

        // ✅ FIX: Only create scheduler once to prevent thread leak
        if (scheduler == null || scheduler.isShutdown()) {
            scheduler = Executors.newScheduledThreadPool(5, r -> {
                Thread t = new Thread(r);
                t.setName("SmsJob-Scheduler-" + t.getId());
                t.setDaemon(true);
                return t;
            });
            log.info("✅ Created new scheduler thread pool");
        }

        ThreadPoolTaskScheduler springTaskScheduler = new ThreadPoolTaskScheduler();
        springTaskScheduler.setPoolSize(1);
        springTaskScheduler.setThreadNamePrefix("WS-HeartBeat-");
        springTaskScheduler.initialize();

        stompClient = new WebSocketStompClient(new StandardWebSocketClient());
        stompClient.setMessageConverter(new StringMessageConverter());
        stompClient.setTaskScheduler(springTaskScheduler);
        stompClient.setDefaultHeartbeat(new long[] { 10_000, 10_000 });

        // ✅ Tạo reconnection manager
        reconnectionManager = new ReconnectionManager("SmsJobSubscriber", () -> connect(stompClient));

        connect(stompClient);
        scheduleHealthCheck();
        schedulePing();
        scheduleStaleCheck();
        scheduleForceReconnect();
        scheduleSubscriptionCheck();
        scheduleCleanup();
        scheduleInboundGuardCleanup();
    }

    // ===========================================================
    // CLEANUP inboundGuard and jobGuard
    // ===========================================================
    private void scheduleInboundGuardCleanup() {
        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            inboundGuard.entrySet().removeIf(e -> now - e.getValue() > INBOUND_TTL_MS);
            jobGuard.entrySet().removeIf(e -> now - e.getValue() > JOB_TTL_MS);
        }, 10_000, 10_000, TimeUnit.MILLISECONDS);
    }

    // ===========================================================
    // Load active sessions from DB
    // ===========================================================
    private void reloadActiveSessions() {
        CompletableFuture.runAsync(() -> {
            try {
                List<SmsSession> active = smsSessionRepository.findAllByExpiredAtAfter(LocalDateTime.now());
                active.forEach(s -> sessions.put(s.getCampaignId() + "|" + s.getPhoneNumber(), s));
                log.info("♻️ Reloaded {} active sessions", active.size());
            } catch (Exception e) {
                log.error("❌ Cannot reload sessions: {}", e.getMessage());
            }
        });
    }

    // ===========================================================
    // WebSocket connect
    // ===========================================================
    private void connect(WebSocketStompClient client) {
        // ✅ Ensure old session is fully disconnected first
        StompSession oldSession = stompSessionRef.get();
        if (oldSession != null && oldSession.isConnected()) {
            try {
                log.info("🔌 Disconnecting old session before reconnect");
                oldSession.disconnect();
                Thread.sleep(500); // Give it time to cleanup
            } catch (Exception e) {
                log.warn("⚠️ Error disconnecting old session: {}", e.getMessage());
            }
        }
        stompSessionRef.set(null);

        log.info("🌐 Connecting to {}", REMOTE_WS_URL);
        subscribed = false;

        client.connectAsync(REMOTE_WS_URL, new StompSessionHandlerAdapter() {

            @Override
            public void afterConnected(StompSession session, StompHeaders headers) {
                log.info("✅ Connected WS");
                stompSessionRef.set(session);
                reconnecting.set(false);
                lastWsActivity.set(System.currentTimeMillis());

                // ✅ Thông báo reconnection manager
                reconnectionManager.onConnected();

                if (!subscribed) {
                    subscribeSmsJobs(session);
                    subscribed = true;
                }
            }

            @Override
            public void handleTransportError(StompSession session, Throwable ex) {
                log.error("❌ WS transport error: {}", ex.getMessage());
                stompSessionRef.set(null);
                subscribed = false;

                // ✅ Sử dụng reconnection manager
                reconnectionManager.onConnectionFailed("transport error");
            }

        }).exceptionally(ex -> {
            log.error("❌ Initial connect failed: {}", ex.getMessage());

            // ✅ Sử dụng reconnection manager
            reconnectionManager.onConnectionFailed("initial connect failed");
            return null;
        });
    }

    // ⚠️ retryConnect() removed - now handled by ReconnectionManager

    // ===========================================================
    // Subscribe job topic
    // ===========================================================
    private void subscribeSmsJobs(StompSession session) {
        session.subscribe(SUB_TOPIC, new StompFrameHandler() {

            @Override
            public Type getPayloadType(StompHeaders headers) {
                return String.class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
                lastWsActivity.set(System.currentTimeMillis());
                try {
                    JSONObject job = new JSONObject(payload.toString());
                    log.info("🧾 Job received →\n{}", job.toString(2));

                    if (!localDeviceName.equalsIgnoreCase(job.optString("deviceName")))
                        return;

                    processSmsJob(job);

                } catch (Exception e) {
                    log.error("❌ handleFrame error", e);
                }
            }
        });

        log.info("👂 Subscribed to {}", SUB_TOPIC);
    }

    private void processSmsJob(JSONObject job) {
        try {
            // nguyên code của bạn — KHÔNG THAY ĐỔI
            String comName = job.optString("comName", "");
            String simPhone = job.optString("simPhoneNumber", "");
            String customerPhone = job.optString("phoneNumber", "");
            String content = job.optString("content", "");
            String campaignId = job.optString("campaignId", "");
            String serviceCode = job.optString("serviceCode", "SMS_JOB");
            String orderId = job.optString("orderId", UUID.randomUUID().toString());
            String smsType = job.optString("smsType", "ONE_WAY");
            String sessionId = job.optString("sessionId", UUID.randomUUID().toString());
            String userId = job.optString("userName", "unknown");
            String localMsgId = job.optString("localMsgId", "unknown");

            // ✅ Deduplication check - prevent duplicate job processing
            String jobKey = orderId + "|" + localMsgId;
            long now = System.currentTimeMillis();
            Long lastProcessed = jobGuard.put(jobKey, now);
            if (lastProcessed != null && (now - lastProcessed) < JOB_TTL_MS) {
                log.warn("⚠️ DUPLICATE job ignored: orderId={}, localMsgId={}", orderId, localMsgId);
                return;
            }

            boolean twoWay = "TWO_WAY".equalsIgnoreCase(smsType);
            boolean oneWay = "ONE_WAY".equalsIgnoreCase(smsType);

            if (customerPhone.isBlank() || comName.isBlank()) {
                log.warn("⚠️ Invalid SMS job - missing phone or comName");
                return;
            }

            log.info("📨 [{}] SMS job: {} → {}", comName, simPhone, customerPhone);

            // ✅ Save outgoing socket SMS to local database history asynchronously to avoid blocking STOMP thread
            CompletableFuture.runAsync(() -> {
                try {
                    SmsMessageEntity localSms = SmsMessageEntity.builder()
                            .comPort(comName)
                            .phoneNumber(customerPhone)
                            .content(content)
                            .type("SENT")
                            .status("PENDING")
                            .isRead(true)
                            .build();
                    smsMessageJpaRepository.save(localSms);
                } catch (Exception e) {
                    log.error("❌ Failed to save outgoing SMS to local DB: {}", e.getMessage());
                }
            });

            SmsSession session = createOrUpdateSession(
                    campaignId, customerPhone, simPhone, comName, sessionId, twoWay);

            PortWorker worker = comManager.getWorker(comName);

            // ✅ IMPROVED: Better worker availability check
            if (worker == null || !worker.isOpen()) {

                Sim sim = simRepository.findFirstByPhoneNumber(simPhone).orElse(
                        Sim.builder()
                                .comName(comName)
                                .phoneNumber(simPhone)
                                .status("active")
                                .deviceName(localDeviceName)
                                .countryCode("JP")
                                .build());

                comManager.startWorker(sim);
                Thread.sleep(500);
                worker = comManager.getWorker(comName);

                // ✅ FIX: Verify worker actually started
                if (worker == null || !worker.isOpen()) {
                    log.error("❌ [{}] Failed to start worker for SMS job", comName);

                    sendSmsResponse(
                            userId, campaignId, simPhone, customerPhone,
                            comName, orderId, content, "FAILED",
                            "Worker not available", localMsgId);
                    return;
                }
            }

            // ✅ CHECK DAILY LIMIT trước khi queue - segment-aware, auto-switch nếu cần
            if (smsDailyLimitService != null && !smsDailyLimitService.canSend(comName, content)) {
                int sent = smsDailyLimitService.getSentToday(comName);
                log.warn("🛑 [{}] SIM đạt limit {}/{} segments/ngày - tìm SIM thay thế...",
                        comName, sent, smsDailyLimitService.getDailyLimit());

                PortWorker altWorker = comManager.findWorkerForRetry(comName, null, content);
                if (altWorker != null) {
                    worker = altWorker;
                    String newCom = altWorker.getSim().getComName();
                    log.info("🔄 [{}] Auto-switch SMS job sang {} (quota còn {} segments)",
                            comName, newCom,
                            smsDailyLimitService.getRemainingQuota(newCom));
                } else {
                    log.error("❌ [{}] Tất cả SIM hết quota segments - reject job!", comName);
                    sendSmsResponse(
                            userId, campaignId, simPhone, customerPhone,
                            comName, orderId, content, "FAILED",
                            "All SIMs reached daily segment limit (" + smsDailyLimitService.getDailyLimit() + ")", localMsgId);
                    return;
                }
            }

            // ✅ FIX: Use sendWithMsgId to pass localMsgId for proper callback matching
            PortWorker.Task smsTask = PortWorker.Task.sendWithMsgId(
                    customerPhone, content, serviceCode, orderId, localMsgId);
            boolean enqueued = worker.enqueue(smsTask);

            // ✅ IMPROVED: Response chính xác — QUEUED nếu thành công, REJECTED nếu queue full
            if (enqueued) {
                sendSmsResponse(
                        userId, campaignId, simPhone, customerPhone,
                        comName, orderId, content, "QUEUED",
                        "SMS queued successfully", localMsgId);
            } else {
                // Queue full → gửi FAILED ngay để backend retry / notify user
                sendSmsResponse(
                        userId, campaignId, simPhone, customerPhone,
                        comName, orderId, content, "FAILED",
                        "Queue full (SIM busy), please retry later", localMsgId);
            }

            if (oneWay) {
                session.setStatus("CLOSED");
                session.setActive(false);
                session.setEndTime(LocalDateTime.now());
                smsSessionRepository.save(session);
            }

        } catch (Exception e) {
            log.error("❌ processSmsJob error", e);

            // ✅ ADDED: Send error response
            try {
                String orderId = job.optString("orderId", "unknown");
                String userId = job.optString("userName", "unknown");
                String campaignId = job.optString("campaignId", "");
                String simPhone = job.optString("simPhoneNumber", "");
                String customerPhone = job.optString("phoneNumber", "");
                String comName = job.optString("comName", "");
                String content = job.optString("content", "");
                String localMsgId = job.optString("localMsgId", "unknown");

                sendSmsResponse(
                        userId, campaignId, simPhone, customerPhone,
                        comName, orderId, content, "FAILED",
                        "Job processing error: " + e.getMessage(), localMsgId);
            } catch (Exception ignored) {
            }
        }
    }

    // ===========================================================
    // OUTBOUND SMS WS callback
    // ===========================================================
    public void sendSmsResponse(String userId, String campaignId, String simPhone,
            String customerPhone, String comName, String orderId,
            String content, String status, String message, String localMsgId) {

        try {
            JSONObject response = new JSONObject();
            response.put("status", status);
            response.put("message", message);
            response.put("orderId", orderId);
            response.put("localMsgId", localMsgId);
            response.put("campaignId", campaignId);
            response.put("simPhone", simPhone);
            response.put("customerPhone", customerPhone);
            response.put("comPort", comName);
            response.put("deviceName", localDeviceName);
            response.put("userName", userId);
            response.put("timestamp", Instant.now().toString());
            response.put("content", content);
            response.put("smsType", "OUTBOX");

            StompSession session = stompSessionRef.get();
            if (session != null && session.isConnected()) {
                // ✅ Synchronize to prevent concurrent WebSocket sends
                synchronized (wsLock) {
                    session.send(LOCAL_RESPONSE_TOPIC, response.toString());
                    lastWsActivity.set(System.currentTimeMillis());
                }
                log.info("📡 WS push OUTBOX");
            } else {
                log.warn("⚠️ STOMP disconnected — skip OUTBOX push");
            }

        } catch (Exception e) {
            log.error("❌ sendSmsResponse error", e);
        }
    }

    /**
     * \u2705 Send final fail notification after all retries exhausted
     */
    public void sendFinalFail(String orderId, String toNumber, int totalAttempts,
            List<String> triedSims, String lastSimPhone) {
        try {
            JSONObject failNotification = new JSONObject();
            failNotification.put("type", "SMS_FINAL_FAIL");
            failNotification.put("orderId", orderId);
            failNotification.put("toNumber", toNumber);
            failNotification.put("totalAttempts", totalAttempts);
            failNotification.put("triedSims", triedSims);
            failNotification.put("lastSimPhone", lastSimPhone);
            failNotification.put("deviceName", localDeviceName);
            failNotification.put("timestamp", Instant.now().toString());

            StompSession session = stompSessionRef.get();
            if (session != null && session.isConnected()) {
                // ✅ Synchronize to prevent concurrent WebSocket sends
                synchronized (wsLock) {
                    session.send("/topic/sms-final-fail", failNotification.toString());
                    lastWsActivity.set(System.currentTimeMillis());
                }
                log.info("\ud83d\udce1 Sent final fail notification for order {}", orderId);
            } else {
                log.warn("\u26a0\ufe0f STOMP disconnected \u2014 cannot send final fail for order {}", orderId);
            }

        } catch (Exception e) {
            log.error("\u274c sendFinalFail error for order {}", orderId, e);
        }
    }

    // ===========================================================
    // HANDLE INBOUND SMS
    // ===========================================================
    public void handleInboundSms(String simSystem, String customerPhone, String content, String comName) {
        try {
            if (content == null || content.isBlank())
                return;

            // ✅ DECODE SMS CONTENT BEFORE SENDING TO CLOUD
            String decodedContent = SmsDecoder.decode(content);

            if (decodedContent == null || decodedContent.isBlank()) {
                log.warn("⚠️ SMS content is empty after decoding, skipping");
                return;
            }

            String key = customerPhone + "|" + decodedContent + "|" + comName;
            long now = System.currentTimeMillis();

            Long last = inboundGuard.put(key, now);
            if (last != null && (now - last) < INBOUND_TTL_MS) {
                log.warn("⚠️ DUPLICATE inbound ignored: {}", key);
                return;
            }

            SmsSession session = smsSessionRepository
                    .findByPhoneNumberAndActiveTrueAndExpiredAtAfter(
                            normalizePhone(customerPhone), LocalDateTime.now())
                    .orElse(null);

            if (session == null) {
                log.info("📥 [UNTRACKED] {} -> {} | {}", customerPhone, simSystem, decodedContent);
                return;
            }

            session.setLastActivityAt(LocalDateTime.now());
            smsSessionRepository.save(session);

            JSONObject inbound = new JSONObject();
            inbound.put("fromPhoneNumber", customerPhone);
            inbound.put("toPhoneNumber", simSystem);
            inbound.put("content", decodedContent); // ✅ Use decoded content
            inbound.put("deviceName", localDeviceName);
            inbound.put("comName", comName);
            inbound.put("timestamp", Instant.now().toString());
            inbound.put("campaignId", session.getCampaignId());
            inbound.put("sessionId", session.getId());

            sendWs(INBOUND_TOPIC, inbound);

        } catch (Exception e) {
            log.error("❌ handleInboundSms error", e);
        }
    }

    private void sendWs(String topic, JSONObject json) {
        try {
            StompSession s = stompSessionRef.get();

            if (s != null && s.isConnected()) {
                // ✅ Synchronize to prevent concurrent WebSocket sends
                synchronized (wsLock) {
                    s.send(topic, json.toString());
                    lastWsActivity.set(System.currentTimeMillis());
                }
                log.info("📡 WS push -> {}", topic);
            } else {
                log.warn("⚠️ WS push skipped (not connected)");
            }

        } catch (Exception e) {
            log.error("❌ sendWs {}: {}", topic, e.getMessage());
        }
    }

    private String normalizePhone(String phone) {
        if (phone == null)
            return "";
        phone = phone.trim().replaceAll("[^0-9+]", "");

        if (phone.startsWith("+81"))
            return "0" + phone.substring(3);
        if (phone.startsWith("81"))
            return "0" + phone.substring(2);

        return phone;
    }

    private SmsSession createOrUpdateSession(
            String campaignId, String customerPhone, String simPhone,
            String comPort, String sessionId, boolean twoWay) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime expireAt = twoWay ? now.plusDays(3) : now;

        SmsSession session = smsSessionRepository
                .findByCampaignIdAndPhoneNumber(campaignId, customerPhone)
                .orElse(
                        SmsSession.builder()
                                .campaignId(campaignId)
                                .phoneNumber(customerPhone)
                                .simId(simPhone)
                                .id(sessionId)
                                .deviceName(localDeviceName)
                                .comPort(comPort)
                                .startTime(now)
                                .active(true)
                                .build());

        session.setExpiredAt(expireAt);
        session.setLastActivityAt(now);
        session.setStatus(twoWay ? "ACTIVE" : "CLOSED");
        session.setActive(twoWay);

        smsSessionRepository.save(session);
        return session;
    }

    // ===========================================================
    // HealthCheck
    // ===========================================================
    private void scheduleHealthCheck() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                StompSession s = stompSessionRef.get();
                if (s == null || !s.isConnected()) {
                    log.warn("⚠ WS session lost");

                    // ✅ Sử dụng reconnection manager
                    reconnectionManager.onConnectionFailed("healthcheck lost");
                }
            } catch (Exception e) {
                log.error("❌ WS health check error: {}", e.getMessage());
            }
        }, HEALTHCHECK_INTERVALMS, HEALTHCHECK_INTERVALMS, TimeUnit.MILLISECONDS);
    }

    // ===========================================================
    // Ping
    // ===========================================================
    private void schedulePing() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                StompSession s = stompSessionRef.get();
                if (s != null && s.isConnected()) {
                    JSONObject ping = new JSONObject().put("id", UUID.randomUUID().toString());
                    // ✅ Synchronize to prevent concurrent WebSocket sends
                    synchronized (wsLock) {
                        s.send("/app/ping", ping.toString());
                    }
                }
            } catch (Exception e) {
                log.error("❌ Ping error: {}", e.getMessage());
            }
        }, PING_INTERVAL_MS, PING_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // ===========================================================
    // Stale check
    // ===========================================================
    private void scheduleStaleCheck() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                StompSession s = stompSessionRef.get();

                if (s == null || !s.isConnected())
                    return;

                long idle = System.currentTimeMillis() - lastWsActivity.get();
                if (idle > STALE_THRESHOLD_MS) {
                    log.warn("🧊 WS stale {} ms → reconnect", idle);

                    try {
                        s.disconnect();
                    } catch (Exception ignored) {
                    }

                    // ✅ Sử dụng reconnection manager
                    reconnectionManager.onConnectionFailed("stale connection");
                }

            } catch (Exception e) {
                log.error("❌ Stale-check error: {}", e.getMessage());
            }
        }, 30_000, 30_000, TimeUnit.MILLISECONDS);
    }

    // ==========================================================
    // ♻️ CLEANUP - Prevent thread leaks
    // ==========================================================
    @PreDestroy
    public void cleanup() {
        try {
            log.info("🧹 Shutting down RemoteSmsJobSubscriber...");

            // ✅ Shutdown reconnection manager
            if (reconnectionManager != null) {
                reconnectionManager.shutdown();
            }

            // ✅ Disconnect WebSocket first
            StompSession s = stompSessionRef.get();
            if (s != null && s.isConnected()) {
                try {
                    s.disconnect();
                    log.info("✅ WebSocket disconnected");
                } catch (Exception e) {
                    log.warn("⚠️ Error disconnecting WebSocket: {}", e.getMessage());
                }
            }
            stompSessionRef.set(null);

            // ✅ Shutdown scheduler with proper timeout
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.warn("⚠️ Scheduler did not terminate in time, forcing shutdown");
                    List<Runnable> pending = scheduler.shutdownNow();
                    log.warn("⚠️ Cancelled {} pending tasks", pending.size());

                    // Wait again after force shutdown
                    if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.error("❌ Scheduler still running after force shutdown!");
                    }
                }
            }

            // ✅ Clear session maps to free memory
            int sessionCount = sessions.size();
            sessions.clear();
            inboundGuard.clear();
            jobGuard.clear();
            log.info("✅ Cleared {} sessions from memory", sessionCount);

            log.info("✅ RemoteSmsJobSubscriber cleanup complete");
        } catch (Exception e) {
            log.error("❌ Error shutting down RemoteSmsJobSubscriber", e);
        }
    }

    // ===========================================================
    // Force reconnect
    // ===========================================================
    private void scheduleForceReconnect() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                log.info("♻ WS force reconnect (periodic refresh)");
                StompSession s = stompSessionRef.get();

                subscribed = false;

                if (s != null && s.isConnected())
                    s.disconnect();

                connect(stompClient);

            } catch (Exception e) {
                log.error("❌ Force reconnect failed: {}", e.getMessage());
            }
        }, FORCE_RECONNECT_MS, FORCE_RECONNECT_MS, TimeUnit.MILLISECONDS);
    }

    // ===========================================================
    // Subscription check
    // ===========================================================
    private void scheduleSubscriptionCheck() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                StompSession s = stompSessionRef.get();

                if (s == null || !s.isConnected())
                    return;

                if (!subscribed) {
                    log.info("🔄 Resubscribing to {}", SUB_TOPIC);
                    subscribeSmsJobs(s);
                    subscribed = true;
                }

            } catch (Exception e) {
                log.error("❌ Subscription check failed: {}", e.getMessage());
            }
        }, 60_000, 60_000, TimeUnit.MILLISECONDS);
    }

    // ===========================================================
    // Cleanup expired sessions
    // ===========================================================
    private void scheduleCleanup() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                LocalDateTime now = LocalDateTime.now();
                List<SmsSession> expired = smsSessionRepository
                        .findAllByActiveTrueAndExpiredAtBefore(now);

                if (expired.isEmpty()) {
                    return;
                }

                // ✅ BATCH LOGGING - Only log summary to prevent spam
                int count = 0;
                for (SmsSession s : expired) {
                    s.setActive(false);
                    s.setStatus("EXPIRED");
                    s.setEndTime(now);
                    smsSessionRepository.save(s);

                    sessions.remove(s.getCampaignId() + "|" + s.getPhoneNumber());
                    count++;

                    // Only log individual sessions if count is small
                    if (count <= 10) {
                        log.debug("🧹 Expired session: {}", s.getPhoneNumber());
                    }
                }

                // ✅ Single summary log instead of spamming
                log.info("🧹 Cleaned up {} expired session(s)", count);

            } catch (Exception e) {
                log.error("❌ Cleanup session error", e);
            }
        }, 60_000, 1_800_000, TimeUnit.MILLISECONDS); // ✅ Changed from 5min to 30min
    }

    // ===========================================================
    // 📊 PUBLIC METHODS - For HeapMonitorService
    // ===========================================================

    /**
     * Lấy số lượng entries trong inboundGuard
     */
    public int getInboundGuardSize() {
        return inboundGuard.size();
    }

    /**
     * Dọn dẹp inbound guard (xóa entries cũ)
     */
    public void cleanupInboundGuard() {
        long now = System.currentTimeMillis();
        int before = inboundGuard.size();
        inboundGuard.entrySet().removeIf(e -> now - e.getValue() > INBOUND_TTL_MS);
        int after = inboundGuard.size();

        if (before > after) {
            log.info("🧹 Cleaned {} inbound guard entries", before - after);
        }
    }
}
