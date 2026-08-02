package app.simsmartgsm.config;

import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.uitils.DeviceIdProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.*;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.socket.sockjs.client.RestTemplateXhrTransport;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ✅ RemoteCallInSubscriberConfig - Subscribe to remote server for incoming call
 * requests
 * 
 * Khi server remote gửi yêu cầu CALL_IN, client sẽ:
 * 1. Đợi cuộc gọi đến từ số điện thoại chỉ định (expectedCaller)
 * 2. Nhấc máy và ghi âm (nếu cần)
 * 3. Gửi callback về server remote với file ghi âm (nếu có)
 * 
 * Tương tự RemoteCallOutSubscriberConfig nhưng cho chiều ngược lại (nhận cuộc
 * gọi thay vì gọi đi)
 */
@Configuration
@Slf4j
@RequiredArgsConstructor
public class RemoteCallInSubscriberConfig {

    private static final String REMOTE_WS_URL = "wss://be-user-sms-global.smsglobalhub.com/ws";
    private static final String SUB_TOPIC = "/topic/receive-call"; // Topic cho CALL_IN
    private static final ObjectMapper mapper = new ObjectMapper();

    private final SimRepository simRepository;
    private final ComManager comManager;

    private final Map<String, Instant> recentCallKeys = new ConcurrentHashMap<>();
    private final AtomicReference<StompSession> stompSessionRef = new AtomicReference<>();

    private ReconnectionManager reconnectionManager;
    private WebSocketStompClient stompClient;
    private ThreadPoolTaskScheduler taskScheduler;
    private String localDeviceName;

    @PostConstruct
    public void subscribeToCallIn() {
        try {
            this.localDeviceName = DeviceIdProvider.getDeviceId();
        } catch (Exception e) {
            this.localDeviceName = "UNKNOWN_HOST";
        }

        log.info("💻 GSM Agent (CALL_IN) host={}", localDeviceName);

        // ✅ Tạo reconnection manager
        reconnectionManager = new ReconnectionManager("CallInSubscriber", this::doConnect);

        // ✅ Tạo scheduler 1 lần duy nhất
        taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setThreadNamePrefix("WS-CallIn-");
        taskScheduler.afterPropertiesSet();

        // ✅ CUSTOM TIMEOUTS for SockJS HTTP requests
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(15_000);
        factory.setReadTimeout(15_000);
        RestTemplate restTemplate = new RestTemplate(factory);

        List<Transport> transports = List.of(
                new WebSocketTransport(new StandardWebSocketClient()),
                new RestTemplateXhrTransport(restTemplate));
        SockJsClient sockJsClient = new SockJsClient(transports);

        // ✅ Tạo client 1 lần duy nhất
        stompClient = new WebSocketStompClient(sockJsClient);
        stompClient.setMessageConverter(new MappingJackson2MessageConverter());
        stompClient.setTaskScheduler(taskScheduler);
        stompClient.setDefaultHeartbeat(new long[] { 10000, 10000 });

        // Bắt đầu kết nối
        doConnect();
    }

    /**
     * ✅ Thực hiện kết nối
     */
    private void doConnect() {
        CompletableFuture<StompSession> futureSession = stompClient.connectAsync(REMOTE_WS_URL,
                new StompSessionHandlerAdapter() {

                    @Override
                    public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
                        log.info("✅ [CALL_IN] Connected to {}", REMOTE_WS_URL);

                        stompSessionRef.set(session);

                        // ✅ Thông báo reconnection manager
                        reconnectionManager.onConnected();

                        subscribeToTopic(session);
                    }

                    @Override
                    public void handleTransportError(StompSession session, Throwable exception) {
                        log.error("❌ [CALL_IN] Transport error: {}", exception.getMessage());

                        stompSessionRef.set(null);

                        // ✅ Sử dụng reconnection manager
                        reconnectionManager.onConnectionFailed("transport error");
                    }
                });

        futureSession.exceptionally(ex -> {
            log.error("❌ [CALL_IN] Failed to connect: {}", ex.getMessage());

            // ✅ Sử dụng reconnection manager
            reconnectionManager.onConnectionFailed("initial connect failed");
            return null;
        });
    }

    private void subscribeToTopic(StompSession session) {
        session.subscribe(SUB_TOPIC, new StompFrameHandler() {

            @Override
            public Type getPayloadType(StompHeaders headers) {
                return Object.class;
            }

            @Override
            @SuppressWarnings("unchecked")
            public void handleFrame(StompHeaders headers, Object payload) {
                try {
                    String json = payload instanceof byte[]
                            ? new String((byte[]) payload, StandardCharsets.UTF_8)
                            : payload.toString();

                    log.info("📞 [CALL_IN] RAW → {}", json);

                    Map<String, Object> job = mapper.readValue(json, Map.class);

                    // --- 1) Kiểm tra deviceName từ job và máy cục bộ ---
                    String jobDeviceName = (String) job.get("deviceName");
                    if (jobDeviceName == null || !jobDeviceName.equalsIgnoreCase(localDeviceName)) {
                        log.debug("⏭ [CALL_IN] Skipped job for deviceName={} (local={})", jobDeviceName,
                                localDeviceName);
                        return;
                    }

                    String comName = (String) job.get("comNumber");
                    String expectedCaller = (String) job.get("expectedCaller"); // Số dự kiến gọi đến

                    if (comName == null) {
                        log.warn("⚠ [CALL_IN] thiếu comNumber: {}", json);
                        return;
                    }

                    // --- 2) Chống trùng trong 5s trên cùng (deviceName, COM, expectedCaller) ---
                    String key = jobDeviceName + "|" + comName + "|"
                            + (expectedCaller != null ? expectedCaller : "ANY");
                    Instant now = Instant.now();
                    Instant last = recentCallKeys.get(key);
                    if (last != null && now.isBefore(last.plusSeconds(5))) {
                        log.debug("⏭ [CALL_IN] duplicated within 5s: {}", key);
                        return;
                    }
                    recentCallKeys.put(key, now);

                    // --- 3) Tìm SIM theo deviceName + comName ---
                    List<Sim> sims = simRepository.findAllByDeviceNameAndComName(jobDeviceName, comName);
                    if (sims == null || sims.isEmpty()) {
                        log.warn("⚠ [CALL_IN] Không tìm thấy SIM cho deviceName={} & COM={}", jobDeviceName, comName);
                        return;
                    }

                    // Ưu tiên SIM ACTIVE
                    Sim sim = sims.stream()
                            .filter(s -> "ACTIVE".equalsIgnoreCase(s.getStatus()))
                            .findFirst()
                            .orElse(sims.get(0));

                    // --- 4) Parse các tham số từ job ---
                    boolean record = false;
                    Object recordObj = job.get("record");
                    if (recordObj instanceof Boolean) {
                        record = (Boolean) recordObj;
                    } else if (recordObj instanceof String) {
                        record = Boolean.parseBoolean((String) recordObj);
                    }

                    boolean acceptHidden = false;
                    Object acceptHiddenObj = job.get("acceptHiddenCaller");
                    if (acceptHiddenObj instanceof Boolean) {
                        acceptHidden = (Boolean) acceptHiddenObj;
                    } else if (acceptHiddenObj instanceof String) {
                        acceptHidden = Boolean.parseBoolean((String) acceptHiddenObj);
                    }

                    // waitTimeout: thời gian đợi cuộc gọi đến (giây)
                    int waitTimeout = 60; // Default 60 giây
                    if (job.get("waitTimeout") != null) {
                        waitTimeout = ((Number) job.get("waitTimeout")).intValue();
                    } else if (job.get("timeWindow") != null) {
                        waitTimeout = ((Number) job.get("timeWindow")).intValue();
                    }

                    // callDuration: thời gian giữ cuộc gọi sau khi nhấc máy (giây)
                    int callDuration = 20; // Default 20 giây
                    if (job.get("callDuration") != null) {
                        callDuration = ((Number) job.get("callDuration")).intValue();
                    } else if (job.get("duration") != null) {
                        callDuration = ((Number) job.get("duration")).intValue();
                    } else if (job.get("callTime") != null) {
                        callDuration = ((Number) job.get("callTime")).intValue();
                    }

                    String serviceCode = (String) job.get("serviceCode");
                    String orderId = (String) job.get("orderId");

                    log.info(
                            "📞 [CALL_IN] dev={} | {} (COM={}) | expectedCaller={} | wait={}s | duration={}s | rec={} | acceptHidden={}",
                            jobDeviceName, sim.getPhoneNumber(), comName,
                            expectedCaller != null ? expectedCaller : "ANY",
                            waitTimeout, callDuration, record, acceptHidden);

                    // --- 5) ✅ SỬ DỤNG ComManager để enqueue CALL_IN task ---
                    // ComManager sẽ tự động tạo/lấy worker và enqueue task
                    try {
                        comManager.waitForIncomingCallAdvanced(
                                sim,
                                expectedCaller, // Số dự kiến gọi đến
                                callDuration, // Thời gian giữ cuộc gọi
                                record, // Có ghi âm không
                                acceptHidden, // Chấp nhận số ẩn
                                waitTimeout, // Time window để accept
                                serviceCode,
                                orderId);

                        log.info(
                                "📥 [CALL_IN] Task enqueued via ComManager: {} | expectedCaller={} | wait={}s | duration={}s | record={}",
                                comName, expectedCaller, waitTimeout, callDuration, record);

                    } catch (Exception e) {
                        log.error("❌ Failed to enqueue CALL_IN for {}: {}", comName, e.getMessage(), e);
                    }

                    // Note: Worker sẽ tự động xử lý task CALL_IN trong queue

                } catch (Exception e) {
                    log.error("❌ Error handling CALL_IN: {}", e.getMessage(), e);
                }
            }
        });

        log.info("👂 [CALL_IN] Subscribed to {}", SUB_TOPIC);
    }

    /**
     * ✅ Cleanup khi shutdown
     */
    @PreDestroy
    public void cleanup() {
        log.info("🧹 Shutting down CallInSubscriber...");

        if (reconnectionManager != null) {
            reconnectionManager.shutdown();
        }

        StompSession session = stompSessionRef.get();
        if (session != null && session.isConnected()) {
            try {
                session.disconnect();
            } catch (Exception e) {
                log.warn("⚠️ Error disconnecting: {}", e.getMessage());
            }
        }

        if (taskScheduler != null) {
            taskScheduler.shutdown();
        }
    }

    public StompSession getSession() {
        return stompSessionRef.get();
    }
}
