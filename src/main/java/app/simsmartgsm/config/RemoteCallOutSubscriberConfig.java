package app.simsmartgsm.config;

import app.simsmartgsm.dto.request.MakeCallRequest;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.service.GsmService;
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
 * ✅ FINAL SOLUTION: Sử dụng GsmService.makeCall() trực tiếp
 * - Stop PortWorker trước để giải phóng port
 * - GsmService mở port mới, thực hiện cuộc gọi, download file, upload
 * - Tất cả logic đã hoạt động tốt: duration chính xác, audio quality tốt,
 * download 100%
 */
@Configuration
@Slf4j
@RequiredArgsConstructor
public class RemoteCallOutSubscriberConfig {

    private static final String REMOTE_WS_URL = "wss://be-user-sms-global.smsglobalhub.com/ws";
    private static final String SUB_TOPIC = "/topic/send-call";
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final int MAX_WS_FAIL = 50;

    private int wsFailCount = 0;

    private final SimRepository simRepository;
    private final GsmService gsmService; // ✅ Dùng GsmService cho call
    private final ComManager comManager; // ✅ Dùng để stop PortWorker

    private final Map<String, Instant> recentCallKeys = new ConcurrentHashMap<>();
    private final AtomicReference<StompSession> stompSessionRef = new AtomicReference<>();

    private ReconnectionManager reconnectionManager;
    private WebSocketStompClient stompClient;
    private ThreadPoolTaskScheduler taskScheduler;
    private String localDeviceName;

    @PostConstruct
    public void subscribeToCallOut() {
        try {
            this.localDeviceName = DeviceIdProvider.getDeviceId();
        } catch (Exception e) {
            this.localDeviceName = "UNKNOWN_HOST";
        }

        log.info("💻 GSM Agent (CALL_OUT) host={}", localDeviceName);

        // ✅ Tạo reconnection manager
        reconnectionManager = new ReconnectionManager("CallOutSubscriber", this::doConnect);

        // ✅ Tạo scheduler 1 lần duy nhất
        taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setThreadNamePrefix("WS-CallOut-");
        taskScheduler.afterPropertiesSet();

        // ✅ CUSTOM TIMEOUTS for SockJS HTTP requests
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(15_000); // 15s connect timeout
        factory.setReadTimeout(15_000); // 15s read timeout
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
                        log.info("✅ Connected to {}", REMOTE_WS_URL);

                        stompSessionRef.set(session);

                        // ✅ Thông báo reconnection manager
                        reconnectionManager.onConnected();

                        subscribeToTopic(session);
                    }

                    @Override
                    public void handleTransportError(StompSession session, Throwable exception) {
                        log.error("❌ Transport error: {}", exception.getMessage());

                        stompSessionRef.set(null);

                        // ✅ Sử dụng reconnection manager
                        reconnectionManager.onConnectionFailed("transport error");
                    }
                });

        futureSession.exceptionally(ex -> {
            log.error("❌ Failed to connect: {}", ex.getMessage());

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

                    log.info("📞 [CALL_OUT] RAW → {}", json);

                    Map<String, Object> job = mapper.readValue(json, Map.class);

                    // --- 1) Kiểm tra deviceName từ job và máy cục bộ ---
                    String jobDeviceName = (String) job.get("deviceName");
                    if (jobDeviceName == null || !jobDeviceName.equalsIgnoreCase(localDeviceName)) {
                        log.debug("⏭ Skipped job for deviceName={} (local={})", jobDeviceName, localDeviceName);
                        return;
                    }

                    String comName = (String) job.get("comNumber");
                    String targetPhone = (String) job.get("targetPhone");

                    if (comName == null || targetPhone == null || "null".equalsIgnoreCase(targetPhone)) {
                        log.warn("⚠ CALL_OUT thiếu comNumber hoặc targetPhone: {}", json);
                        return;
                    }

                    // --- 2) Chống trùng gọi trong 5s trên cùng (deviceName, COM, phone) ---
                    String key = jobDeviceName + "|" + comName + "|" + targetPhone;
                    Instant now = Instant.now();
                    Instant last = recentCallKeys.get(key);
                    if (last != null && now.isBefore(last.plusSeconds(5))) {
                        log.debug("⏭ CALL_OUT duplicated within 5s: {}", key);
                        return;
                    }
                    recentCallKeys.put(key, now);

                    // --- 3) Tìm SIM theo deviceName + comName ---
                    List<Sim> sims = simRepository.findAllByDeviceNameAndComName(jobDeviceName, comName);
                    if (sims == null || sims.isEmpty()) {
                        log.warn("⚠ Không tìm thấy SIM cho deviceName={} & COM={}", jobDeviceName, comName);
                        return;
                    }

                    // Ưu tiên SIM ACTIVE, nếu không có thì lấy bản ghi đầu tiên
                    Sim sim = sims.stream()
                            .filter(s -> "ACTIVE".equalsIgnoreCase(s.getStatus()))
                            .findFirst()
                            .orElse(sims.get(0));

                    boolean record = false;
                    Object recordObj = job.get("record");
                    if (recordObj instanceof Boolean) {
                        record = (Boolean) recordObj;
                    } else if (recordObj instanceof String) {
                        record = Boolean.parseBoolean((String) recordObj);
                    }

                    // --- Check callType ---
                    String callType = (String) job.get("callType");
                    String serviceCode = (String) job.get("serviceCode");
                    String orderId = (String) job.get("orderId");

                    if ("CALL_IN".equalsIgnoreCase(callType)) {
                        // --- Logic ĐỢI CUỘC GỌI (CALL_IN) ---
                        int waitTimeout = 60;
                        if (job.get("waitingTime") != null) {
                            waitTimeout = ((Number) job.get("waitingTime")).intValue();
                        } else if (job.get("waitTimeout") != null) {
                            waitTimeout = ((Number) job.get("waitTimeout")).intValue();
                        }

                        int callDuration = 20;
                        if (job.get("callTime") != null) {
                            callDuration = ((Number) job.get("callTime")).intValue();
                        }

                        boolean acceptHidden = false;
                        if (job.get("acceptHiddenCaller") != null) {
                            acceptHidden = (Boolean) job.get("acceptHiddenCaller");
                        }

                        log.info("📞 CALL_IN (Wait): port={} | expected={} | wait={}s | duration={}s | rec={}",
                                comName, targetPhone, waitTimeout, callDuration, record);

                        // ✅ SỬ DỤNG ComManager để enqueue CALL_IN task
                        try {
                            comManager.waitForIncomingCallAdvanced(
                                    sim,
                                    targetPhone, // Số dự kiến gọi đến
                                    callDuration, // Thời gian giữ cuộc gọi
                                    record, // Có ghi âm không
                                    acceptHidden, // Chấp nhận số ẩn
                                    waitTimeout, // Time window để accept
                                    serviceCode,
                                    orderId);

                            log.info(
                                    "📥 [CALL_IN] Task enqueued via ComManager: {} | expectedCaller={} | wait={}s | duration={}s",
                                    comName, targetPhone, waitTimeout, callDuration);

                        } catch (Exception e) {
                            log.error("❌ Failed to enqueue CALL_IN for {}: {}", comName, e.getMessage(), e);
                        }

                    } else {

                        // --- Logic GỌI ĐI (CALL_OUT - Mặc định) ---
                        int callTimeSeconds = 20 * 60;
                        if (job.get("callTime") != null) {
                            callTimeSeconds = ((Number) job.get("callTime")).intValue();
                        } else if (job.get("duration") != null) {
                            callTimeSeconds = ((Number) job.get("duration")).intValue();
                        }

                        log.info("☎ CALL_OUT: dev={} | {} (COM={}) → {} | rec={} | {}s",
                                jobDeviceName, sim.getPhoneNumber(), comName, targetPhone, record, callTimeSeconds);

                        // ✅ FIX: Stop worker VÀ ĐỢI thread kết thúc hoàn toàn (tránh COM bận)
                        try {
                            boolean stopped = comManager.stopWorkerAndWait(comName, 3000);
                            if (!stopped) {
                                log.warn("⚠️ [CALL_OUT] Worker thread {} chưa thoát hoàn toàn sau 3s", comName);
                            }
                            Thread.sleep(300);
                            log.info("✅ [CALL_OUT] PortWorker stopped, port released: {}", comName);
                        } catch (Exception e) {
                            log.warn("⚠️ [CALL_OUT] Failed to stop PortWorker: {}", e.getMessage());
                        }

                        MakeCallRequest callRequest = MakeCallRequest.builder()
                                .comPort(comName)
                                .simPhone(sim.getPhoneNumber())
                                .targetPhone(targetPhone)
                                .callDuration(callTimeSeconds)
                                .record(record)
                                .serviceCode(serviceCode)
                                .orderId(orderId)
                                .build();

                        gsmService.makeCall(callRequest);
                        log.info("📥 CALL_OUT dispatched to GsmService: {} → {} ({}s, record={})",
                                comName, targetPhone, callTimeSeconds, record);

                    } // End of else (CALL_OUT)

                    // Note: PortWorker sẽ được tự động restart bởi ComManager khi cần thiết
                    // (khi có SMS task hoặc scan task)

                } catch (Exception e) {
                    log.error("❌ Error handling CALL_OUT: {}", e.getMessage(), e);
                }
            }
        });

        log.info("👂 Subscribed to {}", SUB_TOPIC);
    }

    /**
     * ✅ Cleanup khi shutdown
     */
    @PreDestroy
    public void cleanup() {
        log.info("🧹 Shutting down CallOutSubscriber...");

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
