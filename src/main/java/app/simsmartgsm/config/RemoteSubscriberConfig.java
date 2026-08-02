package app.simsmartgsm.config;

import app.simsmartgsm.dto.request.RentSimRequest;
import app.simsmartgsm.entity.Country;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.CountryRepository;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.service.GsmListenerService;
import app.simsmartgsm.uitils.DeviceIdProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class RemoteSubscriberConfig {

    private static final String REMOTE_WS_URL = "wss://be-user-sms-global.smsglobalhub.com/ws";
    private static final String SUB_TOPIC = "/topic/send-otp";
    private static final ObjectMapper mapper = new ObjectMapper();

    // ⚠️ Removed old wsFailCount and MAX_WS_FAIL - now using ReconnectionManager

    private final AtomicReference<StompSession> stompSessionRef = new AtomicReference<>();

    private ReconnectionManager reconnectionManager;
    private WebSocketStompClient stompClient;
    private ThreadPoolTaskScheduler taskScheduler;

    private final GsmListenerService gsmListenerService;
    private final SimRepository simRepository;
    private final CountryRepository countryRepository;

    @PostConstruct
    public void subscribeToRemoteBroker() {
        // ✅ Tạo reconnection manager
        reconnectionManager = new ReconnectionManager("RemoteSubscriber", this::doConnect);

        // ✅ Tạo scheduler 1 lần duy nhất
        taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setThreadNamePrefix("WS-OTP-");
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
            reconnectionManager.onConnectionFailed("connect failed");
            return null;
        });
    }

    private void subscribeToTopic(StompSession session) {
        session.subscribe(SUB_TOPIC, new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
                return Object.class; // nhận raw payload
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
                try {
                    String json;
                    if (payload instanceof byte[]) {
                        json = new String((byte[]) payload, StandardCharsets.UTF_8);
                    } else {
                        json = payload.toString();
                    }

                    log.info("📩 Raw JSON from broker: {}", json);

                    RentSimRequest req = mapper.readValue(json, RentSimRequest.class);
                    log.info("✅ Parsed RentSimRequest: {}", req);

                    String localHostName = DeviceIdProvider.getDeviceId();

                    if (!localHostName.equalsIgnoreCase(req.getDeviceName())) {
                        log.info("⏭️ Skip because deviceName={} != host={}",
                                req.getDeviceName(), localHostName);
                        return;
                    }

                    // ✅ Retry logic với exponential backoff - SIM có thể đang được scan
                    Sim sim = findSimWithRetry(req.getPhoneNumber(), 5, 1000);
                    if (sim == null) {
                        log.error("❌ SIM not found after retries: {} (device={})",
                                req.getPhoneNumber(), req.getDeviceName());
                        logAvailableSims();
                        return;
                    }

                    Country country = countryRepository.findByCountryCode(req.getCountryCode())
                            .orElseThrow(() -> new RuntimeException("Country not found: " + req.getCountryCode()));

                    gsmListenerService.rentSim(
                            sim,
                            req.getAccountId(),
                            req.getServiceCodeList(),
                            req.getRentDuration(),
                            country,
                            req.getOrderId(),
                            req.getType(),
                            req.isRecord(),
                            req.getCallType(),
                            req.getTargetPhone());

                } catch (Exception e) {
                    log.error("❌ Error parsing payload: {}", e.getMessage(), e);
                }
            }
        });

        log.info("👂 Subscribed to {}", SUB_TOPIC);
    }

    /**
     * ✅ Tìm SIM với retry logic (vì SIM có thể đang được scan)
     */
    private Sim findSimWithRetry(String phoneNumber, int maxRetries, long initialDelayMs) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            var simOpt = simRepository.findFirstByPhoneNumber(phoneNumber);
            if (simOpt.isPresent()) {
                if (attempt > 1) {
                    log.info("✅ Found SIM {} after {} attempts", phoneNumber, attempt);
                }
                return simOpt.get();
            }

            if (attempt < maxRetries) {
                long delay = initialDelayMs * (long) Math.pow(2, attempt - 1); // exponential backoff
                log.warn("⏳ SIM {} not found (attempt {}/{}), retrying in {}ms...",
                        phoneNumber, attempt, maxRetries, delay);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        return null;
    }

    /**
     * ✅ Log danh sách SIM có sẵn để debug
     */
    private void logAvailableSims() {
        try {
            List<Sim> allSims = simRepository.findByDeviceName(DeviceIdProvider.getDeviceId());
            log.info("📋 Available SIMs on this device ({}): {}",
                    allSims.size(),
                    allSims.stream()
                            .filter(s -> s.getPhoneNumber() != null && !s.getPhoneNumber().isEmpty())
                            .map(s -> s.getPhoneNumber() + "@" + s.getComName())
                            .limit(20)
                            .toList());
        } catch (Exception e) {
            log.warn("⚠️ Could not log available SIMs: {}", e.getMessage());
        }
    }

    /**
     * ✅ Cleanup khi shutdown
     */
    @PreDestroy
    public void cleanup() {
        log.info("🧹 Shutting down RemoteSubscriber...");

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
