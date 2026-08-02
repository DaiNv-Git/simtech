package app.simsmartgsm.config;

import app.simsmartgsm.uitils.DeviceIdProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.*;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class RemoteCallEventSubscriberConfig {

    private static final String REMOTE_WS_URL = "wss://be-user-sms-global.smsglobalhub.com/ws";
    private static final String SUB_TOPIC = "/topic/call-events";

    // ⚠️ Removed old wsFailCount and MAX_WS_FAIL

    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicReference<StompSession> stompSessionRef = new AtomicReference<>();

    private ReconnectionManager reconnectionManager;
    private WebSocketStompClient stompClient;
    private ThreadPoolTaskScheduler taskScheduler;
    private String localDeviceName;

    @PostConstruct
    public void subscribeToCallEvents() {
        try {
            this.localDeviceName = DeviceIdProvider.getDeviceId();

        } catch (Exception e) {
            this.localDeviceName = "UNKNOWN_HOST";
        }

        log.info("📡 Subscribing CALL_EVENT listener for device {}", localDeviceName);

        // ✅ Tạo reconnection manager
        reconnectionManager = new ReconnectionManager("CallEventSubscriber", this::doConnect);

        // ✅ Tạo scheduler 1 lần duy nhất
        taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setThreadNamePrefix("WS-CallEvent-");
        taskScheduler.afterPropertiesSet();

        List<Transport> transports = List.of(new WebSocketTransport(new StandardWebSocketClient()));
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
        stompClient.connectAsync(REMOTE_WS_URL, new StompSessionHandlerAdapter() {

            @Override
            public void afterConnected(StompSession session, StompHeaders headers) {
                log.info("✅ CALL_EVENT Connected");

                stompSessionRef.set(session);

                // ✅ Thông báo reconnection manager
                reconnectionManager.onConnected();

                subscribe(session);
            }

            @Override
            public void handleTransportError(StompSession session, Throwable ex) {
                log.error("❌ CALL_EVENT transport error: {}", ex.getMessage());

                stompSessionRef.set(null);

                // ✅ Sử dụng reconnection manager
                reconnectionManager.onConnectionFailed("transport error");
            }
        });
    }

    private void subscribe(StompSession session) {
        session.subscribe(SUB_TOPIC, new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
                return Object.class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
                try {
                    String json = payload instanceof byte[]
                            ? new String((byte[]) payload, StandardCharsets.UTF_8)
                            : payload.toString();

                    log.info("📨 CALL_EVENT RAW: {}", json);

                    Map<String, Object> event = mapper.readValue(json, Map.class);

                    // Lọc theo device
                    String device = (String) event.get("deviceName");
                    if (device == null || !device.equalsIgnoreCase(localDeviceName)) {
                        return;
                    }

                    String com = (String) event.get("com");
                    String status = (String) event.get("status");
                    String phone = (String) event.get("phone");

                    log.info("📞 CALL_EVENT: COM={} | phone={} | status={}", com, phone, status);

                    // TODO: callback sang CallService nếu muốn
                    // callService.onStatus(...);

                } catch (Exception e) {
                    log.error("❌ Lỗi xử lý CALL_EVENT: {}", e.getMessage(), e);
                }
            }
        });

        log.info("👂 Listening to {}", SUB_TOPIC);
    }

    public void pushCallStatusToServer(Map<String, Object> event) {
        try {
            StompSession session = stompSessionRef.get();

            if (session == null || !session.isConnected()) {
                log.warn("⚠️ WS not connected → cannot send /topic/receive-call");
                return;
            }

            session.send("/topic/receive-call", mapper.writeValueAsBytes(event));

            log.info("📡 REMOTE SEND → /app/receive-call : {}", event);

        } catch (Exception e) {
            log.error("❌ SEND REMOTE WS FAILED: {}", e.getMessage(), e);
        }
    }

    /**
     * ✅ Cleanup khi shutdown
     */
    @PreDestroy
    public void cleanup() {
        log.info("🧹 Shutting down CallEventSubscriber...");

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
}