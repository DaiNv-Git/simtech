package app.simsmartgsm.config;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Configuration
@Slf4j
public class RemoteStompClientConfig {

    private static final String REMOTE_WS_URL = "wss://be-user-sms-global.smsglobalhub.com/ws";
    private final AtomicReference<StompSession> stompSessionRef = new AtomicReference<>();
    private final List<Consumer<StompSession>> onConnectedCallbacks = new CopyOnWriteArrayList<>();

    private ReconnectionManager reconnectionManager;
    private WebSocketStompClient stompClient;
    private ThreadPoolTaskScheduler taskScheduler;

    @PostConstruct
    public void connectToRemoteBroker() {
        // ✅ Tạo reconnection manager
        reconnectionManager = new ReconnectionManager("RemoteStompClient", this::doConnect);

        // ✅ Tạo scheduler 1 lần duy nhất (tránh memory leak)
        taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setThreadNamePrefix("WS-Stomp-");
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
     * ✅ Thực hiện kết nối (được gọi bởi ReconnectionManager)
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

    /**
     * ✅ Cleanup khi shutdown
     */
    @PreDestroy
    public void cleanup() {
        log.info("🧹 Shutting down RemoteStompClient...");

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

    public void addOnConnectedCallback(Consumer<StompSession> callback) {
        onConnectedCallbacks.add(callback);
    }

    // Trong afterConnected:

    /** ✅ service khác gọi để lấy session */
    public StompSession getSession() {
        return stompSessionRef.get();
    }
}
