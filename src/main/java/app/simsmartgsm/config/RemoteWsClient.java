package app.simsmartgsm.config;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.*;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class RemoteWsClient {

    private static final String WS_URL = "wss://be-user-sms-global.smsglobalhub.com/ws-native";

    // Exponential backoff configuration
    private static final long INITIAL_RETRY_DELAY_MS = 5_000; // 5 seconds
    private static final long MAX_RETRY_DELAY_MS = 60_000; // 60 seconds
    private static final int MAX_QUEUE_SIZE = 1000; // Max offline messages

    private enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED
    }

    private volatile StompSession session;
    private volatile ConnectionState state = ConnectionState.DISCONNECTED;
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    private final AtomicLong lastConnectAttempt = new AtomicLong(0);

    // Offline message queue
    private final BlockingQueue<QueuedMessage> offlineQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "RemoteWsClient-Scheduler");
        t.setDaemon(true);
        return t;
    });

    private WebSocketStompClient stompClient;

    @PostConstruct
    public void init() {
        WebSocketClient client = new StandardWebSocketClient();
        stompClient = new WebSocketStompClient(client);
        stompClient.setMessageConverter(new MappingJackson2MessageConverter());

        // ✅ Configure TaskScheduler for heartbeat support
        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(1);
        taskScheduler.setThreadNamePrefix("ws-heartbeat-");
        taskScheduler.setDaemon(true);
        taskScheduler.initialize();
        stompClient.setTaskScheduler(taskScheduler);

        // ✅ Thêm heartbeat để giữ kết nối sống
        // [clientHeartbeat, serverHeartbeat] - đơn vị: milliseconds
        // 10000 = 10 giây - gửi ping mỗi 10s để tránh timeout
        stompClient.setDefaultHeartbeat(new long[] { 10000, 10000 });

        connect();
        scheduleHealthCheck();
    }

    @PreDestroy
    public void shutdown() {
        try {
            log.info("🧹 Shutting down RemoteWsClient...");

            // ✅ Disconnect session first
            if (session != null && session.isConnected()) {
                session.disconnect();
                log.info("✅ WebSocket session disconnected");
            }
            session = null;

            // ✅ Shutdown scheduler with proper timeout
            if (!scheduler.isShutdown()) {
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

            // ✅ Clear offline queue
            int queueSize = offlineQueue.size();
            offlineQueue.clear();
            if (queueSize > 0) {
                log.info("✅ Cleared {} messages from offline queue", queueSize);
            }

            log.info("✅ RemoteWsClient shutdown complete");
        } catch (Exception e) {
            log.error("❌ Error during shutdown: {}", e.getMessage(), e);
        }
    }

    public synchronized void connect() {
        if (state == ConnectionState.CONNECTING) {
            log.debug("🔄 Already connecting, skipping...");
            return;
        }

        // ✅ Ensure old session is fully disconnected first
        if (session != null && session.isConnected()) {
            try {
                log.info("🔌 Disconnecting old session before reconnect");
                session.disconnect();
                Thread.sleep(500); // Give it time to cleanup
            } catch (Exception e) {
                log.warn("⚠️ Error disconnecting old session: {}", e.getMessage());
            }
        }
        session = null;

        state = ConnectionState.CONNECTING;
        lastConnectAttempt.set(System.currentTimeMillis());

        try {
            log.info("🌐 Connecting to {} (attempt #{})", WS_URL, reconnectAttempts.get() + 1);

            CompletableFuture<StompSession> future = stompClient.connectAsync(
                    WS_URL,
                    new WebSocketHttpHeaders(),
                    new StompHeaders(),
                    new StompSessionHandlerAdapter() {

                        @Override
                        public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
                            RemoteWsClient.this.session = session;
                            state = ConnectionState.CONNECTED;
                            reconnectAttempts.set(0);
                            log.info("✅ WebSocket CONNECTED to {}", WS_URL);

                            // Process offline queue
                            processOfflineQueue();
                        }

                        @Override
                        public void handleTransportError(StompSession session, Throwable ex) {
                            log.error("❌ Transport error: {}", ex.getMessage());
                            state = ConnectionState.DISCONNECTED;
                            scheduleReconnect();
                        }

                        @Override
                        public void handleException(StompSession session, StompCommand command,
                                StompHeaders headers, byte[] payload, Throwable exception) {
                            log.error("❌ STOMP exception: {}", exception.getMessage());
                        }
                    });

            // Wait with timeout
            this.session = future.get(10, TimeUnit.SECONDS);

        } catch (TimeoutException e) {
            log.error("❌ Connection timeout after 10s");
            state = ConnectionState.DISCONNECTED;
            scheduleReconnect();
        } catch (Exception e) {
            log.error("❌ Connect failed: {}", e.getMessage());
            state = ConnectionState.DISCONNECTED;
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        int attempts = reconnectAttempts.incrementAndGet();

        // Exponential backoff: 5s, 10s, 20s, 40s, 60s (max)
        long delayMs = Math.min(
                INITIAL_RETRY_DELAY_MS * (1L << Math.min(attempts - 1, 4)),
                MAX_RETRY_DELAY_MS);

        log.warn("🔄 Scheduling reconnect in {}ms (attempt #{})", delayMs, attempts);

        scheduler.schedule(this::connect, delayMs, TimeUnit.MILLISECONDS);
    }

    private void scheduleHealthCheck() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (session == null || !session.isConnected()) {
                    if (state != ConnectionState.CONNECTING) {
                        log.warn("⚠️ Health check failed - disconnected");
                        state = ConnectionState.DISCONNECTED;
                        connect();
                    }
                }
            } catch (Exception e) {
                log.error("❌ Health check error: {}", e.getMessage());
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    public void send(String topic, Object payload) {
        try {
            if (session != null && session.isConnected()) {
                session.send(topic, payload);
                log.info("📡 Sent WS → {} : {}", topic, payload);
            } else {
                // Queue message for later
                if (offlineQueue.size() < MAX_QUEUE_SIZE) {
                    offlineQueue.offer(new QueuedMessage(topic, payload));
                    log.warn("⚠️ WS offline - queued message ({} in queue)", offlineQueue.size());
                } else {
                    log.error("❌ Offline queue full - message dropped!");
                }

                // Trigger reconnect if not already connecting
                if (state == ConnectionState.DISCONNECTED) {
                    connect();
                }
            }

        } catch (Exception e) {
            log.error("❌ WS send error: {}", e.getMessage());

            // Queue the message and reconnect
            if (offlineQueue.size() < MAX_QUEUE_SIZE) {
                offlineQueue.offer(new QueuedMessage(topic, payload));
            }

            if (state == ConnectionState.CONNECTED) {
                state = ConnectionState.DISCONNECTED;
                scheduleReconnect();
            }
        }
    }

    private void processOfflineQueue() {
        int processed = 0;
        int failed = 0;
        int stale = 0;
        long now = System.currentTimeMillis();
        long staleThreshold = 5 * 60 * 1000; // 5 minutes

        while (!offlineQueue.isEmpty() && session != null && session.isConnected()) {
            QueuedMessage msg = offlineQueue.poll();
            if (msg != null) {
                // ✅ Remove stale messages to prevent unbounded growth
                if (now - msg.timestamp > staleThreshold) {
                    stale++;
                    log.debug("🗑️ Dropped stale message (age: {}ms)", now - msg.timestamp);
                    continue;
                }

                try {
                    session.send(msg.topic, msg.payload);
                    processed++;
                } catch (Exception e) {
                    log.error("❌ Failed to send queued message: {}", e.getMessage());
                    failed++;
                    // Put it back if failed
                    offlineQueue.offer(msg);
                    break;
                }
            }
        }

        if (processed > 0 || stale > 0) {
            log.info("✅ Processed {} offline messages ({} failed, {} stale, {} remaining)",
                    processed, failed, stale, offlineQueue.size());
        }
    }

    public boolean isConnected() {
        return session != null && session.isConnected() && state == ConnectionState.CONNECTED;
    }

    public int getQueueSize() {
        return offlineQueue.size();
    }

    private static class QueuedMessage {
        final String topic;
        final Object payload;
        final long timestamp;

        QueuedMessage(String topic, Object payload) {
            this.topic = topic;
            this.payload = payload;
            this.timestamp = System.currentTimeMillis();
        }
    }
}
