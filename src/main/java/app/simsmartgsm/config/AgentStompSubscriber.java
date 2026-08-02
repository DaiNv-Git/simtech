package app.simsmartgsm.config;

import app.simsmartgsm.uitils.AtCommandHelper;
import app.simsmartgsm.uitils.DeviceIdProvider;
import com.fazecast.jSerialComm.SerialPort;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.stomp.*;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class AgentStompSubscriber {

    private final RemoteStompClientConfig remoteConfig;
    private final PowerShellHelper ps;

    public static String agentId;

    @PostConstruct
    public void init() {
        try {
            // 🔍 Lấy agentId từ modem hoặc IP
            agentId = detectAgentId();
            if (agentId == null || agentId.isBlank()) {
                agentId = "UNKNOWN_AGENT";
            }

            log.info("🆔 Using Agent ID: {}", agentId);

            // Chờ kết nối WS hoàn thành (có timeout)
            int waitCount = 0;
            while (remoteConfig.getSession() == null && waitCount < 10) {
                log.info("⏳ Waiting STOMP connection... ({}s)", waitCount * 2);
                Thread.sleep(2000);
                waitCount++;
            }

            StompSession session = remoteConfig.getSession();
            if (session == null || !session.isConnected()) {
                log.error("❌ STOMP session not connected, cannot subscribe agent topic.");
                return;
            }

            String topic = "/topic/agent/" + agentId;
            session.subscribe(topic, new StompFrameHandler() {
                @Override
                public Type getPayloadType(StompHeaders headers) {
                    return Map.class;
                }

                @Override
                @SuppressWarnings("unchecked")
                public void handleFrame(StompHeaders headers, Object payload) {
                    try {
                        log.info("📩 Received frame from Cloud: {}", payload);
                        Map<String, Object> msg = (Map<String, Object>) payload;

                        // ✅ REMOVED: Proxy handling (AgentMessageHandler deleted)
                        // Now only logs received messages
                        log.info("📬 Agent message received (proxy handling removed): {}", msg);

                    } catch (Exception e) {
                        log.error("❌ Error handling frame: {}", e.getMessage(), e);
                    }
                }
            });

            log.info("✅ Subscribed to {}", topic);

            // 🛰 Gửi ping "agent online" lên server
            sendHello(session);

        } catch (Exception e) {
            log.error("❌ Agent init error: {}", e.getMessage(), e);
        }
    }

    /** Gửi thông báo agent online lên Cloud */
    private void sendHello(StompSession session) {
        try {
            if (session == null || !session.isConnected()) {
                log.warn("⚠️ Session not ready, skip sending hello");
                return;
            }

            Map<String, Object> hello = new HashMap<>();
            hello.put("agentId", agentId);
            hello.put("status", "ONLINE");

            session.send("/app/fromAgent/hello", hello);
            log.info("📡 Sent hello to Cloud (agentId={})", agentId);
        } catch (Exception e) {
            log.error("❌ Failed to send hello: {}", e.getMessage(), e);
        }
    }

    /**
     * 🔎 Lấy agentId từ modem hoặc hostname
     */
    private String detectAgentId() {
        try {
            // Quét tất cả cổng COM để tìm modem
            for (SerialPort port : SerialPort.getCommPorts()) {
                try (AtCommandHelper at = AtCommandHelper.open(port.getSystemPortName(), 115200, 3000, 3000)) {
                    if (at.ping()) {
                        String model = at.sendAndRead("AT+CGMM", 1000);
                        String vendor = at.sendAndRead("AT+CGMI", 1000);
                        String ccid = at.getCcid();
                        String id = (vendor + "-" + model + "-" + ccid)
                                .replaceAll("[^A-Za-z0-9-]", "");
                        if (!id.isBlank()) {
                            return id;
                        }
                    }
                } catch (Exception ignored) {
                }
            }
        } catch (Exception e) {
            log.warn("⚠️ Cannot get ID from modem: {}", e.getMessage());
        }

        // Fallback = IP hoặc hostname
        String ip = ps.getFirstMobileIp();
        if (ip == null || ip.isBlank()) {
            try {
                ip = DeviceIdProvider.getDeviceId();
            } catch (Exception ignored) {
                ip = "UNKNOWN";
            }
        }
        return ip;
    }
}
