package app.simsmartgsm.tool.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

@Service
@Slf4j
public class TelegramService {
    private static final int MESSAGE_LIMIT = 4096;

    private final RestTemplate restTemplate;
    private final boolean enabled;
    private final String botToken;
    private final String chatId;

    public TelegramService(
            @Value("${telegram.enabled:false}") boolean enabled,
            @Value("${telegram.bot-token:}") String botToken,
            @Value("${telegram.chat-id:}") String chatId) {
        this.enabled = enabled;
        this.botToken = trim(botToken);
        this.chatId = trim(chatId);
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(5_000);
        factory.setReadTimeout(10_000);
        this.restTemplate = new RestTemplate(factory);
    }

    public boolean isConfigured() {
        return enabled && !botToken.isBlank() && !chatId.isBlank();
    }

    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void notifyApplicationReady() {
        if (!isConfigured()) {
            log.info("Telegram chưa được cấu hình, bỏ qua thông báo khởi động");
            return;
        }
        sendMessage("✅ simTech đã kết nối Telegram thành công.");
    }

    @Async
    public void sendIncomingSms(String deviceName, String comPort, String simPhone, String sender, String body) {
        sendMessage(formatIncomingSms(deviceName, comPort, simPhone, sender, body));
    }

    static String formatIncomingSms(String deviceName, String comPort, String simPhone, String sender, String body) {
        return value(deviceName, "UNKNOWN-DEVICE") + "\n"
                + "COM: " + value(comPort, "UNKNOWN") + "\n"
                + "FROM: " + value(sender, "UNKNOWN") + "\n"
                + "TO: " + value(simPhone, "UNKNOWN") + "\n"
                + "MSG: " + value(body, "");
    }

    public void sendMessage(String message) {
        if (!isConfigured() || message == null || message.isBlank()) {
            return;
        }
        for (int offset = 0; offset < message.length(); offset += MESSAGE_LIMIT) {
            int end = Math.min(offset + MESSAGE_LIMIT, message.length());
            sendChunk(message.substring(offset, end));
        }
    }

    private void sendChunk(String message) {
        try {
            MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            form.add("chat_id", chatId);
            form.add("text", message);
            form.add("disable_web_page_preview", "true");
            restTemplate.postForEntity(
                    "https://api.telegram.org/bot" + botToken + "/sendMessage",
                    form,
                    String.class);
            log.info("Đã gửi SMS tới Telegram");
        } catch (Exception e) {
            log.error("Gửi Telegram thất bại: {}", e.getMessage());
        }
    }

    private static String trim(String value) {
        return value == null ? "" : value.trim();
    }

    private static String value(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
