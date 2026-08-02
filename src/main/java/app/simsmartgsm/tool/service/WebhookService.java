package app.simsmartgsm.tool.service;

import app.simsmartgsm.tool.dto.WebhookSettingsRequest;
import app.simsmartgsm.tool.model.WebhookSettingsDocument;
import app.simsmartgsm.tool.repository.WebhookSettingsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class WebhookService {
    private static final String SETTINGS_ID = "customer-webhook";

    private final WebhookSettingsRepository repository;
    private final RestTemplate webhookRestTemplate = createRestTemplate();

    public Map<String, Object> getPublicSettings() {
        WebhookSettingsDocument settings = load();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("enabled", settings.isEnabled());
        response.put("url", value(settings.getUrl()));
        response.put("bearerTokenConfigured", hasText(settings.getBearerToken()));
        response.put("signingSecretConfigured", hasText(settings.getSigningSecret()));
        response.put("updatedAt", settings.getUpdatedAt());
        return response;
    }

    public Map<String, Object> update(WebhookSettingsRequest request) {
        String url = value(request.url()).trim();
        if (request.enabled() && url.isBlank()) {
            throw new IllegalArgumentException("Cần nhập Webhook URL khi bật webhook");
        }
        validateUrl(url);

        WebhookSettingsDocument current = load();
        current.setEnabled(request.enabled());
        current.setUrl(url);
        if (request.clearBearerToken()) {
            current.setBearerToken(null);
        } else if (hasText(request.bearerToken())) {
            current.setBearerToken(request.bearerToken().trim());
        }
        if (request.clearSigningSecret()) {
            current.setSigningSecret(null);
        } else if (hasText(request.signingSecret())) {
            current.setSigningSecret(request.signingSecret().trim());
        }
        current.setUpdatedAt(Instant.now());
        repository.save(current);
        return getPublicSettings();
    }

    @Async
    public void sendIncomingSms(String comPort, String simPhone, String sender, String content) {
        WebhookSettingsDocument settings = load();
        if (!settings.isEnabled() || !hasText(settings.getUrl())) {
            return;
        }
        deliver(settings, createPayload(comPort, simPhone, sender, content, false));
    }

    public void sendTest() {
        WebhookSettingsDocument settings = load();
        if (!settings.isEnabled() || !hasText(settings.getUrl())) {
            throw new IllegalStateException("Webhook chưa được bật hoặc chưa có URL");
        }
        if (!deliver(settings, createPayload("TEST", "0000000000", "simTech", "Tin nhắn kiểm tra webhook", true))) {
            throw new IllegalStateException("Webhook không phản hồi thành công");
        }
    }

    private boolean deliver(WebhookSettingsDocument settings, Map<String, Object> payload) {
        try {
            String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(payload);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("User-Agent", "simTech/1.0");
            headers.set("X-SimTech-Event", "sms.received");
            headers.set("X-SimTech-Delivery", String.valueOf(payload.get("eventId")));
            if (hasText(settings.getBearerToken())) {
                headers.setBearerAuth(settings.getBearerToken());
            }
            if (hasText(settings.getSigningSecret())) {
                headers.set("X-SimTech-Signature", "sha256=" + sign(json, settings.getSigningSecret()));
            }
            webhookRestTemplate.postForEntity(settings.getUrl(), new HttpEntity<>(json, headers), String.class);
            log.info("Đã gửi SMS tới webhook khách hàng");
            return true;
        } catch (Exception e) {
            log.error("Gửi webhook thất bại: {}", e.getMessage());
            return false;
        }
    }

    private Map<String, Object> createPayload(
            String comPort, String simPhone, String sender, String content, boolean test) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("eventId", UUID.randomUUID().toString());
        payload.put("event", "sms.received");
        payload.put("test", test);
        payload.put("occurredAt", Instant.now().toString());
        payload.put("comPort", comPort);
        payload.put("simPhone", simPhone);
        payload.put("from", sender);
        payload.put("content", content);
        return payload;
    }

    private String sign(String body, String secret) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }

    private WebhookSettingsDocument load() {
        return repository.findById(SETTINGS_ID)
                .orElseGet(() -> WebhookSettingsDocument.builder().id(SETTINGS_ID).build());
    }

    private void validateUrl(String url) {
        if (url.isBlank()) {
            return;
        }
        URI uri = URI.create(url);
        if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                || uri.getHost() == null) {
            throw new IllegalArgumentException("Webhook URL không hợp lệ");
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static String value(String value) {
        return value == null ? "" : value;
    }

    private static RestTemplate createRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(5_000);
        factory.setReadTimeout(10_000);
        return new RestTemplate(factory);
    }
}
