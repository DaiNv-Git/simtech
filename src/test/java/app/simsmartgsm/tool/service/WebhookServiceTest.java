package app.simsmartgsm.tool.service;

import app.simsmartgsm.tool.model.WebhookSettingsDocument;
import app.simsmartgsm.tool.repository.WebhookSettingsRepository;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WebhookServiceTest {
    @Test
    void sendsSignedJsonWithBearerToken() throws Exception {
        AtomicReference<String> body = new AtomicReference<>();
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<String> signature = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/sms", exchange -> {
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            signature.set(exchange.getRequestHeaders().getFirst("X-SimTech-Signature"));
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        server.start();

        try {
            WebhookSettingsRepository repository = mock(WebhookSettingsRepository.class);
            WebhookSettingsDocument settings = WebhookSettingsDocument.builder()
                    .id("customer-webhook")
                    .enabled(true)
                    .url("http://127.0.0.1:" + server.getAddress().getPort() + "/sms")
                    .bearerToken("customer-token")
                    .signingSecret("customer-secret")
                    .build();
            when(repository.findById("customer-webhook")).thenReturn(Optional.of(settings));

            new WebhookService(repository).sendTest();

            assertThat(authorization.get()).isEqualTo("Bearer customer-token");
            assertThat(signature.get()).startsWith("sha256=");
            assertThat(body.get()).contains("\"event\":\"sms.received\"", "\"test\":true");
        } finally {
            server.stop(0);
        }
    }
}

