package app.simsmartgsm.tool.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@Document(collection = "settings")
public class WebhookSettingsDocument {
    @Id
    @Builder.Default
    private String id = "customer-webhook";
    private boolean enabled;
    private String url;
    private String bearerToken;
    private String signingSecret;
    private Instant updatedAt;
}

