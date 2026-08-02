package app.simsmartgsm.tool.dto;

import jakarta.validation.constraints.Pattern;

public record WebhookSettingsRequest(
        boolean enabled,
        @Pattern(regexp = "^$|https?://.+", message = "Webhook URL phải bắt đầu bằng http:// hoặc https://")
        String url,
        String bearerToken,
        String signingSecret,
        boolean clearBearerToken,
        boolean clearSigningSecret) {
}

