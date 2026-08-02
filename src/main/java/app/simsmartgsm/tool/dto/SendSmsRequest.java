package app.simsmartgsm.tool.dto;

import jakarta.validation.constraints.NotBlank;

public record SendSmsRequest(
        @NotBlank String comPort,
        @NotBlank String toNumber,
        @NotBlank String message) {
}
