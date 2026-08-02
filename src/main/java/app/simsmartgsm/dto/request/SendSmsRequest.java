package app.simsmartgsm.dto.request;

import lombok.*;
import java.util.List;

/**
 * Request gửi SMS đơn
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SendSmsRequest {
    private String comPort;
    private String phoneNumber;
    private String content;
}
