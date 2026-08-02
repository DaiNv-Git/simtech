package app.simsmartgsm.dto.request;

import lombok.*;
import java.util.List;

/**
 * Request gửi SMS hàng loạt (Bulk SMS)
 * Có thể gửi đến nhiều số điện thoại qua nhiều COM port
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BulkSmsRequest {
    /** Danh sách COM port để gửi (gửi song song) */
    private List<String> comPorts;

    /** Danh sách số điện thoại nhận */
    private List<String> phoneNumbers;

    /** Nội dung tin nhắn */
    private String content;
}
