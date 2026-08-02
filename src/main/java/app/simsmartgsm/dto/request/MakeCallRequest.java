package app.simsmartgsm.dto.request;

import lombok.*;

/**
 * Request thực hiện cuộc gọi
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MakeCallRequest {
    /** COM port để gọi */
    private String comPort;

    /** Số điện thoại SIM (optional, sẽ tự detect nếu không có) */
    private String simPhone;

    /** Số điện thoại người nhận */
    private String targetPhone;

    /** Thời gian gọi (giây) - mặc định lấy từ settings */
    private Integer callDuration;

    /** Có ghi âm không - mặc định lấy từ settings */
    private Boolean record;

    /** Service code (for remote calls) */
    private String serviceCode;

    /** Order ID (for remote calls) */
    private String orderId;
}
