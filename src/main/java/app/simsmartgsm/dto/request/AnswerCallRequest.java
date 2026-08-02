package app.simsmartgsm.dto.request;

import lombok.*;

/**
 * Request nhận cuộc gọi đến (Answer incoming call)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnswerCallRequest {
    /** COM port để nhận cuộc gọi */
    private String comPort;

    /** Số điện thoại SIM (optional, sẽ tự detect nếu không có) */
    private String simPhone;

    /** Số điện thoại người gọi đến (optional - dùng để filter đúng cuộc gọi) */
    private String expectedCaller;

    /** Thời gian chờ cuộc gọi đến (giây) - mặc định 60s */
    @Builder.Default
    private Integer waitTimeout = 60;

    /** Thời gian giữ cuộc gọi sau khi nhấc máy (giây) - mặc định 20s */
    @Builder.Default
    private Integer callDuration = 20;

    /** Có ghi âm không - mặc định true */
    @Builder.Default
    private Boolean record = true;

    /** Có chấp nhận cuộc gọi từ số ẩn không - mặc định false */
    @Builder.Default
    private Boolean acceptHiddenCaller = false;

    /** Service code (for remote calls) */
    private String serviceCode;

    /** Order ID (for remote calls) */
    private String orderId;
}
