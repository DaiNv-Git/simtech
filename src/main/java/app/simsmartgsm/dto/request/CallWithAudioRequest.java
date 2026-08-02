package app.simsmartgsm.dto.request;

import lombok.*;

/**
 * Request gọi điện và phát file audio ghi sẵn cho đối phương nghe
 * Sử dụng AT+QPSND để phát audio vào uplink (đối phương nghe được)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CallWithAudioRequest {
    /** COM port để gọi */
    private String comPort;

    /** Số điện thoại người nhận */
    private String targetPhone;

    /**
     * Tên file audio trên modem (VD: "greeting.wav")
     * File này phải được upload lên modem trước bằng API /call/upload-audio
     * Format: PCM 8KHz 16bit Mono (.wav) hoặc AMR (.amr)
     */
    private String audioFileName;

    /**
     * Đường dẫn file audio trên server local (nếu cần upload lên modem trước khi gọi)
     * VD: "/home/audio/greeting.wav"
     * Nếu để trống, sẽ dùng audioFileName đã có sẵn trên modem
     */
    private String localAudioPath;

    /** Có lặp lại audio không (mặc định: false - phát 1 lần) */
    @Builder.Default
    private Boolean repeatAudio = false;

    /** Thời gian chờ sau khi phát xong audio trước khi ngắt (giây, mặc định: 2) */
    @Builder.Default
    private Integer waitAfterAudioSeconds = 2;

    /** Có ghi âm cuộc gọi không (ghi cả audio phát lẫn phản hồi đối phương) */
    @Builder.Default
    private Boolean record = false;

    /** Service code (for remote calls) */
    private String serviceCode;

    /** Order ID (for remote calls) */
    private String orderId;
}
