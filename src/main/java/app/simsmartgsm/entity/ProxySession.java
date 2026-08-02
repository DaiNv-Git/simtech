package app.simsmartgsm.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

/**
 * Entity lưu trạng thái mỗi proxy session.
 * Mỗi SIM (COM port) sẽ có 1 proxy session tương ứng.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "proxy_sessions")
public class ProxySession {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** COM port gắn với modem (e.g., COM3) */
    @Column(unique = true)
    private String comPort;

    /** Số điện thoại SIM */
    private String phoneNumber;

    /** ICCID của SIM */
    private String iccid;

    /** Nhà mạng */
    private String carrier;

    /** Trạng thái proxy: STOPPED, CONNECTING, CONNECTED, ERROR */
    @Builder.Default
    private String status = "STOPPED";

    /** Port proxy HTTP đang listen */
    private Integer proxyPort;

    /** Port proxy SOCKS5 đang listen (optional) */
    private Integer socksPort;

    /** IP public hiện tại từ nhà mạng */
    private String publicIp;

    /** IP local của network adapter */
    private String localIp;

    /** Tên network adapter trên Windows */
    private String networkAdapter;

    /** APN được sử dụng */
    @Builder.Default
    private String apn = "internet";

    /** Số lần rotate IP */
    @Builder.Default
    private int rotateCount = 0;

    /** Tổng requests đã xử lý */
    @Builder.Default
    private long totalRequests = 0;

    /** Tổng bytes transferred */
    @Builder.Default
    private long totalBytes = 0;

    /** Username cho proxy auth (optional) */
    private String username;

    /** Password cho proxy auth (optional) */
    private String password;

    /** Thời điểm bắt đầu */
    private Instant startedAt;

    /** Thời điểm rotate IP lần cuối */
    private Instant lastRotatedAt;

    /** Thời điểm cập nhật cuối */
    private Instant updatedAt;

    /** Error message nếu có */
    private String errorMessage;

    @PreUpdate
    public void preUpdate() {
        this.updatedAt = Instant.now();
    }
}
