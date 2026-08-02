package app.simsmartgsm.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

/**
 * Entity lưu tin nhắn SMS
 */
@Entity
@Table(name = "sms_messages")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SmsMessageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** COM port gửi/nhận */
    @Column(name = "com_port")
    private String comPort;

    /** Số điện thoại SIM */
    @Column(name = "sim_phone")
    private String simPhone;

    /** Số gửi (INBOX) hoặc số nhận (OUTBOX/SENT) */
    @Column(name = "phone_number")
    private String phoneNumber;

    /** Nội dung tin nhắn */
    @Column(name = "content", columnDefinition = "TEXT")
    private String content;

    /** Loại: INBOX, SENT, OUTBOX (lỗi) */
    @Column(name = "type")
    private String type;

    /** Trạng thái: PENDING, SENT, FAILED, READ, UNREAD */
    @Column(name = "status")
    private String status;

    /** Lỗi nếu gửi thất bại */
    @Column(name = "error_message")
    private String errorMessage;

    /** Đã đọc chưa */
    @Column(name = "is_read")
    private boolean isRead;

    /** Thời gian tạo */
    @Column(name = "created_at")
    private LocalDateTime createdAt;

    /** Thời gian cập nhật */
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
