package app.simsmartgsm.entity;

import lombok.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;

/**
 * Entity lưu tin nhắn SMS
 */
@Document(collection = "sms")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SmsMessageEntity {

    @Id
    private String id;

    /** COM port gửi/nhận */
    private String comPort;

    /** Số điện thoại SIM */
    private String simPhone;

    /** Số gửi (INBOX) hoặc số nhận (OUTBOX/SENT) */
    private String phoneNumber;

    /** Nội dung tin nhắn */
    private String content;

    /** Loại: INBOX, SENT, OUTBOX (lỗi) */
    private String type;

    /** Mongo dashboard filter: INBOUND hoặc OUTBOUND. */
    private String direction;

    /** Trạng thái: PENDING, SENT, FAILED, READ, UNREAD */
    private String status;

    /** Lỗi nếu gửi thất bại */
    private String errorMessage;

    private String modemResponse;

    /** Đã đọc chưa */
    private boolean isRead;

    /** Thời gian tạo */
    @CreatedDate
    private LocalDateTime createdAt;

    /** Thời gian cập nhật */
    @LastModifiedDate
    private LocalDateTime updatedAt;
}
