package app.simsmartgsm.entity;

import lombok.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;

/**
 * Entity lưu lịch sử cuộc gọi
 */
@Document(collection = "dashboard_call_records")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CallRecordEntity {

    @Id
    private String id;

    /** COM port thực hiện cuộc gọi */
    private String comPort;

    /** Số điện thoại SIM gọi đi */
    private String simPhone;

    /** Số điện thoại người nhận */
    private String targetPhone;

    /** Thời gian gọi (giây) - setting từ user */
    private Integer callDuration;

    /** Thời gian thực tế (giây) */
    private Integer actualDuration;

    /** Có ghi âm không */
    private boolean isRecorded;

    /** Đường dẫn file ghi âm (nếu có) */
    private String recordingPath;

    /** Trạng thái: PENDING, RINGING, CONNECTED, COMPLETED, FAILED, CANCELLED */
    private String status;

    /** Loại cuộc gọi: OUTGOING (gọi đi), INCOMING (gọi đến) */
    private String callType;

    /** Lỗi nếu thất bại */
    private String errorMessage;

    /** Thời gian bắt đầu gọi */
    private LocalDateTime callStartTime;

    /** Thời gian kết thúc */
    private LocalDateTime callEndTime;

    /** Thời gian tạo record */
    @CreatedDate
    private LocalDateTime createdAt;
}
