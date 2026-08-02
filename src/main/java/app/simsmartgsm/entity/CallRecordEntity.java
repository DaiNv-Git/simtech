package app.simsmartgsm.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

/**
 * Entity lưu lịch sử cuộc gọi
 */
@Entity
@Table(name = "call_records")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CallRecordEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** COM port thực hiện cuộc gọi */
    @Column(name = "com_port")
    private String comPort;

    /** Số điện thoại SIM gọi đi */
    @Column(name = "sim_phone")
    private String simPhone;

    /** Số điện thoại người nhận */
    @Column(name = "target_phone")
    private String targetPhone;

    /** Thời gian gọi (giây) - setting từ user */
    @Column(name = "call_duration")
    private Integer callDuration;

    /** Thời gian thực tế (giây) */
    @Column(name = "actual_duration")
    private Integer actualDuration;

    /** Có ghi âm không */
    @Column(name = "is_recorded")
    private boolean isRecorded;

    /** Đường dẫn file ghi âm (nếu có) */
    @Column(name = "recording_path")
    private String recordingPath;

    /** Trạng thái: PENDING, RINGING, CONNECTED, COMPLETED, FAILED, CANCELLED */
    @Column(name = "status")
    private String status;

    /** Loại cuộc gọi: OUTGOING (gọi đi), INCOMING (gọi đến) */
    @Column(name = "call_type")
    private String callType;

    /** Lỗi nếu thất bại */
    @Column(name = "error_message")
    private String errorMessage;

    /** Thời gian bắt đầu gọi */
    @Column(name = "call_start_time")
    private LocalDateTime callStartTime;

    /** Thời gian kết thúc */
    @Column(name = "call_end_time")
    private LocalDateTime callEndTime;

    /** Thời gian tạo record */
    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }
}
