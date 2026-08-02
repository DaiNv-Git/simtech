package app.simsmartgsm.entity;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.UUID;

/**
 * Bảng SIM chứa danh sách SIM đang có trong hệ thống.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(collection = "sims")
public class Sim {
    
    @Id
    @Builder.Default
    private String id = UUID.randomUUID().toString();

    /** Số điện thoại SIM */
    private String phoneNumber;

    /** Tổng doanh thu từ SIM này */
    private Double revenue;

    /** Trạng thái SIM: new, active, inactive */
    private String status;

    /** Mã quốc gia (mặc định JVM) */
    @Builder.Default
    private String countryCode = "JP";

    /** Thiết bị đang chứa SIM (tên server/vps) */
    private String deviceName;

    /** Tên cổng COM mà SIM được kết nối */
    private String comName;

    /** Nhà mạng cung cấp SIM */
    private String simProvider;

    /** ICCID/CCID của SIM */
    private String ccid;

    /** ICCID gốc từ file import, giữ lại để đối chiếu khi modem trả chuỗi khác. */
    private String importedCcid;

    /** ICCID import đã chuẩn hóa chỉ còn chữ số. */
    private String importedCcidNormalized;

    /** Số thứ tự trong file import gần nhất. */
    private Integer importSequence;

    /** Nguồn tạo/cập nhật record: SCAN hoặc IMPORT. */
    private String dataSource;

    /** Điểm đối chiếu CCID gần nhất, 100 là khớp tuyệt đối. */
    private Integer ccidMatchScore;

    private String imsi;

    private String agentId;

    /** Nội dung (ghi chú / thông tin khác) */
    private String content;

    /** Thời điểm cập nhật cuối cùng */
    private Instant lastUpdated;

    private int missCount;

    /** Ngày SIM được kích hoạt */
    private Instant activeDate;

    @Builder.Default
    private boolean allowSms = true;       // Cho phép gửi SMS
    @Builder.Default
    private int smsFailedCount = 0;        // Số lần gửi lỗi liên tiếp
    @Builder.Default
    private int smsSuccessCount = 0;       // Tổng SMS gửi thành công
    @Builder.Default
    private int smsSentTotal = 0;          // Tổng SMS đã gửi (success + fail)
}
