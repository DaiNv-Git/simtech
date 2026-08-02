package app.simsmartgsm.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDate;

/**
 * 📊 SMS Daily Counter - MongoDB Document
 * 
 * Lưu giới hạn SMS đã gửi mỗi ngày theo deviceId + comName.
 * Khi restart app, dữ liệu vẫn còn → không bị reset counter.
 * 
 * Collection: sms_daily_counters
 * Compound unique index: (date, deviceId, comName)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "sms_daily_counters")
@CompoundIndex(name = "idx_date_device_com", def = "{'date': 1, 'deviceId': 1, 'comName': 1}", unique = true)
public class SmsDailyCounter {

    @Id
    private String id;

    /** Ngày thống kê (YYYY-MM-DD) */
    private LocalDate date;

    /** Device ID (hostname-based, từ DeviceIdProvider) */
    private String deviceId;

    /** COM port name (e.g. COM3, /dev/ttyUSB0) */
    private String comName;

    /** Số segments đã gửi thành công trong ngày */
    @Builder.Default
    private int segmentsSent = 0;

    /** Số lần gửi fail liên tiếp hiện tại */
    @Builder.Default
    private int consecutiveFails = 0;

    /** Tổng số lần fail trong ngày */
    @Builder.Default
    private int dailyFails = 0;

    /** SIM có bị blacklist không */
    @Builder.Default
    private boolean blacklisted = false;
}
