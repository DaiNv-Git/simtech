package app.simsmartgsm.dto.response;

import lombok.*;

/**
 * Response cho thông tin SIM
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SimResponse {
    private String comPort;
    private String status; // ONLINE, OFFLINE
    private String carrier; // Nhà mạng
    private String phoneNumber; // Số điện thoại
    private String iccid; // ICCID
    private String message; // Thông báo thêm
    
    // Thống kê tracking
    private int todaySms;
    private int todayCall;
}
