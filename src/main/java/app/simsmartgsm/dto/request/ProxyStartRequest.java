package app.simsmartgsm.dto.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProxyStartRequest {

    /** COM port để bắt đầu proxy (e.g., "COM3") */
    private String comPort;

    /** APN nhà mạng (mặc định: "internet") */
    @Builder.Default
    private String apn = "internet";

    /** Port HTTP proxy muốn dùng (0 = tự động chọn) */
    @Builder.Default
    private int proxyPort = 0;

    /** Username cho proxy auth (để trống = không auth) */
    private String username;

    /** Password cho proxy auth */
    private String password;
}
