package app.simsmartgsm.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Response chứa thông tin proxy status
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProxyStatusResponse {

    private String comPort;
    private String phoneNumber;
    private String carrier;
    private String status;

    /** proxy address format: host:port */
    private String proxyAddress;
    private Integer proxyPort;
    private String publicIp;
    private String localIp;
    private String networkAdapter;
    private String apn;

    private int rotateCount;
    private long totalRequests;
    private long totalBytes;

    private boolean authRequired;
    private String username;

    private Instant startedAt;
    private Instant lastRotatedAt;
    private String errorMessage;

    /** Uptime in seconds */
    private long uptimeSeconds;
}
