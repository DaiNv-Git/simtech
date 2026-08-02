package app.simsmartgsm.baseGateway;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "gsm")
public class GsmProperties {

    private Api api = new Api();
    private Record record = new Record();
    private Recording recording = new Recording();
    private Sms sms = new Sms();
    private Ssh ssh = new Ssh();
    private boolean otpOnly = true;
    private boolean disableDataOnStartup = true;

    // ===== Sub-config classes =====
    @Data
    public static class Api {
        private String baseUrl;
    }

    @Data
    public static class Record {
        private String uploadUrl;    // ✅ thêm dòng này
        private String publicUrl;
        private String callbackUrl;
        private String uploadDir;
    }

    @Data
    public static class Recording {
        private String localTemp;
    }

    @Data
    public static class Sms {
        /**
         * Khoảng cách tối thiểu giữa 2 SMS liên tiếp trên cùng một SIM.
         */
        private long minIntervalMs = 500;

        /**
         * Jitter ngẫu nhiên cộng thêm vào minIntervalMs để phá pattern gửi đều.
         */
        private long jitterMs = 0;

        /**
         * Backup scan interval for OTP receive when hardware URC is lost.
         */
        private long backupScanDelayMs = 5000;
    }

    @Data
    public static class Ssh {
        private String host;
        private int port = 22;
        private String user;
        private String password;
    }
}
