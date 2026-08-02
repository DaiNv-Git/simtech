package app.simsmartgsm.uitils;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class DeviceIdProvider {

    private static final Path DEVICE_ID_FILE =
            Path.of(System.getProperty("user.home"), ".gsm_device_id");

    public static String getDeviceId() {
        try {
            if (Files.exists(DEVICE_ID_FILE)) {
                return Files.readString(DEVICE_ID_FILE).trim();
            }

            // Lấy hostname
            String hostname;
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                hostname = "DEVICE";
            }

            hostname = hostname.replaceAll("[^A-Za-z0-9-]", "");

            // Short ID (4 ký tự cuối)
            String uuid = UUID.randomUUID().toString();
            String shortId = uuid.substring(uuid.length() - 4).toUpperCase();

            // Ghép ID đẹp
            String deviceId = hostname + "-" + shortId;

            // Lưu vào file
            Files.writeString(DEVICE_ID_FILE, deviceId);

            return deviceId;

        } catch (IOException e) {
            return "DEVICE-ERR";
        }
    }
}
