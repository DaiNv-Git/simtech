package app.simsmartgsm.config;

import app.simsmartgsm.uitils.DeviceIdProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@Component
@Slf4j
public class PowerShellHelper {

    public String getFirstMobileIp() {
        try {
            String cmd = "powershell -Command \"(Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -match 'PPP|WWAN|Mobile' }).IPAddress | Select-Object -First 1\"";
            Process p = Runtime.getRuntime().exec(cmd);
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line = r.readLine();
                if (line != null && !line.isBlank()) {
                    log.info("📡 Found mobile interface IP: {}", line.trim());
                    return line.trim();
                }
            }
        } catch (Exception e) {
            log.warn("⚠️ getFirstMobileIp failed: {}", e.getMessage());
        }
        return null;
    }

    public String getLanIp() {
        try {
            String cmd = "powershell -Command \"(Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -match 'Wi-Fi|Ethernet' -and $_.IPAddress -notmatch '169.254' }).IPAddress | Select-Object -First 1\"";
            Process p = Runtime.getRuntime().exec(cmd);
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line = r.readLine();
                if (line != null && !line.isBlank()) {
                    log.info("📡 Found LAN IP: {}", line.trim());
                    return line.trim();
                }
            }
        } catch (Exception e) {
            log.warn("⚠️ getLanIp failed: {}", e.getMessage());
        }
        return null;
    }

    public String getHostName() {
        try {
            String hostname = DeviceIdProvider.getDeviceId();
            log.info("🖥️ Hostname = {}", hostname);
            return hostname;
        } catch (Exception e) {
            log.error("❌ Cannot get hostname: {}", e.getMessage());
            return "unknown-host";
        }
    }
}
