package app.simsmartgsm.config;
import com.fazecast.jSerialComm.SerialPort;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class PortResolver {

    // Map logical -> system (ví dụ "COM80" -> "/dev/tty.usbmodem14101")
    private final ConcurrentHashMap<String, String> logicalToSystem = new ConcurrentHashMap<>();

    public void register(String logical, String system) {
        if (logical == null || system == null) return;
        logicalToSystem.put(logical.trim(), system.trim());
        log.debug("🔗 PortResolver register: {} -> {}", logical, system);
    }

    public void unregister(String logical) {
        if (logical != null) logicalToSystem.remove(logical.trim());
    }

    public String resolve(String comName) {
        if (comName == null) return null;
        final String os = System.getProperty("os.name","").toLowerCase();

        // Windows dùng luôn
        if (os.contains("win")) {
            // jSerialComm tự hiểu "COM80" hoặc "\\\\.\\COM80"
            return comName;
        }

        // *nix/mac: nếu đã có map, dùng map
        String mapped = logicalToSystem.get(comName);
        if (mapped != null) return mapped;

        // fallback: nếu comName đã là path hợp lệ thì trả về
        if (comName.startsWith("/dev/tty.") || comName.startsWith("/dev/cu.")) {
            return comName;
        }

        // cuối cùng: thử tìm cổng đang online và chọn cái gần đây nhất
        SerialPort[] ports = SerialPort.getCommPorts();
        if (ports != null && ports.length > 0) {
            // heuristic: ưu tiên /dev/tty.* hơn /dev/cu.*
            for (SerialPort p : ports) {
                String sys = p.getSystemPortName();
                if (sys != null && (sys.startsWith("/dev/tty.") || sys.startsWith("/dev/cu."))) {
                    log.warn("⚠️ Fallback resolving {} -> {} (heuristic)", comName, sys);
                    return sys;
                }
            }
        }

        log.error("❌ Không thể resolve cổng '{}' trên {}", comName, os);
        return null;
    }

    public Map<String,String> snapshot() {
        return Map.copyOf(logicalToSystem);
    }
}
