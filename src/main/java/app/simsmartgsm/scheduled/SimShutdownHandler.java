package app.simsmartgsm.scheduled;

import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.SimRepository;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * ✅ Tự động set INACTIVE toàn bộ SIM của thiết bị khi app tắt.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SimShutdownHandler {

    private final SimRepository simRepository;

    private final String deviceName = System.getenv()
            .getOrDefault("DEVICE_NAME", System.getProperty("device.name", "UNKNOWN_DEVICE"));

    @PreDestroy
    public void onShutdown() {
        try {
            log.warn("🛑 App sắp tắt — chuyển toàn bộ SIM của '{}' sang INACTIVE...", deviceName);
            List<Sim> sims = simRepository.findByDeviceName(deviceName);
            if (sims.isEmpty()) {
                log.info("ℹ️ Không có SIM nào thuộc device {}", deviceName);
                return;
            }

            sims.forEach(sim -> {
                sim.setStatus("INACTIVE");
                sim.setLastUpdated(Instant.now());
            });
            simRepository.saveAll(sims);
            log.info("✅ Đã chuyển {} SIM thuộc '{}' sang INACTIVE.", sims.size(), deviceName);
        } catch (Exception e) {
            log.error("❌ Lỗi khi chuyển SIM sang INACTIVE: {}", e.getMessage(), e);
        }
    }
}
