package app.simsmartgsm.scheduled;

import app.simsmartgsm.config.ComManager;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.uitils.DeviceIdProvider;
import app.simsmartgsm.uitils.SimStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 🔄 Auto scanner
 * Quét tin nhắn chưa đọc định kỳ (backup nếu modem không gửi URC)
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SmsAutoScanner {

    private final ComManager comManager;
    private final SimRepository simRepository;

    /**
     * Backup scanner cho OTP: chỉ enqueue SCAN vào PortWorker hiện có.
     * Không mở COM port trực tiếp, nên không tranh serial port với gửi SMS/URC.
     */
    @Scheduled(
            initialDelayString = "${gsm.sms.backup-scan-delay-ms:5000}",
            fixedDelayString = "${gsm.sms.backup-scan-delay-ms:5000}")
    public void autoScan() {
        try {
            log.debug("🔄 OTP backup scan enqueue for active workers");
            ensureActiveWorkers();
            comManager.scanAllActivePorts();
        } catch (Exception e) {
            log.warn("⚠️ AutoScan lỗi toàn cục: {}", e.getMessage());
        }
    }

    private void ensureActiveWorkers() {
        String deviceName = DeviceIdProvider.getDeviceId();
        List<Sim> activeSims = simRepository.findByDeviceNameAndStatus(deviceName, SimStatus.ACTIVE.name());
        if (activeSims == null || activeSims.isEmpty()) {
            return;
        }

        int started = 0;
        for (Sim sim : activeSims) {
            if (sim == null || sim.getComName() == null || sim.getComName().isBlank()) {
                continue;
            }

            String com = sim.getComName();
            if (comManager.isPortLocked(com) || comManager.isWorkerRunning(com)) {
                continue;
            }

            comManager.startWorker(sim);
            started++;
        }

        if (started > 0) {
            log.info("📡 Auto scanner started {} missing PortWorker(s) for ACTIVE SIMs", started);
        }
    }
}
