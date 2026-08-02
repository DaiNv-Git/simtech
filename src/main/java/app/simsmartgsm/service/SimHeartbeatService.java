package app.simsmartgsm.service;

import app.simsmartgsm.baseGateway.GsmProperties;
import app.simsmartgsm.config.ComManager;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.uitils.DeviceIdProvider;
import app.simsmartgsm.uitils.PortWorker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

/**
 * 💓 SimHeartbeatService
 * Gửi heartbeat định kỳ lên backend để backend biết app vẫn đang chạy.
 * Nếu backend không nhận được heartbeat trong 3 phút, sẽ tự động set SIM sang
 * INACTIVE.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SimHeartbeatService {

    private final ComManager comManager;
    private final GsmProperties gsmProperties;
    private final SimRepository simRepository;
    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * 📡 Chạy mỗi 60 giây - gửi danh sách SIM đang active lên backend
     */
    @Scheduled(fixedRate = 60000)
    public void sendHeartbeat() {
        try {
            // Lấy danh sách workers hiện tại
            Map<String, PortWorker> workers = comManager.getWorkers();

            if (workers.isEmpty()) {
                log.debug("💤 No active workers, skipping heartbeat");
                return;
            }

            // Thu thập thông tin SIM
            List<Map<String, Object>> simList = new ArrayList<>();
            for (PortWorker worker : workers.values()) {
                try {
                    Sim sim = worker.getSim();
                    if (sim != null) {
                        Map<String, Object> simData = new HashMap<>();
                        simData.put("comName", sim.getComName());
                        simData.put("phoneNumber", sim.getPhoneNumber());
                        simData.put("status", sim.getStatus());
                        simData.put("ccid", sim.getCcid());
                        simList.add(simData);
                    }
                } catch (Exception e) {
                    log.warn("⚠️ Error collecting SIM data from worker: {}", e.getMessage());
                }
            }

            if (simList.isEmpty()) {
                log.debug("💤 No valid SIMs found, skipping heartbeat");
                return;
            }

            // Lấy device name
            String deviceName;
            try {
                deviceName = DeviceIdProvider.getDeviceId();
            } catch (Exception e) {
                deviceName = "UNKNOWN";
                log.warn("⚠️ Could not get device ID: {}", e.getMessage());
            }

            // Build payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("deviceName", deviceName);
            payload.put("sims", simList);

            // Gửi heartbeat
            String baseUrl = gsmProperties.getApi().getBaseUrl();
            if (!baseUrl.endsWith("/")) {
                baseUrl += "/";
            }
            String url = baseUrl + "api/sim/heartbeat";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(payload, headers);

            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);

            log.info("📡 Heartbeat sent: {} SIM(s) | device: {} | status: {}",
                    simList.size(), deviceName, response.getStatusCode().value());

        } catch (Exception e) {
            log.warn("⚠️ Failed to send heartbeat: {}", e.getMessage());
            // Don't crash - just log and continue
        }
    }

    /**
     * Cập nhật lastUpdated local mỗi 10 phút cho SIM đang có PortWorker.
     * Job này chỉ ghi DB, không mở COM port và không scan SIM nên không làm đứt URC.
     */
    @Scheduled(fixedRate = 600_000)
    public void updateLocalLastUpdated() {
        try {
            Map<String, PortWorker> workers = comManager.getWorkers();
            if (workers.isEmpty()) {
                log.debug("💤 No active workers, skipping local SIM lastUpdated heartbeat");
                return;
            }

            java.time.Instant now = java.time.Instant.now();
            List<Sim> simsToUpdate = new ArrayList<>();

            for (PortWorker worker : workers.values()) {
                try {
                    Sim sim = worker.getSim();
                    if (sim != null && sim.getId() != null) {
                        sim.setLastUpdated(now);
                        simsToUpdate.add(sim);
                    }
                } catch (Exception e) {
                    log.debug("⚠️ Error collecting SIM for local heartbeat: {}", e.getMessage());
                }
            }

            if (!simsToUpdate.isEmpty()) {
                simRepository.saveAll(simsToUpdate);
                log.debug("💓 Updated local lastUpdated for {} active SIM(s)", simsToUpdate.size());
            }
        } catch (Exception e) {
            log.warn("⚠️ Failed to update local SIM lastUpdated heartbeat: {}", e.getMessage());
        }
    }
}
