package app.simsmartgsm.service;

import app.simsmartgsm.dto.response.SmsMessageUser;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.SimRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * ⚠️ SERVICE NÀY ĐANG BỊ VÔ HIỆU HÓA
 * 
 * Lý do:
 * 1. Conflict với SimSyncService (đã handle detection tốt hơn)
 * 2. Dùng anchor hardcode sai (COM6 không tồn tại)
 * 3. Phone normalization sai (Vietnam format thay vì Japan)
 * 4. Không xử lý được trường hợp nhiều anchor SIM
 * 
 * → SỬ DỤNG SimSyncService THAY THẾ
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SimNumberWriterService {

    private final PortManager portManager;
    private final SmsReaderService smsReaderService;
    private final SimRepository simRepository;

    // === CONFIG ===
    private static final long WAIT_AFTER_SEND_MS = 5_000L;
    private static final long DETECT_TIMEOUT_MS = 30_000L;
    private static final long POLL_INTERVAL_MS = 2_000L;

    // ✨ SIM anchor — bạn chỉnh số này cho đúng
    private static final String ANCHOR_COM = "COM6";
    private static final String ANCHOR_NUMBER = "09033726097";

    // ❌ TẠM VÔ HIỆU HÓA - Đang conflict với SimSyncService
    // SimSyncService đã handle detection tốt hơn với dynamic anchor
    // @Scheduled(fixedRate = 600_000)
    public void scheduledDetectNumbers() {
        // DISABLED - Sử dụng SimSyncService thay thế
        log.debug("⏭️ SimNumberWriterService disabled - using SimSyncService instead");
        return;

        /*
         * OLD CODE - COMMENTED OUT
         * try {
         * log.info("🕒 Bắt đầu auto detect SIM numbers...");
         * detectAndWriteNumbers(ANCHOR_COM, ANCHOR_NUMBER);
         * } catch (Exception e) {
         * log.error("❌ Lỗi auto detect SIM numbers: {}", e.getMessage(), e);
         * }
         */
    }

    /**
     * Detect toàn bộ SIM chưa có số và ghi vào SIM.
     */
    public void detectAndWriteNumbers(String anchorCom, String anchorNumber) {
        List<Sim> targets = simRepository.findAll().stream()
                .filter(s -> s.getPhoneNumber() == null || s.getPhoneNumber().isBlank())
                .toList();

        if (targets.isEmpty()) {
            log.info("✅ Không có SIM nào cần detect số.");
            return;
        }

        log.info("🔎 anchor={} ({}) - detect {} SIM chưa có số", anchorCom, anchorNumber, targets.size());

        for (Sim sim : targets) {
            try {
                if (sim.getComName() == null || sim.getComName().isBlank()) {
                    log.warn("⚠️ Sim id={} không có COM, bỏ qua.", sim.getId());
                    continue;
                }

                boolean ok = detectAndWriteSingle(sim, anchorCom, anchorNumber, DETECT_TIMEOUT_MS);
                if (ok) {
                    log.info("✅ Đã detect và ghi số cho sim={} com={}", sim.getCcid(), sim.getComName());
                } else {
                    log.warn("❌ Không detect được số cho sim={} com={}", sim.getCcid(), sim.getComName());
                }
            } catch (Exception e) {
                log.error("❌ Lỗi detect sim {}: {}", sim.getComName(), e.getMessage(), e);
            }
        }
    }

    /**
     * Thử gửi detect SMS từ 1 SIM tới anchor, và đọc SMS trả về.
     */
    private boolean detectAndWriteSingle(Sim sim, String anchorCom, String anchorNumber, long timeoutMs) {
        final String senderCom = sim.getComName();
        final String marker = "SIM_DETECT_" + UUID.randomUUID().toString().substring(0, 8);
        final String payload = "DETECT:" + marker;

        log.info("➡️ Gửi detect SMS từ {} -> {} | body='{}'", senderCom, anchorNumber, payload);

        // Gửi SMS test
        Boolean sendOk = portManager.withPort(senderCom, helper -> {
            try {
                helper.setTextMode(true);
                helper.setCharset("GSM");
                return helper.sendTextSms(anchorNumber, payload, Duration.ofSeconds(20));
            } catch (Exception e) {
                log.warn("⚠️ Lỗi gửi detect SMS trên {}: {}", senderCom, e.getMessage());
                return false;
            }
        }, 10_000L);

        if (sendOk == null || !sendOk)
            return false;

        // Đợi anchor nhận tin
        try {
            Thread.sleep(WAIT_AFTER_SEND_MS);
        } catch (InterruptedException ignored) {
        }

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            try {
                List<SmsMessageUser> inbox = smsReaderService.readAllSms(anchorCom);
                if (inbox != null) {
                    Optional<SmsMessageUser> found = inbox.stream()
                            .filter(m -> m.getBody() != null && m.getBody().contains(marker))
                            .findFirst();

                    if (found.isPresent()) {
                        String detectedNumber = found.get().getSender();
                        log.info("📩 Anchor nhận detect từ {} → số={}", senderCom, detectedNumber);
                        return writeNumberToSimAndSave(sim, detectedNumber);
                    }
                }
            } catch (Exception e) {
                log.warn("⚠️ Lỗi đọc inbox anchor: {}", e.getMessage());
            }

            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException ignored) {
            }
        }

        log.warn("⏱️ Timeout không tìm thấy SMS detect trên anchor {}.", anchorCom);
        return false;
    }

    /**
     * Ghi số vào SIM bằng AT+CPBW và lưu vào DB.
     */
    private boolean writeNumberToSimAndSave(Sim sim, String number) {
        String com = sim.getComName();
        if (com == null || com.isBlank())
            return false;
        String normalized = normalizeNumber(number);

        Boolean writeOk = portManager.withPort(com, helper -> {
            try {
                String cmd = "AT+CPBW=1,\"" + normalized + "\",129,\"OwnNumber\"";
                String resp = helper.sendAndRead(cmd, 4000);
                log.info("🖊️ Ghi số lên SIM {}: resp={}", com, resp);
                return resp != null && resp.contains("OK");
            } catch (Exception e) {
                log.warn("⚠️ Lỗi ghi số vào SIM {}: {}", com, e.getMessage());
                return false;
            }
        }, 8000L);

        sim.setPhoneNumber(normalized);
        sim.setLastUpdated(Instant.now());
        simRepository.save(sim);

        return Boolean.TRUE.equals(writeOk);
    }

    private String normalizeNumber(String num) {
        if (num == null)
            return null;
        String s = num.trim();
        if (s.matches("^0\\d+")) {
            s = "+84" + s.substring(1);
        }
        return s;
    }
}
