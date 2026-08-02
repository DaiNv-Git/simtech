package app.simsmartgsm.service;

import app.simsmartgsm.uitils.AtCommandHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * ✅ PortManager
 * Quản lý lock theo từng COM để đảm bảo thread-safe khi truy cập modem.
 * Mọi thao tác AT command phải đi qua đây để tránh tranh chấp tài nguyên
 * KHI KHÔNG sử dụng PortWorker cho cùng COM.
 */
@Component
@Slf4j
public class PortManager {

    private final Map<String, ReentrantLock> comLocks = new ConcurrentHashMap<>();

    /**
     * ------------------------------
     * ⚙️ Kiểm tra port đang bận (đang bị lock)
     * ------------------------------
     */
    public boolean isPortBusy(String comPort) {
        ReentrantLock lock = comLocks.get(formatCom(comPort));
        return lock != null && lock.isLocked();
    }

    /**
     * ------------------------------
     * 🔧 Hàm xử lý có retry (mặc định 3 lần)
     * - Dùng cho thao tác cần mở/đóng cổng tạm thời.
     * - KHÔNG nên dùng với COM đang gắn vào PortWorker.
     * ------------------------------
     */
    public <T> T withPort(String comPort, Function<AtCommandHelper, T> task, long timeoutMs) {
        return withPort(comPort, task, timeoutMs, true); // Full init by default
    }

    /**
     * ------------------------------
     * 🚀 FAST SCAN MODE - Minimal initialization for quick scanning
     * ------------------------------
     */
    public <T> T withPortFastScan(String comPort, Function<AtCommandHelper, T> task, long timeoutMs) {
        return withPort(comPort, task, timeoutMs, false); // Fast mode - minimal init
    }

    /**
     * Internal method with configurable initialization level
     */
    private <T> T withPort(String comPort, Function<AtCommandHelper, T> task, long timeoutMs, boolean fullInit) {
        String com = formatCom(comPort);
        ReentrantLock lock = comLocks.computeIfAbsent(com, k -> new ReentrantLock());
        boolean acquired = false;

        int maxRetries = fullInit ? 3 : 2; // 🔧 FIX: Fast scan retry 2 lần thay vì 1

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                acquired = lock.tryLock(timeoutMs, TimeUnit.MILLISECONDS);
                if (!acquired) {
                    log.debug("⏳ Không lấy được lock cho {} trong {}ms", com, timeoutMs);
                    continue;
                }

                try (AtCommandHelper helper = AtCommandHelper.open(com, 115200, 2000, 2000)) {
                    // 1. Quick ping modem
                    String atResp = helper.sendAndRead("AT", 1500);
                    if (atResp == null || !atResp.contains("OK")) {
                        log.debug("⚠️ {} không phản hồi 'AT OK'", com);
                        continue;
                    }

                    // 2. Quick SIM check - only 1 attempt for fast scan
                    String cpinResp = helper.sendAndRead("AT+CPIN?", 1500);
                    log.debug("📱 [{}] CPIN: {}", com, cpinResp != null ? cpinResp.replace("\n", " ") : "null");

                    boolean simReady = cpinResp != null && (cpinResp.contains("READY") ||
                            cpinResp.contains("+CPIN: READY") ||
                            (cpinResp.contains("OK") && !cpinResp.contains("ERROR")));

                    if (!simReady) {
                        // 🔧 FIX: Fast scan retry thay vì skip ngay
                        if (!fullInit && attempt < maxRetries) {
                            log.debug("⏭️ [{}] SIM not ready, sẽ retry (fast scan attempt {}/{})",
                                    com, attempt, maxRetries);
                            safeSleep(500);
                            continue;
                        } else if (!fullInit) {
                            log.debug("⏭️ [{}] SIM not ready sau {} lần thử (fast scan)", com, maxRetries);
                            return null;
                        }

                        // Full init: try more attempts
                        if (cpinResp != null && (cpinResp.contains("SIM PIN") || cpinResp.contains("SIM PUK"))) {
                            log.error("❌ [{}] SIM requires PIN/PUK - cannot proceed", com);
                            return null;
                        }

                        // Retry logic for full init only
                        if (attempt < maxRetries) {
                            log.debug("⏭️ [{}] SIM not ready, retrying... ({}/{})", com, attempt, maxRetries);
                            safeSleep(500);
                            continue;
                        }
                        log.debug("⏭️ [{}] SIM not ready after {} attempts, skipping", com, maxRetries);
                        return null;
                    }

                    // 3. Minimal config for fast scan, full config otherwise
                    if (fullInit) {
                        helper.sendAndRead("ATE0", 500);
                        helper.sendAndRead("AT+CMEE=2", 500);
                        helper.sendAndRead("AT+CSCS=\"UCS2\"", 800);
                        helper.sendAndRead("AT+CMGF=1", 500);
                        helper.sendAndRead("AT+CPMS=\"SM\",\"SM\",\"SM\"", 800);
                        helper.sendAndRead("AT+CNMI=2,1,0,0,0", 500);
                    } else {
                        // Fast scan: only essential commands
                        helper.sendAndRead("ATE0", 300);
                        helper.sendAndRead("AT+CMEE=2", 300);
                    }

                    return task.apply(helper);
                }

            } catch (Exception e) {
                log.debug("❌ Lỗi thao tác với {}: {}", com, e.getMessage());
                if (fullInit && attempt < maxRetries) {
                    safeSleep(300);
                }
            } finally {
                if (acquired) {
                    lock.unlock();
                    acquired = false;
                }
            }
        }

        log.debug("❌ {} thất bại sau {} lần thử", com, maxRetries);
        return null;
    }

    /**
     * ------------------------------
     * 🧩 Safe scan – không retry, chỉ quét nhanh nếu port rảnh
     * ------------------------------
     */
    public <T> T safeScan(String comPort, Function<AtCommandHelper, T> task, long timeoutMs) {
        String com = formatCom(comPort);
        ReentrantLock lock = comLocks.computeIfAbsent(com, k -> new ReentrantLock());

        if (!lock.tryLock()) {
            log.debug("⚠️ Port {} đang bị chiếm, skip safeScan()", com);
            return null;
        }

        try (AtCommandHelper helper = AtCommandHelper.open(com, 115200, 3000, 3000)) {
            return task.apply(helper);
        } catch (Exception e) {
            log.warn("❌ SafeScan lỗi {}: {}", com, e.getMessage());
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * ------------------------------
     * 🧩 SafeWithPort – tương tự safeScan nhưng không có timeout param
     * ------------------------------
     */
    public <T> T safeWithPort(String comPort, Function<AtCommandHelper, T> task) {
        String com = formatCom(comPort);
        ReentrantLock lock = comLocks.computeIfAbsent(com, k -> new ReentrantLock());

        if (!lock.tryLock()) {
            log.debug("⏭️ Port {} đang bận, skip safeWithPort()", com);
            return null;
        }

        try (AtCommandHelper helper = AtCommandHelper.open(com, 115200, 3000, 3000)) {
            return task.apply(helper);
        } catch (Exception e) {
            log.warn("❌ SafeWithPort lỗi {}: {}", com, e.getMessage());
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * ------------------------------
     * 🔍 Kiểm tra trạng thái lock
     * ------------------------------
     */
    public boolean isLocked(String comPort) {
        ReentrantLock lock = comLocks.get(formatCom(comPort));
        return lock != null && lock.isLocked();
    }

    /**
     * ------------------------------
     * 🧰 Helper format COM path (Windows cần prefix)
     * ------------------------------
     */
    private String formatCom(String com) {
        if (com == null)
            return null;
        return (com.startsWith("\\\\.\\")) ? com : "\\\\.\\" + com;
    }

    /**
     * ------------------------------
     * ⏸️ Safe sleep utility
     * ------------------------------
     */
    private void safeSleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
