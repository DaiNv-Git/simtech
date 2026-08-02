package app.simsmartgsm.session;

import app.simsmartgsm.uitils.AtCommandHelper;
import com.fazecast.jSerialComm.SerialPort;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * ✅ SMS Session - Simple SMS sending
 */
@Slf4j
public class SmsSession implements TaskSession {

    private static final int BAUD_RATE = 115200;

    private final SmsTask task;

    private SerialPort port;
    private AtCommandHelper helper;

    private Instant startTime;
    private Instant endTime;

    public SmsSession(SmsTask task) {
        this.task = task;
    }

    @Override
    public void openPort() throws Exception {
        String comPort = task.getSim().getComName();

        // ✅ OPTIMIZATION: Port retry mechanism
        int maxRetries = 3;
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                port = SerialPort.getCommPort(comPort);
                port.setBaudRate(BAUD_RATE);
                port.setComPortTimeouts(SerialPort.TIMEOUT_READ_BLOCKING, 1000, 1000);

                if (!port.openPort()) {
                    throw new IOException("Port open returned false");
                }

                helper = new AtCommandHelper(port);

                // Init modem with retry
                String atResp = helper.sendAndRead("AT", 500);
                if (atResp == null || !atResp.contains("OK")) {
                    throw new IOException("Modem not responding");
                }

                helper.sendAndRead("ATE0", 500);
                helper.sendAndRead("AT+CMEE=2", 500);
                helper.sendAndRead("AT+CMGF=1", 500); // Text mode

                log.info("✅ [{}] Port opened for SMS (attempt {})", comPort, attempt);
                return; // Success

            } catch (Exception e) {
                lastException = e;
                log.warn("⚠️ [{}] Port open failed (attempt {}/{}): {}",
                        comPort, attempt, maxRetries, e.getMessage());

                // Cleanup before retry
                if (port != null && port.isOpen()) {
                    try {
                        port.closePort();
                    } catch (Exception ignored) {
                    }
                }

                if (attempt < maxRetries) {
                    Thread.sleep(500 * attempt); // Exponential backoff
                }
            }
        }

        throw new IOException("Cannot open port after " + maxRetries + " attempts: " +
                (lastException != null ? lastException.getMessage() : "unknown error"));
    }

    @Override
    public SessionResult execute() throws Exception {
        String comPort = task.getSim().getComName();
        startTime = Instant.now();

        try {
            // Check if Unicode needed
            boolean isUnicode = !task.getMessage().matches("^[\\x00-\\x7F]*$");
            String content = task.getMessage();
            String phone = task.getTargetPhone();

            if (isUnicode) {
                // Use UCS2 for Unicode
                helper.sendAndRead("AT+CSCS=\"UCS2\"", 500);
                helper.sendAndRead("AT+CSMP=17,167,0,8", 500);
                content = encodeUCS2(content);
                phone = encodeUCS2(normalizePhone(phone));
            } else {
                helper.sendAndRead("AT+CSCS=\"GSM\"", 500);
                phone = normalizePhone(phone).replace("+", "");
            }

            // Send CMGS command
            String cmgsResp = helper.sendAndRead("AT+CMGS=\"" + phone + "\"", 1000);

            if (!cmgsResp.contains(">") && cmgsResp.contains("ERROR")) {
                throw new Exception("CMGS failed: " + cmgsResp);
            }

            // Send content
            OutputStream os = port.getOutputStream();
            os.write(content.getBytes(StandardCharsets.US_ASCII));
            os.flush();
            Thread.sleep(300);

            // Send Ctrl+Z
            os.write(26);
            os.flush();

            // Wait for result
            Thread.sleep(3000);
            String result = helper.readAll();

            endTime = Instant.now();

            if (result.contains("OK") || result.contains("+CMGS")) {
                log.info("✅ [{}] SMS sent to {}", comPort, task.getTargetPhone());

                return SessionResult.builder()
                        .orderId(task.getOrderId())
                        .taskType("SMS")
                        .status("SUCCESS")
                        .startTime(startTime)
                        .endTime(endTime)
                        .build();
            } else {
                throw new Exception("SMS send failed: " + result);
            }

        } catch (Exception e) {
            endTime = Instant.now();
            log.error("❌ [{}] SMS send failed: {}", comPort, e.getMessage());

            return SessionResult.failed(task.getOrderId(), "SMS", e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (port != null && port.isOpen()) {
                port.closePort();
                log.info("🔒 [{}] Port closed", task.getSim().getComName());
            }
        } catch (Exception e) {
            log.warn("⚠️ Error closing port: {}", e.getMessage());
        }
    }

    // ============== Helper Methods ==============

    private String normalizePhone(String phone) {
        // Remove spaces and normalize
        return phone.replaceAll("\\s+", "").trim();
    }

    private String encodeUCS2(String text) {
        StringBuilder hex = new StringBuilder();
        for (char c : text.toCharArray()) {
            hex.append(String.format("%04X", (int) c));
        }
        return hex.toString();
    }
}
