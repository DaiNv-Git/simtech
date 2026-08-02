package app.simsmartgsm.uitils;

import com.fazecast.jSerialComm.SerialPort;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class AtCommandHelper implements Closeable {

    /**
     * Custom exception for port disconnection - allows caller to handle
     * reconnection
     */
    public static class PortDisconnectedException extends IOException {
        private final String portName;

        public PortDisconnectedException(String portName, String message) {
            super(message);
            this.portName = portName;
        }

        public PortDisconnectedException(String portName, String message, Throwable cause) {
            super(message, cause);
            this.portName = portName;
        }

        public String getPortName() {
            return portName;
        }
    }

    private final SerialPort port;
    private final boolean ownsPort;
    private final InputStream in;
    private final OutputStream out;

    // ✅ Buffer để chứa URC notifications bị đọc lẫn khi flush/đọc AT response.
    // PortWorker sẽ consume buffer này để trigger scan lại thay vì mất +CMTI/+CMT.
    private final StringBuilder pendingUrcBuffer = new StringBuilder();

    // ---------- Factory ----------
    public static AtCommandHelper open(String portName,
            int baudRate,
            int readTimeoutMs,
            int writeTimeoutMs) throws IOException {
        SerialPort port = SerialPort.getCommPort(portName);

        // ✅ FIX: Check if port is already open (might be from previous session)
        if (port.isOpen()) {
            log.warn("⚠️ Port {} is already open, closing first...", portName);
            try {
                port.closePort();
                Thread.sleep(200); // Wait for port to be fully released
            } catch (Exception e) {
                log.warn("⚠️ Error closing already-open port {}: {}", portName, e.getMessage());
            }
        }

        port.setBaudRate(baudRate);
        port.setComPortTimeouts(SerialPort.TIMEOUT_READ_SEMI_BLOCKING, readTimeoutMs, writeTimeoutMs);

        boolean opened = port.openPort();
        if (!opened) {
            for (int i = 0; i < 3 && !opened; i++) {
                try {
                    // Exponential backoff: 200ms, 400ms, 800ms
                    long delayMs = 200L * (1L << i);
                    log.debug("⏳ Retry opening port {} in {}ms (attempt {})", portName, delayMs, i + 1);
                    Thread.sleep(delayMs);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                opened = port.openPort();
            }
        }
        if (!opened) {
            throw new IOException("❌ Cannot open port " + portName + " after retries");
        }

        log.debug("✅ Port {} opened successfully", portName);
        return new AtCommandHelper(port, true);
    }

    /**
     * Xoá sạch dữ liệu đang tồn trong buffer input của modem.
     */
    public void flush() {
        try {
            InputStream in = port.getInputStream();
            while (in.available() > 0) {
                in.read(); // đọc bỏ từng byte
            }
            Thread.sleep(20);
        } catch (Exception ignored) {
        }
    }

    public void send(String cmd) {
        try {
            if (!cmd.endsWith("\r"))
                cmd += "\r";
            port.getOutputStream().write(cmd.getBytes(StandardCharsets.US_ASCII));
            port.getOutputStream().flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String readAll() {
        try {
            InputStream in = port.getInputStream();
            StringBuilder sb = new StringBuilder();

            long timeout = System.currentTimeMillis() + 800; // 0.8s
            byte[] buf = new byte[1024];

            while (System.currentTimeMillis() < timeout) {
                int available = in.available();
                if (available <= 0) {
                    Thread.sleep(20);
                    continue;
                }

                int len = in.read(buf);
                if (len > 0) {
                    sb.append(new String(buf, 0, len, StandardCharsets.US_ASCII));
                    timeout = System.currentTimeMillis() + 200; // reset khi có data
                }
            }

            return sb.toString();

        } catch (Exception e) {
            return "";
        }
    }

    // ---------- Constructor ----------
    public AtCommandHelper(SerialPort port) {
        this(port, false);
    }

    private AtCommandHelper(SerialPort port, boolean ownsPort) {
        this.port = port;
        this.ownsPort = ownsPort;
        this.in = port.getInputStream();
        this.out = port.getOutputStream();
    }

    public boolean isPortOpen() {
        return port != null && port.isOpen();
    }

    // ---------- Core IO ----------
    public synchronized String sendAndRead(String command, int timeoutMs) throws IOException, InterruptedException {
        ensureOpen();
        flushInput();

        StringBuilder response = new StringBuilder();

        // ✅ IMPROVEMENT: Wrap write in try-catch to detect disconnect early
        try {
            // Double-check port before write (race condition protection)
            if (!port.isOpen()) {
                throw new PortDisconnectedException(port.getSystemPortName(),
                        "Port closed before write: " + port.getSystemPortName());
            }
            out.write((command + "\r").getBytes(StandardCharsets.US_ASCII));
            out.flush();
        } catch (com.fazecast.jSerialComm.SerialPortIOException e) {
            // ✅ Wrap SerialPortIOException with our custom exception for easier handling
            String portName = port != null ? port.getSystemPortName() : "UNKNOWN";
            throw new PortDisconnectedException(portName,
                    "Port disconnected during write: " + e.getMessage(), e);
        }

        long start = System.currentTimeMillis();
        byte[] buffer = new byte[1024];

        while (System.currentTimeMillis() - start < timeoutMs) {
            int available = in.available();
            if (available > 0) {
                int len = in.read(buffer, 0, Math.min(buffer.length, available));
                if (len > 0) {
                    response.append(new String(buffer, 0, len, StandardCharsets.ISO_8859_1));
                    String text = response.toString();
                    if (text.endsWith("\r\nOK\r\n") || text.endsWith("\nOK\n") || text.equals("OK\r\n")
                            || text.equals("OK\n") ||
                            text.endsWith("\r\nERROR\r\n") || text.endsWith("\nERROR\n") || text.equals("ERROR\r\n")
                            || text.equals("ERROR\n") ||
                            (text.contains("+CMS ERROR") && text.endsWith("\n")) ||
                            (text.contains("+CME ERROR") && text.endsWith("\n")) ||
                            text.endsWith("> ") || text.endsWith(">")) {
                        break;
                    }
                }
            } else {
                Thread.sleep(40);
            }
        }

        String result = response.toString().trim();
        capturePendingUrc(result, "sendAndRead(" + command + ")");
        log.debug("📡 AT [{}] -> {}", command, result.replace("\r", " ").replace("\n", " "));
        return result.isEmpty() ? "" : result;
    }

    public boolean sendAtOk(String command, int timeoutMs) throws IOException, InterruptedException {
        String r = sendAndRead(command, timeoutMs);
        return r.contains("OK") && !r.contains("ERROR");
    }

    private void ensureOpen() throws IOException {
        if (!port.isOpen())
            throw new IOException("Port not open: " + port.getSystemPortName());
    }

    public void flushInput() {
        try {
            StringBuilder flushed = new StringBuilder();
            while (in.available() > 0) {
                int n = Math.min(in.available(), 4096);
                if (n <= 0)
                    break;
                byte[] buf = new byte[n];
                int read = in.read(buf);
                if (read > 0) {
                    flushed.append(new String(buf, 0, read, StandardCharsets.US_ASCII));
                }
            }
            if (flushed.length() > 0) {
                capturePendingUrc(flushed.toString(), "flushInput()");
            }
        } catch (IOException ignored) {
        }
    }

    private static boolean containsIncomingSmsUrc(String data) {
        return data != null && (data.contains("+CMTI:") || data.contains("+CMT:"));
    }

    private synchronized void capturePendingUrc(String data, String source) {
        if (!containsIncomingSmsUrc(data)) {
            return;
        }

        if (pendingUrcBuffer.length() > 0) {
            pendingUrcBuffer.append('\n');
        }
        pendingUrcBuffer.append(data);

        if (pendingUrcBuffer.length() > 8192) {
            pendingUrcBuffer.delete(0, pendingUrcBuffer.length() - 4096);
        }

        log.warn("⚠️ {} rescued SMS URC: {}",
                source, data.replace("\r", "\\r").replace("\n", "\\n"));
    }

    /**
     * ✅ Check and consume pending URC data saved by flushInput()
     * Called by PortWorker.checkForUrcNotification()
     * @return URC data if any was preserved, null otherwise
     */
    public synchronized String consumePendingUrc() {
        if (pendingUrcBuffer.length() == 0) {
            return null;
        }
        String data = pendingUrcBuffer.toString();
        pendingUrcBuffer.setLength(0);
        return data;
    }

    public void writeCtrlZ() throws IOException {
        ensureOpen();
        out.write(0x1A); // Ctrl+Z
        out.flush();
    }

    // ---------- High-level Modem helpers ----------
    public boolean ping() throws IOException, InterruptedException {
        return sendAtOk("AT", 800);
    }

    public boolean echoOff() throws IOException, InterruptedException {
        return sendAtOk("ATE0", 800);
    }

    public boolean setTextMode(boolean textMode) throws IOException, InterruptedException {
        return sendAtOk("AT+CMGF=" + (textMode ? "1" : "0"), 1200);
    }

    public boolean setCharset(String cs) throws IOException, InterruptedException {
        return sendAtOk("AT+CSCS=\"" + cs + "\"", 1200);
    }

    public boolean setNewMessageIndicationDefault() throws IOException, InterruptedException {
        return sendAtOk("AT+CNMI=2,1,0,0,0", 1500);
    }

    // ---------- SMS ----------
    /**
     * Normalize Japanese phone numbers to proper format
     * Handles malformed numbers like 8108027836543 → +818027836543 or 08027836543
     * 
     * @param phoneNumber Raw phone number
     * @return Normalized phone number
     */
    private String normalizeJapanesePhoneNumber(String phoneNumber) {
        if (phoneNumber == null || phoneNumber.isEmpty()) {
            return phoneNumber;
        }

        // Remove all non-digit characters except +
        String cleaned = phoneNumber.replaceAll("[^0-9+]", "");

        // Case 1: Already in correct format (+81XXXXXXXXXX or 0XXXXXXXXXX)
        if (cleaned.startsWith("+81") && cleaned.length() >= 12) {
            log.debug("📱 Phone number already in international format: {}", cleaned);
            return cleaned;
        }
        if (cleaned.startsWith("0") && cleaned.length() >= 10 && cleaned.length() <= 11) {
            log.debug("📱 Phone number already in domestic format: {}", cleaned);
            return cleaned;
        }

        // Case 2: Malformed 810XXXXXXXXX → +81XXXXXXXXX (remove the extra 0)
        if (cleaned.startsWith("810") && cleaned.length() >= 12) {
            String normalized = "+81" + cleaned.substring(3);
            log.warn("⚠️ Malformed phone number detected: {} → {}", phoneNumber, normalized);
            return normalized;
        }

        // Case 3: Starts with 81 but no + → add +
        if (cleaned.startsWith("81") && !cleaned.startsWith("+") && cleaned.length() >= 11) {
            String normalized = "+" + cleaned;
            log.warn("⚠️ Missing + prefix: {} → {}", phoneNumber, normalized);
            return normalized;
        }

        // Case 4: Pure digits starting with 70, 80, 90 (mobile) → add 0 prefix
        if (cleaned.matches("^[7-9]0\\d{8,9}$")) {
            String normalized = "0" + cleaned;
            log.warn("⚠️ Missing 0 prefix: {} → {}", phoneNumber, normalized);
            return normalized;
        }

        // Case 5: International format without + (81XXXXXXXXXX)
        if (cleaned.matches("^81[0-9]{9,10}$")) {
            String normalized = "+" + cleaned;
            log.warn("⚠️ International format without +: {} → {}", phoneNumber, normalized);
            return normalized;
        }

        // Default: return as-is if no pattern matches
        log.debug("📱 Phone number format unchanged: {}", cleaned);
        return cleaned;
    }

    public boolean sendTextSms(String toNumber, String content, Duration totalTimeout)
            throws IOException, InterruptedException {

        ensureOpen();

        // ✅ Normalize Japanese phone number format
        toNumber = normalizeJapanesePhoneNumber(toNumber);
        log.info("📱 Sending SMS to: {} (content: {} chars)", toNumber, content.length());

        // ✅ FIX: Ping modem trước khi gửi — verify modem còn sống
        // Tránh gửi SMS khi modem đang hang → giảm false-fail đáng kể
        String pingResp = sendAndRead("AT", 1000);
        if (pingResp == null || (!pingResp.contains("OK") && !pingResp.isEmpty())) {
            log.error("❌ Modem not responding before SMS send: {}", pingResp);
            return false;
        }

        // ✅ FIX: Verify text mode — nếu fail thì return sớm
        if (!setTextMode(true)) {
            log.error("❌ Cannot set text mode (AT+CMGF=1)");
            return false;
        }

        // ✅ Check if content is ASCII-only (GSM 7-bit compatible)
        boolean isAsciiOnly = content.chars().allMatch(c -> c < 128);
        int contentLength = content.length();

        if (isAsciiOnly) {
            // 📤 ASCII content: Use GSM 7-bit encoding (160 chars per SMS)
            log.debug("📤 Using GSM 7-bit encoding for ASCII content ({} chars)", contentLength);

            if (contentLength > 160) {
                log.warn("⚠️ SMS length {} chars exceeds single GSM 7-bit SMS limit (160 chars). " +
                        "Message may be split into {} parts.",
                        contentLength, (contentLength / 153) + 1);
            }

            // Set charset to GSM (default) or IRA for ASCII
            setCharset("GSM");
            // Default CSMP for GSM 7-bit: validity=167 (24h), PID=0, DCS=0 (GSM 7-bit)
            sendAndRead("AT+CSMP=17,167,0,0", 500);

            // Send phone number directly (no UCS2 encoding needed)
            String cmgsResp = sendAndRead("AT+CMGS=\"" + toNumber + "\"", 3000);
            if (!cmgsResp.contains(">")) {
                // ✅ FIX: Chờ prompt bằng đọc trực tiếp, KHÔNG gửi \r thừa
                String extra = waitForSmsResponse(2000);
                if (!extra.contains(">")) {
                    log.error("❌ No '>' prompt for SMS: {}{}", cmgsResp, extra);
                    try { out.write(0x1B); out.flush(); } catch (Exception ignored) {} // ESC cancel
                    return false;
                }
            }

            Thread.sleep(80);

            // Send content directly
            out.write(content.getBytes(StandardCharsets.US_ASCII));
            writeCtrlZ();

        } else {
            // 📤 Unicode content: Use UCS2 encoding (70 chars per SMS)
            log.debug("📤 Using UCS2 encoding for Unicode content ({} chars)", contentLength);

            if (contentLength > 70) {
                log.warn("⚠️ SMS length {} chars exceeds single UCS2 SMS limit (70 chars). " +
                        "Message may be split into {} parts or truncated.",
                        contentLength, (contentLength / 67) + 1);
            }

            // Use UCS2 for both number and content
            setCharset("UCS2");
            // CSMP for UCS2: validity=167 (24h), PID=0, DCS=8 (UCS2)
            sendAndRead("AT+CSMP=17,167,0,8", 500);

            // Encode phone number to UCS2
            String ucs2Number = toUcs2(toNumber);

            String cmgsResp = sendAndRead("AT+CMGS=\"" + ucs2Number + "\"", 3000);
            if (!cmgsResp.contains(">")) {
                // ✅ FIX: Chờ prompt bằng đọc trực tiếp, KHÔNG gửi \r thừa
                String extra = waitForSmsResponse(2000);
                if (!extra.contains(">")) {
                    log.error("❌ No '>' prompt (UCS2): {}{}", cmgsResp, extra);
                    try { out.write(0x1B); out.flush(); } catch (Exception ignored) {} // ESC cancel
                    return false;
                }
            }

            Thread.sleep(80);

            // Encode content to UCS2
            String ucs2Content = toUcs2(content);
            log.debug("📤 UCS2 content length: {} chars → {} hex chars", contentLength, ucs2Content.length());

            out.write(ucs2Content.getBytes(StandardCharsets.US_ASCII));
            writeCtrlZ();
        }

        // ✅ FIX CRITICAL: Đọc response TRỰC TIẾP từ InputStream
        // KHÔNG dùng sendAndRead("") vì nó gọi flushInput() → nuốt mất "+CMGS: xx"
        // → hệ thống tưởng FAIL dù nhà mạng ĐÃ NHẬN SMS (nguyên nhân #1 gây 30% fail)
        String finalResp = waitForSmsResponse((int) Math.max(8000, totalTimeout.toMillis()));

        // ✅ Check ERROR first (CMS ERROR, CME ERROR, or plain ERROR)
        boolean hasError = finalResp.contains("+CMS ERROR") ||
                finalResp.contains("+CME ERROR") ||
                (finalResp.contains("ERROR") && !finalResp.contains("+CMGS"));

        // ✅ Check SUCCESS indicators
        boolean hasSuccess = finalResp.contains("OK") || finalResp.contains("+CMGS");

        // Final decision: SUCCESS only if no error AND has success indicator
        boolean ok = !hasError && hasSuccess;

        log.info("📤 SMS result: {} | resp: {}", ok ? "✅ OK" : "❌ FAIL",
                finalResp.replace("\n", " ").replace("\r", " ").trim());

        // ✅ FIX: Log CMS ERROR code cụ thể để debug
        if (hasError) {
            Matcher errMatcher = Pattern.compile("\\+C[ME]S ERROR:\\s*(.+?)(?:\\r|\\n|$)").matcher(finalResp);
            if (errMatcher.find()) {
                log.error("📛 SMS Error Detail: {}", errMatcher.group(1).trim());
            }
        }

        // ✅ Luôn khôi phục charset UCS2 + CNMI sau khi gửi SMS
        try {
            setCharset("UCS2");
            sendAndRead("AT+CSMP=17,167,0,8", 500); // DCS=8 (UCS2)
        } catch (Exception e) {
            log.warn("⚠️ Failed to restore UCS2 charset after SMS send: {}", e.getMessage());
        }
        try {
            setNewMessageIndicationDefault();
        } catch (Exception ignored) {
        }

        return ok;
    }

    private String toUcs2(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            sb.append(String.format("%04X", (int) c));
        }
        return sb.toString();
    }

    /**
     * ✅ FIX: Đọc response từ modem TRỰC TIẾP mà KHÔNG flush input trước.
     * Dùng sau khi gửi SMS content + Ctrl+Z, chờ +CMGS/OK/ERROR.
     * Khác với sendAndRead(): KHÔNG gọi flushInput(), KHÔNG gửi command thừa.
     * Đây là fix chính cho vấn đề ~30% false-fail.
     */
    private String waitForSmsResponse(int timeoutMs) {
        StringBuilder response = new StringBuilder();
        long start = System.currentTimeMillis();
        byte[] buffer = new byte[1024];

        try {
            while (System.currentTimeMillis() - start < timeoutMs) {
                int available = in.available();
                if (available > 0) {
                    int len = in.read(buffer, 0, Math.min(buffer.length, available));
                    if (len > 0) {
                        response.append(new String(buffer, 0, len, StandardCharsets.ISO_8859_1));
                        String text = response.toString();

                        // Check for terminal responses
                        if (text.endsWith("\r\nOK\r\n") || text.endsWith("\nOK\n") ||
                                text.contains("+CMS ERROR") || text.contains("+CME ERROR") ||
                                (text.contains("+CMGS:") && text.contains("OK")) ||
                                text.endsWith("\r\nERROR\r\n") || text.endsWith("\nERROR\n") ||
                                text.endsWith("> ") || text.endsWith(">")) {
                            break;
                        }
                    }
                } else {
                    Thread.sleep(50);
                }
            }
        } catch (Exception e) {
            log.warn("⚠️ waitForSmsResponse error: {}", e.getMessage());
        }

        String result = response.toString().trim();
        capturePendingUrc(result, "waitForSmsResponse()");
        if (!result.isEmpty()) {
            log.debug("📡 SMS raw response: {}",
                    result.replace("\r", "\\r").replace("\n", "\\n"));
        }
        return result.isEmpty() ? "" : result;
    }

    public List<SmsRecord> listUnreadSmsText(int timeoutMs) throws IOException, InterruptedException {
        setTextMode(true);
        String out = sendAndRead("AT+CMGL=\"REC UNREAD\"", timeoutMs);
        return parseCmglText(out);
    }

    public List<SmsRecord> listAllSmsText(int timeoutMs) throws IOException, InterruptedException {
        setTextMode(true);
        String out = sendAndRead("AT+CMGL=\"ALL\"", timeoutMs);
        return parseCmglText(out);
    }

    public boolean deleteSms(int index) throws IOException, InterruptedException {
        return sendAtOk("AT+CMGD=" + index, 2000);
    }

    public boolean deleteAllSms() throws IOException, InterruptedException {
        return sendAtOk("AT+CMGD=1,4", 3000);
    }

    // ---------- SIM Info ----------
    public String getCcid() throws IOException, InterruptedException {
        // ⏰ Increased timeout for Japanese SIM cards (Softbank, Docomo, AU)
        String r = sendAndRead("AT+CCID", 2500);
        Matcher m = Pattern.compile("\\+?CCID\\s*:\\s*([0-9A-Fa-f]+)").matcher(r);
        return m.find() ? m.group(1) : sanitizeSingleLine(r);
    }

    public String getImsi() throws IOException, InterruptedException {
        String r = sendAndRead("AT+CIMI", 1500);
        Matcher m = Pattern.compile("(?m)^(\\d{5,20})$").matcher(r);
        return m.find() ? m.group(1) : r.replaceAll("[^0-9]", "");
    }

    public String getCnum() throws IOException, InterruptedException {
        // ⏰ Increased timeout for Japanese SIM cards (Softbank, Docomo, AU)

        // Try multiple times as Japanese SIM cards can be slow to respond
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                String r = sendAndRead("AT+CNUM", 3000);
                log.debug("📱 CNUM response (attempt {}): {}", attempt, r.replace("\r", " ").replace("\n", " "));

                if (r == null || r.trim().isEmpty() || r.equals("OK")) {
                    log.debug("⚠️ Empty CNUM response, retrying...");
                    Thread.sleep(500);
                    continue;
                }

                // Pattern 1: Standard format with name: +CNUM: "Name","+819012345678",145
                Matcher m = Pattern.compile("\\+?CNUM:.*?\"([^\"]*)\",\"(\\+?\\d{6,20})\"").matcher(r);
                if (m.find()) {
                    String number = m.group(2);
                    log.info("✅ Found phone number (pattern 1): {}", number);
                    return number;
                }

                // Pattern 2: Without name: +CNUM: ,"+819012345678",145
                m = Pattern.compile("\\+?CNUM:\\s*,\"(\\+?\\d{6,20})\"").matcher(r);
                if (m.find()) {
                    String number = m.group(1);
                    log.info("✅ Found phone number (pattern 2): {}", number);
                    return number;
                }

                // Pattern 3: Just number in quotes: +CNUM: "+819012345678"
                m = Pattern.compile("\\+?CNUM:.*?\"(\\+?\\d{6,20})\"").matcher(r);
                if (m.find()) {
                    String number = m.group(1);
                    log.info("✅ Found phone number (pattern 3): {}", number);
                    return number;
                }

                // Pattern 4: Number without quotes: +CNUM: +819012345678
                m = Pattern.compile("\\+?CNUM:.*?(\\+?\\d{6,20})").matcher(r);
                if (m.find()) {
                    String number = m.group(1);
                    log.info("✅ Found phone number (pattern 4): {}", number);
                    return number;
                }

                // Pattern 5: UCS2 encoded number (Japanese SIM may return hex)
                m = Pattern.compile("\\+?CNUM:.*?\"([0-9A-Fa-f]{16,})\"").matcher(r);
                if (m.find()) {
                    String hexNumber = m.group(1);
                    try {
                        byte[] bytes = hexToBytes(hexNumber);
                        String decoded = new String(bytes, StandardCharsets.UTF_16BE).trim();
                        if (decoded.matches(".*\\d{6,}.*")) {
                            log.info("✅ Decoded UCS2 phone number: {} → {}", hexNumber, decoded);
                            return decoded;
                        }
                    } catch (Exception e) {
                        log.debug("⚠️ Failed to decode UCS2 number: {}", e.getMessage());
                    }
                }

                log.warn("⚠️ Could not parse phone number from CNUM response (attempt {}): {}", attempt, r);
                Thread.sleep(500);

            } catch (Exception e) {
                log.warn("⚠️ CNUM query failed (attempt {}): {}", attempt, e.getMessage());
                if (attempt < 3) {
                    Thread.sleep(1000);
                }
            }
        }

        log.warn("❌ Failed to get phone number after 3 attempts");
        return null;
    }

    /**
     * Fast CNUM probe used by bulk SIM scans.
     *
     * Bulk scanning must not spend up to ten seconds retrying a command that many
     * SIMs legitimately do not support. The regular {@link #getCnum()} keeps its
     * conservative retry behaviour for interactive/recovery flows.
     */
    public String getCnumFast() throws IOException, InterruptedException {
        String response = sendAndRead("AT+CNUM", 1500);
        if (response == null || response.isBlank() || "OK".equals(response.trim())) {
            return null;
        }

        Matcher matcher = Pattern.compile("\\+?CNUM:.*?\"[^\"]*\",\"(\\+?\\d{6,20})\"").matcher(response);
        if (matcher.find()) {
            return matcher.group(1);
        }

        matcher = Pattern.compile("\\+?CNUM:\\s*,\"(\\+?\\d{6,20})\"").matcher(response);
        if (matcher.find()) {
            return matcher.group(1);
        }

        matcher = Pattern.compile("\\+?CNUM:.*?\"(\\+?\\d{6,20})\"").matcher(response);
        if (matcher.find()) {
            return matcher.group(1);
        }

        matcher = Pattern.compile("\\+?CNUM:.*?(\\+?\\d{6,20})").matcher(response);
        return matcher.find() ? matcher.group(1) : null;
    }

    public String queryOperator() throws IOException, InterruptedException {
        // ⏰ Tăng timeout cho SIM Nhật (Softbank, Docomo, AU)
        String resp = sendAndRead("AT+COPS?", 3000);

        log.debug("📡 COPS response: {}", resp.replace("\r", " ").replace("\n", " "));

        // 🔍 Try multiple patterns for different modem responses

        // Pattern 1: Standard format with quotes: +COPS: 0,0,"Softbank",2
        Matcher m = Pattern.compile("\\+COPS:\\s*\\d+,\\d+,\"([^\"]+)\"").matcher(resp);
        if (m.find()) {
            String operator = m.group(1);
            // Decode UCS2 if needed (Japanese carriers may return hex)
            return decodeOperatorName(operator);
        }

        // Pattern 2: Without mode/format: +COPS: "Softbank"
        m = Pattern.compile("\\+COPS:\\s*\"([^\"]+)\"").matcher(resp);
        if (m.find()) {
            return decodeOperatorName(m.group(1));
        }

        // Pattern 3: Numeric only: +COPS: 0,2,"44020"
        m = Pattern.compile("\\+COPS:\\s*\\d+,\\d+,\"(\\d{5,6})\"").matcher(resp);
        if (m.find()) {
            String mccMnc = m.group(1);
            return mapMccMncToOperator(mccMnc);
        }

        // Pattern 4: Short format: +COPS: 0
        if (resp.contains("+COPS:")) {
            // Try AT+COPS=3,0 then AT+COPS? to get alphanumeric name
            try {
                sendAndRead("AT+COPS=3,0", 2000); // Set to long alphanumeric
                Thread.sleep(200);
                String resp2 = sendAndRead("AT+COPS?", 3000);
                m = Pattern.compile("\\+COPS:\\s*\\d+,\\d+,\"([^\"]+)\"").matcher(resp2);
                if (m.find()) {
                    return decodeOperatorName(m.group(1));
                }
            } catch (Exception e) {
                log.debug("⚠️ Failed to query operator with format change: {}", e.getMessage());
            }
        }

        log.warn("⚠️ Could not parse operator from: {}", resp);
        return "UNKNOWN";
    }

    /**
     * Decode operator name - handles UCS2 encoding for Japanese carriers
     */
    private String decodeOperatorName(String name) {
        if (name == null || name.isEmpty()) {
            return "UNKNOWN";
        }

        // Check if it's UCS2 hex encoded (Japanese carriers often use this)
        if (name.matches("^[0-9A-Fa-f]+$") && name.length() % 4 == 0 && name.length() >= 8) {
            try {
                byte[] bytes = hexToBytes(name);
                String decoded = new String(bytes, StandardCharsets.UTF_16BE).trim();
                if (!decoded.isEmpty() && decoded.matches(".*[\\p{L}\\p{N}].*")) {
                    log.info("✅ Decoded UCS2 operator: {} → {}", name, decoded);
                    return decoded;
                }
            } catch (Exception e) {
                log.debug("⚠️ Failed to decode UCS2 operator name: {}", e.getMessage());
            }
        }

        return name;
    }

    /**
     * Map MCC+MNC to operator name for Japanese carriers
     */
    private String mapMccMncToOperator(String mccMnc) {
        // Japanese carriers (MCC = 440)
        return switch (mccMnc) {
            case "44020" -> "SoftBank"; // Softbank
            case "44010" -> "NTT DoCoMo"; // Docomo
            case "44050", "44051", "44053", "44054" -> "KDDI/AU"; // AU/KDDI
            case "44000", "44001", "44002", "44003" -> "Y!mobile"; // Y!mobile (Softbank MVNO)
            default -> {
                log.info("📱 Unknown MCC+MNC: {}", mccMnc);
                yield mccMnc; // Return as-is
            }
        };
    }

    private static String sanitizeSingleLine(String s) {
        if (s == null)
            return null;
        String t = s.replaceAll("\\r|\\n|OK|ERROR", "").trim();
        return t.isBlank() ? null : t;
    }

    // ---------- Parser for +CMGL ----------
    public static List<SmsRecord> parseCmglText(String out) {
        List<SmsRecord> list = new ArrayList<>();
        if (out == null || out.isBlank())
            return list;

        String[] lines = out.split("\\r?\\n");
        SmsRecord cur = null;
        for (String line : lines) {
            if (line.startsWith("+CMGL:")) {
                if (cur != null)
                    list.add(cur);
                cur = new SmsRecord();
                Matcher m = Pattern.compile(
                        "\\+CMGL:\\s*(\\d+)\\s*,\"([^\"]*)\"\\s*,\"([^\"]*)\"(?:,.*,\"([^\"]*)\")?").matcher(line);
                if (m.find()) {
                    cur.index = Integer.parseInt(m.group(1));
                    cur.status = m.group(2);
                    cur.sender = m.group(3);
                    cur.timestamp = m.group(4);
                }
            } else if (!line.isBlank() && cur != null) {
                String trimmedLine = line.trim();
                // Bỏ qua dòng chỉ chứa "OK" hoặc "ERROR" do modem trả về ở cuối response
                if (!trimmedLine.equals("OK") && !trimmedLine.equals("ERROR")) {
                    cur.body = (cur.body == null) ? line : cur.body + "\n" + line;
                }
            }
        }
        if (cur != null)
            list.add(cur);
        return list;
    }

    public void writeRaw(byte[] data) throws IOException {
        ensureOpen();
        out.write(data);
        out.flush();
    }

    // Thêm phương thức kiểm tra trạng thái cuộc gọi
    public String getCallStatus() throws IOException, InterruptedException {
        return sendAndRead("AT+CLCC", 800);
    }

    // Thêm phương thức kiểm tra hỗ trợ ghi âm
    public boolean supportsCallRecording() throws IOException, InterruptedException {
        String response = sendAndRead("AT+CREC=?", 1000);
        return response != null && response.contains("+CREC");
    }

    // Phương thức bắt đầu ghi âm
    public boolean startCallRecording(String filename) throws IOException, InterruptedException {
        String command = String.format("AT+CREC=4,\"%s\",0,0", filename);
        String response = sendAndRead(command, 1000);
        return response.contains("OK");
    }

    // Phương thức dừng ghi âm
    public boolean stopCallRecording() throws IOException, InterruptedException {
        String response = sendAndRead("AT+CREC=0", 1000);
        return response.contains("OK");
    }

    // ---------- Lifecycle ----------
    @Override
    public void close() {
        String portName = port != null ? port.getSystemPortName() : "UNKNOWN";

        if (ownsPort && port != null) {
            try {
                // ✅ FIX: Close port FIRST before closing streams
                // This ensures port is released properly
                if (port.isOpen()) {
                    log.debug("🔒 Closing port: {}", portName);
                    port.closePort();

                    // ✅ FIX: Wait a bit to ensure port is fully released
                    // This prevents "port busy" error on next open
                    // Reduced from 200ms to 100ms for faster release
                    Thread.sleep(100);

                    log.debug("✅ Port closed: {}", portName);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("⚠️ Interrupted while closing port {}", portName);
            } catch (Exception e) {
                log.warn("⚠️ Error closing port {}: {}", portName, e.getMessage());
            }
        }

        // Close streams after port is closed
        try {
            if (in != null) {
                in.close();
            }
        } catch (Exception ignored) {
        }
        try {
            if (out != null) {
                out.close();
            }
        } catch (Exception ignored) {
        }
    }

    public byte[] readRawBytes(int maxBytes) {
        return readRawBytes(maxBytes, 5000); // timeout mặc định 5s
    }

    public byte[] readRawBytes(int maxBytes, int timeoutMs) {
        try {
            var in = port.getInputStream();
            port.setComPortTimeouts(
                    SerialPort.TIMEOUT_READ_BLOCKING,
                    timeoutMs,
                    timeoutMs);

            byte[] buffer = new byte[maxBytes];
            int offset = 0;
            long start = System.currentTimeMillis();

            while (offset < maxBytes &&
                    (System.currentTimeMillis() - start) < timeoutMs) {

                int available = port.bytesAvailable();
                if (available <= 0) {
                    Thread.sleep(10);
                    continue;
                }

                int toRead = Math.min(available, maxBytes - offset);
                int read = in.read(buffer, offset, toRead);

                if (read > 0) {
                    offset += read;

                    // nếu modem ngưng gửi dữ liệu → đợi 50ms rồi break
                    Thread.sleep(50);
                    if (port.bytesAvailable() == 0)
                        break;

                } else {
                    Thread.sleep(10);
                }
            }

            if (offset == 0)
                return new byte[0];

            byte[] result = new byte[offset];
            System.arraycopy(buffer, 0, result, 0, offset);

            return result;

        } catch (Exception e) {
            e.printStackTrace();
            return new byte[0];
        }
    }

    // ---------- DTO ----------
    public static class SmsRecord {
        public Integer index;
        public String storage;
        public String status;
        public String sender;
        public String timestamp;
        public String body;
        public String language; // 🌐 auto-detected language (set in PortWorker)

        @Override
        public String toString() {
            return "SmsRecord{" +
                    "index=" + index +
                    ", storage='" + storage + '\'' +
                    ", status='" + status + '\'' +
                    ", sender='" + sender + '\'' +
                    ", timestamp='" + timestamp + '\'' +
                    ", body='" + body + '\'' +
                    ", language='" + language + '\'' +
                    '}';
        }
    }

    public static String ultimateTextDecode(String text) {
        if (text == null)
            return null;

        // Remove common modem responses
        text = text.trim()
                .replaceAll("(?i)\\bOK\\b", "")
                .replaceAll("(?i)\\+CMGL:\\s*\\d+", "")
                .replaceAll("(?i)\\+CMGR:\\s*", "")
                .replaceAll("(?i)\\+CMT:\\s*", "")
                .replaceAll("(?i)\\+CMTI:\\s*\"[^\"]*\"\\s*,\\s*\\d+", "") // ✅ Remove +CMTI: "SM",0
                .trim();

        if (text.isEmpty())
            return "";

        // Try HEX decode (UCS2)
        try {
            // Remove all whitespace for hex check
            String hexText = text.replaceAll("\\s+", "");

            // Check if it's valid HEX string (even length, only hex chars)
            // Relaxed length check to >= 4 (1 char)
            if (hexText.matches("^[0-9A-Fa-f]+$") && hexText.length() % 4 == 0 && hexText.length() >= 4) {
                byte[] bytes = hexToBytes(hexText);
                String decoded = new String(bytes, StandardCharsets.UTF_16BE).trim();

                // Verify decoded text is readable (có chứa ký tự printable)
                if (decoded.matches(".*[\\p{L}\\p{N}].*")) {
                    return decoded;
                }
            }
        } catch (Exception e) {
            log.debug("⚠️ HEX decode failed: {}", e.getMessage());
        }

        // Remove excessive whitespace for return if not hex
        return text.replaceAll("[\\r\\n]+", " ")
                .replaceAll("\\s+", " ")
                .trim();
    }

    private static byte[] hexToBytes(String hex) {
        hex = hex.replaceAll("[^0-9A-Fa-f]", "");
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < hex.length(); i += 2) {
            bytes[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
        }
        return bytes;
    }

}
