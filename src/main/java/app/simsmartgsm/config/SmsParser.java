package app.simsmartgsm.config;

import app.simsmartgsm.dto.response.SmsMessageUser;
import app.simsmartgsm.uitils.SmsDecoder;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SmsParser {

    private static final Pattern CMGL_PATTERN = Pattern
            .compile("\\+CMGL:\\s*(\\d+),\"[^\"]*\",\"(\\+?\\d+)?\",,\"([^\"]+)\"");

    private static final Pattern CMGR_PATTERN = Pattern.compile("\\+CMGR:\\s*\"[^\"]*\",\"(\\+?\\d+)?\",,\"([^\"]+)\"");

    // Standard +CMT pattern (regular phone number)
    private static final Pattern CMT_PATTERN = Pattern.compile("\\+CMT:\\s*\"(\\+?\\d+)?\",,\"([^\"]+)\"");

    // UCS2-encoded +CMT pattern (hex-encoded phone number)
    // Matches: +CMT:"003400380034...",,"25/12/01,18:03:19+36"
    private static final Pattern CMT_UCS2_PATTERN = Pattern.compile("\\+CMT:\\s*\"([0-9A-Fa-f]+)\",,\"([^\"]+)\"");

    private static final Pattern CMTI_PATTERN = Pattern.compile("\\+CMTI:\\s*\"\\w+\",(\\d+)");

    /**
     * Parse danh sách tin nhắn từ phản hồi của AT+CMGL hoặc AT+CMGR
     */
    public static List<SmsMessageUser> parseMulti(String resp) {
        List<SmsMessageUser> messages = new ArrayList<>();
        if (resp == null || resp.isBlank())
            return messages;

        String[] lines = resp.split("\r\n|\n");
        int i = 0;
        while (i < lines.length) {
            String line = lines[i].trim();

            Matcher mList = CMGL_PATTERN.matcher(line);
            Matcher mSingle = CMGR_PATTERN.matcher(line);

            Integer index = null;
            String sender = "UNKNOWN";
            String timestamp = "";
            StringBuilder body = new StringBuilder();

            if (mList.find()) {
                index = Integer.parseInt(mList.group(1));
                sender = mList.group(2) != null ? mList.group(2) : "UNKNOWN";
                timestamp = mList.group(3);
                i++;
                while (i < lines.length && !lines[i].trim().startsWith("+CMGL:")
                        && !lines[i].trim().startsWith("+CMGR:")
                        && !lines[i].trim().startsWith("OK")) {
                    if (!lines[i].trim().isEmpty())
                        body.append(lines[i].trim()).append("\n");
                    i++;
                }

                // Decode body if it's UCS2-encoded
                String bodyText = body.toString().trim();
                String decodedBody = SmsDecoder.decode(bodyText);

                messages.add(new SmsMessageUser(index, sender, timestamp, decodedBody));
                log.info("✅ Parsed SMS [index={}, from={}, time={}]", index, sender, timestamp);
                continue;
            } else if (mSingle.find()) {
                sender = mSingle.group(1) != null ? mSingle.group(1) : "UNKNOWN";
                timestamp = mSingle.group(2);
                i++;
                while (i < lines.length && !lines[i].trim().startsWith("+CMGL:")
                        && !lines[i].trim().startsWith("OK")) {
                    if (!lines[i].trim().isEmpty())
                        body.append(lines[i].trim()).append("\n");
                    i++;
                }

                // Decode body if it's UCS2-encoded
                String bodyText = body.toString().trim();
                String decodedBody = SmsDecoder.decode(bodyText);

                messages.add(new SmsMessageUser(null, sender, timestamp, decodedBody));
                log.info("✅ Parsed SMS [from={}, time={}]", sender, timestamp);
                continue;
            }

            i++;
        }

        return messages;
    }

    /**
     * Parse 1 SMS (từ AT+CMGR hoặc URC +CMT)
     */
    public static SmsMessageUser parse(String resp) {
        if (resp == null || resp.isBlank())
            return null;

        try {
            // Case 1a: URC +CMT with UCS2-encoded phone number
            // Example: +CMT:"00340038003400380035...",,"25/12/01,18:03:19+36"
            Matcher mCmtUcs2 = CMT_UCS2_PATTERN.matcher(resp);
            if (mCmtUcs2.find()) {
                String hexPhone = mCmtUcs2.group(1);
                String timestamp = mCmtUcs2.group(2);

                // Check if it's actually a hex-encoded phone number (not a regular number)
                if (hexPhone != null && hexPhone.length() >= 8 && hexPhone.matches("^[0-9A-Fa-f]+$")
                        && hexPhone.length() % 4 == 0) {
                    // Decode UCS2 phone number
                    String decodedPhone = decodeUcs2Phone(hexPhone);
                    String body = extractBody(resp);

                    // Decode body as well (it's likely also UCS2-encoded)
                    String decodedBody = SmsDecoder.decode(body);

                    log.info("📩 Parsed UCS2-encoded +CMT: phone={} (decoded from hex), timestamp={}",
                            decodedPhone, timestamp);

                    return new SmsMessageUser(null, decodedPhone, timestamp, decodedBody);
                }
            }

            // Case 1b: URC +CMT (standard format with regular phone number)
            Matcher mCmt = CMT_PATTERN.matcher(resp);
            if (mCmt.find()) {
                String sender = mCmt.group(1) != null ? mCmt.group(1) : "UNKNOWN";
                String timestamp = mCmt.group(2);
                String body = extractBody(resp);

                // Try to decode body in case it's UCS2-encoded
                String decodedBody = SmsDecoder.decode(body);

                return new SmsMessageUser(null, sender, timestamp, decodedBody);
            }

            // Case 2: +CMTI (chỉ báo index)
            Matcher mCmti = CMTI_PATTERN.matcher(resp);
            if (mCmti.find()) {
                int idx = Integer.parseInt(mCmti.group(1));
                log.info("📩 New SMS notification at index {}", idx);
                return new SmsMessageUser(idx, "UNKNOWN", "", "[INDEX:" + idx + "]");
            }

            // Case 3: AT+CMGR / CMGL
            List<SmsMessageUser> list = parseMulti(resp);
            if (!list.isEmpty())
                return list.get(0);

        } catch (Exception e) {
            log.warn("⚠️ Error parsing SMS: {}", e.getMessage());
        }

        return null;
    }

    /**
     * Decode UCS2-encoded phone number from hex string
     * Example: "00340038003400380035..." -> "048485..."
     */
    private static String decodeUcs2Phone(String hex) {
        if (hex == null || hex.isEmpty() || hex.length() % 4 != 0) {
            return hex;
        }

        try {
            byte[] bytes = new byte[hex.length() / 2];
            for (int i = 0; i < hex.length(); i += 2) {
                bytes[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
            }

            // Decode as UTF-16 Big Endian
            String decoded = new String(bytes, StandardCharsets.UTF_16BE).trim();

            // Remove any non-digit characters except + at the start
            decoded = decoded.replaceAll("[^0-9+]", "");

            log.debug("✅ Decoded UCS2 phone: {} -> {}", hex, decoded);
            return decoded;
        } catch (Exception e) {
            log.warn("⚠️ Failed to decode UCS2 phone number: {} - {}", hex, e.getMessage());
            return hex;
        }
    }

    /** Trích nội dung phần body sau khi bỏ header AT */
    private static String extractBody(String resp) {
        String[] lines = resp.split("\r\n|\n");
        StringBuilder sb = new StringBuilder();
        boolean contentStarted = false;
        for (String l : lines) {
            if (l.startsWith("+CMT:") || l.startsWith("+CMGL:") || l.startsWith("+CMGR:")) {
                // If we already started collecting content and hit another header, stop
                if (contentStarted) {
                    break;
                }
                contentStarted = true;
                continue;
            }
            if (contentStarted) {
                if (l.trim().equals("OK"))
                    break;
                // Also stop if we encounter another +CMT in the middle (concatenated messages)
                if (l.contains("+CMT:"))
                    break;
                if (!l.trim().isEmpty())
                    sb.append(l.trim()).append("\n");
            }
        }
        return sb.toString().trim();
    }
}
