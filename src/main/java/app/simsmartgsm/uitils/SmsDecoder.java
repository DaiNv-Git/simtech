package app.simsmartgsm.uitils;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/**
 * Utility class for decoding SMS messages that may be in UTF-16 hex format.
 * This ensures all SMS content sent to the cloud is properly decoded.
 */
@Slf4j
public class SmsDecoder {

    /**
     * Decode SMS text that may be in hexadecimal UTF-16 format.
     * 
     * @param text The text to decode (may be hex-encoded or plain text)
     * @return Decoded readable text
     */
    public static String decode(String text) {
        if (text == null) {
            return null;
        }

        // Remove common modem responses and clean up
        text = text.trim()
                .replaceAll("(?i)\\bOK\\b", "")
                .replaceAll("(?i)\\+CMGL:\\s*\\d+", "")
                .replaceAll("(?i)\\+CMGR:\\s*", "")
                .replaceAll("(?i)\\+CMT:\\s*", "")
                .replaceAll("(?i)\\+CMTI:\\s*\"[^\"]*\"\\s*,\\s*\\d+", "") // ✅ Remove +CMTI: "SM",0
                .trim();

        // Remove excessive whitespace
        text = text.replaceAll("[\\r\\n]+", " ")
                .replaceAll("\\s+", " ")
                .trim();

        if (text.isEmpty()) {
            return "";
        }

        // ✅ NEW: Extract hex portion if mixed with other text
        // Pattern: hex string followed by or preceded by non-hex content
        String hexPart = extractHexPortion(text);
        if (hexPart != null && !hexPart.equals(text)) {
            log.debug("📋 Extracted hex portion: {} chars from mixed content", hexPart.length());
            text = hexPart;
        }

        // Try UTF-16 HEX decode (UCS2)
        try {
            // Check if it's a valid HEX string:
            // - Only contains hex characters (0-9, A-F, a-f)
            // - Length is divisible by 4 (each UTF-16 char = 4 hex digits)
            // - Minimum length of 8 (at least 2 characters)
            if (text.matches("^[0-9A-Fa-f]+$") && text.length() % 4 == 0 && text.length() >= 8) {
                byte[] bytes = hexToBytes(text);
                String decoded = new String(bytes, StandardCharsets.UTF_16BE).trim();

                // Verify decoded text is readable (contains printable characters)
                // Check for letters, numbers, spaces, or common punctuation
                // This prevents false positives where random hex strings get decoded
                if (decoded.length() > 0 && !decoded.matches("^[\\x00-\\x1F]+$")) {
                    // Not just control characters
                    log.debug("✅ Decoded UTF-16 hex: {} chars → {}", text.length() / 4, decoded.length());
                    return decoded;
                }
            }
        } catch (Exception e) {
            log.debug("⚠️ HEX decode failed for text: {} - {}",
                    text.substring(0, Math.min(50, text.length())), e.getMessage());
        }

        // Return original text if not hex-encoded
        return text;
    }

    /**
     * Extract hex portion from mixed content.
     * Handles cases like: "30D730E9...306730593002 +CMTI: \"SM\",0"
     * 
     * @param text Text that may contain hex mixed with other content
     * @return The hex portion if found, or null if no valid hex portion
     */
    private static String extractHexPortion(String text) {
        if (text == null || text.isEmpty()) {
            return null;
        }

        // Pattern to find a long hex string (at least 8 chars, divisible by 4)
        java.util.regex.Pattern hexPattern = java.util.regex.Pattern.compile("([0-9A-Fa-f]{8,})");
        java.util.regex.Matcher matcher = hexPattern.matcher(text);

        String longestHex = null;
        while (matcher.find()) {
            String candidate = matcher.group(1);
            // Must be divisible by 4 for valid UCS2
            if (candidate.length() % 4 == 0) {
                if (longestHex == null || candidate.length() > longestHex.length()) {
                    longestHex = candidate;
                }
            }
        }

        return longestHex;
    }

    /**
     * Convert hexadecimal string to byte array.
     * 
     * @param hex Hexadecimal string (e.g., "00540065006C0065")
     * @return Byte array
     */
    private static byte[] hexToBytes(String hex) {
        // Remove any non-hex characters (just in case)
        hex = hex.replaceAll("[^0-9A-Fa-f]", "");

        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < hex.length(); i += 2) {
            bytes[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
        }
        return bytes;
    }

    /**
     * Check if a string appears to be UTF-16 hex encoded.
     * 
     * @param text Text to check
     * @return true if the text appears to be hex-encoded
     */
    public static boolean isHexEncoded(String text) {
        if (text == null || text.isEmpty()) {
            return false;
        }

        text = text.trim();

        // Must be only hex chars, divisible by 4, and at least 8 chars long
        return text.matches("^[0-9A-Fa-f]+$")
                && text.length() % 4 == 0
                && text.length() >= 8;
    }
}
