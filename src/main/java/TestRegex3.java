import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRegex3 {
    public static void main(String[] args) {
        String sender = "+81901234"; // Length 9, stripped +, "81901234" is 8!
        String candidate = extractHexPortion("+81901234");
        System.out.println("Candidate for +81901234: " + candidate);

        candidate = extractHexPortion("+849012345678"); // 849012345678 length 12
        System.out.println("Candidate for +849012345678: " + candidate);

        if (candidate != null) {
            try {
                byte[] bytes = hexToBytes(candidate);
                String decoded = new String(bytes, StandardCharsets.UTF_16BE).trim();
                System.out.println("Decoded: " + decoded);
                if (decoded.matches(".*[\\p{L}\\p{N}].*")) {
                    System.out.println("Valid decoded!");
                }
            } catch (Exception e) {}
        }
    }
    
    private static byte[] hexToBytes(String hex) {
        hex = hex.replaceAll("[^0-9A-Fa-f]", "");
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < hex.length(); i += 2) {
            bytes[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
        }
        return bytes;
    }

    private static String extractHexPortion(String text) {
        if (text == null || text.isEmpty()) {
            return null;
        }
        Pattern hexPattern = Pattern.compile("([0-9A-Fa-f]{8,})");
        Matcher matcher = hexPattern.matcher(text);

        String longestHex = null;
        while (matcher.find()) {
            String candidate = matcher.group(1);
            if (candidate.length() % 4 == 0) {
                if (longestHex == null || candidate.length() > longestHex.length()) {
                    longestHex = candidate;
                }
            }
        }
        return longestHex;
    }
}
