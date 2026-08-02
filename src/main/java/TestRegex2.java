import java.nio.charset.StandardCharsets;

public class TestRegex2 {
    public static void main(String[] args) {
        String decoded = "test";
        if (decoded.matches(".*[\\p{L}\\p{N}].*")) {
            System.out.println("Matches logic for decoding.");
        }
        String outgoingBody = "gửi tin nhắn thành công";
        System.out.println(outgoingBody.matches(".*[\\p{L}\\p{N}].*"));
    }
}
