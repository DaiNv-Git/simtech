import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRegex {
    public static void main(String[] args) {
        String line = "+CMGL: 1,\"REC READ\",\"+84912345678\",,\"24/04/09,10:20:30+28\"";
        Matcher m = Pattern.compile("\\+CMGL:\\s*(\\d+)\\s*,\"([^\"]*)\"\\s*,\"([^\"]*)\"(?:,.*,\"([^\"]*)\")?").matcher(line);
        if (m.find()) {
            System.out.println("Match!");
            System.out.println("1: " + m.group(1));
            System.out.println("2: " + m.group(2));
            System.out.println("3: " + m.group(3));
            System.out.println("4: " + m.group(4));
        } else {
            System.out.println("No match!");
        }
        
        String line2 = "+CMGL: 1,\"REC READ\",\"002B003800340039\",,\"24/04/09,10:20:30+28\"";
        m = Pattern.compile("\\+CMGL:\\s*(\\d+)\\s*,\"([^\"]*)\"\\s*,\"([^\"]*)\"(?:,.*,\"([^\"]*)\")?").matcher(line2);
        if (m.find()) {
            System.out.println("Match 2!");
            System.out.println("3: " + m.group(3));
        }
        
        String ucs2Line = "+CMGL: 2,\"REC UNREAD\",\"002B0038003400330032003800330034003600320038\",\"\",\"24/02/10,21:49:18+28\"";
        m = Pattern.compile("\\+CMGL:\\s*(\\d+)\\s*,\"([^\"]*)\"\\s*,\"([^\"]*)\"(?:,.*,\"([^\"]*)\")?").matcher(ucs2Line);
        if (m.find()) {
            System.out.println("Match 3!");
            System.out.println("3: " + m.group(3));
        }
    }
}
