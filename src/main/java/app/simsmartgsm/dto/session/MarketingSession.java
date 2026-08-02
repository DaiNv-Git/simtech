package app.simsmartgsm.dto.session;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;

@Data
@NoArgsConstructor // ✅ cho Jackson (constructor rỗng)
@AllArgsConstructor // ✅ constructor đầy đủ
public class MarketingSession {
    private String simNumber;       // Số SIM trong GSM
    private String customerPhone;   // Số KH
    private String campaignId;
    private String sessionId;
    private String localMsgId;
    private String simId;
    private Instant expiresAt;
}
