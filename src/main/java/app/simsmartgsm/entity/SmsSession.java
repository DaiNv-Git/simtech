package app.simsmartgsm.entity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Document(collection = "sms_session")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@CompoundIndexes({
        @CompoundIndex(name = "unique_campaign_phone", def = "{'campaignId': 1, 'phoneNumber': 1}", unique = true)
})
public class SmsSession {
    @Id
    private String id;
    private String campaignId;
    private String simId;
    private String status;
    private String deviceName;
    private String comPort;
    private String phoneNumber;

    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private LocalDateTime expiredAt;

    private boolean active;

    // NEW: thời gian hoạt động gần nhất (khi có inbound/outbound SMS)
    private LocalDateTime lastActivityAt;
}

