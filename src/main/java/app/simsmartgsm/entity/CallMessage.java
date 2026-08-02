package app.simsmartgsm.entity;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.Instant;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "call_messages")
public class CallMessage {
    @Id
    private String id;

    private String orderId;
    private Long accountId;

    private String simPhone;
    private String fromNumber;
    private String toNumber;

    private Instant startTime;
    private Instant endTime;

    private String status; // CALLING, COMPLETED, FAILED, MISSED
    private String recordingPath; // file ghi âm nếu có

    /**
     * Calculate call duration in seconds
     * 
     * @return duration in seconds, or null if call is ongoing or not started
     */
    public Long getDurationSeconds() {
        if (startTime != null && endTime != null) {
            return java.time.Duration.between(startTime, endTime).toSeconds();
        }
        return null;
    }
}
