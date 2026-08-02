package app.simsmartgsm.tool.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@Document(collection = "sms")
public class SmsDocument {
    @Id
    private String id;

    @Indexed(unique = true, sparse = true)
    private String fingerprint;

    @Indexed
    private String comPort;
    private String simPhone;
    private String phoneNumber;
    private String direction;
    private String content;
    private String status;
    private String modemTimestamp;
    private String modemResponse;

    @Indexed
    private Instant createdAt;
}
