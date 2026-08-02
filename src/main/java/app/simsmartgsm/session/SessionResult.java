package app.simsmartgsm.session;

import lombok.Builder;
import lombok.Getter;

import java.time.Instant;

/**
 * ✅ Kết quả của session
 */
@Getter
@Builder
public class SessionResult {

    private String orderId;
    private String taskType;
    private String status; // SUCCESS, FAILED, NO_ANSWER, TIMEOUT
    private String errorMessage;

    private String recordFileUrl; // URL file ghi âm (nếu có)

    private Instant startTime;
    private Instant endTime;

    private long totalDurationSeconds;
    private long connectedDurationSeconds;

    /**
     * ✅ Factory methods
     */
    public static SessionResult success(String orderId, String taskType, String recordFileUrl) {
        return SessionResult.builder()
                .orderId(orderId)
                .taskType(taskType)
                .status("SUCCESS")
                .recordFileUrl(recordFileUrl)
                .build();
    }

    public static SessionResult failed(String orderId, String taskType, String errorMessage) {
        return SessionResult.builder()
                .orderId(orderId)
                .taskType(taskType)
                .status("FAILED")
                .errorMessage(errorMessage)
                .build();
    }

    public static SessionResult noAnswer(String orderId, String taskType) {
        return SessionResult.builder()
                .orderId(orderId)
                .taskType(taskType)
                .status("NO_ANSWER")
                .build();
    }

    public static SessionResult timeout(String orderId, String taskType) {
        return SessionResult.builder()
                .orderId(orderId)
                .taskType(taskType)
                .status("TIMEOUT")
                .build();
    }
}
