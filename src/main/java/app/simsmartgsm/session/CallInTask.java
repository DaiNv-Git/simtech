package app.simsmartgsm.session;

import app.simsmartgsm.entity.Sim;
import lombok.Getter;

/**
 * ✅ CALL_IN Task
 */
@Getter
public class CallInTask extends SessionTask {

    private final String expectedCaller;
    private final int durationSeconds;
    private final boolean record;
    private final boolean acceptHidden;
    private final int timeWindowSeconds;
    public CallInTask(Sim sim, String expectedCaller, int durationSeconds,
            boolean record, boolean acceptHidden, int timeWindowSeconds,
            String serviceCode, String orderId) {
        super(sim, serviceCode, orderId);
        this.expectedCaller = expectedCaller;
        this.durationSeconds = durationSeconds;
        this.record = record;
        this.acceptHidden = acceptHidden;
        this.timeWindowSeconds = timeWindowSeconds;
    }

    @Override
    public TaskSession createSession() throws Exception {
        return new CallInSession(this);
    }

    @Override
    public String getTaskType() {
        return "CALL_IN";
    }
}
