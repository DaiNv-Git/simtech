package app.simsmartgsm.session;

import app.simsmartgsm.baseGateway.CloudGateway;
import app.simsmartgsm.config.RemoteWsClient;
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
    private final CloudGateway cloudGateway;
    private final RemoteWsClient remoteWsClient;

    public CallInTask(Sim sim, String expectedCaller, int durationSeconds,
            boolean record, boolean acceptHidden, int timeWindowSeconds,
            String serviceCode, String orderId,
            CloudGateway cloudGateway, RemoteWsClient remoteWsClient) {
        super(sim, serviceCode, orderId);
        this.expectedCaller = expectedCaller;
        this.durationSeconds = durationSeconds;
        this.record = record;
        this.acceptHidden = acceptHidden;
        this.timeWindowSeconds = timeWindowSeconds;
        this.cloudGateway = cloudGateway;
        this.remoteWsClient = remoteWsClient;
    }

    @Override
    public TaskSession createSession() throws Exception {
        return new CallInSession(this, cloudGateway, remoteWsClient);
    }

    @Override
    public String getTaskType() {
        return "CALL_IN";
    }
}
