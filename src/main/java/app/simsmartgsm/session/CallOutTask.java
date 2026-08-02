package app.simsmartgsm.session;

import app.simsmartgsm.baseGateway.CloudGateway;
import app.simsmartgsm.config.RemoteWsClient;
import app.simsmartgsm.entity.Sim;
import lombok.Getter;

/**
 * ✅ CALL_OUT Task
 */
@Getter
public class CallOutTask extends SessionTask {

    private final String targetPhone;
    private final int durationSeconds;
    private final boolean record;
    private final CloudGateway cloudGateway;
    private final RemoteWsClient remoteWsClient;

    public CallOutTask(Sim sim, String targetPhone, int durationSeconds,
            boolean record, String serviceCode, String orderId,
            CloudGateway cloudGateway, RemoteWsClient remoteWsClient) {
        super(sim, serviceCode, orderId);
        this.targetPhone = targetPhone;
        this.durationSeconds = durationSeconds;
        this.record = record;
        this.cloudGateway = cloudGateway;
        this.remoteWsClient = remoteWsClient;
    }

    @Override
    public TaskSession createSession() throws Exception {
        return new CallOutSession(this, cloudGateway, remoteWsClient);
    }

    @Override
    public String getTaskType() {
        return "CALL_OUT";
    }
}
