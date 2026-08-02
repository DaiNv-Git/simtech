package app.simsmartgsm.session;

import app.simsmartgsm.entity.Sim;
import lombok.Getter;

/**
 * ✅ SMS Task
 */
@Getter
public class SmsTask extends SessionTask {

    private final String targetPhone;
    private final String message;

    public SmsTask(Sim sim, String targetPhone, String message,
            String serviceCode, String orderId) {
        super(sim, serviceCode, orderId);
        this.targetPhone = targetPhone;
        this.message = message;
    }

    @Override
    public TaskSession createSession() throws Exception {
        return new SmsSession(this);
    }

    @Override
    public String getTaskType() {
        return "SMS";
    }
}
