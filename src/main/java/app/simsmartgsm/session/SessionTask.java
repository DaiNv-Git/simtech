package app.simsmartgsm.session;

import app.simsmartgsm.entity.Sim;
import lombok.Getter;

/**
 * ✅ Base class cho tất cả session tasks
 */
@Getter
public abstract class SessionTask {

    protected final Sim sim;
    protected final String serviceCode;
    protected final String orderId;

    public SessionTask(Sim sim, String serviceCode, String orderId) {
        this.sim = sim;
        this.serviceCode = serviceCode;
        this.orderId = orderId;
    }

    /**
     * ✅ Tạo session để thực thi task
     */
    public abstract TaskSession createSession() throws Exception;

    /**
     * ✅ Get task type for logging
     */
    public abstract String getTaskType();
}
