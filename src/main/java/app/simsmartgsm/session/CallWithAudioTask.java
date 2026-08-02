package app.simsmartgsm.session;

import app.simsmartgsm.entity.Sim;
import lombok.Getter;

/**
 * ✅ CALL_WITH_AUDIO Task
 * Gọi điện tới 1 số và phát file audio ghi sẵn qua uplink (đối phương nghe được)
 */
@Getter
public class CallWithAudioTask extends SessionTask {

    private final String targetPhone;
    private final String audioFileName;       // Tên file trên modem
    private final String localAudioPath;      // Đường dẫn file local (optional)
    private final boolean repeatAudio;
    private final int waitAfterAudioSeconds;
    private final boolean record;
    public CallWithAudioTask(Sim sim, String targetPhone, String audioFileName,
            String localAudioPath, boolean repeatAudio, int waitAfterAudioSeconds,
            boolean record, String serviceCode, String orderId) {
        super(sim, serviceCode, orderId);
        this.targetPhone = targetPhone;
        this.audioFileName = audioFileName;
        this.localAudioPath = localAudioPath;
        this.repeatAudio = repeatAudio;
        this.waitAfterAudioSeconds = waitAfterAudioSeconds;
        this.record = record;
    }

    @Override
    public TaskSession createSession() throws Exception {
        return new CallWithAudioSession(this);
    }

    @Override
    public String getTaskType() {
        return "CALL_WITH_AUDIO";
    }
}
