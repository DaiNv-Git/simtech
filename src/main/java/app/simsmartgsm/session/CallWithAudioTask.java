package app.simsmartgsm.session;

import app.simsmartgsm.baseGateway.CloudGateway;
import app.simsmartgsm.config.RemoteWsClient;
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
    private final CloudGateway cloudGateway;
    private final RemoteWsClient remoteWsClient;

    public CallWithAudioTask(Sim sim, String targetPhone, String audioFileName,
            String localAudioPath, boolean repeatAudio, int waitAfterAudioSeconds,
            boolean record, String serviceCode, String orderId,
            CloudGateway cloudGateway, RemoteWsClient remoteWsClient) {
        super(sim, serviceCode, orderId);
        this.targetPhone = targetPhone;
        this.audioFileName = audioFileName;
        this.localAudioPath = localAudioPath;
        this.repeatAudio = repeatAudio;
        this.waitAfterAudioSeconds = waitAfterAudioSeconds;
        this.record = record;
        this.cloudGateway = cloudGateway;
        this.remoteWsClient = remoteWsClient;
    }

    @Override
    public TaskSession createSession() throws Exception {
        return new CallWithAudioSession(this, cloudGateway, remoteWsClient);
    }

    @Override
    public String getTaskType() {
        return "CALL_WITH_AUDIO";
    }
}
