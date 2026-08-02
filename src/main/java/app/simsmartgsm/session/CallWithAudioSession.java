package app.simsmartgsm.session;

import app.simsmartgsm.uitils.AtCommandHelper;
import com.fazecast.jSerialComm.SerialPort;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ✅ CALL_WITH_AUDIO Session
 * Flow:
 *   1. Upload file audio lên modem (nếu cần)
 *   2. Gọi tới số điện thoại (ATD)
 *   3. Chờ đối phương bắt máy (AT+CLCC)
 *   4. Phát file audio qua uplink (AT+QPSND) → đối phương nghe được
 *   5. Chờ phát xong → ngắt cuộc gọi (ATH)
 */
@Slf4j
public class CallWithAudioSession implements TaskSession {

    private static final int BAUD_RATE = 115200;

    private final CallWithAudioTask task;

    private SerialPort port;
    private AtCommandHelper helper;

    private Instant callStartTime;
    private Instant callEndTime;
    private boolean callConnected = false;
    private boolean audioPlayed = false;
    private long connectedDuration = 0;

    public CallWithAudioSession(CallWithAudioTask task) {
        this.task = task;
    }

    @Override
    public void openPort() throws Exception {
        String comPort = task.getSim().getComName();
        int maxRetries = 3;
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                port = SerialPort.getCommPort(comPort);
                port.setBaudRate(BAUD_RATE);
                port.setComPortTimeouts(SerialPort.TIMEOUT_READ_BLOCKING, 1000, 1000);

                if (!port.openPort()) {
                    throw new IOException("Port open returned false");
                }

                helper = new AtCommandHelper(port);

                String atResp = helper.sendAndRead("AT", 500);
                if (atResp == null || !atResp.contains("OK")) {
                    throw new IOException("Modem not responding");
                }

                helper.sendAndRead("ATE0", 500);
                helper.sendAndRead("AT+CMEE=2", 500);

                log.info("✅ [{}] Port opened for CALL_WITH_AUDIO (attempt {})", comPort, attempt);
                return;

            } catch (Exception e) {
                lastException = e;
                log.warn("⚠️ [{}] Port open failed (attempt {}/{}): {}",
                        comPort, attempt, maxRetries, e.getMessage());

                if (port != null && port.isOpen()) {
                    try { port.closePort(); } catch (Exception ignored) {}
                }

                if (attempt < maxRetries) {
                    Thread.sleep(500 * attempt);
                }
            }
        }

        throw new IOException("Cannot open port after " + maxRetries + " attempts: " +
                (lastException != null ? lastException.getMessage() : "unknown error"));
    }

    @Override
    public SessionResult execute() throws Exception {
        String comPort = task.getSim().getComName();
        callStartTime = Instant.now();

        try {
            notifyStatus("PREPARING");

            // ✅ Step 1: Check modem type
            String modemInfo = helper.sendAndRead("ATI", 1000).toUpperCase();
            boolean isEC2x = modemInfo.contains("EC25") || modemInfo.contains("EC21");

            if (!isEC2x) {
                throw new Exception("Module không phải EC25/EC21, không hỗ trợ AT+QPSND");
            }

            log.info("📱 [{}] Modem: EC2x confirmed", comPort);

            // ✅ Step 2: Upload audio file to modem (if localAudioPath is provided)
            String modemAudioFile = task.getAudioFileName();
            if (task.getLocalAudioPath() != null && !task.getLocalAudioPath().isEmpty()) {
                notifyStatus("UPLOADING_AUDIO");
                modemAudioFile = uploadAudioToModem(task.getLocalAudioPath());
                if (modemAudioFile == null) {
                    throw new Exception("Failed to upload audio file to modem");
                }
            }

            // ✅ Step 3: Verify audio file exists on modem
            notifyStatus("VERIFYING_AUDIO");
            if (!verifyAudioFileOnModem(modemAudioFile)) {
                throw new Exception("Audio file '" + modemAudioFile + "' not found on modem. "
                        + "Please upload it first via /api/gsm/call/upload-audio");
            }

            log.info("✅ [{}] Audio file verified: {}", comPort, modemAudioFile);

            // ✅ Step 4: Configure voice call
            notifyStatus("DIALING");
            helper.sendAndRead("AT+QPCMV=1,8,10000", 1000);

            // ✅ Step 5: Dial
            String dialCmd = "ATD" + task.getTargetPhone() + ";";
            String dialResp = helper.sendAndRead(dialCmd, 2000);

            if (dialResp.contains("ERROR")) {
                throw new Exception("Dial failed: " + dialResp);
            }

            log.info("📞 [{}] Dialing {} ...", comPort, task.getTargetPhone());
            notifyStatus("RINGING");

            // ✅ Step 6: Start recording if needed
            String modemRecordPath = null;
            if (task.isRecord()) {
                modemRecordPath = startRecording();
            }

            // ✅ Step 7: Monitor call state - wait for answer
            boolean connected = monitorCallState();

            if (connected) {
                callConnected = true;
                notifyStatus("CONNECTED");

                Instant connectedAt = Instant.now();
                log.info("✅ [{}] Call connected! Playing audio: {}", comPort, modemAudioFile);

                // ✅ Step 8: Play audio via AT+QPSND (uplink - đối phương nghe được)
                notifyStatus("PLAYING_AUDIO");
                audioPlayed = playAudioOnCall(modemAudioFile);

                if (audioPlayed) {
                    log.info("🔊 [{}] Audio playback completed", comPort);
                    notifyStatus("AUDIO_COMPLETED");
                } else {
                    log.warn("⚠️ [{}] Audio playback may have failed, trying fallback...", comPort);
                    // Fallback: try AT+QAUDPLAY
                    audioPlayed = playAudioFallback(modemAudioFile);
                    if (audioPlayed) {
                        log.info("🔊 [{}] Fallback audio playback completed", comPort);
                        notifyStatus("AUDIO_COMPLETED");
                    }
                }

                // ✅ Step 9: Wait after audio
                if (task.getWaitAfterAudioSeconds() > 0) {
                    log.info("⏳ [{}] Waiting {}s after audio...", comPort, task.getWaitAfterAudioSeconds());
                    Thread.sleep(task.getWaitAfterAudioSeconds() * 1000L);
                }

                connectedDuration = (Instant.now().toEpochMilli() - connectedAt.toEpochMilli()) / 1000;
            }

            // ✅ Step 10: Hangup
            helper.sendAndRead("AT+QPSND=0", 500); // Stop audio playback
            helper.sendAndRead("ATH", 1000);
            callEndTime = Instant.now();

            // ✅ Step 11: Handle recording
            String uploadedUrl = null;
            if (task.isRecord() && modemRecordPath != null) {
                notifyStatus("RECORDING_STOPPING");
                stopRecording();
                // TODO: Download and upload recording (same as CallOutSession)
            }

            String status = callConnected ? (audioPlayed ? "SUCCESS" : "AUDIO_FAILED") : "NO_ANSWER";
            notifyStatus(status);

            return SessionResult.builder()
                    .orderId(task.getOrderId())
                    .taskType("CALL_WITH_AUDIO")
                    .status(status)
                    .recordFileUrl(uploadedUrl)
                    .startTime(callStartTime)
                    .endTime(callEndTime)
                    .totalDurationSeconds((callEndTime.toEpochMilli() - callStartTime.toEpochMilli()) / 1000)
                    .connectedDurationSeconds(connectedDuration)
                    .build();

        } catch (Exception e) {
            callEndTime = Instant.now();
            log.error("❌ [{}] CALL_WITH_AUDIO failed: {}", comPort, e.getMessage(), e);

            // Cleanup: stop audio & hangup
            try { helper.sendAndRead("AT+QPSND=0", 300); } catch (Exception ignored) {}
            try { helper.sendAndRead("ATH", 500); } catch (Exception ignored) {}

            notifyStatus("FAILED");
            return SessionResult.failed(task.getOrderId(), "CALL_WITH_AUDIO", e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (port != null && port.isOpen()) {
                port.closePort();
                log.info("🔒 [{}] Port closed", task.getSim().getComName());
            }
        } catch (Exception e) {
            log.warn("⚠️ Error closing port: {}", e.getMessage());
        }
    }

    // ============== Audio Methods ==============

    /**
     * ✅ Upload file audio từ server local lên modem (UFS)
     * Sử dụng AT+QFUPL
     */
    private String uploadAudioToModem(String localPath) throws Exception {
        String comPort = task.getSim().getComName();
        File localFile = new File(localPath);

        if (!localFile.exists()) {
            log.error("❌ [{}] Local audio file not found: {}", comPort, localPath);
            return null;
        }

        long fileSize = localFile.length();
        String modemFileName = localFile.getName();

        log.info("📤 [{}] Uploading audio to modem: {} ({} bytes)", comPort, modemFileName, fileSize);

        // Delete existing file on modem first
        helper.sendAndRead("AT+QFDEL=\"" + modemFileName + "\"", 1000);
        Thread.sleep(200);

        // Upload file using AT+QFUPL
        String uplCmd = "AT+QFUPL=\"" + modemFileName + "\"," + fileSize;
        String uplResp = helper.sendAndRead(uplCmd, 3000);

        if (!uplResp.contains("CONNECT")) {
            log.error("❌ [{}] QFUPL failed: {}", comPort, uplResp.trim());
            return null;
        }

        // Send file data
        byte[] fileData = Files.readAllBytes(localFile.toPath());
        OutputStream out = port.getOutputStream();

        int chunkSize = 1024;
        int offset = 0;
        while (offset < fileData.length) {
            int len = Math.min(chunkSize, fileData.length - offset);
            out.write(fileData, offset, len);
            out.flush();
            offset += len;

            int progress = (int) (offset * 100 / fileSize);
            if (progress % 20 == 0) {
                notifyStatus("UPLOADING_AUDIO", progress);
            }

            Thread.sleep(10); // Small delay to prevent buffer overflow
        }

        // Wait for OK
        Thread.sleep(1000);
        String result = helper.readAll();
        if (result.contains("OK") || result.contains("+QFUPL")) {
            log.info("✅ [{}] Audio uploaded: {} ({} bytes)", comPort, modemFileName, fileSize);
            return modemFileName;
        } else {
            log.error("❌ [{}] Upload completed but no OK: {}", comPort, result.trim());
            return null;
        }
    }

    /**
     * ✅ Kiểm tra file audio có tồn tại trên modem không
     */
    private boolean verifyAudioFileOnModem(String fileName) throws Exception {
        String resp = helper.sendAndRead("AT+QFLST=\"" + fileName + "\"", 2000);
        return resp.contains(fileName) && !resp.contains("ERROR");
    }

    /**
     * ✅ Phát audio qua uplink bằng AT+QPSND
     * Đối phương sẽ nghe được file audio này
     * Micro local bị mute tự động khi phát
     *
     * Syntax: AT+QPSND=<control>,"<filename>",<repeat>
     *   control: 1=play, 0=stop
     *   repeat: 0=play once, 1=repeat
     */
    private boolean playAudioOnCall(String fileName) throws Exception {
        String comPort = task.getSim().getComName();
        int repeat = task.isRepeatAudio() ? 1 : 0;

        // ✅ Play audio via AT+QPSND (uplink playback)
        String playCmd = "AT+QPSND=1,\"" + fileName + "\"," + repeat;
        log.info("🔊 [{}] Playing audio on call: {}", comPort, playCmd);

        String playResp = helper.sendAndRead(playCmd, 3000);

        if (playResp.contains("OK") && !playResp.contains("ERROR")) {
            log.info("✅ [{}] AT+QPSND started successfully", comPort);

            // Wait for playback to finish (monitor with polling)
            return waitForAudioPlayback();
        } else {
            log.warn("⚠️ [{}] AT+QPSND failed: {}", comPort, playResp.trim());
            return false;
        }
    }

    /**
     * ✅ Fallback: Phát audio bằng AT+QAUDPLAY (local playback)
     * Lưu ý: AT+QAUDPLAY phát ở local, không chắc đối phương nghe được
     * Chỉ dùng làm fallback khi QPSND không hoạt động
     */
    private boolean playAudioFallback(String fileName) throws Exception {
        String comPort = task.getSim().getComName();

        // AT+QAUDPLAY="filename",repeat,volume,channel
        // channel: 0=Receiver, 1=Headset, 2=Loud Speaker
        String playCmd = "AT+QAUDPLAY=\"" + fileName + "\",0,100,0";
        log.info("🔊 [{}] Fallback audio play: {}", comPort, playCmd);

        String playResp = helper.sendAndRead(playCmd, 3000);

        if (playResp.contains("OK") && !playResp.contains("ERROR")) {
            log.info("✅ [{}] AT+QAUDPLAY started", comPort);
            return waitForAudioPlayback();
        } else {
            log.warn("⚠️ [{}] AT+QAUDPLAY also failed: {}", comPort, playResp.trim());
            return false;
        }
    }

    /**
     * ✅ Chờ audio phát xong
     * Monitor bằng URC: +QPSNDIND hoặc +QAUDPIND
     * Timeout: 120s max
     */
    private boolean waitForAudioPlayback() throws Exception {
        String comPort = task.getSim().getComName();
        int timeoutSeconds = 120; // Max 2 phút cho 1 file audio
        int pollIntervalMs = 2000;
        int maxAttempts = timeoutSeconds * 1000 / pollIntervalMs;

        log.info("⏳ [{}] Waiting for audio playback to finish (max {}s)...", comPort, timeoutSeconds);

        for (int i = 0; i < maxAttempts; i++) {
            Thread.sleep(pollIntervalMs);

            // Check for playback finished URC
            String response = helper.readAll();
            if (response.contains("+QPSNDIND") || response.contains("+QAUDPIND")) {
                log.info("✅ [{}] Audio playback finished (URC detected)", comPort);
                return true;
            }

            // Also check if call is still active
            String clcc = helper.sendAndRead("AT+CLCC", 800);
            if (clcc == null || (!clcc.contains("+CLCC:") && !clcc.contains("OK"))) {
                log.warn("⚠️ [{}] Call disconnected during audio playback", comPort);
                return false;
            }

            // If no active call entries in CLCC response
            if (!clcc.contains("+CLCC:")) {
                log.warn("⚠️ [{}] Call ended during audio playback", comPort);
                return false;
            }
        }

        log.warn("⚠️ [{}] Audio playback timeout ({}s)", comPort, timeoutSeconds);
        // Stop playback
        helper.sendAndRead("AT+QPSND=0", 500);
        return true; // Consider it played despite timeout
    }

    // ============== Call Monitoring ==============

    /**
     * ✅ Monitor call state - wait for answer
     */
    private boolean monitorCallState() throws Exception {
        int timeoutSeconds = 60;
        int pollIntervalMs = 2000;
        int maxAttempts = timeoutSeconds * 1000 / pollIntervalMs;

        for (int i = 0; i < maxAttempts; i++) {
            String clcc = helper.sendAndRead("AT+CLCC", 1000);

            if (clcc != null && clcc.contains("+CLCC:")) {
                // +CLCC: 1,0,0,0,0,"<number>",129
                // state position 3: 0=active, 2=dialing, 3=alerting
                if (clcc.matches(".*,\\d,0,.*")) {
                    log.info("✅ [{}] Call connected after {}s",
                            task.getSim().getComName(), (i * pollIntervalMs) / 1000);
                    return true;
                }
            }

            Thread.sleep(pollIntervalMs);
        }

        log.warn("⚠️ [{}] No answer after {}s", task.getSim().getComName(), timeoutSeconds);
        return false;
    }

    // ============== Recording Methods ==============

    private String startRecording() throws Exception {
        String comPort = task.getSim().getComName();
        String timestamp = String.valueOf(System.currentTimeMillis());
        String modemPath = "call_audio_" + comPort.replace("/dev/tty", "") + "_" + timestamp + ".amr";

        helper.sendAndRead("AT+QAUDCFG=\"record\",1", 500);

        String recordCmd = "AT+QAUDRD=1,\"" + modemPath + "\",3";
        String response = helper.sendAndRead(recordCmd, 3000);

        if (response.contains("OK") && !response.contains("ERROR")) {
            log.info("🎙️ [{}] Recording started: {}", comPort, modemPath);
            return modemPath;
        } else {
            log.warn("⚠️ [{}] Recording failed: {}", comPort, response.trim());
            return null;
        }
    }

    private void stopRecording() throws Exception {
        helper.sendAndRead("AT+QAUDRD=0", 1000);
        Thread.sleep(500);
        log.info("⏹️ [{}] Recording stopped", task.getSim().getComName());
    }

    // ============== Notification ==============

    private void notifyStatus(String status) {
        notifyStatus(status, null, null);
    }

    private void notifyStatus(String status, Integer progress) {
        notifyStatus(status, progress, null);
    }

    private void notifyStatus(String status, Integer progress, String message) {
        try {
            log.debug("[{}] CALL_WITH_AUDIO local status={}, progress={}, message={}",
                    task.getSim().getComName(), status, progress, message);
        } catch (Exception e) {
            log.debug("⚠️ Failed to notify status: {}", e.getMessage());
        }
    }
}
