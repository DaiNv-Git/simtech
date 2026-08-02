package app.simsmartgsm.session;

import app.simsmartgsm.baseGateway.CloudGateway;
import app.simsmartgsm.config.RemoteWsClient;
import app.simsmartgsm.uitils.AtCommandHelper;
import com.fazecast.jSerialComm.SerialPort;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

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
 * ✅ CALL_IN Session - Extract & optimize from PortWorker.IncomingCallSession
 */
@Slf4j
public class CallInSession implements TaskSession {

    private static final int BAUD_RATE = 115200;
    private static final Path RECORD_DIR = Paths.get("/home/record");
    private static final String UPLOAD_URL = "http://localhost:8888/api/file/upload";

    private final CallInTask task;
    private final CloudGateway cloudGateway;
    private final RemoteWsClient remoteWsClient;

    private SerialPort port;
    private AtCommandHelper helper;
    private ScheduledExecutorService scheduler;

    private Instant callStartTime;
    private Instant callEndTime;
    private String callerNumber;

    public CallInSession(CallInTask task, CloudGateway cloudGateway, RemoteWsClient remoteWsClient) {
        this.task = task;
        this.cloudGateway = cloudGateway;
        this.remoteWsClient = remoteWsClient;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void openPort() throws Exception {
        String comPort = task.getSim().getComName();

        // ✅ OPTIMIZATION: Port retry mechanism
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

                // Init modem with retry
                String atResp = helper.sendAndRead("AT", 500);
                if (atResp == null || !atResp.contains("OK")) {
                    throw new IOException("Modem not responding");
                }

                helper.sendAndRead("ATE0", 500);
                helper.sendAndRead("AT+CMEE=2", 500);

                // Enable CLIP (Calling Line Identification Presentation)
                helper.sendAndRead("AT+CLIP=1", 500);

                log.info("✅ [{}] Port opened for CALL_IN (attempt {}) | waiting for: {}",
                        comPort, attempt, task.getExpectedCaller());
                return; // Success

            } catch (Exception e) {
                lastException = e;
                log.warn("⚠️ [{}] Port open failed (attempt {}/{}): {}",
                        comPort, attempt, maxRetries, e.getMessage());

                // Cleanup before retry
                if (port != null && port.isOpen()) {
                    try {
                        port.closePort();
                    } catch (Exception ignored) {
                    }
                }

                if (attempt < maxRetries) {
                    Thread.sleep(500 * attempt); // Exponential backoff
                }
            }
        }

        throw new IOException("Cannot open port after " + maxRetries + " attempts: " +
                (lastException != null ? lastException.getMessage() : "unknown error"));
    }

    @Override
    public SessionResult execute() throws Exception {
        String comPort = task.getSim().getComName();

        try {
            notifyStatus("WAITING_INCOMING_CALL");

            // Wait for RING
            if (!waitForRing()) {
                throw new Exception("Timeout waiting for incoming call");
            }

            callStartTime = Instant.now();

            // Answer call
            notifyStatus("ANSWERING");
            helper.sendAndRead("ATA", 2000);
            log.info("📞 [{}] Answered call from {}", comPort, callerNumber);

            notifyStatus("CONNECTED");

            // Start recording if needed
            String modemRecordPath = null;
            if (task.isRecord()) {
                notifyStatus("RECORDING_START");
                modemRecordPath = startRecording();
            }

            // Configure audio
            helper.sendAndRead("AT+QPCMV=1,8,10000", 1000);

            // Hold call for duration
            Thread.sleep(task.getDurationSeconds() * 1000L);

            // Hangup
            helper.sendAndRead("ATH", 1000);
            callEndTime = Instant.now();

            // Stop recording and download
            String uploadedUrl = null;
            if (task.isRecord() && modemRecordPath != null) {
                notifyStatus("RECORDING_STOPPING");
                stopRecording();

                File localFile = downloadRecording(modemRecordPath);
                if (localFile != null) {
                    uploadedUrl = uploadToServer(localFile);
                }
            }

            // Send to cloud
            cloudGateway.forwardCall(task.getSim(), callerNumber, uploadedUrl);

            notifyStatus("SUCCESS");

            long duration = (callEndTime.toEpochMilli() - callStartTime.toEpochMilli()) / 1000;

            return SessionResult.builder()
                    .orderId(task.getOrderId())
                    .taskType("CALL_IN")
                    .status("SUCCESS")
                    .recordFileUrl(uploadedUrl)
                    .startTime(callStartTime)
                    .endTime(callEndTime)
                    .totalDurationSeconds(duration)
                    .connectedDurationSeconds(duration)
                    .build();

        } catch (Exception e) {
            log.error("❌ [{}] CALL_IN failed: {}", comPort, e.getMessage(), e);

            notifyStatus("FAILED");

            return SessionResult.failed(task.getOrderId(), "CALL_IN", e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
        } catch (Exception ignored) {
        }

        try {
            if (port != null && port.isOpen()) {
                port.closePort();
                log.info("🔒 [{}] Port closed", task.getSim().getComName());
            }
        } catch (Exception e) {
            log.warn("⚠️ Error closing port: {}", e.getMessage());
        }
    }

    // ============== Helper Methods ==============

    private boolean waitForRing() throws Exception {
        String comPort = task.getSim().getComName();
        long deadline = System.currentTimeMillis() + (task.getTimeWindowSeconds() * 1000L);

        InputStream in = port.getInputStream();
        StringBuilder buffer = new StringBuilder();

        while (System.currentTimeMillis() < deadline) {
            int available = in.available();
            if (available > 0) {
                // ✅ OPTIMIZATION: Read multiple bytes at once
                for (int i = 0; i < available && i < 100; i++) {
                    char c = (char) in.read();
                    buffer.append(c);
                }
                String text = buffer.toString();

                // Check for RING
                if (text.contains("RING")) {
                    notifyStatus("RINGING");
                    log.info("📞 [{}] RING detected", comPort);

                    // ✅ OPTIMIZATION: Enhanced CLIP parsing
                    notifyStatus("ANALYZING_CALLER");
                    callerNumber = extractCallerNumber(text);

                    if (callerNumber == null || callerNumber.equals("UNKNOWN")) {
                        // Hidden number
                        if (!task.isAcceptHidden()) {
                            log.warn("⚠️ [{}] Rejected hidden number call", comPort);
                            helper.sendAndRead("ATH", 500);
                            buffer.setLength(0);
                            continue;
                        }
                        callerNumber = "HIDDEN";
                    } else {
                        // Check if caller matches expected
                        if (task.getExpectedCaller() != null && !task.getExpectedCaller().isEmpty()) {
                            if (!isCallerMatch(callerNumber, task.getExpectedCaller())) {
                                if (!task.isAcceptHidden()) {
                                    log.warn("⚠️ [{}] Rejected mismatched caller: {} (expected: {})",
                                            comPort, callerNumber, task.getExpectedCaller());

                                    // ✅ NEW: Notify about mismatch so user knows WHY it didn't answer
                                    notifyStatus("MISMATCHED_CALLER", null, "Caller " + callerNumber
                                            + " does not match expected " + task.getExpectedCaller());

                                    helper.sendAndRead("ATH", 500);
                                    buffer.setLength(0);
                                    continue;
                                }
                            }
                        }
                    }

                    log.info("📞 [{}] Accepted call from: {}", comPort, callerNumber);
                    return true;
                }

                // Keep buffer manageable
                if (buffer.length() > 500) {
                    buffer.delete(0, buffer.length() - 200);
                }
            } else {
                Thread.sleep(20); // ✅ OPTIMIZATION: Reduced from 50ms
            }
        }

        log.warn("⏰ [{}] Timeout waiting for RING ({}s)", comPort, task.getTimeWindowSeconds());
        return false;
    }

    // ✅ OPTIMIZATION: Robust CLIP parsing
    private String extractCallerNumber(String text) {
        log.debug("🔍 [{}] Parsing CLIP from text: {}", task.getSim().getComName(), text);

        // Pattern 1: +CLIP: "+84..."
        Pattern p1 = Pattern.compile("\\+CLIP:\\s*\"([^\"]+)\"");
        Matcher m1 = p1.matcher(text);
        if (m1.find()) {
            return normalizePhoneNumber(m1.group(1));
        }

        // Pattern 2: +CLIP: "0..." (some modems don't include quotes or use different
        // format)
        Pattern p2 = Pattern.compile("\\+CLIP:\\s*\"?(\\+?\\d+)\"?");
        Matcher m2 = p2.matcher(text);
        if (m2.find()) {
            return normalizePhoneNumber(m2.group(1));
        }

        return "UNKNOWN";
    }

    private String normalizePhoneNumber(String phone) {
        if (phone == null)
            return null;

        // Remove all non-digits except +
        phone = phone.replaceAll("[^0-9+]", "");

        // Convert various international formats to local 0xxx
        if (phone.startsWith("+84")) {
            phone = "0" + phone.substring(3);
        } else if (phone.startsWith("84") && phone.length() > 9) {
            phone = "0" + phone.substring(2);
        } else if (phone.startsWith("0084")) {
            phone = "0" + phone.substring(4);
        }

        return phone;
    }

    private boolean isCallerMatch(String actual, String expected) {
        if (actual == null || expected == null)
            return false;

        String a = normalizePhoneNumber(actual);
        String e = normalizePhoneNumber(expected);

        // Exact match
        if (a.equals(e))
            return true;

        // Contains match (handle partial numbers)
        if (a.contains(e) || e.contains(a))
            return true;

        // Last 9 digits match (ignore country code)
        if (a.length() >= 9 && e.length() >= 9) {
            String aLast9 = a.substring(a.length() - 9);
            String eLast9 = e.substring(e.length() - 9);
            if (aLast9.equals(eLast9))
                return true;
        }

        return false;
    }

    private String startRecording() throws Exception {
        String comPort = task.getSim().getComName();
        String timestamp = String.valueOf(System.currentTimeMillis());
        String baseFilename = "callin_" + comPort.replace("COM", "").replace("/dev/tty", "") + "_" + timestamp;

        try {
            // ✅ Check current recording status and stop if active
            String currentStatus = helper.sendAndRead("AT+QAUDRD?", 500);
            log.info("📊 [{}] Current recording status: {}", comPort, currentStatus.trim());
            if (currentStatus.contains("+QAUDRD: 1")) {
                log.info("⚠️ [{}] Recording already active, stopping first...", comPort);
                helper.sendAndRead("AT+QAUDRD=0", 1000);
                Thread.sleep(500);
            }

            // ✅ Identify modem type
            String modemInfo = helper.sendAndRead("ATI", 1000).toUpperCase();
            boolean isEC2x = modemInfo.contains("EC25") || modemInfo.contains("EC21");
            log.info("📱 [{}] Modem Identification: {} (isEC2x: {})", comPort, modemInfo.trim(), isEC2x);

            if (isEC2x) {
                // ✅ Configure EC25 audio path
                log.info("🔊 [{}] Configuring EC25 digital audio path...", comPort);
                helper.sendAndRead("AT+QAUDCFG=\"record\",1", 500);
                helper.sendAndRead("AT+QAUDCFG=\"audiopath\",0", 500);
                helper.sendAndRead("AT+QSIDET=0", 500);
                helper.sendAndRead("AT+QMIC=0,10", 500);
            } else {
                log.info("🔊 [{}] Configuring legacy audio channel...", comPort);
                helper.sendAndRead("AT+QAUDCH=0", 500);
            }

            // ✅ Pre-delete old files
            String[] extensions = { ".wav", ".amr" };
            log.info("🗑️ [{}] Checking and deleting existing recording files before start...", comPort);
            for (String ext : extensions) {
                helper.sendAndRead("AT+QFDEL=\"UFS:" + baseFilename + ext + "\"", 300);
            }

            if (isEC2x) {
                // ✅ EC25: Try PCM first, then AMR
                int[] ec2xFormats = { 13, 3 };
                String[] ec2xExt = { ".wav", ".amr" };

                // ✅ FIRST: Try 4-parameter syntax (format + channel)
                for (int i = 0; i < ec2xFormats.length; i++) {
                    String modemPath = baseFilename + ec2xExt[i];
                    // ✅ CRITICAL: EC25 needs 4 parameters - format and channel=2 for mixed audio
                    String recordCmd = String.format("AT+QAUDRD=1,\"%s\",%d,2", modemPath, ec2xFormats[i]);
                    log.info("🎙️ [{}] EC25 recording attempt (Mixed): {}", comPort, recordCmd);

                    String response = helper.sendAndRead(recordCmd, 3000);
                    if (response.contains("OK") && !response.contains("ERROR")) {
                        log.info("✅ [{}] EC25 Recording STARTED (format={}, channel=2)", comPort, ec2xFormats[i]);
                        return modemPath;
                    } else {
                        log.warn("⚠️ [{}] EC25 format {} (4-param) failed: {}", comPort, ec2xFormats[i],
                                response.replace("\r", " ").replace("\n", " ").trim());
                    }
                }

                // ✅ FALLBACK: Try 3-parameter syntax
                log.info("🔄 [{}] Trying 3-parameter syntax (no channel)...", comPort);

                // ✅ Configure audio gains for 3-param mode to prioritize downlink (remote
                // party)
                log.info("🔊 [{}] Configuring audio for downlink priority (remote voice only)...", comPort);
                try {
                    helper.sendAndRead("AT+QAUDCFG=\"rxgain\",0,15", 500); // Max RX (remote)
                    helper.sendAndRead("AT+QAUDCFG=\"txgain\",0,0", 500); // Min TX (local)
                    log.info("✅ [{}] Audio configured: RX=15 (max), TX=0 (muted)", comPort);
                } catch (Exception e) {
                    log.warn("⚠️ [{}] Failed to configure audio gains: {}", comPort, e.getMessage());
                }

                for (int i = 0; i < ec2xFormats.length; i++) {
                    String modemPath = baseFilename + ec2xExt[i];
                    String recordCmd = String.format("AT+QAUDRD=1,\"%s\",%d", modemPath, ec2xFormats[i]);
                    log.info("🎙️ [{}] EC25 recording attempt (3-param): {}", comPort, recordCmd);

                    String response = helper.sendAndRead(recordCmd, 3000);
                    if (response.contains("OK") && !response.contains("ERROR")) {
                        log.info("✅ [{}] EC25 Recording STARTED (format={}, 3-param, downlink-only)", comPort,
                                ec2xFormats[i]);
                        return modemPath;
                    } else {
                        log.warn("⚠️ [{}] EC25 format {} (3-param) failed: {}", comPort, ec2xFormats[i],
                                response.replace("\r", " ").replace("\n", " ").trim());
                    }
                }

                log.error("❌ [{}] EC25 recording failed with all formats.", comPort);
                return null;
            } else {
                // ✅ Legacy GSM: Try AMR
                String modemPath = baseFilename + ".amr";
                String recordCmd = String.format("AT+QAUDRD=1,\"%s\",3", modemPath);
                log.info("🎙️ [{}] Legacy recording attempt: {}", comPort, recordCmd);

                String response = helper.sendAndRead(recordCmd, 3000);
                if (response.contains("OK") && !response.contains("ERROR")) {
                    log.info("✅ [{}] Legacy Recording STARTED successfully", comPort);
                    return modemPath;
                }

                log.error("❌ [{}] Failed to start recording", comPort);
                return null;
            }

        } catch (Exception e) {
            log.error("❌ [{}] Error starting recording: {}", comPort, e.getMessage(), e);
            return null;
        }
    }

    private void stopRecording() throws Exception {
        helper.sendAndRead("AT+QAUDRD=0", 1000);
        Thread.sleep(500);
        log.info("⏹️ [{}] Recording stopped", task.getSim().getComName());
    }

    private File downloadRecording(String modemPath) throws Exception {
        log.info("📥 [{}] Downloading recording: {}", task.getSim().getComName(), modemPath);

        // Get file size
        String listResp = helper.sendAndRead("AT+QFLST", 2000);
        Pattern sizePattern = Pattern.compile("\"" + Pattern.quote(modemPath) + "\",(\\d+)");
        Matcher m = sizePattern.matcher(listResp);

        if (!m.find()) {
            log.warn("⚠️ Recording file not found on modem");
            return null;
        }

        long fileSize = Long.parseLong(m.group(1));

        // Open file
        String openResp = helper.sendAndRead("AT+QFOPEN=\"" + modemPath + "\",0", 2000);
        Pattern handlePattern = Pattern.compile("\\+QFOPEN:\\s*(\\d+)");
        Matcher hm = handlePattern.matcher(openResp);

        if (!hm.find()) {
            log.error("❌ Cannot open modem file");
            return null;
        }

        int handle = Integer.parseInt(hm.group(1));

        // Read file
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        long bytesRead = 0;
        int chunkSize = 2048; // ✅ Increased chunk size for faster download
        int lastProgress = -1;

        while (bytesRead < fileSize) {
            int toRead = (int) Math.min(chunkSize, fileSize - bytesRead);
            byte[] chunk = readChunk(handle, toRead);
            if (chunk == null)
                break;
            data.write(chunk);
            bytesRead += chunk.length;

            // ✅ OPTIMIZATION: Notify download progress
            int progress = (int) (bytesRead * 100 / fileSize);
            if (progress / 10 > lastProgress / 10) { // Notify every 10%
                notifyStatus("DOWNLOADING", progress,
                        "Downloaded " + (bytesRead / 1024) + "KB/" + (fileSize / 1024) + "KB");
                lastProgress = progress;
            }
        }

        // Close file
        helper.sendAndRead("AT+QFCLOSE=" + handle, 1000);

        // Save to local file
        Files.createDirectories(RECORD_DIR);
        String localFilename = "callin_" + task.getOrderId() + "_" + System.currentTimeMillis() + ".amr";
        File localFile = RECORD_DIR.resolve(localFilename).toFile();
        Files.write(localFile.toPath(), data.toByteArray());

        log.info("✅ [{}] Downloaded {} bytes to {}", task.getSim().getComName(), bytesRead, localFile.getName());
        notifyStatus("DOWNLOAD_COMPLETE", 100);

        return localFile;
    }

    private byte[] readChunk(int handle, int size) throws Exception {
        helper.send("AT+QFREAD=" + handle + "," + size);

        // Wait for CONNECT
        long deadline = System.currentTimeMillis() + 3000;
        InputStream in = port.getInputStream();
        StringBuilder buf = new StringBuilder();

        while (System.currentTimeMillis() < deadline) {
            int available = in.available();
            if (available > 0) {
                for (int i = 0; i < available && i < 100; i++) {
                    char c = (char) in.read();
                    buf.append(c);
                }
                if (buf.toString().contains("CONNECT")) {
                    break;
                }
            } else {
                Thread.sleep(5); // ✅ OPTIMIZATION: Reduced from 10ms
            }
        }

        // ✅ OPTIMIZATION: Read with minimal sleep
        byte[] data = new byte[size];
        int offset = 0;
        deadline = System.currentTimeMillis() + 10000;

        while (offset < size && System.currentTimeMillis() < deadline) {
            int available = in.available();
            if (available > 0) {
                int toRead = Math.min(available, size - offset);
                int read = in.read(data, offset, toRead);
                if (read > 0) {
                    offset += read;
                }
            } else {
                Thread.sleep(2); // ✅ OPTIMIZATION: Reduced from 10ms to 2ms
            }
        }

        // Consume OK
        Thread.sleep(200); // ✅ OPTIMIZATION: Reduced from 500ms
        helper.readAll();

        return offset == size ? data : null;
    }

    private String uploadToServer(File file) {
        // ✅ OPTIMIZATION: Upload retry with exponential backoff
        int maxRetries = 3;
        int[] delayMs = { 1000, 3000, 5000 }; // Exponential backoff

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
                body.add("file", new FileSystemResource(file));
                body.add("simNumber", task.getSim().getPhoneNumber());
                body.add("orderId", task.getOrderId());
                body.add("serviceCode", task.getServiceCode());

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.MULTIPART_FORM_DATA);

                HttpEntity<MultiValueMap<String, Object>> request = new HttpEntity<>(body, headers);
                RestTemplate restTemplate = new RestTemplate();

                notifyStatus("UPLOADING", (attempt * 30) + 10);
                ResponseEntity<Map> response = restTemplate.postForEntity(UPLOAD_URL, request, Map.class);

                if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                    String url = (String) response.getBody().get("url");
                    log.info("☁️ [{}] Uploaded (attempt {}): {}",
                            task.getSim().getComName(), attempt + 1, url);

                    notifyStatus("UPLOAD_SUCCESS", 100);
                    // Delete local file only after successful upload
                    Files.deleteIfExists(file.toPath());
                    return url;
                }
            } catch (Exception e) {
                log.warn("⚠️ [{}] Upload failed (attempt {}/{}): {}",
                        task.getSim().getComName(), attempt + 1, maxRetries, e.getMessage());

                if (attempt < maxRetries - 1) {
                    try {
                        Thread.sleep(delayMs[attempt]);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        // ✅ Keep file for manual recovery if upload fails
        log.error("❌ [{}] Upload failed after {} retries, keeping file: {}",
                task.getSim().getComName(), maxRetries, file.getAbsolutePath());
        return null;
    }

    private void notifyStatus(String status) {
        notifyStatus(status, null, null);
    }

    private void notifyStatus(String status, Integer progress) {
        notifyStatus(status, progress, null);
    }

    private void notifyStatus(String status, Integer progress, String message) {
        try {
            Map<String, Object> event = new HashMap<>();
            event.put("orderId", task.getOrderId());
            event.put("service", task.getServiceCode());
            event.put("status", status);
            event.put("deviceName", task.getSim().getDeviceName());
            event.put("phone", callerNumber);
            event.put("fromNumber", callerNumber);
            event.put("com", task.getSim().getComName());
            event.put("timestamp", Instant.now().toEpochMilli());

            if (progress != null) {
                event.put("progress", progress);
            }
            if (message != null) {
                event.put("message", message);
            }

            if (remoteWsClient != null && remoteWsClient.isConnected()) {
                remoteWsClient.send("/topic/receive-call", event);
            }
        } catch (Exception e) {
            log.debug("⚠️ Failed to notify status: {}", e.getMessage());
        }
    }
}
