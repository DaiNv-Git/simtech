package app.simsmartgsm.service;

import app.simsmartgsm.baseGateway.CloudGateway;
import app.simsmartgsm.config.ComManager;
import app.simsmartgsm.config.PortResolver;
import app.simsmartgsm.entity.CallRecord;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.CallRecordRepository;
import app.simsmartgsm.uitils.AtCommandHelper;
import app.simsmartgsm.uitils.PortWorker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.*;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Service
@RequiredArgsConstructor
@Slf4j
public class CallService {

    private final CallRecordRepository callRecordRepository;
    private final SimpMessagingTemplate messagingTemplate;
    private final CloudGateway cloudGateway;
    private final PortResolver portResolver;
    private final ComManager comManager;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    // ==============================
    // 🔧 Task Tracking - Prevent thread leaks
    // ==============================
    /** Track scheduled tasks per call for proper cleanup */
    private final java.util.Map<String, java.util.List<ScheduledFuture<?>>> callTasks = new java.util.concurrent.ConcurrentHashMap<>();

    // ==============================
    // 🔧 Config từ application.yml
    // ==============================

    /** Thư mục lưu file ghi âm tạm */
    @Value("${gsm.record.upload-dir:/var/www/html/recordings}")
    private String uploadDir;

    /** API đích để upload file lên cloud */
    @Value("${gsm.record.upload-url}")
    private String uploadUrl;

    /** URL public để nghe lại file */
    @Value("${gsm.record.public-url}")
    private String publicUrl;

    // ==============================
    // 📞 Gọi và xử lý ghi âm
    // ==============================
    public CallRecord makeCall(Sim sim, String targetNumber, boolean record, int callTimeSec) {
        log.info("📞 Yêu cầu gọi từ {} (COM={}) → {} | record={} | callTime={}s",
                sim.getPhoneNumber(), sim.getComName(), targetNumber, record, callTimeSec);

        CallRecord recordEntity = new CallRecord();
        recordEntity.setSimPhone(sim.getPhoneNumber());
        recordEntity.setFromNumber(sim.getPhoneNumber());
        recordEntity.setDeviceName(sim.getDeviceName());
        recordEntity.setComPort(sim.getComName());
        recordEntity.setStatus("PENDING");
        recordEntity.setCallStartTime(Instant.now());
        recordEntity.setCreatedAt(Instant.now());
        recordEntity.setUpdatedAt(Instant.now());
        callRecordRepository.save(recordEntity);

        try {
            // Nếu worker đã có sẵn thì gọi qua queue
            PortWorker worker = comManager.getWorker(sim.getComName());
            if (worker != null && worker.isOpen()) {
                log.info("☎️ [CALL] Gửi CALL task vào worker {}", sim.getComName());
                worker.enqueue(PortWorker.Task.call(targetNumber, callTimeSec, record, "CALL", recordEntity.getId()));
                recordEntity.setStatus("QUEUED");
                recordEntity.setUpdatedAt(Instant.now());
                callRecordRepository.save(recordEntity);
                return recordEntity;
            }

            // Nếu chưa có worker → fallback gọi trực tiếp
            log.warn("⚠️ Worker chưa sẵn sàng → fallback gọi trực tiếp trên {}", sim.getComName());
            directCall(sim, targetNumber, record, callTimeSec, recordEntity);

        } catch (Exception e) {
            log.error("❌ Lỗi makeCall: {}", e.getMessage(), e);
            recordEntity.setStatus("FAILED");
            recordEntity.setUpdatedAt(Instant.now());
            callRecordRepository.save(recordEntity);
        }

        return recordEntity;
    }

    // ==============================
    // 🧩 Fallback: Gọi trực tiếp qua AT
    // ==============================
    private void directCall(Sim sim, String targetNumber, boolean record, int callTimeSec, CallRecord recordEntity) {
        // ✅ Track tasks for this call
        String taskId = recordEntity.getId().toString();
        java.util.List<ScheduledFuture<?>> tasks = new java.util.ArrayList<>();

        AtCommandHelper helper = null;
        AtomicBoolean connected = new AtomicBoolean(false);
        AtomicBoolean callEnded = new AtomicBoolean(false);

        // ✅ Use array holder for effectively final variables
        final AtomicReference<ScheduledFuture<?>> monitorHolder = new AtomicReference<>();
        final String[] recordFileHolder = new String[1];

        try {
            String resolvedPort = portResolver.resolve(sim.getComName());
            if (resolvedPort == null)
                throw new IOException("Cannot resolve port mapping for " + sim.getComName());

            // ✅ FIX: Lock port để ngăn recovery job và scan job mở lại port
            comManager.lockPort(sim.getComName());

            // ✅ FIX: LUÔN stop worker và đợi thread kết thúc
            boolean workerStopped = comManager.stopWorkerAndWait(sim.getComName(), 3000);
            if (!workerStopped) {
                log.warn("⚠️ Worker thread {} chưa thoát hoàn toàn sau 3s", sim.getComName());
            }
            Thread.sleep(300);

            // ✅ FIX: Retry opening port with smart delays
            // AtCommandHelper.open() already has retry, but we add extra check here
            int maxOpenAttempts = 3;
            Exception lastException = null;

            for (int attempt = 1; attempt <= maxOpenAttempts; attempt++) {
                try {
                    log.info("🔓 Opening port: {} (attempt {}/{})", resolvedPort, attempt, maxOpenAttempts);
                    helper = AtCommandHelper.open(resolvedPort, 115200, 4000, 2000);
                    log.info("✅ Port opened: {}", resolvedPort);
                    break; // Success
                } catch (IOException e) {
                    lastException = e;
                    log.warn("⚠️ Failed to open port {} (attempt {}/{}): {}",
                            resolvedPort, attempt, maxOpenAttempts, e.getMessage());

                    if (attempt < maxOpenAttempts) {
                        // Exponential backoff: 200ms, 400ms, 800ms
                        long delayMs = 200L * (1L << (attempt - 1));
                        log.info("⏳ Waiting {}ms before retry...", delayMs);
                        Thread.sleep(delayMs);
                    }
                }
            }

            if (helper == null) {
                throw new IOException("Cannot open port " + resolvedPort + " after " + maxOpenAttempts + " attempts: " +
                        (lastException != null ? lastException.getMessage() : "unknown error"));
            }
            helper.sendAndRead("AT", 300);
            helper.sendAndRead("ATE0", 300);
            helper.sendAndRead("AT+CLIP=1", 300);
            helper.sendAndRead("AT+CRC=1", 300);

            if (record) {
                Files.createDirectories(Paths.get(uploadDir));
                recordFileHolder[0] = new File(uploadDir,
                        "call_" + targetNumber + "_" + System.currentTimeMillis() + ".amr")
                        .getAbsolutePath();
                startRecording(helper, recordFileHolder[0]);
            }

            // --- THỰC HIỆN DIAL ---
            String resp = helper.sendAndRead("ATD" + targetNumber + ";", 1000);
            log.info("☎️ [DIRECT] DIAL RESP: {}", resp);

            // --- CHECK LỖI NGAY LẬP TỨC ---
            if (resp.contains("BUSY")) {
                endCall(sim, helper, recordEntity, recordFileHolder[0], targetNumber, record, "BUSY", taskId);
                return;
            }
            if (resp.contains("NO CARRIER")) {
                endCall(sim, helper, recordEntity, recordFileHolder[0], targetNumber, record, "NO_ANSWER", taskId);
                return;
            }
            if (resp.contains("NO DIALTONE")) {
                endCall(sim, helper, recordEntity, recordFileHolder[0], targetNumber, record, "FAILED", taskId);
                return;
            }

            // --- UPDATE trạng thái DB ---
            recordEntity.setStatus("ONGOING");
            recordEntity.setUpdatedAt(Instant.now());
            callRecordRepository.save(recordEntity);

            // ✅ Helper references for lambda
            AtCommandHelper finalHelper1 = helper;

            // --- BẮT ĐẦU POLL CLCC ĐỂ XÁC ĐỊNH CONNECT ---
            ScheduledFuture<?> monitor = scheduler.scheduleAtFixedRate(() -> {
                if (callEnded.get())
                    return;

                try {
                    String clcc = finalHelper1.sendAndRead("AT+CLCC", 500);
                    if (clcc.contains("+CLCC")) {
                        if (clcc.contains(",0,")) { // 0 = ACTIVE
                            if (connected.compareAndSet(false, true)) {
                                log.info("📞 [DIRECT] CALL CONNECTED {}", targetNumber);
                            }
                        }
                    } else if (!clcc.trim().isEmpty() && !clcc.contains("OK")) {
                        // ✅ EARLY TERMINATION DETECTION: Call dropped but response not empty
                        if (callEnded.compareAndSet(false, true)) {
                            log.info("📞 [DIRECT] Call dropped detected, ending early");
                            // Cancel stop task
                            ScheduledFuture<?> monitorTask = monitorHolder.get();
                            tasks.forEach(t -> {
                                if (t != monitorTask && !t.isDone()) {
                                    t.cancel(true);
                                }
                            });
                            // End call with current status
                            String finalStatus = connected.get() ? "SUCCESS" : "NO_ANSWER";
                            endCall(sim, finalHelper1, recordEntity,
                                    recordFileHolder[0], targetNumber, record, finalStatus, taskId);
                        }
                    }
                } catch (Exception ignore) {
                }
            }, 0, 1, TimeUnit.SECONDS);

            monitorHolder.set(monitor);
            tasks.add(monitor);

            // --- LỊCH DỪNG CUỆC GỌI ---
            AtCommandHelper finalHelper = helper;

            ScheduledFuture<?> stopTask = scheduler.schedule(() -> {
                callEnded.set(true);
                monitor.cancel(true);

                String finalStatus = connected.get() ? "SUCCESS" : "NO_ANSWER";

                endCall(sim, finalHelper, recordEntity, recordFileHolder[0], targetNumber, record, finalStatus, taskId);

            }, callTimeSec, TimeUnit.SECONDS);

            tasks.add(stopTask);

            // ✅ Store tasks for cleanup
            callTasks.put(taskId, tasks);

        } catch (Exception e) {
            log.error("❌ Lỗi khi gọi trực tiếp: {}", e.getMessage(), e);
            // ✅ FIX: Update status to FAILED on exception
            recordEntity.setStatus("FAILED");
            recordEntity.setCallEndTime(Instant.now());
            recordEntity.setUpdatedAt(Instant.now());
            callRecordRepository.save(recordEntity);

            // ✅ FIX: Notify client about failure
            try {
                messagingTemplate.convertAndSend("/topic/call-status", java.util.Map.of(
                        "id", recordEntity.getId(),
                        "sim", sim.getPhoneNumber(),
                        "com", sim.getComName(),
                        "target", targetNumber,
                        "status", "FAILED",
                        "error", e.getMessage(),
                        "timestamp", Instant.now().toString()));
            } catch (Exception notifyEx) {
                log.warn("⚠️ Failed to send failure notification: {}", notifyEx.getMessage());
            }

            // ✅ Cleanup tracked tasks
            tasks.forEach(t -> t.cancel(true));
            callTasks.remove(taskId);

            // ✅ FIX: Always close helper, even on exception
            closeHelper(helper);
            helper = null; // Clear reference to prevent reuse
        }
    }

    // ==============================
    // 🛑 Dừng cuộc gọi & upload record
    // ==============================
    private void endCall(Sim sim, AtCommandHelper helper, CallRecord call,
            String recordFile, String target, boolean record, String finalStatus, String taskId) {
        try {
            if (helper != null) {
                helper.sendAndRead("ATH", 500);
                log.info("🛑 Kết thúc cuộc gọi trên {}", sim.getComName());
            }

            String uploadedUrl = null;
            if (recordFile != null) {
                stopRecording(helper);
                uploadedUrl = uploadRecordFile(recordFile);
            }

            // 🎯 Set trạng thái đúng theo thực tế
            String status;
            switch (finalStatus) {
                case "SUCCESS" -> status = "SUCCESS";
                case "NO_ANSWER" -> status = "NO_ANSWER";
                case "BUSY" -> status = "BUSY";
                case "REJECTED" -> status = "REJECTED";
                case "NOT_REACHABLE" -> status = "NOT_REACHABLE";
                default -> status = "FAILED";
            }

            call.setStatus(status);
            call.setRecordFile(uploadedUrl);
            call.setCallEndTime(Instant.now());
            call.setUpdatedAt(Instant.now());
            callRecordRepository.save(call);

            // ⛳ Gửi socket realtime
            messagingTemplate.convertAndSend("/topic/call-status", Map.of(
                    "id", call.getId(),
                    "sim", sim.getPhoneNumber(),
                    "com", sim.getComName(),
                    "target", target,
                    "recordUrl", uploadedUrl,
                    "status", status,
                    "duration", call.getCallEndTime().getEpochSecond()
                            - call.getCallStartTime().getEpochSecond(),
                    "timestamp", Instant.now().toString()));

            // ☁️ Callback Cloud
            cloudGateway.sendCallRecord(
                    sim,
                    target,
                    call.getCallStartTime(),
                    call.getCallEndTime(),
                    uploadedUrl,
                    call.getOrderId());

        } catch (Exception e) {
            log.error("❌ Lỗi khi dừng call: {}", e.getMessage());
            call.setStatus("FAILED");
            call.setUpdatedAt(Instant.now());
            callRecordRepository.save(call);

            cloudGateway.sendCallEvent(
                    "FAILED", sim, target,
                    call.getCallStartTime(), Instant.now(),
                    call.getOrderId(), record, e.getMessage());
        } finally {
            // ✅ Cleanup tracked tasks
            if (taskId != null) {
                callTasks.remove(taskId);
            }
            // ✅ FIX: Always close helper in finally block to ensure port is released
            closeHelper(helper);
            helper = null; // Clear reference to prevent reuse

            // ✅ FIX: LUÔN unlock port trước tiên
            if (sim != null) {
                comManager.unlockPort(sim.getComName());
            }

            // ✅ FIX: Khởi động lại PortWorker sau khi call xong để lắng nghe URC
            // Điều này đảm bảo hệ thống vẫn nhận được cuộc gọi đến và SMS sau khi call xong
            try {
                // Đợi một chút để port được giải phóng hoàn toàn
                Thread.sleep(300);

                // Kiểm tra xem SIM có còn ACTIVE không
                if (sim != null && "ACTIVE".equals(sim.getStatus())) {
                    // Kiểm tra xem worker đã đang chạy chưa
                    PortWorker existingWorker = comManager.getWorker(sim.getComName());
                    if (existingWorker == null || !existingWorker.isRunning()) {
                        log.info("🔄 Khởi động lại PortWorker cho {} để lắng nghe URC sau khi call xong",
                                sim.getComName());
                        comManager.startWorker(sim);
                    } else {
                        log.debug("✅ PortWorker cho {} đã đang chạy, không cần khởi động lại",
                                sim.getComName());
                    }
                }
            } catch (Exception e) {
                log.warn("⚠️ Không thể khởi động lại PortWorker sau khi call xong: {}", e.getMessage());
            }
        }
    }

    // ==============================
    // 🎙️ Recording Helper
    // ==============================
    private void startRecording(AtCommandHelper helper, String path) {
        try {
            String fileName = new File(path).getName();
            helper.sendAndRead("AT+CREC=1,\"" + fileName + "\"", 300);
            log.info("🎙️ Bắt đầu ghi âm: {}", fileName);
        } catch (Exception e) {
            log.error("❌ Không thể bắt đầu ghi âm: {}", e.getMessage());
        }
    }

    private void stopRecording(AtCommandHelper helper) {
        try {
            if (helper != null)
                helper.sendAndRead("AT+CREC=0", 300);
            log.info("🛑 Dừng ghi âm");
        } catch (Exception e) {
            log.error("❌ Lỗi khi dừng ghi âm: {}", e.getMessage());
        }
    }

    // ==============================
    // ☁️ Upload File Ghi Âm lên Cloud
    // ==============================
    private String uploadRecordFile(String localPath) {
        try {
            File file = new File(localPath);
            if (!file.exists() || file.length() == 0)
                throw new IllegalStateException("File không tồn tại hoặc trống: " + localPath);

            log.info("☁️ Upload file {} lên {}", file.getName(), uploadUrl);

            RestTemplate restTemplate = new RestTemplate();
            MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
            body.add("file", new FileSystemResource(file));

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.MULTIPART_FORM_DATA);

            HttpEntity<MultiValueMap<String, Object>> request = new HttpEntity<>(body, headers);
            ResponseEntity<Map> response = restTemplate.exchange(uploadUrl, HttpMethod.POST, request, Map.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                Object url = response.getBody() != null ? response.getBody().get("url") : null;
                String publicLink = url != null ? url.toString() : publicUrl + file.getName();
                log.info("✅ Upload thành công: {}", publicLink);
                return publicLink;
            } else {
                log.warn("⚠️ Upload thất bại: HTTP {}", response.getStatusCode());
                return null;
            }

        } catch (Exception e) {
            log.error("❌ Upload record thất bại: {}", e.getMessage(), e);
            return null;
        }
    }

    private void closeHelper(AtCommandHelper helper) {
        try {
            if (helper != null) {
                log.debug("🔒 Closing AtCommandHelper...");
                helper.close();
                // ✅ FIX: Reduced delay - AtCommandHelper.close() already has delay
                // This prevents "port busy" error on next call
                Thread.sleep(100);
                log.debug("✅ AtCommandHelper closed");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("⚠️ Interrupted while closing helper");
        } catch (Exception e) {
            log.warn("⚠️ Error closing helper: {}", e.getMessage());
        }
    }

    // ==============================
    // ♻️ CLEANUP - Prevent thread leaks
    // ==============================
    @jakarta.annotation.PreDestroy
    public void cleanup() {
        try {
            log.info("🧹 Shutting down CallService...");

            // ✅ Cancel all tracked call tasks first
            if (!callTasks.isEmpty()) {
                log.info("🧹 Cancelling {} tracked call tasks...", callTasks.size());
                int cancelledCount = 0;
                for (java.util.List<ScheduledFuture<?>> tasks : callTasks.values()) {
                    for (ScheduledFuture<?> task : tasks) {
                        if (!task.isDone()) {
                            task.cancel(true);
                            cancelledCount++;
                        }
                    }
                }
                callTasks.clear();
                log.info("✅ Cancelled {} tracked tasks", cancelledCount);
            }

            // Shutdown scheduler
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.warn("⚠️ Scheduler did not terminate in time, forcing shutdown");
                    java.util.List<Runnable> pending = scheduler.shutdownNow();
                    log.warn("⚠️ Cancelled {} pending tasks", pending.size());
                }
            }

            log.info("✅ CallService cleanup complete");
        } catch (Exception e) {
            log.error("❌ Error shutting down CallService", e);
        }
    }
}
