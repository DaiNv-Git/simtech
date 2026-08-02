package app.simsmartgsm.service;

import app.simsmartgsm.dto.request.AnswerCallRequest;
import app.simsmartgsm.dto.request.BulkSmsRequest;
import app.simsmartgsm.dto.request.CallWithAudioRequest;
import app.simsmartgsm.dto.request.MakeCallRequest;
import app.simsmartgsm.dto.request.SendSmsRequest;
import app.simsmartgsm.dto.response.SimResponse;
import app.simsmartgsm.uitils.PortWorker;
import app.simsmartgsm.entity.CallRecordEntity;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.entity.SmsMessageEntity;
import app.simsmartgsm.repository.CallRecordJpaRepository;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.repository.SmsMessageJpaRepository;
import app.simsmartgsm.uitils.DeviceIdProvider;
import app.simsmartgsm.uitils.SmsDecoder;
import com.fazecast.jSerialComm.SerialPort;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.awt.Desktop;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

/**
 * Service chính xử lý tất cả logic GSM
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class GsmService {

    private final SmsMessageJpaRepository smsRepository;
    private final CallRecordJpaRepository callRepository;
    private final SimpMessagingTemplate messagingTemplate;
    private final SimSyncService simSyncService;
    private final SimRepository simRepository;
    private final app.simsmartgsm.config.ComManager comManager;
    private final app.simsmartgsm.config.PortResolver portResolver;
    private final SimCleanupService simCleanupService;
    private final SmsDailyLimitService smsDailyLimitService;

    private final app.simsmartgsm.repository.CallMessageRepository callMessageRepository;

    @Value("${gsm.recording-path:./recordings}")
    private String defaultRecordingPath;

    @Value("${gsm.default-call-duration:10}")
    private int defaultCallDuration;

    @Value("${gsm.auto-scan-on-startup:true}")
    private boolean autoScanOnStartup;

    // Cache danh sách SIM online
    private final Map<String, SimResponse> onlineSims = new ConcurrentHashMap<>();

    /**
     * ✅ Getter cho SimSyncService - dùng từ Controller
     */
    public SimSyncService getSimSyncService() {
        return simSyncService;
    }

    // Executor cho async tasks
    private final ExecutorService executor = Executors.newFixedThreadPool(10);

    // Scheduler cho auto-hangup
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

    // Active calls map
    private final Map<String, ScheduledFuture<?>> activeCalls = new ConcurrentHashMap<>();

    // ✅ Local device name for filtering SIMs
    private String localDeviceName;

    @PostConstruct
    public void init() {
        // Get local device name
        try {
            this.localDeviceName = DeviceIdProvider.getDeviceId();
            log.info("💻 Local device: {}", localDeviceName);
        } catch (Exception e) {
            this.localDeviceName = "UNKNOWN";
            log.warn("⚠️ Cannot get device name: {}", e.getMessage());
        }

        // Tạo thư mục recordings nếu chưa có
        try {
            Files.createDirectories(Paths.get(getRecordingPath()));
            log.info("📁 Recording path: {}", getRecordingPath());
        } catch (Exception e) {
            log.warn("Không thể tạo thư mục recordings: {}", e.getMessage());
        }

        // Auto scan được xử lý bởi SimSyncService scheduled task
        // Không cần scan ở đây để tránh conflict COM ports
        log.info("✅ GsmService initialized - SIM scanning delegated to SimSyncService");
    }

    // ==================== SIM METHODS ====================

    /**
     * Scan SIM sử dụng SimSyncService (tránh conflict COM ports)
     * Progressive loading: mỗi SIM tìm thấy được push ngay qua WebSocket
     * 
     * ✅ OPTIMIZED: Chỉ scan, không block để detect số
     * Detect số chạy riêng trong background job
     */
    public List<SimResponse> scanAllSims() {
        log.info("🔍 Starting SIM scan via SimSyncService...");

        // Clear cache
        onlineSims.clear();

        try {
            // ✅ OPTIMIZED: Chỉ scan, không detect số (không block)
            List<Sim> scannedSims = simSyncService.scanSimsOnly();

            List<SimResponse> results = new ArrayList<>();

            for (Sim sim : scannedSims) {
                SimResponse response = convertToSimResponse(sim);
                results.add(response);
                onlineSims.put(response.getComPort(), response);
            }

            log.info("✅ Scan complete: Found {} SIMs", results.size());
            // Gửi final list để frontend có full data
            messagingTemplate.convertAndSend("/topic/sims", results);

            // 🆕 Refresh stats cache sau khi scan xong
            refreshStatsCache();

            // ❌ REMOVED: SMS detection - số điện thoại chỉ detect qua AT commands (CNUM,
            // phonebook)
            // Không gửi SMS detect để tránh tốn chi phí và gây phức tạp

            return results;

        } catch (Exception e) {
            log.error("❌ Scan failed: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Convert Sim entity sang SimResponse DTO
     */
    private SimResponse convertToSimResponse(Sim sim) {
        // Map status từ DB sang UI-friendly status
        String uiStatus = "ACTIVE".equals(sim.getStatus()) ? "ONLINE"
                : "INACTIVE".equals(sim.getStatus()) ? "INACTIVE" : "OFFLINE";

        String phoneDisplay = sim.getPhoneNumber() != null ? sim.getPhoneNumber() : "Đang phát hiện...";

        return SimResponse.builder()
                .comPort(sim.getComName())
                .status(uiStatus)
                .carrier(sim.getSimProvider())
                .phoneNumber(phoneDisplay)
                .iccid(sim.getCcid())
                .message("OK")
                .todaySms(smsDailyLimitService.getSentToday(sim.getComName()))
                .todayCall((int) callRepository.countByComPortAndCreatedAtGreaterThanEqual(sim.getComName(), java.time.LocalDate.now().atStartOfDay()))
                .build();
    }

    // scanSinglePort đã được thay thế bởi SimSyncService.scanOnePort()

    /**
     * Lấy danh sách SIM online từ cache hoặc database
     */
    public List<SimResponse> getOnlineSims() {
        if (!onlineSims.isEmpty()) {
            return new ArrayList<>(onlineSims.values());
        }

        // Fallback: đọc từ database - trả về TẤT CẢ SIM
        try {
            String deviceName = DeviceIdProvider.getDeviceId();
            List<Sim> dbSims = simRepository.findByDeviceName(deviceName);

            return dbSims.stream()
                    .map(this::convertToSimResponse)
                    .collect(java.util.stream.Collectors.toList());
        } catch (Exception e) {
            log.error("Error getting online SIMs: {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Lấy thông tin 1 SIM theo comPort
     */
    public SimResponse getSimInfo(String comPort) {
        if (onlineSims.containsKey(comPort)) {
            return onlineSims.get(comPort);
        }

        // Đọc từ database
        try {
            Optional<Sim> simOpt = simRepository.findFirstByComName(comPort);
            if (simOpt.isPresent()) {
                return convertToSimResponse(simOpt.get());
            }
        } catch (Exception e) {
            log.debug("Error getting SIM info for {}: {}", comPort, e.getMessage());
        }

        return SimResponse.builder().comPort(comPort).status("OFFLINE").message("Not found").build();
    }

    /**
     * Rescan a single COM port and update cache + WebSocket
     */
    /**
     * Rescan 1 COM port và cập nhật cache + WebSocket
     */
    public SimResponse rescanSinglePort(String comPort) {
        log.info("🔄 Rescanning single port: {}", comPort);

        try {
            // Trigger full resync (SimSyncService sẽ xử lý)
            simSyncService.syncAndResolve();

            // Đọc lại từ database
            Optional<Sim> simOpt = simRepository.findFirstByComName(comPort);
            if (simOpt.isPresent() && "ACTIVE".equals(simOpt.get().getStatus())) {
                SimResponse response = convertToSimResponse(simOpt.get());
                onlineSims.put(comPort, response);
                // 🔧 Unified: Use /topic/sims for all SIM updates (single SIM wrapped in array)
                messagingTemplate.convertAndSend("/topic/sims", java.util.List.of(response));
                log.info("✅ Port {} rescanned successfully", comPort);
                return response;
            } else {
                onlineSims.remove(comPort);
                log.info("❌ Port {} is OFFLINE", comPort);
                return SimResponse.builder().comPort(comPort).status("OFFLINE").message("Not found").build();
            }
        } catch (Exception e) {
            log.error("Error rescanning {}: {}", comPort, e.getMessage());
            return SimResponse.builder().comPort(comPort).status("OFFLINE").message(e.getMessage()).build();
        }
    }

    // ==================== SMS METHODS ====================

    /**
     * Gửi SMS sử dụng PortWorker (tránh conflict COM ports)
     * Kết quả gửi sẽ được callback qua GsmListenerService.onSmsSentResult()
     */
    public SmsMessageEntity sendSms(SendSmsRequest request) {
        SmsMessageEntity sms = SmsMessageEntity.builder()
                .comPort(request.getComPort())
                .phoneNumber(request.getPhoneNumber())
                .content(request.getContent())
                .type("SENT")
                .status("PENDING")
                .isRead(true)
                .build();

        sms = smsRepository.save(sms);
        final Long smsId = sms.getId();

        try {
            // Tìm SIM theo comPort
            Optional<Sim> simOpt = simRepository.findFirstByDeviceNameAndComName(localDeviceName, request.getComPort());
            if (simOpt.isEmpty()) {
                log.error("❌ Không tìm thấy SIM cho COM port: {}", request.getComPort());
                sms.setStatus("FAILED");
                sms.setType("OUTBOX");
                sms.setErrorMessage("Không tìm thấy SIM");
                sms = smsRepository.save(sms);
                messagingTemplate.convertAndSend("/topic/sms/status", sms);
                return sms;
            }

            Sim sim = simOpt.get();

            // Lấy PortWorker cho SIM này
            app.simsmartgsm.uitils.PortWorker worker = comManager.getOrCreateWorker(sim);
            if (worker == null) {
                log.error("❌ Không thể tạo PortWorker cho: {}", request.getComPort());
                sms.setStatus("FAILED");
                sms.setType("OUTBOX");
                sms.setErrorMessage("Không thể kết nối modem");
                sms = smsRepository.save(sms);
                messagingTemplate.convertAndSend("/topic/sms/status", sms);
                return sms;
            }

            // Đẩy task vào queue của PortWorker
            // Kết quả sẽ được callback qua GsmListenerService.onSmsSentResult()
            worker.enqueue(
                    app.simsmartgsm.uitils.PortWorker.TaskType.SEND,
                    request.getPhoneNumber(),
                    request.getContent(),
                    "WEB_UI",
                    String.valueOf(smsId));

            log.info("📤 Đã đẩy SMS vào queue: {} -> {}", request.getComPort(), request.getPhoneNumber());

            // Notify - status vẫn là PENDING, sẽ update khi có callback
            messagingTemplate.convertAndSend("/topic/sms/status", sms);
            return sms;

        } catch (Exception e) {
            log.error("❌ Lỗi gửi SMS: {}", e.getMessage(), e);
            sms.setStatus("FAILED");
            sms.setType("OUTBOX");
            sms.setErrorMessage(e.getMessage());
            sms = smsRepository.save(sms);
            messagingTemplate.convertAndSend("/topic/sms/status", sms);
            return sms;
        }
    }

    public List<SmsMessageEntity> sendBulkSms(BulkSmsRequest request) {
        List<SmsMessageEntity> results = new CopyOnWriteArrayList<>();
        List<String> comPorts = request.getComPorts();
        List<String> phoneNumbers = request.getPhoneNumbers();
        String content = request.getContent();

        // Phân phối số điện thoại cho các COM port
        int portIndex = 0;
        List<Future<?>> futures = new ArrayList<>();

        for (String phoneNumber : phoneNumbers) {
            String comPort = comPorts.get(portIndex % comPorts.size());
            portIndex++;

            futures.add(executor.submit(() -> {
                SendSmsRequest singleRequest = SendSmsRequest.builder()
                        .comPort(comPort)
                        .phoneNumber(phoneNumber)
                        .content(content)
                        .build();
                SmsMessageEntity result = sendSms(singleRequest);
                results.add(result);
            }));
        }

        // Wait for all
        for (Future<?> future : futures) {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("Bulk SMS error: {}", e.getMessage());
            }
        }

        return results;
    }

    private boolean sendSmsViaModem(String comPort, String phoneNumber, String content) throws Exception {
        // 🔧 FIX: Resolve COM port through PortResolver
        String resolvedPort = portResolver.resolve(comPort);
        if (resolvedPort == null) {
            throw new Exception("Cannot resolve port mapping for " + comPort);
        }

        SerialPort port = SerialPort.getCommPort(resolvedPort);
        port.setComPortParameters(115200, 8, 1, 0);
        port.setComPortTimeouts(SerialPort.TIMEOUT_READ_SEMI_BLOCKING, 3000, 3000);

        if (!port.openPort()) {
            throw new Exception("Cannot open port: " + resolvedPort);
        }

        try {
            Thread.sleep(300);

            // Set text mode
            sendATCommand(port, "ATZ", 500);
            sendATCommand(port, "AT", 300);
            sendATCommand(port, "ATE0", 300);
            sendATCommand(port, "AT+CMGF=1", 500);

            // Check for Unicode
            boolean isUnicode = !content.matches("^[\\x00-\\x7F]*$");
            String actualContent = content;
            String normalizedPhone = normalizePhoneNumber(phoneNumber);

            if (isUnicode) {
                sendATCommand(port, "AT+CSCS=\"UCS2\"", 500);
                sendATCommand(port, "AT+CSMP=17,167,0,8", 500);
                actualContent = encodeUCS2(content);
                normalizedPhone = encodeUCS2(normalizedPhone);
            } else {
                normalizedPhone = normalizedPhone.replace("+", "");
            }

            // Send CMGS command
            String cmgsResponse = sendATCommand(port, "AT+CMGS=\"" + normalizedPhone + "\"", 1000);

            if (!cmgsResponse.contains(">") && cmgsResponse.contains("ERROR")) {
                throw new Exception("CMGS failed: " + cmgsResponse);
            }

            // Send content
            OutputStream os = port.getOutputStream();
            os.write(actualContent.getBytes(StandardCharsets.US_ASCII));
            os.flush();
            Thread.sleep(300);

            // Send Ctrl+Z
            os.write(26);
            os.flush();

            // Wait for result
            Thread.sleep(3000);
            String result = sendATCommand(port, "", 1000);

            return result.contains("OK") || result.contains("+CMGS");

        } finally {
            port.closePort();
        }
    }

    public List<SmsMessageEntity> readSmsFromModem(String comPort) {
        List<SmsMessageEntity> messages = new ArrayList<>();
        SerialPort port = null;

        try {
            PortWorker worker = comManager.getWorker(comPort);
            if (worker != null && worker.isRunning()) {
                long beforeCount = smsRepository.countByTypeAndComPort("INBOX", comPort);
                log.info("📥 [{}] Worker đang chạy, enqueue SMS scan thay vì mở COM trực tiếp", comPort);

                worker.forceScan();
                waitForWorkerSmsScan(comPort, beforeCount, 12000);

                messages = smsRepository.findTop50ByTypeAndComPortOrderByCreatedAtDesc("INBOX", comPort);
                messages.forEach(this::decodeMessageIfNeeded);
                return messages;
            }

            // 🔧 FIX: Resolve COM port through PortResolver
            String resolvedPort = portResolver.resolve(comPort);
            if (resolvedPort == null) {
                throw new Exception("Cannot resolve port mapping for " + comPort);
            }

            port = SerialPort.getCommPort(resolvedPort);
            port.setComPortParameters(115200, 8, 1, 0);
            port.setComPortTimeouts(SerialPort.TIMEOUT_READ_SEMI_BLOCKING, 3000, 3000);

            if (!port.openPort()) {
                throw new Exception("Cannot open port: " + resolvedPort);
            }

            Thread.sleep(300);

            sendATCommand(port, "AT", 300);
            sendATCommand(port, "ATE0", 300);
            sendATCommand(port, "AT+CMGF=1", 500);
            sendATCommand(port, "AT+CSCS=\"UCS2\"", 500);

            // Read all messages
            String response = sendATCommand(port, "AT+CMGL=\"ALL\"", 3000);
            messages = parseSmsMessages(response, comPort);

            // Save to database
            for (SmsMessageEntity sms : messages) {
                sms.setType("INBOX");
                sms.setRead(false);
                smsRepository.save(sms);
            }

            // Notify new messages
            if (!messages.isEmpty()) {
                messagingTemplate.convertAndSend("/topic/sms/new", messages);
            }

        } catch (Exception e) {
            log.error("Error reading SMS: {}", e.getMessage());
        } finally {
            if (port != null && port.isOpen()) {
                port.closePort();
            }
        }

        return messages;
    }

    private void waitForWorkerSmsScan(String comPort, long beforeCount, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            try {
                long currentCount = smsRepository.countByTypeAndComPort("INBOX", comPort);
                if (currentCount != beforeCount) {
                    return;
                }
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.debug("⚠️ [{}] Error while waiting for worker SMS scan: {}", comPort, e.getMessage());
                return;
            }
        }
    }

    public Page<SmsMessageEntity> getInbox(int page, int size) {
        Pageable pageable = PageRequest.of(page, size);
        Page<SmsMessageEntity> messages = smsRepository.findByTypeOrderByCreatedAtDesc("INBOX", pageable);

        // ✅ Decode SMS content if still encoded (for old data)
        messages.forEach(this::decodeMessageIfNeeded);

        return messages;
    }

    /**
     * Decode SMS message content if it's still UCS2 encoded
     */
    private void decodeMessageIfNeeded(SmsMessageEntity message) {
        if (message.getContent() != null && !message.getContent().isEmpty()) {
            // Check if content looks like hex (only hex chars, length divisible by 4, long
            // enough)
            String content = message.getContent().trim();
            if (content.matches("^[0-9A-Fa-f]+$") && content.length() % 4 == 0 && content.length() >= 8) {
                String decoded = SmsDecoder.decode(content);
                if (!decoded.equals(content)) {
                    message.setContent(decoded);
                    log.debug("✅ Decoded SMS #{}: {} chars → {} chars", message.getId(), content.length(),
                            decoded.length());
                }
            }
        }

        // Also decode sender if needed
        if (message.getPhoneNumber() != null && !message.getPhoneNumber().isEmpty()) {
            String sender = message.getPhoneNumber().trim();
            if (sender.matches("^[0-9A-Fa-f]+$") && sender.length() % 4 == 0 && sender.length() >= 8) {
                String decoded = SmsDecoder.decode(sender);
                if (!decoded.equals(sender)) {
                    message.setPhoneNumber(decoded);
                    log.debug("✅ Decoded sender #{}: {} → {}", message.getId(), sender, decoded);
                }
            }
        }
    }

    public Page<SmsMessageEntity> getSent(int page, int size) {
        Pageable pageable = PageRequest.of(page, size);
        Page<SmsMessageEntity> messages = smsRepository.findByTypeOrderByCreatedAtDesc("SENT", pageable);

        // ✅ Decode SMS content if still encoded (for old data)
        messages.forEach(this::decodeMessageIfNeeded);

        return messages;
    }

    public Page<SmsMessageEntity> getOutbox(int page, int size) {
        Pageable pageable = PageRequest.of(page, size);
        Page<SmsMessageEntity> messages = smsRepository.findByTypeOrderByCreatedAtDesc("OUTBOX", pageable);

        // ✅ Decode SMS content if still encoded (for old data)
        messages.forEach(this::decodeMessageIfNeeded);

        return messages;
    }

    /** Get inbox with date filter */
    public Page<SmsMessageEntity> getInboxByDate(int page, int size, LocalDateTime startDate, LocalDateTime endDate) {
        Pageable pageable = PageRequest.of(page, size);
        Page<SmsMessageEntity> messages = smsRepository.findByTypeAndCreatedAtBetweenOrderByCreatedAtDesc("INBOX",
                startDate, endDate, pageable);

        // ✅ Decode SMS content if still encoded (for old data)
        messages.forEach(this::decodeMessageIfNeeded);

        return messages;
    }

    /** Get sent with date filter */
    public Page<SmsMessageEntity> getSentByDate(int page, int size, LocalDateTime startDate, LocalDateTime endDate) {
        Pageable pageable = PageRequest.of(page, size);
        Page<SmsMessageEntity> messages = smsRepository.findByTypeAndCreatedAtBetweenOrderByCreatedAtDesc("SENT",
                startDate, endDate, pageable);

        // ✅ Decode SMS content if still encoded (for old data)
        messages.forEach(this::decodeMessageIfNeeded);

        return messages;
    }

    /** Get outbox with date filter */
    public Page<SmsMessageEntity> getOutboxByDate(int page, int size, LocalDateTime startDate, LocalDateTime endDate) {
        Pageable pageable = PageRequest.of(page, size);
        Page<SmsMessageEntity> messages = smsRepository.findByTypeAndCreatedAtBetweenOrderByCreatedAtDesc("OUTBOX",
                startDate, endDate, pageable);

        // ✅ Decode SMS content if still encoded (for old data)
        messages.forEach(this::decodeMessageIfNeeded);

        return messages;
    }

    /** Get inbox by phone number search */
    public Page<SmsMessageEntity> getInboxByPhone(int page, int size, String phoneNumber) {
        Pageable pageable = PageRequest.of(page, size);
        Page<SmsMessageEntity> messages = smsRepository.findByTypeAndPhoneNumberContainingOrderByCreatedAtDesc(
                "INBOX", phoneNumber, pageable);
        messages.forEach(this::decodeMessageIfNeeded);
        return messages;
    }

    /** Get sent by phone number search */
    public Page<SmsMessageEntity> getSentByPhone(int page, int size, String phoneNumber) {
        Pageable pageable = PageRequest.of(page, size);
        Page<SmsMessageEntity> messages = smsRepository.findByTypeAndPhoneNumberContainingOrderByCreatedAtDesc(
                "SENT", phoneNumber, pageable);
        messages.forEach(this::decodeMessageIfNeeded);
        return messages;
    }

    /** Get outbox by phone number search */
    public Page<SmsMessageEntity> getOutboxByPhone(int page, int size, String phoneNumber) {
        Pageable pageable = PageRequest.of(page, size);
        Page<SmsMessageEntity> messages = smsRepository.findByTypeAndPhoneNumberContainingOrderByCreatedAtDesc(
                "OUTBOX", phoneNumber, pageable);
        messages.forEach(this::decodeMessageIfNeeded);
        return messages;
    }

    public long getUnreadCount() {
        return smsRepository.countByTypeAndIsReadFalse("INBOX");
    }

    /**
     * ✅ Decode lại toàn bộ SMS trong database (chạy 1 lần để fix old data)
     * 
     * @return số lượng SMS đã được decode
     */
    @org.springframework.transaction.annotation.Transactional
    public int decodeAllSmsInDatabase() {
        log.info("🔄 Starting SMS decoding migration...");
        int decodedCount = 0;

        try {
            // Get all SMS messages (no pagination for migration)
            List<SmsMessageEntity> allMessages = smsRepository.findAll();
            log.info("📊 Found {} total SMS messages to check", allMessages.size());

            for (SmsMessageEntity message : allMessages) {
                boolean changed = false;

                // Decode content if needed
                if (message.getContent() != null && !message.getContent().isEmpty()) {
                    String content = message.getContent().trim();
                    if (content.matches("^[0-9A-Fa-f]+$") && content.length() % 4 == 0 && content.length() >= 8) {
                        String decoded = SmsDecoder.decode(content);
                        if (!decoded.equals(content)) {
                            message.setContent(decoded);
                            changed = true;
                            log.debug("✅ Decoded SMS #{} content: {} chars → {} chars",
                                    message.getId(), content.length(), decoded.length());
                        }
                    }
                }

                // Decode phone number if needed
                if (message.getPhoneNumber() != null && !message.getPhoneNumber().isEmpty()) {
                    String phoneNumber = message.getPhoneNumber().trim();
                    if (phoneNumber.matches("^[0-9A-Fa-f]+$") && phoneNumber.length() % 4 == 0
                            && phoneNumber.length() >= 8) {
                        String decoded = SmsDecoder.decode(phoneNumber);
                        if (!decoded.equals(phoneNumber)) {
                            message.setPhoneNumber(decoded);
                            changed = true;
                            log.debug("✅ Decoded SMS #{} phone: {} → {}",
                                    message.getId(), phoneNumber, decoded);
                        }
                    }
                }

                if (changed) {
                    smsRepository.save(message);
                    decodedCount++;
                }
            }

            log.info("✅ SMS decoding migration completed: {}/{} messages decoded",
                    decodedCount, allMessages.size());

        } catch (Exception e) {
            log.error("❌ Error during SMS decoding migration: {}", e.getMessage(), e);
            throw new RuntimeException("SMS decoding migration failed: " + e.getMessage());
        }

        return decodedCount;
    }

    public void markAsRead(Long id) {
        smsRepository.findById(id).ifPresent(sms -> {
            sms.setRead(true);
            smsRepository.save(sms);
        });
    }

    /** Đánh dấu tất cả tin nhắn INBOX là đã đọc */
    @org.springframework.transaction.annotation.Transactional
    public int markAllAsRead() {
        int count = smsRepository.markAllAsReadByType("INBOX");
        log.info("✅ Marked {} messages as read", count);
        // Push updated unread count
        messagingTemplate.convertAndSend("/topic/sms/unread-count", 0);
        return count;
    }

    /** 🗑️ Xóa toàn bộ tin nhắn local trong database */
    @org.springframework.transaction.annotation.Transactional
    public int deleteAllSmsMessages() {
        try {
            long totalCount = smsRepository.count();
            log.warn("🗑️ Đang xóa toàn bộ {} tin nhắn local...", totalCount);

            smsRepository.deleteAll();

            log.info("✅ Đã xóa thành công {} tin nhắn", totalCount);

            // Push notification qua WebSocket
            messagingTemplate.convertAndSend("/topic/sms/unread-count", 0);
            messagingTemplate.convertAndSend("/topic/sms/deleted-all", Map.of(
                    "deletedCount", totalCount,
                    "timestamp", java.time.Instant.now().toString()));

            return (int) totalCount;
        } catch (Exception e) {
            log.error("❌ Lỗi khi xóa tin nhắn: {}", e.getMessage(), e);
            throw new RuntimeException("Không thể xóa tin nhắn: " + e.getMessage());
        }
    }

    public SmsMessageEntity resendSms(Long id) {
        SmsMessageEntity original = smsRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Không tìm thấy tin nhắn"));

        SendSmsRequest request = SendSmsRequest.builder()
                .comPort(original.getComPort())
                .phoneNumber(original.getPhoneNumber())
                .content(original.getContent())
                .build();

        // Delete old record
        smsRepository.delete(original);

        return sendSms(request);
    }

    public void deleteSms(Long id) {
        smsRepository.deleteById(id);
    }

    // ==================== CALL METHODS ====================

    public CallRecordEntity makeCall(MakeCallRequest request) {
        int callDuration = request.getCallDuration() != null
                ? request.getCallDuration()
                : this.defaultCallDuration;

        boolean shouldRecord = request.getRecord() != null
                ? request.getRecord()
                : false;

        CallRecordEntity call = CallRecordEntity.builder()
                .comPort(request.getComPort())
                .simPhone(request.getSimPhone())
                .targetPhone(request.getTargetPhone())
                .callDuration(callDuration)
                .isRecorded(shouldRecord)
                .status("PENDING")
                .build();

        call = callRepository.save(call);

        final Long callId = call.getId();

        // Execute call in background
        String finalSimPhone = (request.getSimPhone() != null) ? request.getSimPhone() : "UNKNOWN";
        executor.submit(() -> executeCall(callId, request.getComPort(), request.getTargetPhone(),
                callDuration, shouldRecord, finalSimPhone,
                request.getServiceCode(), request.getOrderId()));

        return call;
    }

    /**
     * ✅ Gọi điện và phát file audio ghi sẵn cho đối phương nghe
     * Sử dụng AT+QPSND để phát audio vào uplink
     */
    public CallRecordEntity makeCallWithAudio(CallWithAudioRequest request) {
        // Validate
        if (request.getAudioFileName() == null && request.getLocalAudioPath() == null) {
            throw new RuntimeException("Phải cung cấp audioFileName hoặc localAudioPath");
        }

        boolean shouldRecord = request.getRecord() != null ? request.getRecord() : false;
        boolean repeatAudio = request.getRepeatAudio() != null ? request.getRepeatAudio() : false;
        int waitAfter = request.getWaitAfterAudioSeconds() != null ? request.getWaitAfterAudioSeconds() : 2;

        // Create call record
        CallRecordEntity call = CallRecordEntity.builder()
                .comPort(request.getComPort())
                .targetPhone(request.getTargetPhone())
                .isRecorded(shouldRecord)
                .status("PENDING")
                .callType("OUTGOING_WITH_AUDIO")
                .build();

        call = callRepository.save(call);
        final Long callId = call.getId();

        // Execute in background
        executor.submit(() -> {
            try {
                // Resolve port
                String resolvedPort = portResolver.resolve(request.getComPort());
                if (resolvedPort == null) {
                    updateCallStatus(callId, "FAILED", "Cannot resolve port: " + request.getComPort());
                    return;
                }

                // Get SIM info
                Sim sim = simRepository.findFirstByComName(request.getComPort()).orElse(null);
                if (sim == null) {
                    updateCallStatus(callId, "FAILED", "SIM not found for port: " + request.getComPort());
                    return;
                }

                // Lock port and stop worker
                comManager.lockPort(request.getComPort());
                comManager.stopWorkerAndWait(request.getComPort(), 3000);
                Thread.sleep(300);

                updateCallStatus(callId, "DIALING", null);
                sendCallStatusUpdate(request.getComPort(), request.getTargetPhone(), "DIALING", 0,
                        "Chuẩn bị gọi và phát audio...");

                // Create and execute session
                app.simsmartgsm.session.CallWithAudioTask audioTask =
                        new app.simsmartgsm.session.CallWithAudioTask(
                                sim, request.getTargetPhone(),
                                request.getAudioFileName(),
                                request.getLocalAudioPath(),
                                repeatAudio, waitAfter, shouldRecord,
                                request.getServiceCode(), request.getOrderId());

                try (app.simsmartgsm.session.TaskSession session = audioTask.createSession()) {
                    session.openPort();
                    app.simsmartgsm.session.SessionResult result = session.execute();

                    // Update call record
                    updateCallStatus(callId, result.getStatus(), result.getErrorMessage());
                    if (result.getRecordFileUrl() != null) {
                        updateCallRecordingPath(callId, result.getRecordFileUrl());
                    }

                    log.info("✅ [{}] CALL_WITH_AUDIO completed: status={}", request.getComPort(), result.getStatus());
                }

            } catch (Exception e) {
                log.error("❌ [{}] CALL_WITH_AUDIO error: {}", request.getComPort(), e.getMessage(), e);
                updateCallStatus(callId, "FAILED", e.getMessage());
            } finally {
                // Unlock port and restart worker
                comManager.unlockPort(request.getComPort());
                try {
                    Sim sim = simRepository.findFirstByComName(request.getComPort()).orElse(null);
                    if (sim != null) {
                        comManager.startWorker(sim);
                    }
                } catch (Exception e) {
                    log.warn("⚠️ Failed to restart worker: {}", e.getMessage());
                }
            }
        });

        return call;
    }

    /**
     * ✅ Upload file audio lên modem để sử dụng với makeCallWithAudio
     * Trả về tên file trên modem
     */
    public Map<String, Object> uploadAudioToModem(String comPort, String localFilePath) throws Exception {
        String resolvedPort = portResolver.resolve(comPort);
        if (resolvedPort == null) {
            throw new Exception("Cannot resolve port: " + comPort);
        }

        File localFile = new File(localFilePath);
        if (!localFile.exists()) {
            throw new Exception("File not found: " + localFilePath);
        }

        // Lock port
        comManager.lockPort(comPort);
        comManager.stopWorkerAndWait(comPort, 3000);
        Thread.sleep(300);

        SerialPort port = null;
        try {
            port = SerialPort.getCommPort(resolvedPort);
            port.setComPortParameters(115200, 8, 1, 0);
            port.setComPortTimeouts(SerialPort.TIMEOUT_READ_SEMI_BLOCKING, 3000, 3000);

            if (!port.openPort()) {
                throw new Exception("Cannot open port: " + resolvedPort);
            }

            Thread.sleep(300);
            sendATCommand(port, "AT", 300);
            sendATCommand(port, "ATE0", 300);

            // Delete existing file
            String modemFileName = localFile.getName();
            sendATCommand(port, "AT+QFDEL=\"" + modemFileName + "\"", 1000);
            Thread.sleep(200);

            // Upload
            long fileSize = localFile.length();
            String uplCmd = "AT+QFUPL=\"" + modemFileName + "\"," + fileSize;
            String uplResp = sendATCommand(port, uplCmd, 3000);

            if (!uplResp.contains("CONNECT")) {
                throw new Exception("QFUPL failed: " + uplResp);
            }

            // Send file data
            byte[] fileData = java.nio.file.Files.readAllBytes(localFile.toPath());
            OutputStream out = port.getOutputStream();
            out.write(fileData);
            out.flush();

            Thread.sleep(1000);
            String result = sendATCommand(port, "", 3000);

            Map<String, Object> response = new HashMap<>();
            response.put("modemFileName", modemFileName);
            response.put("fileSize", fileSize);
            response.put("success", result.contains("OK") || result.contains("+QFUPL"));
            response.put("modemResponse", result.trim());

            log.info("✅ [{}] Audio uploaded to modem: {} ({} bytes)", comPort, modemFileName, fileSize);
            return response;

        } finally {
            if (port != null && port.isOpen()) {
                port.closePort();
            }
            comManager.unlockPort(comPort);
            try {
                Sim sim = simRepository.findFirstByComName(comPort).orElse(null);
                if (sim != null) {
                    comManager.startWorker(sim);
                }
            } catch (Exception e) {
                log.warn("⚠️ Failed to restart worker: {}", e.getMessage());
            }
        }
    }

    /**
     * ✅ Liệt kê file audio đã upload trên modem
     */
    public Map<String, Object> listAudioFilesOnModem(String comPort) throws Exception {
        String resolvedPort = portResolver.resolve(comPort);
        if (resolvedPort == null) {
            throw new Exception("Cannot resolve port: " + comPort);
        }

        comManager.lockPort(comPort);
        comManager.stopWorkerAndWait(comPort, 3000);
        Thread.sleep(300);

        SerialPort port = null;
        try {
            port = SerialPort.getCommPort(resolvedPort);
            port.setComPortParameters(115200, 8, 1, 0);
            port.setComPortTimeouts(SerialPort.TIMEOUT_READ_SEMI_BLOCKING, 3000, 3000);

            if (!port.openPort()) {
                throw new Exception("Cannot open port: " + resolvedPort);
            }

            Thread.sleep(300);
            sendATCommand(port, "AT", 300);
            sendATCommand(port, "ATE0", 300);

            // List files
            String listResp = sendATCommand(port, "AT+QFLST", 3000);

            Map<String, Object> response = new HashMap<>();
            response.put("comPort", comPort);
            response.put("rawResponse", listResp.trim());

            // Parse files
            java.util.List<Map<String, Object>> files = new java.util.ArrayList<>();
            String[] lines = listResp.split("\\r?\\n");
            for (String line : lines) {
                if (line.contains("+QFLST:")) {
                    java.util.regex.Matcher m = java.util.regex.Pattern
                            .compile("\"([^\"]+)\",(\\d+)").matcher(line);
                    if (m.find()) {
                        Map<String, Object> file = new HashMap<>();
                        file.put("name", m.group(1));
                        file.put("size", Long.parseLong(m.group(2)));
                        files.add(file);
                    }
                }
            }
            response.put("files", files);
            response.put("totalFiles", files.size());

            return response;

        } finally {
            if (port != null && port.isOpen()) {
                port.closePort();
            }
            comManager.unlockPort(comPort);
            try {
                Sim sim = simRepository.findFirstByComName(comPort).orElse(null);
                if (sim != null) {
                    comManager.startWorker(sim);
                }
            } catch (Exception e) {
                log.warn("⚠️ Failed to restart worker: {}", e.getMessage());
            }
        }
    }

    /**
     * ✅ Nhận cuộc gọi đến (Answer incoming call)
     * Đợi cuộc gọi đến trong khoảng thời gian waitTimeout, sau đó nhấc máy và ghi
     * âm (nếu cần)
     */
    public CallRecordEntity answerCall(AnswerCallRequest request) {
        int waitTimeout = request.getWaitTimeout() != null
                ? request.getWaitTimeout()
                : 60; // Mặc định đợi 60 giây

        int callDuration = request.getCallDuration() != null
                ? request.getCallDuration()
                : this.defaultCallDuration;

        boolean shouldRecord = request.getRecord() != null
                ? request.getRecord()
                : false;

        boolean acceptHidden = request.getAcceptHiddenCaller() != null
                ? request.getAcceptHiddenCaller()
                : false;

        // Tạo record cuộc gọi với loại INCOMING
        CallRecordEntity call = CallRecordEntity.builder()
                .comPort(request.getComPort())
                .simPhone(request.getSimPhone())
                .targetPhone(request.getExpectedCaller()) // Số gọi đến dự kiến
                .callDuration(callDuration)
                .isRecorded(shouldRecord)
                .status("WAITING_CALL") // Đang đợi cuộc gọi đến
                .callType("INCOMING") // Đánh dấu là cuộc gọi đến
                .build();

        call = callRepository.save(call);

        final Long callId = call.getId();

        // Execute in background
        String finalSimPhone = (request.getSimPhone() != null) ? request.getSimPhone() : "UNKNOWN";
        executor.submit(() -> executeAnswerCall(callId, request.getComPort(),
                request.getExpectedCaller(), waitTimeout, callDuration, shouldRecord,
                acceptHidden, finalSimPhone,
                request.getServiceCode(), request.getOrderId()));

        return call;
    }

    /**
     * ✅ Execute answer call logic
     */
    private void executeAnswerCall(Long callId, String comPort, String expectedCaller,
            int waitTimeout, int callDuration, boolean shouldRecord, boolean acceptHidden,
            String simPhone, String serviceCode, String orderId) {
        try {
            log.info(
                    "📞 [{}] Starting INCOMING CALL session - expectedCaller={}, wait={}s, duration={}s, record={}, acceptHidden={}",
                    comPort, expectedCaller, waitTimeout, callDuration, shouldRecord, acceptHidden);

            // Get SIM from database
            Sim sim = simRepository.findFirstByComName(comPort).orElse(null);
            if (sim == null) {
                updateCallStatus(callId, "FAILED", "SIM not found for port: " + comPort);
                sendCallStatusUpdate(comPort, expectedCaller, "FAILED", 0, "Không tìm thấy SIM");
                notifyIncomingCallStatus("FAILED", comPort, simPhone, expectedCaller, orderId, serviceCode);
                return;
            }

            // Update status: WAITING
            updateCallStatus(callId, "WAITING_CALL", null);
            sendCallStatusUpdate(comPort, expectedCaller, "WAITING_CALL", 0, "Đang đợi cuộc gọi đến...");
            notifyIncomingCallStatus("WAITING_CALL", comPort, simPhone, expectedCaller, orderId, serviceCode);

            // Use ComManager to enqueue CALL_IN task
            PortWorker worker = comManager.getWorker(comPort);
            if (worker == null) {
                // Start worker if not running
                comManager.startWorker(sim);
                worker = comManager.getWorker(comPort);
            }

            if (worker == null || !worker.isOpen()) {
                updateCallStatus(callId, "FAILED", "Cannot open port: " + comPort);
                sendCallStatusUpdate(comPort, expectedCaller, "FAILED", 0, "Không thể mở cổng COM");
                notifyIncomingCallStatus("FAILED", comPort, simPhone, expectedCaller, orderId, serviceCode);
                return;
            }

            // Create CALL_IN task with all options
            PortWorker.Task callInTask = PortWorker.Task.callInWithHidden(
                    expectedCaller, // Số dự kiến gọi đến
                    callDuration, // Thời gian giữ cuộc gọi
                    shouldRecord, // Có ghi âm không
                    acceptHidden, // Có chấp nhận số ẩn không
                    waitTimeout, // Thời gian đợi cuộc gọi
                    serviceCode, // Service code cho remote
                    orderId // Order ID cho remote
            );

            // Enqueue task
            worker.enqueue(callInTask);
            log.info("📞 [{}] CALL_IN task enqueued - orderId={}", comPort, orderId);

            // ✅ Set internal reference to track call for status updates
            // The actual call handling is done by PortWorker.IncomingCallSession
            // Status updates will be sent via WebSocket from there

        } catch (Exception e) {
            log.error("❌ [{}] executeAnswerCall error: {}", comPort, e.getMessage(), e);
            updateCallStatus(callId, "FAILED", e.getMessage());
            sendCallStatusUpdate(comPort, expectedCaller, "FAILED", 0, "Lỗi: " + e.getMessage());
            notifyIncomingCallStatus("FAILED", comPort, simPhone, expectedCaller, orderId, serviceCode);
        }
    }

    /**
     * ✅ Send incoming call status notification via WebSocket
     */
    private void notifyIncomingCallStatus(String status, String comPort, String simPhone,
            String callerNumber, String orderId, String serviceCode) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("type", "INCOMING_CALL_STATUS");
            payload.put("status", status);
            payload.put("comPort", comPort);
            payload.put("simPhone", simPhone);
            payload.put("callerNumber", callerNumber);
            payload.put("orderId", orderId);
            payload.put("serviceCode", serviceCode);
            payload.put("timestamp", Instant.now().toString());

            // Send to local WebSocket topic
            messagingTemplate.convertAndSend("/topic/call/incoming", payload);
            log.debug("📡 [{}] Incoming call status: {} -> /topic/call/incoming", comPort, status);

        } catch (Exception e) {
            log.error("❌ Failed to send incoming call status: {}", e.getMessage());
        }
    }

    private void executeCall(Long callId, String comPort, String targetPhone,
            int callDuration, boolean shouldRecord, String simPhone,
            String serviceCode, String orderId) {
        SerialPort port = null;
        String recordingFile = null;
        boolean wasConnected = false;

        try {
            // 🔧 FIX: Resolve COM port through PortResolver (COM292 -> /dev/ttyUSB0 on
            // Linux/Mac)
            String resolvedPort = portResolver.resolve(comPort);
            if (resolvedPort == null) {
                String errorMsg = "Cannot resolve port mapping for " + comPort;
                log.error("❌ {}", errorMsg);
                updateCallStatus(callId, "FAILED", errorMsg);
                sendCallStatusUpdate(comPort, targetPhone, "FAILED", 0, "Không thể mở cổng COM");
                if (serviceCode != null && orderId != null) {
                    recordLocalCallStatus("FAILED", comPort, simPhone, targetPhone, orderId, serviceCode);
                }
                return;
            }

            log.info("🔗 Resolved {} -> {}", comPort, resolvedPort);

            // ✅ FIX: Lock port để ngăn recovery job và scan job mở lại port
            comManager.lockPort(comPort);

            // ✅ FIX: LUÔN stop worker (kể cả khi port chưa open, thread vẫn chạy và tranh
            // chấp)
            boolean workerStopped = comManager.stopWorkerAndWait(comPort, 3000);
            if (!workerStopped) {
                log.warn("⚠️ Worker thread {} chưa thoát hoàn toàn sau 3s, thử tiếp...", comPort);
            }
            // Đợi thêm 300ms để OS giải phóng port handle
            Thread.sleep(300);

            // ✅ FIX: Retry opening port with smart delays
            int maxOpenAttempts = 3;
            Exception lastException = null;
            boolean portOpened = false;

            for (int attempt = 1; attempt <= maxOpenAttempts; attempt++) {
                try {
                    port = SerialPort.getCommPort(resolvedPort);
                    port.setComPortParameters(115200, 8, 1, 0);
                    port.setComPortTimeouts(SerialPort.TIMEOUT_READ_SEMI_BLOCKING, 3000, 3000);

                    log.info("🔓 Opening port: {} (attempt {}/{})", resolvedPort, attempt, maxOpenAttempts);
                    portOpened = port.openPort();

                    if (portOpened) {
                        log.info("✅ Port opened: {}", resolvedPort);
                        break; // Success
                    } else {
                        lastException = new IOException("port.openPort() returned false");
                        log.warn("⚠️ Failed to open port {} (attempt {}/{}): port.openPort() returned false",
                                resolvedPort, attempt, maxOpenAttempts);
                    }
                } catch (Exception e) {
                    lastException = e;
                    log.warn("⚠️ Failed to open port {} (attempt {}/{}): {}",
                            resolvedPort, attempt, maxOpenAttempts, e.getMessage());
                }

                if (attempt < maxOpenAttempts) {
                    // Exponential backoff: 200ms, 400ms, 800ms
                    long delayMs = 200L * (1L << (attempt - 1));
                    log.info("⏳ Waiting {}ms before retry...", delayMs);
                    Thread.sleep(delayMs);
                }
            }

            if (!portOpened || port == null) {
                String errorMsg = "Cannot open port " + resolvedPort + " after " + maxOpenAttempts + " attempts: " +
                        (lastException != null ? lastException.getMessage() : "unknown error");
                log.error("❌ {}", errorMsg);
                updateCallStatus(callId, "FAILED", errorMsg);
                sendCallStatusUpdate(comPort, targetPhone, "FAILED", 0, "Không thể mở cổng COM: " + resolvedPort);
                if (serviceCode != null && orderId != null) {
                    recordLocalCallStatus("FAILED", comPort, simPhone, targetPhone, orderId, serviceCode);
                }
                return;
            }

            Thread.sleep(500);

            // ✅ Robust Initialization matching PortWorker & CallService proven logic
            sendATCommand(port, "AT", 300);
            sendATCommand(port, "ATE0", 300);
            sendATCommand(port, "AT+CMEE=2", 300);
            sendATCommand(port, "AT+CLIP=1", 300);
            sendATCommand(port, "AT+CRC=1", 300);

            // Set status: DIALING
            updateCallStatus(callId, "DIALING", null);
            sendCallStatusUpdate(comPort, targetPhone, "DIALING", 0, "Đang quay số...");
            if (serviceCode != null && orderId != null) {
                recordLocalCallStatus("DIALING", comPort, simPhone, targetPhone, orderId, serviceCode);
            }

            // Make call
            String dialResponse = sendATCommand(port, "ATD" + targetPhone + ";", 2000);

            if (dialResponse.contains("ERROR") || dialResponse.contains("NO CARRIER")
                    || dialResponse.contains("BUSY")) {
                updateCallStatus(callId, "FAILED", "Dial failed: " + dialResponse);
                sendCallStatusUpdate(comPort, targetPhone, "FAILED", 0, "Không thể gọi: " + dialResponse);
                if (serviceCode != null && orderId != null) {
                    recordLocalCallStatus("FAILED", comPort, simPhone, targetPhone, orderId, serviceCode);
                }
                return;
            }

            // Set status: RINGING
            updateCallStatus(callId, "RINGING", null);
            sendCallStatusUpdate(comPort, targetPhone, "RINGING", 0, "Đang đổ chuông...");
            // ✅ Notify remote for RINGING status
            if (serviceCode != null && orderId != null) {
                recordLocalCallStatus("RINGING", comPort, simPhone, targetPhone, orderId, serviceCode);
            }

            // ✅ MATCHED WITH PortWorker: Poll CLCC to monitor call state
            // Using the EXACT logic from commit c76313a22ce6604a8a1a29ab59cecdfaae98f034
            final int MAX_WAIT_FOR_CONNECT = 30;
            int waitedSeconds = 0;
            int activeConfirmCount = 0;
            int noCallCount = 0;
            long connectedAtMillis = 0;
            final java.util.concurrent.atomic.AtomicBoolean recordingStarted = new java.util.concurrent.atomic.AtomicBoolean(
                    false);
            final java.util.concurrent.atomic.AtomicReference<String> recordingFileRef = new java.util.concurrent.atomic.AtomicReference<>(
                    null);

            log.info("🔵 [{}] Starting CLCC monitoring loop (max wait: {}s)", comPort, MAX_WAIT_FOR_CONNECT);

            while (waitedSeconds < MAX_WAIT_FOR_CONNECT && !wasConnected) {
                Thread.sleep(1000);
                waitedSeconds++;

                String clccResponse = sendATCommand(port, "AT+CLCC", 800);

                if (clccResponse == null) {
                    log.warn("⚠️ [{}] CLCC returned NULL - modem not responding", comPort);
                    continue;
                }

                // ✅ CRITICAL: Check for terminal statuses FIRST and INDEPENDENTLY
                if (clccResponse.contains("NO CARRIER") || clccResponse.contains("BUSY")
                        || clccResponse.contains("NO ANSWER")) {
                    log.warn("❌ [{}] Call ended/failed: {}", comPort, clccResponse.trim());
                    break;
                }

                boolean matchedOutgoing = false;
                if (clccResponse.contains("+CLCC")) {
                    log.info("📡 [{}] CLCC RAW → {}", comPort, clccResponse.replace("\r", " ").replace("\n", " "));

                    // ✅ ONLY match outgoing calls (direction=0)
                    java.util.regex.Pattern p = java.util.regex.Pattern.compile("\\+CLCC:\\s*\\d+,0,(\\d+),");
                    java.util.regex.Matcher m = p.matcher(clccResponse);

                    while (m.find()) {
                        matchedOutgoing = true;
                        int stat = Integer.parseInt(m.group(1));
                        log.info("🔵 [{}] CLCC stat={} (dir=0)", comPort, stat);

                        switch (stat) {
                            case 3: // DIALING
                                if (!wasConnected) {
                                    log.info("📞 [{}] CLCC stat=3 (DIALING)", comPort);
                                    sendCallStatusUpdate(comPort, targetPhone, "DIALING", waitedSeconds,
                                            "Đang quay số...");
                                }
                                break;

                            case 2: // ALERTING (RINGING)
                                if (!wasConnected) {
                                    log.info("📞 [{}] CLCC stat=2 (RINGING)", comPort);
                                    sendCallStatusUpdate(comPort, targetPhone, "RINGING", waitedSeconds,
                                            "Đang chờ trả lời... (" + waitedSeconds + "s)");
                                }
                                break;

                            case 0: // ACTIVE (connected)
                                activeConfirmCount++;
                                log.info("🔵 [{}] CLCC stat=0 (ACTIVE) - count={}", comPort, activeConfirmCount);

                                if (!wasConnected && activeConfirmCount >= 1) { // Reduced to 1 for faster response
                                                                                // after filtering
                                    wasConnected = true;
                                    connectedAtMillis = System.currentTimeMillis();

                                    updateCallStatus(callId, "IN_CALL", null);
                                    updateCallStartTime(callId);
                                    sendCallStatusUpdate(comPort, targetPhone, "IN_CALL", 0, "Đã kết nối");
                                    if (serviceCode != null && orderId != null) {
                                        recordLocalCallStatus("IN_CALL", comPort, simPhone, targetPhone, orderId,
                                                serviceCode);
                                    }
                                    log.info("📞 [{}] ✅ CALL CONNECTED (CLCC stat=0, confirm={})", comPort,
                                            activeConfirmCount);

                                    if (shouldRecord && recordingStarted.compareAndSet(false, true)) {
                                        long recordingStartTime = System.currentTimeMillis();
                                        log.info("🎙️ [{}] Starting recording initialization...", comPort);

                                        recordingFile = startRecording(port, comPort, targetPhone);
                                        recordingFileRef.set(recordingFile);
                                        updateCallRecordingPath(callId, recordingFile);

                                        long recordingInitTime = System.currentTimeMillis() - recordingStartTime;
                                        log.info("✅ [{}] Recording initialized in {}ms", comPort, recordingInitTime);

                                        connectedAtMillis = System.currentTimeMillis();
                                        log.info(
                                                "⏱️ [{}] Timer start point updated to after recording init (delay: {}ms)",
                                                comPort, recordingInitTime);
                                    }
                                }
                                break;

                            case 6: // DISCONNECTED
                                log.info("📞 [{}] CLCC stat=6 (DISCONNECTED) - ending call", comPort);
                                break;
                        }
                    }
                }

                if (!matchedOutgoing && (clccResponse.trim().equals("OK") || clccResponse.trim().isEmpty() ||
                        (clccResponse.contains("+CLCC") && !matchedOutgoing))) {
                    noCallCount++;
                    if (noCallCount >= 2) {
                        log.warn("⚠️ [{}] Outgoing call not found in CLCC (waited {}s)", comPort, waitedSeconds);
                        break;
                    }
                } else {
                    noCallCount = 0;
                }
            }

            if (!wasConnected) {
                // Determine failure reason and appropriate status
                String failReason;
                String wsStatus;

                if (waitedSeconds >= 30) {
                    failReason = "Không nhấc máy (timeout)";
                    wsStatus = "TIMEOUT";
                } else if (waitedSeconds < 5) {
                    failReason = "Cuộc gọi bị từ chối";
                    wsStatus = "REJECTED";
                } else {
                    failReason = "Không nhấc máy";
                    wsStatus = "ENDED_NO_ANSWER";
                }

                updateCallStatus(callId, "FAILED", failReason);
                sendCallStatusUpdate(comPort, targetPhone, wsStatus, 0, failReason);
                if (serviceCode != null && orderId != null) {
                    recordLocalCallStatus(wsStatus, comPort, simPhone, targetPhone, orderId, serviceCode);
                }
                log.warn("❌ [{}] Call to {} {} - {}", comPort, targetPhone, wsStatus, failReason);

                // Hangup just in case
                try {
                    sendATCommand(port, "ATH", 500);
                } catch (Exception ignored) {
                }

                return;
            }

            // ✅ CRITICAL: Schedule auto-hangup based on EXACT connected time
            // Calculate remaining time from the moment recording is ready
            // time
            final SerialPort finalPort = port;
            final long finalConnectedAtMillis = connectedAtMillis;

            // Calculate EXACT time remaining from connection moment
            long elapsedSinceConnect = System.currentTimeMillis() - connectedAtMillis;
            long remainingMs = (callDuration * 1000L) - elapsedSinceConnect;
            if (remainingMs < 1000)
                remainingMs = 1000; // Minimum 1 second

            log.info("⏱️ [{}] Scheduling hangup in {}ms (callDuration={}s, elapsed={}ms)",
                    comPort, remainingMs, callDuration, elapsedSinceConnect);

            // Update timer every second
            final ScheduledFuture<?>[] timerTaskRef = new ScheduledFuture<?>[1];
            ScheduledFuture<?> timerTask = scheduler.scheduleAtFixedRate(() -> {
                try {
                    long elapsedSeconds = (System.currentTimeMillis() - finalConnectedAtMillis) / 1000;
                    sendCallStatusUpdate(comPort, targetPhone, "IN_CALL", (int) elapsedSeconds, null);
                } catch (Exception e) {
                    log.error("Error updating call timer: {}", e.getMessage());
                }
            }, 1, 1, TimeUnit.SECONDS);
            timerTaskRef[0] = timerTask;

            // ❌ DISABLED: Call monitor was causing calls to end early
            // The monitor was canceling the hangupTask when it detected call state changes,
            // causing calls to end before the configured duration.
            // The hangupTask is sufficient, and the modem will auto-detect if other party
            // hangs up.
            /*
             * // ✅ FIX: Monitor call state to detect early disconnect and cancel timer
             * ScheduledFuture<?> callMonitorTask = scheduler.scheduleAtFixedRate(() -> {
             * try {
             * String clccResponse = sendATCommand(finalPort, "AT+CLCC", 800);
             * if (clccResponse == null || !clccResponse.contains("+CLCC") ||
             * clccResponse.contains(",6,") || // stat=6 means DISCONNECTED
             * clccResponse.contains("NO CARRIER") ||
             * clccResponse.trim().equals("OK")) {
             * // Call has ended early - cancel timer and hangup task
             * log.info("📞 [{}] Call ended early, canceling timer and hangup task",
             * comPort);
             * if (timerTaskRef[0] != null) {
             * timerTaskRef[0].cancel(false);
             * }
             * ScheduledFuture<?> task = activeCalls.remove(comPort);
             * if (task != null) {
             * task.cancel(false);
             * }
             * // Clean up and update status
             * long actualDuration = (System.currentTimeMillis() - finalConnectedAtMillis) /
             * 1000;
             * updateCallEndTime(callId, (int) actualDuration);
             * updateCallStatus(callId, "COMPLETED", null);
             * sendCallStatusUpdate(comPort, targetPhone, "ENDED_SUCCESS", (int)
             * actualDuration,
             * "Cuộc gọi đã kết thúc");
             * log.info("✅ [{}] Call ended early - Duration: {}s", comPort, actualDuration);
             * }
             * } catch (Exception e) {
             * // Ignore errors in monitoring
             * }
             * }, 2, 2, TimeUnit.SECONDS);
             */

            // ✅ CRITICAL FIX: Schedule hangup IMMEDIATELY (before recording starts!)
            // This ensures call ends at exactly callDuration from connection
            ScheduledFuture<?> hangupTask = scheduler.schedule(() -> {
                try {
                    // Cancel timer and monitor
                    if (timerTaskRef[0] != null) {
                        timerTaskRef[0].cancel(false);
                    }
                    // callMonitorTask is now disabled - no need to cancel
                    // if (callMonitorTask != null) {
                    // callMonitorTask.cancel(false);
                    // }

                    long actualDuration = (System.currentTimeMillis() - finalConnectedAtMillis) / 1000;
                    // Cap duration at configured value
                    int maxDuration = callDuration > 0 ? callDuration : 30;
                    if (actualDuration > maxDuration) {
                        log.info("⏱️ [{}] Capping duration from {}s to {}s", comPort, actualDuration, maxDuration);
                        actualDuration = maxDuration;
                    }

                    // Notify ending
                    sendCallStatusUpdate(comPort, targetPhone, "ENDING", (int) actualDuration,
                            "Đang kết thúc cuộc gọi...");

                    // Get recording file from reference (may have been set by async recording)
                    String finalRecordingFile = recordingFileRef.get();

                    // Stop recording on the current port BEFORE hangup
                    // According to Quectel doc: AT+QAUDRD=0
                    if (shouldRecord && finalRecordingFile != null) {
                        stopRecordingOnPort(finalPort, comPort);
                    }

                    // Hangup: ATH
                    sendATCommand(finalPort, "ATH", 1000);
                    finalPort.closePort();

                    // ✅ UPDATE STATUS WITH APPROPRIATE ENDED STATUS (matching PortWorker)
                    final long reportedDuration = actualDuration;
                    updateCallEndTime(callId, (int) reportedDuration);
                    updateCallStatus(callId, "COMPLETED", null);
                    // ✅ Send ENDED_SUCCESS to match PortWorker status
                    sendCallStatusUpdate(comPort, targetPhone, "ENDED_SUCCESS", (int) reportedDuration,
                            "Cuộc gọi đã kết thúc thành công");
                    log.info("✅ [{}] Call to {} COMPLETED - Duration: {}s", comPort, targetPhone, reportedDuration);

                    // Download recording file ASYNC (don't block status update!)
                    if (shouldRecord && finalRecordingFile != null) {
                        final String finalServiceCode = serviceCode;
                        final String finalOrderId = orderId;
                        final Instant callStartTime = Instant.now().minusSeconds(reportedDuration);
                        final Instant callEndTime = Instant.now();
                        final String finalSimPhone = simPhone;

                        log.info("📥 [{}] Recording file path: {}, shouldRecord: {}", comPort, finalRecordingFile,
                                shouldRecord);

                        scheduler.submit(() -> {
                            try {
                                log.info("📥 [{}] Starting async recording download...", comPort);

                                // ✅ Send WebSocket notification if remote call
                                if (finalServiceCode != null && finalOrderId != null) {
                                    recordLocalCallStatus("DOWNLOADING_RECORDING", comPort, finalSimPhone,
                                            targetPhone, finalOrderId, finalServiceCode, 0,
                                            "Bắt đầu tải file ghi âm...");
                                }

                                sendCallStatusUpdate(comPort, targetPhone, "DOWNLOADING_RECORDING",
                                        (int) reportedDuration,
                                        "Đang tải file ghi âm...");

                                // Download with remote notification support
                                downloadRecordingFromModem(comPort, finalRecordingFile,
                                        targetPhone, finalOrderId, finalServiceCode, finalSimPhone);
                                log.info("✅ [{}] Recording download completed", comPort);

                                // ✅ If this is a remote call (has serviceCode/orderId), upload and send
                                // callback
                                if (finalServiceCode != null && finalOrderId != null) {
                                    // Find downloaded file in recordings directory
                                    String fileName = finalRecordingFile.replace("UFS:", "");
                                    Path recordingsPath = Paths.get(getRecordingPath());
                                    File downloadedFile = recordingsPath.resolve(fileName).toFile();

                                    String uploadedUrl = null;
                                    if (downloadedFile.exists()) {
                                        // ⚠️ BROWSER DOESN'T SUPPORT ALAW FORMAT
                                        // Keep original modem header (PCM) for browser compatibility
                                        // ✅ FIX WAV HEADER: Modem records ALAW but marks as PCM
                                        // Need to change audio format from 1 (PCM) to 6 (ALAW)
                                        // try {
                                        // byte[] fileData = Files.readAllBytes(downloadedFile.toPath());
                                        // if (fileData.length > 44 && fileData[0] == 'R' && fileData[1] == 'I') {
                                        // // Check if audio format is 1 (PCM) - bytes 20-21
                                        // int audioFormat = (fileData[21] & 0xFF) << 8 | (fileData[20] & 0xFF);
                                        // if (audioFormat == 1) {
                                        // log.info("🔧 [{}] Fixing WAV header: PCM → ALAW", comPort);
                                        //
                                        // // Change audio format to 6 (ALAW)
                                        // fileData[20] = 0x06; // Format code = 6 (ALAW)
                                        // fileData[21] = 0x00;
                                        //
                                        // // ALAW: 1 channel, 8000 Hz, 8 bits = 8000 bytes/sec
                                        // // Update byte rate (bytes 28-31)
                                        // int byteRate = 8000; // 8000 samples/sec * 1 byte/sample
                                        // fileData[28] = (byte) (byteRate & 0xFF);
                                        // fileData[29] = (byte) ((byteRate >> 8) & 0xFF);
                                        // fileData[30] = (byte) ((byteRate >> 16) & 0xFF);
                                        // fileData[31] = (byte) ((byteRate >> 24) & 0xFF);
                                        //
                                        // // Block align = 1 (bytes 32-33)
                                        // fileData[32] = 0x01;
                                        // fileData[33] = 0x00;
                                        //
                                        // // Bits per sample = 8 (bytes 34-35)
                                        // fileData[34] = 0x08;
                                        // fileData[35] = 0x00;
                                        //
                                        // // Write back
                                        // Files.write(downloadedFile.toPath(), fileData);
                                        // log.info("✅ [{}] WAV header fixed successfully", comPort);
                                        // }
                                        // }
                                        // } catch (Exception e) {
                                        // log.warn("⚠️ [{}] Failed to fix WAV header: {}", comPort, e.getMessage());
                                        // }

                                        // Upload to server
                                        uploadedUrl = uploadRecordingFile(downloadedFile.getAbsolutePath(),
                                                comPort, finalSimPhone, finalOrderId, finalServiceCode);
                                    } else {
                                        log.warn("⚠️ [{}] Downloaded file not found: {}", comPort, downloadedFile);
                                    }

                                    // Send remote callback (API + DB + WebSocket)
                                    saveLocalCallResult(comPort, finalSimPhone, targetPhone,
                                            callStartTime, callEndTime, uploadedUrl,
                                            finalOrderId, reportedDuration, true,
                                            null, finalServiceCode);
                                }

                            } catch (Exception e) {
                                log.error("❌ [{}] Failed to download recording: {}", comPort, e.getMessage());
                                sendCallStatusUpdate(comPort, "", "RECORDING_FAILED", 0,
                                        "Lỗi tải file ghi âm: " + e.getMessage());

                                // ✅ Send failure callback if remote call
                                if (finalServiceCode != null && finalOrderId != null) {
                                    saveLocalCallResult(comPort, finalSimPhone, targetPhone,
                                            callStartTime, callEndTime, null,
                                            finalOrderId, reportedDuration, true,
                                            "RECORDING_FAILED: " + e.getMessage(), finalServiceCode);
                                }
                            }
                        });
                    } else if (serviceCode != null && orderId != null) {
                        // ✅ No recording, but still send callback for remote calls
                        final String finalServiceCode = serviceCode;
                        final String finalOrderId = orderId;
                        final Instant callStartTime = Instant.now().minusSeconds(reportedDuration);
                        final Instant callEndTime = Instant.now();
                        final String finalSimPhone = simPhone;

                        scheduler.submit(() -> {
                            saveLocalCallResult(comPort, finalSimPhone, targetPhone,
                                    callStartTime, callEndTime, null,
                                    finalOrderId, reportedDuration, true,
                                    null, finalServiceCode);
                        });
                    }

                } catch (Exception e) {
                    log.error("Error on auto-hangup: {}", e.getMessage());
                    // Even on error, update status
                    try {
                        updateCallStatus(callId, "FAILED", e.getMessage());
                        sendCallStatusUpdate(comPort, targetPhone, "FAILED", 0, "Lỗi: " + e.getMessage());
                    } catch (Exception ex) {
                        log.error("Failed to update status: {}", ex.getMessage());
                    }

                    // ✅ FIX: Cố gắng recovery recording khi hangup task bị lỗi
                    String errorRecordingFile = recordingFileRef.get();
                    if (shouldRecord && errorRecordingFile != null) {
                        log.warn("⚠️ [{}] Hangup task failed but recording exists. Attempting recovery...", comPort);
                        final String errServiceCode = serviceCode;
                        final String errOrderId = orderId;
                        final String errSimPhone = simPhone;
                        scheduler.submit(() -> {
                            try {
                                // Stop recording nếu chưa stop
                                try {
                                    stopRecordingOnPort(finalPort, comPort);
                                } catch (Exception ignored) {
                                }
                                // Hangup nếu chưa
                                try {
                                    sendATCommand(finalPort, "ATH", 500);
                                } catch (Exception ignored) {
                                }
                                try {
                                    finalPort.closePort();
                                } catch (Exception ignored) {
                                }

                                log.info("📥 [{}] Starting error recovery recording download...", comPort);
                                downloadRecordingFromModem(comPort, errorRecordingFile,
                                        targetPhone, errOrderId, errServiceCode, errSimPhone);
                                log.info("✅ [{}] Error recovery recording download completed", comPort);

                                // Upload và callback nếu là remote call
                                if (errServiceCode != null && errOrderId != null) {
                                    String fileName = errorRecordingFile.replace("UFS:", "");
                                    Path recordingsPath = Paths.get(getRecordingPath());
                                    File downloadedFile = recordingsPath.resolve(fileName).toFile();

                                    String uploadedUrl = null;
                                    if (downloadedFile.exists()) {
                                        uploadedUrl = uploadRecordingFile(downloadedFile.getAbsolutePath(),
                                                comPort, errSimPhone, errOrderId, errServiceCode);
                                    }
                                    saveLocalCallResult(comPort, errSimPhone, targetPhone,
                                            Instant.now().minusSeconds(30), Instant.now(), uploadedUrl,
                                            errOrderId, 0, true,
                                            "HANGUP_ERROR_WITH_RECORDING", errServiceCode);
                                }
                            } catch (Exception recEx) {
                                log.error("❌ [{}] Error recovery recording download failed: {}", comPort,
                                        recEx.getMessage());
                            }
                        });
                    }
                }
            }, remainingMs, TimeUnit.MILLISECONDS);

            activeCalls.put(comPort, hangupTask);

            // ✅ NOW start recording (if not already started in loop!)
            if (shouldRecord && recordingStarted.compareAndSet(false, true)) {
                sendCallStatusUpdate(comPort, targetPhone, "RECORDING_START", 0, "Đang bật ghi âm...");
                recordingFile = startRecordingOnPort(port, comPort);
                recordingFileRef.set(recordingFile); // Store for hangup task to use
                updateCallRecordingPath(callId, recordingFile);
                log.info("🎙️ [{}] Recording started: {}", comPort, recordingFile);
            }

        } catch (Exception e) {
            log.error("❌ Call error: {}", e.getMessage());
            updateCallStatus(callId, "FAILED", e.getMessage());
            sendCallStatusUpdate(comPort, targetPhone, "FAILED", 0, "Lỗi: " + e.getMessage());

            // ✅ FIX: Cố gắng recovery recording khi cuộc gọi bị lỗi giữa chừng
            // Dùng recordingFile (method scope) - sẽ non-null nếu recording đã bắt đầu
            final boolean wasConnectedFinal = wasConnected;
            if (shouldRecord && recordingFile != null) {
                log.warn("⚠️ [{}] Call error with active recording! Attempting recovery...", comPort);
                try {
                    // Stop recording trước
                    if (port != null && port.isOpen()) {
                        try {
                            stopRecordingOnPort(port, comPort);
                        } catch (Exception stopEx) {
                            log.warn("⚠️ [{}] Failed to stop recording: {}", comPort, stopEx.getMessage());
                        }
                        // Hangup
                        try {
                            sendATCommand(port, "ATH", 500);
                        } catch (Exception athEx) {
                            log.warn("⚠️ [{}] ATH failed: {}", comPort, athEx.getMessage());
                        }
                        port.closePort();
                    }

                    // Download recording file async
                    final String errRecFile = recordingFile;
                    final String errServiceCode = serviceCode;
                    final String errOrderId = orderId;
                    final String errSimPhone = simPhone;
                    scheduler.submit(() -> {
                        try {
                            log.info("📥 [{}] Starting error recovery recording download...", comPort);
                            sendCallStatusUpdate(comPort, targetPhone, "DOWNLOADING_RECORDING", 0,
                                    "Đang tải file ghi âm (recovery)...");

                            downloadRecordingFromModem(comPort, errRecFile,
                                    targetPhone, errOrderId, errServiceCode, errSimPhone);
                            log.info("✅ [{}] Error recovery recording download completed", comPort);

                            // Upload và callback nếu là remote call
                            if (errServiceCode != null && errOrderId != null) {
                                String fileName = errRecFile.replace("UFS:", "");
                                Path recordingsPath = Paths.get(getRecordingPath());
                                File downloadedFile = recordingsPath.resolve(fileName).toFile();

                                String uploadedUrl = null;
                                if (downloadedFile.exists()) {
                                    uploadedUrl = uploadRecordingFile(downloadedFile.getAbsolutePath(),
                                            comPort, errSimPhone, errOrderId, errServiceCode);
                                }
                                saveLocalCallResult(comPort, errSimPhone, targetPhone,
                                        Instant.now().minusSeconds(30), Instant.now(), uploadedUrl,
                                        errOrderId, 0, wasConnectedFinal,
                                        "CALL_ERROR_WITH_RECORDING", errServiceCode);
                            }
                        } catch (Exception recEx) {
                            log.error("❌ [{}] Error recovery recording download failed: {}", comPort,
                                    recEx.getMessage());
                            // Gửi callback lỗi nếu là remote call
                            if (errServiceCode != null && errOrderId != null) {
                                saveLocalCallResult(comPort, errSimPhone, targetPhone,
                                        Instant.now().minusSeconds(30), Instant.now(), null,
                                        errOrderId, 0, wasConnectedFinal,
                                        "RECORDING_RECOVERY_FAILED: " + recEx.getMessage(), errServiceCode);
                            }
                        }
                    });
                } catch (Exception recoverEx) {
                    log.error("❌ [{}] Recording recovery setup failed: {}", comPort, recoverEx.getMessage());
                    if (port != null && port.isOpen()) {
                        port.closePort();
                    }
                }
            } else {
                if (port != null && port.isOpen()) {
                    port.closePort();
                }
            }
        } finally {
            // ✅ FIX: LUÔN unlock port trước tiên
            comManager.unlockPort(comPort);

            // ✅ FIX: Khởi động lại PortWorker sau khi call xong để lắng nghe URC
            // Điều này đảm bảo hệ thống vẫn nhận được cuộc gọi đến và SMS sau khi call xong
            try {
                // Đợi một chút để port được giải phóng hoàn toàn
                Thread.sleep(300);

                // Tìm SIM theo comPort để khởi động lại worker
                Optional<Sim> simOpt = simRepository.findFirstByComName(comPort);
                if (simOpt.isPresent()) {
                    Sim sim = simOpt.get();
                    // Kiểm tra xem SIM có còn ACTIVE không
                    if ("ACTIVE".equals(sim.getStatus())) {
                        // Kiểm tra xem worker đã đang chạy chưa
                        PortWorker existingWorker = comManager.getWorker(comPort);
                        if (existingWorker == null || !existingWorker.isRunning()) {
                            log.info("🔄 Khởi động lại PortWorker cho {} để lắng nghe URC sau khi call xong",
                                    comPort);
                            comManager.startWorker(sim);
                        } else {
                            log.debug("✅ PortWorker cho {} đã đang chạy, không cần khởi động lại",
                                    comPort);
                        }
                    }
                } else {
                    log.debug("⚠️ Không tìm thấy SIM cho {} để khởi động lại PortWorker", comPort);
                }
            } catch (Exception e) {
                log.warn("⚠️ Không thể khởi động lại PortWorker sau khi call xong: {}", e.getMessage());
            }
        }
    }

    /**
     * ✅ Wrapper method to match PortWorker signature
     * Delegates to existing startRecordingOnPort implementation
     */
    private String startRecording(SerialPort port, String comPort, String targetPhone) {
        return startRecordingOnPort(port, comPort);
    }

    /**
     * Start recording using AT+QAUDRD command on existing port
     * According to Quectel GSM Recording AT Commands Manual:
     * - AT+QAUDRD=1,"filename",format - Start recording
     * - Format: 3=AMR, 13=WAV_PCM16, 16=WAV_ADPCM
     * - When module is on calling, WAV_ADPCM (16) is recommended
     * - Recording auto-stops when call ends (URC: +QAUDRIND: 0,<code>)
     * 
     * @param port    The existing SerialPort from the call
     * @param comPort COM port name for logging
     * @return modem file path if successful, null otherwise
     */
    private String startRecordingOnPort(SerialPort port, String comPort) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String modemFileName = "call_" + comPort.replace("COM", "").replace("/dev/tty", "") + "_" + timestamp;

        // According to Quectel GSM Recording Manual:
        // - WAV_PCM16 (13) - 960KB/min - best quality BUT very large file, slow
        // download
        // - WAV_ALAW (14) - 480KB/min - BEST for telephony (European standard, clearer
        // voice)
        // - WAV_ULAW (15) - 480KB/min - good quality, telephony standard (US/Japan)
        // - WAV_ADPCM (16) - 240KB/min - good balance but may cause distortion on some
        // modems
        // - AMR (3) - 40KB/min - smallest BUT lowest quality, causes noise/distortion
        //
        // ✅ PRIORITY OPTIMIZED FOR THIS MODEM:
        // Try WAV_PCM16 FIRST because it's the only format that works consistently.
        // This reduces initialization time from 14s to ~3s, ensuring full call
        // recording.
        int[] formats = { 13, 14, 15, 16, 3 }; // PCM16 first!
        String[] extensions = { ".wav", ".wav", ".wav", ".wav", ".amr" };

        try {
            // First, check current recording status and stop if active
            String currentStatus = sendATCommand(port, "AT+QAUDRD?", 500);
            log.info("📊 [{}] Current recording status: {}", comPort, currentStatus.trim());
            if (currentStatus.contains("+QAUDRD: 1")) {
                log.info("⚠️ [{}] Recording already active, stopping first...", comPort);
                sendATCommand(port, "AT+QAUDRD=0", 1000);
                Thread.sleep(500);
            }

            // ✅ Identify modem type EARLY to send correct config commands
            String modemInfo = sendATCommand(port, "ATI", 1000).toUpperCase();
            boolean isEC2x = modemInfo.contains("EC25") || modemInfo.contains("EC21");
            log.info("📱 [{}] Modem Identification: {} (isEC2x: {})", comPort, modemInfo.trim(), isEC2x);

            if (isEC2x) {
                // ✅ FORCED CONFIGS FOR EC25 "No Sound" problem
                log.info("🔊 [{}] Configuring EC25 digital audio path...", comPort);
                sendATCommand(port, "AT+QAUDCFG=\"record\",1", 500);
                sendATCommand(port, "AT+QAUDCFG=\"audiopath\",1", 500); // 1 = Headset/PCM path (better for recording)
                sendATCommand(port, "AT+QSIDET=0", 500);
                sendATCommand(port, "AT+QMIC=0,10", 500);
            } else {
                // Legacy GSM logic (M95, M66)
                log.info("🔊 [{}] Configuring legacy audio channel...", comPort);
                sendATCommand(port, "AT+QAUDCH=0", 500);
            }

            String[] ec2xExt = { ".wav", ".amr" };
            // ✅ PRE-RECORDING CLEANUP: Delete potential duplicate files on modem
            log.info("🗑️ [{}] Checking and deleting existing recording files before start...", comPort);
            for (String ext : ec2xExt) {
                sendATCommand(port, "AT+QFDEL=\"UFS:" + modemFileName + ext + "\"", 300);
            }

            if (isEC2x) {
                int[] ec2xFormats = { 13, 16, 3 };
                String[] ec2xExtFull = { ".wav", ".wav", ".amr" };

                // ✅ FIRST: Try 4-parameter syntax (format + channel)
                for (int i = 0; i < ec2xFormats.length; i++) {
                    String modemPath = modemFileName + ec2xExtFull[i];
                    // ✅ CRITICAL: On EC25, MUST use 4th parameter (channel=2) to get sound!
                    String recordCmd = String.format("AT+QAUDRD=1,\"%s\",%d,2", modemPath, ec2xFormats[i]);
                    log.info("🎙️ [{}] EC25 recording attempt (Mixed): {}", comPort, recordCmd);

                    String response = sendATCommand(port, recordCmd, 3000);
                    if (response.contains("OK") && !response.contains("ERROR")) {
                        log.info("✅ [{}] EC25 Recording STARTED (format={}, channel=2)", comPort, ec2xFormats[i]);
                        return modemPath;
                    } else {
                        log.warn("⚠️ [{}] EC25 format {} (4-param) failed: {}", comPort, ec2xFormats[i],
                                response.replace("\r", " ").replace("\n", " ").trim());
                    }
                }

                // ✅ FALLBACK: Try 3-parameter syntax (some EC25 firmware doesn't support
                // channel param)
                log.info("🔄 [{}] Trying 3-parameter syntax (no channel)...", comPort);

                // ✅ Configure audio gains for 3-param mode to prioritize downlink (remote
                // party)
                log.info("🔊 [{}] Configuring audio for downlink priority (remote voice only)...", comPort);
                try {
                    // Maximize RX gain (downlink - remote party voice)
                    sendATCommand(port, "AT+QAUDCFG=\"rxgain\",0,15", 500);
                    // Minimize TX gain (uplink - local SIM voice)
                    sendATCommand(port, "AT+QAUDCFG=\"txgain\",0,0", 500);
                    log.info("✅ [{}] Audio configured: RX=15 (max), TX=0 (muted)", comPort);
                } catch (Exception e) {
                    log.warn("⚠️ [{}] Failed to configure audio gains: {}", comPort, e.getMessage());
                }

                for (int i = 0; i < ec2xFormats.length; i++) {
                    String modemPath = modemFileName + ec2xExtFull[i];
                    String recordCmd = String.format("AT+QAUDRD=1,\"%s\",%d", modemPath, ec2xFormats[i]);
                    log.info("🎙️ [{}] EC25 recording attempt (3-param): {}", comPort, recordCmd);

                    String response = sendATCommand(port, recordCmd, 3000);
                    if (response.contains("OK") && !response.contains("ERROR")) {
                        log.info("✅ [{}] EC25 Recording STARTED (format={}, 3-param, downlink-only)", comPort,
                                ec2xFormats[i]);
                        return modemPath;
                    } else {
                        log.warn("⚠️ [{}] EC25 format {} (3-param) failed: {}", comPort, ec2xFormats[i],
                                response.replace("\r", " ").replace("\n", " ").trim());
                    }
                }

                log.error("❌ [{}] EC25 recording failed with all formats (tried 4-param and 3-param).", comPort);
                return null;
            }

            // Fallback for Legacy GSM (GSM-only modems like M95)
            for (int i = 0; i < formats.length; i++) {
                String fullFileName = modemFileName + extensions[i];
                String modemPath = fullFileName;

                String recordCmd = String.format("AT+QAUDRD=1,\"%s\",%d", modemPath, formats[i]);
                log.info("🎙️ [{}] Legacy recording attempt: {}", comPort, recordCmd);

                String response = sendATCommand(port, recordCmd, 3000);
                if (response.contains("OK") && !response.contains("ERROR")) {
                    log.info("✅ [{}] Legacy Recording STARTED successfully", comPort);
                    return modemPath;
                }
            }

            log.error(
                    "❌ [{}] Failed to start recording with ALL formats (tried: WAV_PCM16, WAV_ADPCM, WAV_ALAW, WAV_ULAW, AMR)",
                    comPort);
            return null;

        } catch (Exception e) {
            log.error("❌ [{}] Error starting recording: {}", comPort, e.getMessage(), e);
            return null;
        }
    }

    /**
     * 🆕 Delete old recording files on modem before starting new recording
     * This prevents downloading old cached files
     * Deletes ALL .wav and .amr files on modem storage (not just for this COM port)
     * 
     * @param port    The existing SerialPort from the call
     * @param comPort COM port name for logging
     */
    private void deleteOldRecordingFilesOnModem(SerialPort port, String comPort) {
        try {
            log.info("🗑️ [{}] === BẮT ĐẦU XÓA FILE CŨ TRÊN MODEM ===", comPort);

            // Retry up to 3 times to ensure all files are deleted
            for (int attempt = 1; attempt <= 3; attempt++) {
                log.info("🔄 [{}] Lần thử {}/3: Liệt kê files trên modem...", comPort, attempt);

                // List all files on modem - try multiple formats
                String listResponse = sendATCommand(port, "AT+QFLST=\"UFS:*\"", 3000);
                log.info("📋 [{}] AT+QFLST=\"UFS:*\" response: {}", comPort,
                        listResponse.replace("\r", "\\r").replace("\n", "\\n").trim());

                if (listResponse.contains("ERROR") || !listResponse.contains("+QFLST")) {
                    // Try without filter
                    listResponse = sendATCommand(port, "AT+QFLST", 3000);
                    log.info("📋 [{}] AT+QFLST response: {}", comPort,
                            listResponse.replace("\r", "\\r").replace("\n", "\\n").trim());
                }

                if (!listResponse.contains("+QFLST")) {
                    log.info("✅ [{}] Không còn file nào trên modem storage", comPort);
                    break; // No files left, exit loop
                }

                // Parse file list and delete ALL recording files (.wav and .amr)
                // Format: +QFLST: "UFS:filename.wav",12345
                Pattern filePattern = Pattern.compile("\\+QFLST:\\s*\"([^\"]+)\"\\s*,\\s*(\\d+)");
                Matcher matcher = filePattern.matcher(listResponse);

                List<String> filesToDelete = new ArrayList<>();
                while (matcher.find()) {
                    String fileName = matcher.group(1);
                    String fileSize = matcher.group(2);

                    // Get just the filename without UFS: prefix for extension check
                    String pureFileName = fileName.replace("UFS:", "").toLowerCase();

                    // Delete ALL .wav and .amr files
                    if (pureFileName.endsWith(".wav") || pureFileName.endsWith(".amr")) {
                        log.info("📁 [{}] Tìm thấy file cần xóa: {} ({} bytes)", comPort, fileName, fileSize);
                        filesToDelete.add(fileName);
                    }
                }

                if (filesToDelete.isEmpty()) {
                    log.info("✅ [{}] Không có file .wav/.amr nào trên modem", comPort);
                    break; // No audio files, exit loop
                }

                // Delete each file
                int deletedCount = 0;
                for (String fileName : filesToDelete) {
                    // Normalize path for deletion - always use UFS: prefix
                    String deletePath = fileName;
                    if (!deletePath.toUpperCase().startsWith("UFS:")) {
                        deletePath = "UFS:" + deletePath;
                    }

                    log.info("🗑️ [{}] Đang xóa: {}", comPort, deletePath);
                    String deleteResp = sendATCommand(port, "AT+QFDEL=\"" + deletePath + "\"", 2000);
                    log.info("📝 [{}] Kết quả xóa {}: {}", comPort, fileName,
                            deleteResp.replace("\r", "\\r").replace("\n", "\\n").trim());

                    if (deleteResp.contains("OK") && !deleteResp.contains("ERROR")) {
                        log.info("✅ [{}] Đã xóa: {}", comPort, fileName);
                        deletedCount++;
                    } else {
                        log.warn("⚠️ [{}] Không thể xóa {}: {}", comPort, fileName, deleteResp.trim());
                    }

                    Thread.sleep(300); // Delay between deletions
                }

                log.info("📊 [{}] Lần {}: Đã xóa {}/{} files", comPort, attempt, deletedCount, filesToDelete.size());

                // If all files deleted, verify by listing again
                if (deletedCount == filesToDelete.size()) {
                    Thread.sleep(500); // Wait for modem to process

                    // Verify deletion
                    String verifyResponse = sendATCommand(port, "AT+QFLST=\"UFS:*\"", 2000);
                    log.info("🔍 [{}] Xác nhận sau xóa: {}", comPort,
                            verifyResponse.replace("\r", "\\r").replace("\n", "\\n").trim());

                    if (!verifyResponse.contains(".wav") && !verifyResponse.contains(".amr")) {
                        log.info("✅ [{}] === XÁC NHẬN: ĐÃ XÓA SẠCH FILE AUDIO TRÊN MODEM ===", comPort);
                        break;
                    }
                }

                // Small delay before next attempt
                Thread.sleep(500);
            }

        } catch (Exception e) {
            log.error("❌ [{}] Lỗi khi xóa file cũ: {}", comPort, e.getMessage(), e);
            // Don't throw - continue with recording even if cleanup fails
        }
    }

    /**
     * Stop recording on existing port
     * According to Quectel documentation:
     * - AT+QAUDRD=0 - Stop recording
     * - Recording also auto-stops when call ends (URC: +QAUDRIND: 0,<code>)
     * 
     * @param port    The existing SerialPort from the call
     * @param comPort COM port name for logging
     */
    private void stopRecordingOnPort(SerialPort port, String comPort) {
        log.info("🛑 [{}] Stopping recording...", comPort);

        try {
            // Check if recording is active
            // According to doc: AT+QAUDRD? returns +QAUDRD: <state> (0=not recording,
            // 1=recording)
            String status = sendATCommand(port, "AT+QAUDRD?", 500);
            log.info("📊 [{}] Recording status: {}", comPort, status);

            if (status.contains("+QAUDRD: 1")) {
                // Stop recording: AT+QAUDRD=0
                String stopResp = sendATCommand(port, "AT+QAUDRD=0", 2000);
                if (stopResp.contains("OK")) {
                    log.info("✅ [{}] Recording stopped successfully", comPort);
                } else {
                    log.warn("⚠️ [{}] Stop recording response: {}", comPort, stopResp);
                }
            } else {
                log.info("ℹ️ [{}] Recording already stopped (auto-stop on call end)", comPort);
            }

            // Wait for modem to finalize file
            Thread.sleep(500);

        } catch (Exception e) {
            log.error("❌ [{}] Error stopping recording: {}", comPort, e.getMessage());
        }
    }

    /**
     * Download recording file from modem storage to local disk
     * Uses Quectel file commands: AT+QFLST, AT+QFOPEN, AT+QFREAD, AT+QFCLOSE
     */
    @SuppressWarnings("unused")
    private void downloadRecordingFromModem(String comPort, String modemFilePath) throws Exception {
        downloadRecordingFromModem(comPort, modemFilePath, null, null, null, null);
    }

    /**
     * Download recording file from modem storage to local disk with remote
     * notification support
     * Uses Quectel file commands: AT+QFLST, AT+QFOPEN, AT+QFREAD, AT+QFCLOSE
     * 
     * @param comPort       COM port
     * @param modemFilePath Modem file path
     * @param targetPhone   Target phone (for remote notification)
     * @param orderId       Order ID (for remote notification)
     * @param serviceCode   Service code (for remote notification)
     * @param simPhone      SIM phone (for remote notification)
     */
    private void downloadRecordingFromModem(String comPort, String modemFilePath,
            String targetPhone, String orderId, String serviceCode, String simPhone) throws Exception {
        SerialPort port = null;
        Integer fileHandle = null;

        try {
            // 🔧 FIX: Resolve COM port through PortResolver
            String resolvedPort = portResolver.resolve(comPort);
            if (resolvedPort == null) {
                log.error("❌ Cannot resolve port mapping for {}", comPort);
                return;
            }

            port = SerialPort.getCommPort(resolvedPort);
            port.setComPortParameters(115200, 8, 1, 0);
            port.setComPortTimeouts(SerialPort.TIMEOUT_READ_SEMI_BLOCKING, 5000, 5000);

            if (!port.openPort()) {
                log.error("❌ Cannot open port {} for recording download", resolvedPort);
                return;
            }

            Thread.sleep(300);

            // Step 1: List files on modem to find recording
            log.info("📂 [{}] Searching for recording file: {}", comPort, modemFilePath);

            // Try different AT+QFLST formats
            String listResponse = sendATCommand(port, "AT+QFLST=\"UFS:*\"", 3000);
            log.info("📋 [{}] AT+QFLST response: {}", comPort, listResponse);

            if (listResponse.contains("ERROR") || !listResponse.contains("+QFLST")) {
                // Try without filter
                listResponse = sendATCommand(port, "AT+QFLST", 3000);
                log.info("📋 [{}] AT+QFLST (no filter) response: {}", comPort, listResponse);
            }

            // Extract just the filename without UFS: prefix for matching
            String searchFileName = modemFilePath.replace("UFS:", "");
            long fileSize = parseFileSize(listResponse, searchFileName);

            // Also try with the full path
            if (fileSize == 0) {
                fileSize = parseFileSize(listResponse, modemFilePath);
            }

            if (fileSize == 0) {
                log.error("❌ [{}] Recording file NOT FOUND on modem!", comPort);
                log.error("❌ [{}] Expected file: {}", comPort, modemFilePath);
                log.error("❌ [{}] Modem file list: {}", comPort, listResponse);
                sendCallStatusUpdate(comPort, "", "RECORDING_FAILED", 0,
                        "File ghi âm không tồn tại trên modem");
                return;
            }

            log.info("✅ [{}] File found! Size: {} bytes ({} KB)", comPort, fileSize, fileSize / 1024);

            // Step 2: Normalize file path and Open file on modem
            // Add UFS: prefix if not present (needed for QFOPEN)
            String normalizedPath = modemFilePath;
            if (!normalizedPath.toUpperCase().startsWith("UFS:") &&
                    !normalizedPath.toUpperCase().startsWith("RAM:") &&
                    !normalizedPath.toUpperCase().startsWith("SD:")) {
                normalizedPath = "UFS:" + normalizedPath;
            }

            String openCmd = "AT+QFOPEN=\"" + normalizedPath + "\",0";
            log.info("📂 [{}] Opening file: {}", comPort, openCmd);
            String openResp = sendATCommand(port, openCmd, 2000);

            // Parse file handle from response: +QFOPEN: <handle>
            java.util.regex.Matcher handleMatcher = java.util.regex.Pattern
                    .compile("\\+QFOPEN:\\s*(\\d+)")
                    .matcher(openResp);

            if (!handleMatcher.find()) {
                log.error("❌ [{}] Cannot open file on modem: {}", comPort, openResp);
                return;
            }

            fileHandle = Integer.parseInt(handleMatcher.group(1));
            log.info("📂 [{}] File opened with handle: {}", comPort, fileHandle);

            // Wait for file to be ready
            Thread.sleep(500);

            // Seek to beginning of file (important!)
            String seekCmd = "AT+QFSEEK=" + fileHandle + ",0,0"; // handle, offset=0, origin=0 (beginning)
            String seekResp = sendATCommand(port, seekCmd, 1000);
            log.info("📍 [{}] Seek response: {}", comPort, seekResp.trim());

            // Step 3: Read file in chunks using InputStream for better binary handling
            String localFileName = modemFilePath.replace("UFS:", "");
            log.info("📂 [{}] Preparing local path: {} / {}", comPort, getRecordingPath(), localFileName);

            Path localPath = Paths.get(getRecordingPath(), localFileName);
            log.info("📂 [{}] Full path: {}", comPort, localPath.toAbsolutePath());

            Files.createDirectories(localPath.getParent());
            log.info("📂 [{}] Directory created/exists", comPort);

            ByteArrayOutputStream audioData = new ByteArrayOutputStream((int) fileSize);
            long totalRead = 0;
            long remaining = fileSize;
            int chunkSize = 10240; // 10KB - maximum supported by Quectel AT+QFREAD
            int consecutiveFailures = 0;
            int maxFailures = 10; // Increased tolerance
            long downloadStartTime = System.currentTimeMillis();
            long downloadTimeout = 5 * 60 * 1000; // 5 minutes max timeout
            int lastLoggedPercent = -1;

            log.info("📥 [{}] Starting download: {} bytes in {} chunks (chunkSize={})",
                    comPort, fileSize, fileSize / chunkSize + 1, chunkSize);
            sendCallStatusUpdate(comPort, "", "RECORDING_DOWNLOAD", 0, "Đang tải file ghi âm...");

            int chunkCount = 0;
            while (remaining > 0 && consecutiveFailures < maxFailures) {
                // Check overall timeout
                if (System.currentTimeMillis() - downloadStartTime > downloadTimeout) {
                    log.error("❌ [{}] Download timeout after {} seconds", comPort,
                            (System.currentTimeMillis() - downloadStartTime) / 1000);
                    break;
                }

                chunkCount++;
                int bytesToRead = (int) Math.min(chunkSize, remaining);

                // Send read command
                String readCmd = "AT+QFREAD=" + fileHandle + "," + bytesToRead;

                // Log every 10 chunks for visibility
                if (chunkCount <= 5 || chunkCount % 20 == 0) {
                    log.info("📤 [{}] Chunk {}/{}: reading {} bytes", comPort, chunkCount,
                            (fileSize / chunkSize + 1), bytesToRead);
                }

                port.writeBytes((readCmd + "\r").getBytes(), (readCmd + "\r").length());

                Thread.sleep(80); // Slightly increased for stability

                // Wait for CONNECT - after CONNECT, binary data follows immediately
                ByteArrayOutputStream chunkBuffer = new ByteArrayOutputStream();
                long deadline = System.currentTimeMillis() + 5000;
                boolean gotConnect = false;
                boolean hasError = false;

                while (System.currentTimeMillis() < deadline) {
                    int available = port.bytesAvailable();
                    if (available > 0) {
                        byte[] buf = new byte[available];
                        int read = port.readBytes(buf, available);
                        if (read > 0) {
                            chunkBuffer.write(buf, 0, read);
                        }

                        String currentData = chunkBuffer.toString();

                        if (currentData.contains("ERROR")) {
                            log.warn("⚠️ [{}] Chunk {} error: {}", comPort, chunkCount,
                                    currentData.replace("\r", " ").replace("\n", " ").trim());
                            consecutiveFailures++;
                            hasError = true;
                            break;
                        }

                        if (currentData.contains("CONNECT")) {
                            gotConnect = true;
                            // Continue reading until we have enough data or hit OK
                            if (chunkBuffer.size() >= bytesToRead + 20 || currentData.contains("\r\nOK")) {
                                break;
                            }
                        }
                    } else {
                        Thread.sleep(10);
                    }
                }

                if (hasError || !gotConnect) {
                    if (!gotConnect) {
                        consecutiveFailures++;
                        log.warn("⚠️ [{}] Chunk {} no CONNECT after 5s (failures: {}/{})",
                                comPort, chunkCount, consecutiveFailures, maxFailures);
                    }
                    continue;
                }

                // Extract binary data after CONNECT
                byte[] rawData = chunkBuffer.toByteArray();

                // Find CONNECT position - format: "CONNECT\r\n<binary data>\r\nOK"
                // We need to find the exact start of binary data after "CONNECT\r\n"
                int connectPos = -1;
                for (int i = 0; i < rawData.length - 7; i++) {
                    if (rawData[i] == 'C' && rawData[i + 1] == 'O' && rawData[i + 2] == 'N' &&
                            rawData[i + 3] == 'N' && rawData[i + 4] == 'E' && rawData[i + 5] == 'C'
                            && rawData[i + 6] == 'T') {
                        connectPos = i;
                        break;
                    }
                }

                if (connectPos >= 0) {
                    // ✅ Find CONNECT position and skip to binary data
                    // ACTUAL format from EC25: "CONNECT 10240\r\n<binary data>\r\nOK\r\n"
                    // We need to skip: "CONNECT" + " " + "10240" + "\r\n"
                    int dataStart = connectPos + 7; // After "CONNECT"

                    // Skip space after CONNECT
                    while (dataStart < rawData.length && rawData[dataStart] == ' ') {
                        dataStart++;
                    }

                    // Skip the byte count number (e.g., "10240")
                    while (dataStart < rawData.length &&
                            rawData[dataStart] >= '0' && rawData[dataStart] <= '9') {
                        dataStart++;
                    }

                    // Skip \r\n after the byte count
                    while (dataStart < rawData.length &&
                            (rawData[dataStart] == '\r' || rawData[dataStart] == '\n')) {
                        dataStart++;
                    }

                    // ✅ According to Quectel documentation:
                    // After CONNECT\r\n, modem sends EXACTLY bytesToRead bytes of binary data,
                    // followed by \r\nOK\r\n

                    // Calculate how many bytes we can safely extract
                    int availableData = rawData.length - dataStart;

                    // ✅ CRITICAL FIX: Always take EXACTLY bytesToRead bytes (no more, no less)
                    // This prevents audio corruption from including trailing "OK" text
                    int dataToWrite = Math.min(bytesToRead, availableData);

                    if (dataToWrite > 0) {
                        // ✅ Write pure binary audio data (no text contamination)
                        audioData.write(rawData, dataStart, dataToWrite);
                        totalRead += dataToWrite;
                        remaining -= dataToWrite;
                        consecutiveFailures = 0;

                        // Debug logging for first few chunks
                        if (chunkCount <= 5) {
                            log.info("✅ [{}] Chunk {} read {} bytes (total: {})",
                                    comPort, chunkCount, dataToWrite, totalRead);
                        }

                        // Progress update
                        int percent = (int) ((totalRead * 100) / fileSize);
                        if (percent / 10 > lastLoggedPercent / 10) {
                            lastLoggedPercent = percent;
                            log.info("📥 [{}] Download progress: {}% ({}/{} bytes)",
                                    comPort, percent, totalRead, fileSize);

                            String progressMsg = String.format("Đang tải: %d%% (%d/%d KB)",
                                    percent, totalRead / 1024, fileSize / 1024);

                            // Send to local WebSocket
                            sendCallStatusUpdate(comPort, "", "RECORDING_DOWNLOAD", percent, progressMsg);

                            // Send to remote server if this is a remote call
                            if (orderId != null && serviceCode != null && targetPhone != null) {
                                recordLocalCallStatus("RECORDING_DOWNLOAD", comPort, simPhone, targetPhone,
                                        orderId, serviceCode, percent, progressMsg);
                            }
                        }
                    } else {
                        log.warn("⚠️ [{}] Chunk {} has no data to write (available: {}, requested: {})",
                                comPort, chunkCount, availableData, bytesToRead);
                    }
                }

                // Small delay before next chunk
                Thread.sleep(50);
            }

            // Log final status
            long downloadDuration = (System.currentTimeMillis() - downloadStartTime) / 1000;
            log.info("📥 [{}] Download finished: {} bytes in {}s (speed: {} KB/s)",
                    comPort, totalRead, downloadDuration,
                    downloadDuration > 0 ? totalRead / 1024 / downloadDuration : 0);

            // Step 4: Close file on modem
            if (fileHandle != null) {
                sendATCommand(port, "AT+QFCLOSE=" + fileHandle, 1000);
                log.info("📂 [{}] File handle closed", comPort);
            }

            // Step 5: Save to local file
            if (audioData.size() > 0) {
                sendCallStatusUpdate(comPort, "", "RECORDING_SAVE", 100, "Đang lưu file ghi âm...");

                byte[] rawAudio = audioData.toByteArray();

                // Verify and log WAV header info (for debugging only)
                // DO NOT modify the audio data - Quectel creates valid WAV files
                // Modifying the header will cause audio corruption (clicking sounds)
                verifyAndLogWavHeader(rawAudio, comPort, localFileName);

                Files.write(localPath, rawAudio);
                log.info("💾 [{}] Saved recording: {} ({} bytes)", comPort, localPath, rawAudio.length);

                // Step 6: Delete from modem storage (use normalizedPath with UFS: prefix)
                sendCallStatusUpdate(comPort, "", "RECORDING_CLEANUP", 100, "Đang dọn dẹp bộ nhớ modem...");
                sendATCommand(port, "AT+QFDEL=\"" + normalizedPath + "\"", 1000);
                log.info("🗑️ [{}] Deleted from modem storage: {}", comPort, normalizedPath);

                sendCallStatusUpdate(comPort, "", "RECORDING_COMPLETE", 100, "Hoàn tất ghi âm!");
            } else {
                log.warn("⚠️ [{}] No data received from modem", comPort);
                sendCallStatusUpdate(comPort, "", "RECORDING_FAILED", 0, "Không nhận được dữ liệu");
            }

        } finally

        {
            // Ensure file handle is closed
            if (fileHandle != null && port != null && port.isOpen()) {
                try {
                    sendATCommand(port, "AT+QFCLOSE=" + fileHandle, 500);
                } catch (Exception ignored) {
                }
            }

            if (port != null && port.isOpen()) {
                port.closePort();
            }
        }
    }

    /**
     * Verify WAV header and log detailed info for debugging
     * WAV format: RIFF....WAVE fmt ....data....
     */
    private boolean verifyAndLogWavHeader(byte[] data, String comPort, String fileName) {
        if (data == null || data.length < 44) {
            log.error("❌ [{}] File too small for WAV header: {} bytes", comPort, data != null ? data.length : 0);
            return false;
        }

        // Check RIFF magic bytes
        String riff = new String(data, 0, 4);
        // Check WAVE format
        String wave = new String(data, 8, 4);

        log.info("🔍 [{}] WAV Header Analysis for {}:", comPort, fileName);
        log.info("   - File size: {} bytes", data.length);
        log.info("   - Magic bytes [0-3]: '{}' (expected: 'RIFF')", riff);
        log.info("   - Format [8-11]: '{}' (expected: 'WAVE')", wave);

        // Log first 44 bytes as hex for debugging
        StringBuilder hexDump = new StringBuilder();
        for (int i = 0; i < Math.min(44, data.length); i++) {
            hexDump.append(String.format("%02X ", data[i]));
            if ((i + 1) % 16 == 0)
                hexDump.append("\n   ");
        }
        log.info("   - Header hex dump:\n   {}", hexDump.toString());

        boolean isValid = "RIFF".equals(riff) && "WAVE".equals(wave);

        if (isValid) {
            // Parse more WAV details
            try {
                String fmtChunk = new String(data, 12, 4);
                int audioFormat = ((data[20] & 0xFF)) | ((data[21] & 0xFF) << 8);
                int numChannels = ((data[22] & 0xFF)) | ((data[23] & 0xFF) << 8);
                int sampleRate = ((data[24] & 0xFF)) | ((data[25] & 0xFF) << 8) |
                        ((data[26] & 0xFF) << 16) | ((data[27] & 0xFF) << 24);
                int bitsPerSample = ((data[34] & 0xFF)) | ((data[35] & 0xFF) << 8);

                log.info("   - fmt chunk: '{}'", fmtChunk);
                log.info("   - Audio format: {} (1=PCM, 6=ALAW, 7=ULAW, 17=ADPCM)", audioFormat);
                log.info("   - Channels: {}", numChannels);
                log.info("   - Sample rate: {} Hz", sampleRate);
                log.info("   - Bits per sample: {}", bitsPerSample);
            } catch (Exception e) {
                log.warn("   - Error parsing WAV details: {}", e.getMessage());
            }
            log.info("✅ [{}] WAV header is VALID", comPort);
        } else {
            log.error("❌ [{}] WAV header is INVALID!", comPort);
        }

        return isValid;
    }

    /**
     * Fix WAV header if it's corrupted or missing
     * Creates a standard WAV header for ADPCM 8kHz mono audio
     */
    private byte[] fixWavHeader(byte[] data, String comPort) {
        if (data == null || data.length == 0) {
            return data;
        }

        // Check if header already exists and is valid
        if (data.length >= 4) {
            String magic = new String(data, 0, 4);
            if ("RIFF".equals(magic)) {
                log.info("📝 [{}] RIFF header exists, checking for corruption...", comPort);
                // Header exists but may be corrupted - just return as-is for now
                return data;
            }
        }

        log.info("📝 [{}] Creating new WAV header for raw audio data ({} bytes)...", comPort, data.length);

        // Create WAV header for audio from Quectel modem
        // Quectel WAV_ADPCM: 8kHz, mono
        int sampleRate = 8000;
        int numChannels = 1;

        // WAV header is 44 bytes minimum, but ADPCM needs extended header
        // For simplicity, create a basic PCM-like header that most players can handle
        ByteArrayOutputStream wavFile = new ByteArrayOutputStream();

        try {
            int audioDataSize = data.length;
            int fileSize = audioDataSize + 36; // 44 - 8 = 36

            // RIFF header
            wavFile.write("RIFF".getBytes());
            wavFile.write(intToLittleEndian(fileSize));
            wavFile.write("WAVE".getBytes());

            // fmt chunk - use PCM format for better compatibility
            wavFile.write("fmt ".getBytes());
            wavFile.write(intToLittleEndian(16)); // Subchunk1Size for PCM
            wavFile.write(shortToLittleEndian((short) 1)); // AudioFormat: 1 = PCM
            wavFile.write(shortToLittleEndian((short) numChannels));
            wavFile.write(intToLittleEndian(sampleRate));
            wavFile.write(intToLittleEndian(sampleRate * numChannels * 2)); // ByteRate for 16-bit
            wavFile.write(shortToLittleEndian((short) (numChannels * 2))); // BlockAlign
            wavFile.write(shortToLittleEndian((short) 16)); // BitsPerSample: 16

            // data chunk
            wavFile.write("data".getBytes());
            wavFile.write(intToLittleEndian(audioDataSize));

            // Audio data
            wavFile.write(data);

            log.info("✅ [{}] Created WAV file with header: {} bytes total", comPort, wavFile.size());
            return wavFile.toByteArray();

        } catch (Exception e) {
            log.error("❌ [{}] Error creating WAV header: {}", comPort, e.getMessage());
            return data;
        }
    }

    private byte[] intToLittleEndian(int value) {
        return new byte[] {
                (byte) (value & 0xFF),
                (byte) ((value >> 8) & 0xFF),
                (byte) ((value >> 16) & 0xFF),
                (byte) ((value >> 24) & 0xFF)
        };
    }

    private byte[] shortToLittleEndian(short value) {
        return new byte[] {
                (byte) (value & 0xFF),
                (byte) ((value >> 8) & 0xFF)
        };
    }

    /**
     * Parse file size from AT+QFLST response
     * Example responses:
     * +QFLST: "UFS:call_3_20241206.wav",12345
     * +QFLST: "call_3_20241206.wav",12345
     */
    private long parseFileSize(String qflstResponse, String filePath) {
        try {
            log.debug("🔍 Parsing file size for: {} in response: {}", filePath, qflstResponse);

            // Handle different patterns
            // Pattern 1: exact match with quotes and comma
            String escapedPath = Pattern.quote(filePath);
            String pattern1 = "\"" + escapedPath + "\"\\s*,\\s*(\\d+)";

            // Pattern 2: with UFS: prefix
            String pattern2 = "\"UFS:" + escapedPath + "\"\\s*,\\s*(\\d+)";

            // Pattern 3: just the filename anywhere in response
            String fileName = filePath.replace("UFS:", "");
            String pattern3 = "\"[^\"]*" + Pattern.quote(fileName) + "\"\\s*,\\s*(\\d+)";

            String[] patterns = { pattern1, pattern2, pattern3 };

            for (String pattern : patterns) {
                java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern,
                        java.util.regex.Pattern.CASE_INSENSITIVE);
                java.util.regex.Matcher m = p.matcher(qflstResponse);

                if (m.find()) {
                    long size = Long.parseLong(m.group(1));
                    log.debug("✅ Found file size: {} bytes (pattern: {})", size, pattern);
                    return size;
                }
            }

            log.warn("⚠️ No file size found for: {}", filePath);
        } catch (Exception e) {
            log.error("❌ Failed to parse file size: {}", e.getMessage());
        }
        return 0;
    }

    /**
     * Read available text from serial port without blocking
     */
    private String readAvailableText(SerialPort port) {
        try {
            int available = port.bytesAvailable();
            if (available > 0) {
                byte[] buffer = new byte[available];
                int bytesRead = port.readBytes(buffer, available);
                if (bytesRead > 0) {
                    return new String(buffer, 0, bytesRead);
                }
            }
        } catch (Exception e) {
            // Ignore
        }
        return null;
    }

    /**
     * Send call status update via WebSocket
     */
    private void sendCallStatusUpdate(String comPort, String phoneNumber, String status, int duration, String message) {
        try {
            Map<String, Object> statusData = new HashMap<>();
            statusData.put("comPort", comPort);
            statusData.put("phoneNumber", phoneNumber);
            statusData.put("status", status);
            statusData.put("duration", duration);
            if (message != null) {
                statusData.put("message", message);
            }
            statusData.put("timestamp", LocalDateTime.now().toString());

            messagingTemplate.convertAndSend("/topic/call/status", statusData);
            log.debug("📡 Call status: {} - {}s - {}", status, duration, phoneNumber);
        } catch (Exception e) {
            log.error("Failed to send call status update: {}", e.getMessage());
        }
    }

    public void hangupCall(String comPort) {

        // Cancel scheduled hangup
        ScheduledFuture<?> task = activeCalls.remove(comPort);
        if (task != null) {
            task.cancel(false);
        }

        // Send hangup command
        try {
            // 🔧 FIX: Resolve COM port through PortResolver
            String resolvedPort = portResolver.resolve(comPort);
            if (resolvedPort != null) {
                SerialPort port = SerialPort.getCommPort(resolvedPort);
                port.setComPortParameters(115200, 8, 1, 0);
                port.setComPortTimeouts(SerialPort.TIMEOUT_READ_SEMI_BLOCKING, 2000, 2000);

                if (port.openPort()) {
                    sendATCommand(port, "ATH", 1000);
                    port.closePort();
                }
            }
        } catch (Exception e) {
            log.error("Error hanging up: {}", e.getMessage());
        }
    }

    public Page<CallRecordEntity> getCallHistory(int page, int size, String comPort) {
        Pageable pageable = PageRequest.of(page, size);
        if (comPort != null && !comPort.isEmpty()) {
            return callRepository.findByComPortOrderByCreatedAtDesc(comPort, pageable);
        }
        return callRepository.findAllByOrderByCreatedAtDesc(pageable);
    }

    public Page<CallRecordEntity> getCallHistoryByPhone(int page, int size, String phoneNumber) {
        Pageable pageable = PageRequest.of(page, size);
        return callRepository.findByTargetPhoneContainingIgnoreCaseOrSimPhoneContainingIgnoreCaseOrderByCreatedAtDesc(
                phoneNumber, phoneNumber, pageable);
    }

    public CallRecordEntity getCallById(Long id) {
        return callRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Không tìm thấy cuộc gọi"));
    }

    private void updateCallStatus(Long callId, String status, String error) {
        callRepository.findById(callId).ifPresent(call -> {
            call.setStatus(status);
            if (error != null) {
                call.setErrorMessage(error);
            }
            callRepository.save(call);
            messagingTemplate.convertAndSend("/topic/call/status", call);
        });
    }

    private void updateCallStartTime(Long callId) {
        callRepository.findById(callId).ifPresent(call -> {
            call.setCallStartTime(LocalDateTime.now());
            callRepository.save(call);
        });
    }

    private void updateCallEndTime(Long callId, int duration) {
        callRepository.findById(callId).ifPresent(call -> {
            call.setCallEndTime(LocalDateTime.now());
            call.setActualDuration(duration);
            callRepository.save(call);
        });
    }

    private void updateCallRecordingPath(Long callId, String path) {
        callRepository.findById(callId).ifPresent(call -> {
            call.setRecordingPath(path);
            callRepository.save(call);
        });
    }

    // ==================== RECORDING METHODS ====================

    public List<Map<String, Object>> getRecordingFiles() {
        List<Map<String, Object>> files = new ArrayList<>();
        Path recordingDir = Paths.get(getRecordingPath());

        if (!Files.exists(recordingDir)) {
            return files;
        }
        // List both .wav and .amr files
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(recordingDir, "*.{wav,amr}")) {
            for (Path file : stream) {
                Map<String, Object> fileInfo = new HashMap<>();
                fileInfo.put("name", file.getFileName().toString());
                fileInfo.put("path", file.toAbsolutePath().toString());
                fileInfo.put("size", Files.size(file));
                fileInfo.put("modified", Files.getLastModifiedTime(file).toMillis());
                files.add(fileInfo);
            }
        } catch (Exception e) {
            log.error("Error listing recordings: {}", e.getMessage());
        }

        // Sort by modified time descending
        files.sort((a, b) -> Long.compare((Long) b.get("modified"), (Long) a.get("modified")));

        return files;
    }

    public ResponseEntity<?> downloadRecording(String fileName) {
        Path filePath = Paths.get(getRecordingPath(), fileName);

        if (!Files.exists(filePath)) {
            return ResponseEntity.notFound().build();
        }

        FileSystemResource resource = new FileSystemResource(filePath.toFile());

        // Determine content type based on file extension
        String contentType = fileName.toLowerCase().endsWith(".amr")
                ? "audio/amr"
                : "audio/wav";

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                .contentType(MediaType.parseMediaType(contentType))
                .body(resource);
    }

    public String openRecordingFile(String fileName) throws Exception {
        Path filePath = Paths.get(getRecordingPath(), fileName);

        if (!Files.exists(filePath)) {
            throw new Exception("File not found: " + fileName);
        }

        // Open with default application
        if (Desktop.isDesktopSupported()) {
            Desktop.getDesktop().open(filePath.toFile());
        } else {
            // Windows specific
            Runtime.getRuntime().exec("cmd /c start \"\" \"" + filePath.toAbsolutePath() + "\"");
        }

        return filePath.toAbsolutePath().toString();
    }

    /**
     * Open folder containing the recording file
     */
    public String openRecordingFolder(String fileName) throws Exception {
        Path filePath = Paths.get(getRecordingPath(), fileName);

        if (!Files.exists(filePath)) {
            throw new Exception("File not found: " + fileName);
        }

        Path folderPath = filePath.getParent();

        // Open folder
        if (Desktop.isDesktopSupported()) {
            Desktop.getDesktop().open(folderPath.toFile());
        } else {
            // Windows specific - open folder and select file
            Runtime.getRuntime().exec("explorer /select," + filePath.toAbsolutePath());
        }

        return folderPath.toAbsolutePath().toString();
    }

    /**
     * Open recording folder directly (without needing a file name)
     */
    public String openRecordingFolderDirect() throws Exception {
        Path folderPath = Paths.get(getRecordingPath());

        // Ensure folder exists
        if (!Files.exists(folderPath)) {
            Files.createDirectories(folderPath);
        }

        // Open folder based on OS
        if (Desktop.isDesktopSupported()) {
            Desktop.getDesktop().open(folderPath.toFile());
        } else {
            // Fallback for systems without Desktop support
            String os = System.getProperty("os.name").toLowerCase();
            if (os.contains("win")) {
                Runtime.getRuntime().exec("explorer " + folderPath.toAbsolutePath());
            } else if (os.contains("mac")) {
                Runtime.getRuntime().exec("open " + folderPath.toAbsolutePath());
            } else if (os.contains("nix") || os.contains("nux")) {
                Runtime.getRuntime().exec("xdg-open " + folderPath.toAbsolutePath());
            }
        }

        log.info("📂 Opened recording folder: {}", folderPath.toAbsolutePath());
        return folderPath.toAbsolutePath().toString();
    }

    public void deleteRecording(String fileName) throws Exception {
        Path filePath = Paths.get(getRecordingPath(), fileName);
        Files.deleteIfExists(filePath);
    }

    public String getRecordingPath() {
        String path = defaultRecordingPath;
        log.debug("📁 Using recording path from config: {}", path);

        // Ensure path is absolute (for Windows paths like C:/recording)
        // Also ensure directory exists
        try {
            Path recordingDir = Paths.get(path);
            if (!Files.exists(recordingDir)) {
                Files.createDirectories(recordingDir);
                log.info("📁 Created recording directory: {}", recordingDir.toAbsolutePath());
            }
            return recordingDir.toAbsolutePath().toString();
        } catch (Exception e) {
            log.error("❌ Error creating recording directory: {}", e.getMessage());
            return path;
        }
    }



    // ==================== STATS METHODS ====================

    // 🆕 Cache stats để tránh query DB liên tục
    private volatile Map<String, Object> cachedStats = new HashMap<>();
    private volatile long lastStatsCacheTime = 0;
    private static final long STATS_CACHE_TTL_MS = 5000; // Cache 5 giây

    public Map<String, Object> getStats() {
        long now = System.currentTimeMillis();

        // Trả về cache nếu còn hạn
        if (now - lastStatsCacheTime < STATS_CACHE_TTL_MS && !cachedStats.isEmpty()) {
            return cachedStats;
        }

        Map<String, Object> stats = new HashMap<>();

        try {
            String deviceName = DeviceIdProvider.getDeviceId();

            // 🔧 FIX: Đếm SIM từ database thay vì cache (chính xác hơn)
            // Đếm SIM ACTIVE (có số điện thoại, đang hoạt động)
            long simsActive = simRepository.countByDeviceNameAndStatus(deviceName, "ACTIVE");
            // Đếm SIM INACTIVE (chưa có số điện thoại)
            long simsInactive = simRepository.countByDeviceNameAndStatus(deviceName, "INACTIVE");
            // Đếm tổng SIM có CCID (có thông tin)
            long simsTotal = simRepository.countByDeviceNameAndCcidNotNull(deviceName);

            stats.put("simsOnline", simsActive);
            stats.put("simsInactive", simsInactive);
            stats.put("simsTotal", simsTotal);

        } catch (Exception e) {
            log.warn("⚠️ Error counting SIMs: {}", e.getMessage());
            // Fallback to cache
            stats.put("simsOnline", onlineSims.size());
            stats.put("simsInactive", 0);
            stats.put("simsTotal", onlineSims.size());
        }

        // SMS stats
        stats.put("totalMessages", smsRepository.count());
        stats.put("unreadMessages", smsRepository.countByTypeAndIsReadFalse("INBOX"));
        stats.put("inboxCount", smsRepository.countByType("INBOX"));
        stats.put("sentCount", smsRepository.countByType("SENT"));
        stats.put("outboxCount", smsRepository.countByType("OUTBOX"));

        // Call stats
        stats.put("totalCalls", callRepository.count());

        // Recording stats - cache vì đọc file chậm
        stats.put("recordingsCount", getRecordingFiles().size());

        // Update cache
        cachedStats = stats;
        lastStatsCacheTime = now;

        return stats;
    }

    /**
     * 🆕 Force refresh stats cache (gọi sau khi scan xong)
     */
    public void refreshStatsCache() {
        lastStatsCacheTime = 0;
        getStats(); // Re-fetch
    }

    // ==================== HELPER METHODS ====================

    private String sendATCommand(SerialPort port, String command, int delayMs) throws Exception {
        OutputStream os = port.getOutputStream();
        InputStream is = port.getInputStream();

        // Send command
        if (!command.isEmpty()) {
            os.write((command + "\r").getBytes(StandardCharsets.US_ASCII));
            os.flush();
        }

        Thread.sleep(delayMs);

        // Read response
        StringBuilder response = new StringBuilder();
        while (is.available() > 0) {
            response.append((char) is.read());
        }

        return response.toString();
    }

    private String normalizePhoneNumber(String phone) {
        phone = phone.replaceAll("[^0-9+]", "");
        if (phone.startsWith("0")) {
            return "+84" + phone.substring(1);
        }
        return phone;
    }

    private String encodeUCS2(String text) {
        StringBuilder hex = new StringBuilder();
        for (char c : text.toCharArray()) {
            hex.append(String.format("%04X", (int) c));
        }
        return hex.toString();
    }

    private String decodeUCS2(String hex) {
        if (hex == null || hex.isEmpty())
            return "";

        // ✅ Use SmsDecoder which handles mixed content (hex + CMTI, etc.)
        return SmsDecoder.decode(hex);
    }

    private String parsePhoneNumber(String response) {
        // +CNUM: "","+84901234567",145
        try {
            String[] parts = response.split("\"");
            if (parts.length >= 4) {
                return parts[3].replace("+84", "0");
            }
        } catch (Exception e) {
            log.debug("Cannot parse phone: {}", e.getMessage());
        }
        return null;
    }

    private String parseCarrier(String response) {
        // +COPS: 0,0,"VIETTEL",7
        try {
            String[] parts = response.split("\"");
            if (parts.length >= 2) {
                return parts[1];
            }
        } catch (Exception e) {
            log.debug("Cannot parse carrier: {}", e.getMessage());
        }
        return null;
    }

    private String parseIccid(String response) {
        try {
            String[] lines = response.split("\n");
            for (String line : lines) {
                line = line.trim();
                if (line.matches("\\d{19,20}")) {
                    return line;
                }
            }
        } catch (Exception e) {
            log.debug("Cannot parse ICCID: {}", e.getMessage());
        }
        return null;
    }

    private List<SmsMessageEntity> parseSmsMessages(String raw, String comPort) {
        List<SmsMessageEntity> messages = new ArrayList<>();

        if (raw == null || raw.isEmpty()) {
            return messages;
        }

        String[] lines = raw.split("\r\n");
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.startsWith("+CMGL:")) {
                try {
                    String[] parts = line.split(",");
                    String sender = parts[2].replace("\"", "").trim();
                    String content = (i + 1 < lines.length) ? lines[i + 1].trim() : "";

                    SmsMessageEntity sms = SmsMessageEntity.builder()
                            .comPort(comPort)
                            .phoneNumber(decodeUCS2(sender))
                            .content(decodeUCS2(content))
                            .type("INBOX")
                            .status("RECEIVED")
                            .isRead(false)
                            .build();

                    messages.add(sms);
                } catch (Exception e) {
                    log.warn("Cannot parse SMS: {}", line);
                }
            }
        }

        return messages;
    }

    // ==================== CLEANUP METHODS ====================

    /**
     * 🧹 Cleanup duplicate SIM
     */
    public int cleanupDuplicateSims() {
        return simCleanupService.cleanupDuplicateSims();
    }

    /**
     * 🗑️ Cleanup SIM REPLACED cũ
     */
    public int cleanupOldReplacedSims() {
        return simCleanupService.cleanupOldReplacedSims();
    }

    /**
     * 📊 Thống kê SIM
     */
    public Map<String, Object> getSimStatistics() {
        return simCleanupService.getSimStatistics();
    }

    /**
     * 🔍 Tìm duplicate SIM
     * /**
     * 🔍 Tìm duplicate SIM
     */
    public Map<String, List<Sim>> findDuplicateSims() {
        return simCleanupService.findDuplicateSims();
    }

    /**
     * 🧹 Cleanup duplicate SIM theo phoneNumber
     */
    public int cleanupDuplicateByPhoneNumber() {
        return simCleanupService.cleanupDuplicateByPhoneNumber();
    }

    /**
     * 🔍 Tìm duplicate SIM theo phoneNumber
     */
    public Map<String, List<Sim>> findDuplicateByPhoneNumber() {
        return simCleanupService.findDuplicateByPhoneNumber();
    }

    // ==================================================================================
    // LOCAL CALL RESULT HELPERS
    // ==================================================================================

    /**
     * Notify remote server via WebSocket about call status
     * Similar to PortWorker.notifyCallStatus()
     */
    private void recordLocalCallStatus(String status, String comPort, String simPhone,
            String targetPhone, String orderId, String serviceCode) {
        recordLocalCallStatus(status, comPort, simPhone, targetPhone, orderId, serviceCode, null, null);
    }

    /**
     * Notify remote server via WebSocket about call status with optional progress
     * Similar to PortWorker.notifyCallStatus()
     * 
     * @param status      Call status
     * @param comPort     COM port
     * @param simPhone    SIM phone number
     * @param targetPhone Target phone number
     * @param orderId     Order ID
     * @param serviceCode Service code
     * @param progress    Optional progress percentage (0-100) for download status
     * @param message     Optional message
     */
    private void recordLocalCallStatus(String status, String comPort, String simPhone,
            String targetPhone, String orderId, String serviceCode, Integer progress, String message) {
        log.debug("📋 [{}] Call status local: status={}, target={}, progress={}, message={}",
                comPort, status, targetPhone, progress, message);
    }

    /**
     * Upload recording file to server
     * Similar to PortWorker.uploadRecordFile()
     */
    private String uploadRecordingFile(String localPath, String comPort, String simPhone,
            String orderId, String serviceCode) {
        File file = new File(localPath);
        if (!file.isFile() || file.length() == 0) {
            log.error("❌ [{}] Recording không tồn tại hoặc rỗng: {}", comPort, localPath);
            return null;
        }
        log.info("💾 [{}] Giữ recording tại máy: {}", comPort, file.getAbsolutePath());
        return file.getAbsolutePath();
    }

    /**
     * Send callback to remote API server
     * Similar to PortWorker.sendCallCallback()
     */
    private void saveLocalCallResult(String comPort, String simPhone, String targetPhone,
            Instant startTime, Instant endTime, String recordingUrl,
            String orderId, long connectedDuration, boolean connected,
            String failReason, String serviceCode) {
        try {
            Sim sim = simRepository.findFirstByDeviceNameAndComName(localDeviceName, comPort).orElse(null);
            if (sim == null) {
                log.warn("⚠️ [{}] Cannot find SIM for callback", comPort);
                return;
            }

            // Save to local DB
            if (callMessageRepository != null) {
                String status = failReason != null ? "FAILED" : (connected ? "SUCCESS" : "NO_ANSWER");

                app.simsmartgsm.entity.CallMessage callMessage = app.simsmartgsm.entity.CallMessage.builder()
                        .orderId(orderId)
                        .simPhone(simPhone)
                        .fromNumber(simPhone)
                        .toNumber(targetPhone)
                        .startTime(startTime)
                        .endTime(endTime)
                        .status(status)
                        .recordingPath(recordingUrl)
                        .build();

                callMessageRepository.save(callMessage);
                log.info("✅ [{}] Saved CallMessage: orderId={}, status={}, duration={}s",
                        comPort, orderId, status, connectedDuration);
            }

            // Send final WebSocket notification
            recordLocalCallStatus("ENDED_SUCCESS", comPort, simPhone, targetPhone, orderId, serviceCode);

            if (failReason != null) {
                log.info("📊 [{}] Callback - FAILED: {}", comPort, failReason);
            } else if (connected) {
                log.info("📊 [{}] Callback - SUCCESS: {} seconds", comPort, connectedDuration);
            } else {
                log.info("📊 [{}] Callback - NO_ANSWER", comPort);
            }

        } catch (Exception e) {
            log.error("❌ [{}] Failed to send callback: {}", comPort, e.getMessage());
        }
    }

}
