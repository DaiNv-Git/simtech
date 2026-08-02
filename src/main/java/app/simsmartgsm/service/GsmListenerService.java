package app.simsmartgsm.service;

import app.simsmartgsm.config.ComManager;
import app.simsmartgsm.entity.Country;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.uitils.SimStatus;
import app.simsmartgsm.uitils.OtpSessionType;
import app.simsmartgsm.uitils.PortWorker;
import app.simsmartgsm.uitils.SmsDecoder;
import app.simsmartgsm.tool.model.SmsDocument;
import app.simsmartgsm.tool.repository.ToolSmsRepository;
import app.simsmartgsm.tool.service.TelegramService;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class GsmListenerService {

    private final ComManager comManager;
    private final CallRecordService callRecordService;
    private final TelegramService telegramService;
    private final ToolSmsRepository mongoSmsRepository;
    private final SimpMessagingTemplate simpMessagingTemplate;
    private final SimRepository simRepository;
    private final app.simsmartgsm.repository.SmsMessageJpaRepository smsRepository;
    private final app.simsmartgsm.service.SmsDailyLimitService smsDailyLimitService;

    private final Map<String, PortWorker> workers = new ConcurrentHashMap<>();
    private final Map<String, List<RentSession>> activeSessions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    // ✅ Retry state tracking
    private final Map<String, RetryState> retryStates = new ConcurrentHashMap<>();

    /**
     * Track retry attempts for each order
     */
    @Data
    static class RetryState {
        int totalAttempts = 0; // Total attempts across all SIMs (max 6)
        int currentSimAttempts = 0; // Attempts on current SIM (max 2)
        List<String> triedSims = new ArrayList<>(); // List of SIM phones tried (max 3)
        String currentSimPhone; // Current SIM being used
        String toNumber; // Destination number
        String content; // SMS content
        String serviceCode; // Service/campaign code
        String localMsgId; // ✅ NEW: Preserve original message ID for callback
    }

    // ==========================================================
    // 🧭 Thuê SIM - Khởi tạo session quản lý
    // ==========================================================
    public void rentSim(Sim sim, Long accountId, List<String> services,
            int durationMinutes, Country country, String orderId,
            String type, Boolean record, String callType, String targetNumber) {

        RentSession session = new RentSession(
                accountId, services, Instant.now(), durationMinutes,
                country, orderId, OtpSessionType.fromString(type),
                false, type, record != null && record, false, null, null);

        activeSessions.computeIfAbsent(sim.getId(), k -> new CopyOnWriteArrayList<>()).add(session);
        startWorkerForSim(sim);

        log.info("🟢 Rent SIM {} (acc={}, type={}, duration={}m)",
                sim.getPhoneNumber(), accountId, type, durationMinutes);

        // Tự động dọn session khi hết hạn
        scheduler.schedule(() -> closeSession(sim, session),
                durationMinutes, TimeUnit.MINUTES);

        // Nếu là dịch vụ CALL_OUT thì thực hiện gọi
        if ("buy.call.service".equalsIgnoreCase(type) &&
                "CALL_OUT".equalsIgnoreCase(callType)) {
            processOutgoingCall(sim, session, targetNumber, record);
        }
    }

    // ==========================================================
    // 📨 NHẬN SMS - LƯU LOCAL + CẬP NHẬT UI + GỬI TELEGRAM
    // ==========================================================
    public void processSms(Sim sim, String sender, String body) {
        try {
            // ✅ CHỈ decode nếu thực sự là UCS2 hex, KHÔNG decode số điện thoại bình thường
            String decodedSender = isLikelyUcs2Hex(sender) ? SmsDecoder.decode(sender) : sender;
            String decodedBody = isLikelyUcs2Hex(body) ? SmsDecoder.decode(body) : body;

            log.debug("📩 [{}] SMS → From: {} | Body: {}", sim.getComName(), decodedSender, decodedBody);

            // ✅ Step 1: Lưu SMS vào database local (với nội dung đã decode)
            app.simsmartgsm.entity.SmsMessageEntity smsEntity = app.simsmartgsm.entity.SmsMessageEntity.builder()
                    .comPort(sim.getComName())
                    .simPhone(sim.getPhoneNumber())
                    .phoneNumber(decodedSender)
                    .content(decodedBody)
                    .type("INBOX")
                    .status("RECEIVED")
                    .isRead(false)
                    .build();

            smsEntity = smsRepository.save(smsEntity);
            log.debug("💾 [{}] SMS saved to local DB: id={}", sim.getComName(), smsEntity.getId());


            // ✅ Step 2: Đếm số tin chưa đọc
            long unreadCount = smsRepository.countByTypeAndIsReadFalse("INBOX");

            // ✅ Step 3a: Bắn WebSocket notification cho inbox list update
            Map<String, Object> inboxNotification = new HashMap<>();
            inboxNotification.put("type", "NEW_SMS");
            inboxNotification.put("sms", smsEntity);
            inboxNotification.put("unreadCount", unreadCount);
            inboxNotification.put("comPort", sim.getComName());
            inboxNotification.put("simPhone", sim.getPhoneNumber());
            inboxNotification.put("sender", decodedSender);
            inboxNotification.put("timestamp", java.time.Instant.now().toString());
            simpMessagingTemplate.convertAndSend("/topic/sms/inbox", inboxNotification);

            // ✅ Step 3b: Bắn WebSocket notification cho new SMS alert (frontend subscribe
            // /topic/sms/new)
            Map<String, Object> newSmsNotification = new HashMap<>();
            newSmsNotification.put("id", smsEntity.getId());
            newSmsNotification.put("phoneNumber", decodedSender); // Frontend expect this field
            newSmsNotification.put("content", decodedBody);
            newSmsNotification.put("comPort", sim.getComName());
            newSmsNotification.put("simPhone", sim.getPhoneNumber());
            newSmsNotification.put("type", "INBOX");
            newSmsNotification.put("status", "RECEIVED");
            newSmsNotification.put("isRead", false);
            newSmsNotification.put("createdAt", smsEntity.getCreatedAt() != null ? smsEntity.getCreatedAt().toString()
                    : java.time.LocalDateTime.now().toString());
            newSmsNotification.put("unreadCount", unreadCount);
            simpMessagingTemplate.convertAndSend("/topic/sms/new", newSmsNotification);

            // ✅ Step 3c: Update unread count badge
            simpMessagingTemplate.convertAndSend("/topic/sms/unread-count", unreadCount);

            log.info(
                    "📡 [{}] WebSocket notifications sent: /topic/sms/inbox, /topic/sms/new, /topic/sms/unread-count (unread={})",
                    sim.getComName(), unreadCount);

            // Lưu vào collection Mongo "sms" rồi đẩy Telegram. Dashboard local vẫn
            // dùng bản H2 ở trên để giữ nguyên tương thích.
            saveMongoSms(sim, decodedSender, decodedBody, "INBOUND", "RECEIVED", null);
            telegramService.sendIncomingSms(sim.getComName(), sim.getPhoneNumber(), decodedSender, decodedBody);

        } catch (Exception e) {
            log.error("❌ [{}] processSms error: {}", sim.getComName(), e.getMessage(), e);
        }
    }

    /**
     * ✅ Check nếu string có thể là UCS2 hex (không phải số điện thoại bình thường)
     * UCS2 hex: chỉ chứa [0-9A-F], độ dài chia hết cho 4, >= 8 ký tự
     * Số điện thoại: có +, không đủ dài, hoặc có ký tự khác
     */
    private boolean isLikelyUcs2Hex(String text) {
        if (text == null || text.isEmpty()) {
            return false;
        }

        String trimmed = text.trim();

        // Nếu có dấu + hoặc - (số điện thoại) → không phải UCS2
        if (trimmed.contains("+") || trimmed.contains("-") || trimmed.contains(" ")) {
            return false;
        }

        // Nếu quá ngắn (< 8 ký tự) → không phải UCS2
        if (trimmed.length() < 8) {
            return false;
        }

        // Nếu có ký tự không phải hex → không phải UCS2
        if (!trimmed.matches("^[0-9A-Fa-f]+$")) {
            return false;
        }

        // Nếu không chia hết cho 4 → không phải UCS2
        if (trimmed.length() % 4 != 0) {
            return false;
        }

        // Có vẻ là UCS2 hex
        return true;
    }

    // ==========================================================
    // ☎️ CUỘC GỌI RA (CALL_OUT) - ĐÃ SỬA
    // ==========================================================
    public void processOutgoingCall(Sim sim, RentSession session,
            String targetPhone, boolean record) {
        try {
            log.info("📞 [CALL_OUT] {} → {} (record: {})", sim.getPhoneNumber(), targetPhone, record);

            // Sử dụng PortWorker để thực hiện cuộc gọi thay vì gửi AT command trực tiếp
            PortWorker worker = comManager.getWorker(sim.getComName());
            if (worker != null) {
                // Tạo task gọi điện với thời lượng 20 giây
                PortWorker.Task callTask = PortWorker.Task.call(targetPhone, 20, record,
                        session.getServiceType(), session.getOrderId());

                worker.enqueue(callTask);

                session.setCallHandled(true);
                session.setCallStartTime(Instant.now());

                log.info("✅ [CALL_OUT] Đã đưa vào queue: {} → {}", sim.getPhoneNumber(), targetPhone);
            } else {
                log.error("❌ [CALL_OUT] Không tìm thấy worker cho SIM {}", sim.getPhoneNumber());
            }

        } catch (Exception e) {
            log.error("❌ [CALL_OUT] failed: {}", e.getMessage(), e);
        }
    }

    // ==========================================================
    // 📤 KẾT QUẢ GỬI SMS - SMART RETRY WITH SIM SWITCHING
    // ✅ REFACTORED: Chỉ gửi callback khi SUCCESS hoặc FINAL FAIL
    // ==========================================================
    public void onSmsSentResult(Sim sim, String to, String content, boolean success,
            String orderId, String serviceCode, String localMsgId) {
        try {
            log.debug("📡 [SMS_RESULT] {} -> {} ({}) orderId={}",
                    sim.getPhoneNumber(), to, success ? "✅ SUCCESS" : "❌ FAILED", orderId);

            if (success) {
                // ✅ SUCCESS: Cleanup retry state and send SUCCESS callback
                RetryState state = retryStates.remove(orderId);

                // ✅ Update local database
                updateLocalSmsStatus(orderId, true, null);

                // ✅ Send SUCCESS callback to cloud
                String msgId = getMsgId(localMsgId, orderId, state);
                recordSmsResult(sim, to, content, orderId, serviceCode, msgId, true,
                        state != null ? state.triedSims : null);
                saveMongoSms(sim, to, content, "OUTBOUND", "SENT", orderId);

                if (state != null && state.triedSims.size() > 1) {
                    log.info("✅ [SMS_RESULT] SUCCESS after switching SIMs! Tried: {} -> Final: {}",
                            state.triedSims, sim.getPhoneNumber());
                }

                // Cập nhật lên Dashboard qua WebSocket
                String uiStatus = "ACTIVE".equals(sim.getStatus()) ? "ONLINE"
                        : "INACTIVE".equals(sim.getStatus()) ? "INACTIVE" : "OFFLINE";
                app.simsmartgsm.dto.response.SimResponse simUpdate = app.simsmartgsm.dto.response.SimResponse.builder()
                        .comPort(sim.getComName())
                        .status(uiStatus)
                        .carrier(sim.getSimProvider())
                        .phoneNumber(sim.getPhoneNumber() != null ? sim.getPhoneNumber() : "Đang phát hiện...")
                        .iccid(sim.getCcid())
                        .message("OK")
                        .todaySms(smsDailyLimitService.getSentToday(sim.getComName()))
                        .build();
                simpMessagingTemplate.convertAndSend("/topic/sims", java.util.Collections.singletonList(simUpdate));
            } else {
                // ✅ FIX: PortWorker.retryOnDifferentSim() đã TỰ retry trên SIM khác rồi!
                // Khi đến đây = tất cả retry đã hết → gửi FINAL FAIL callback trực tiếp
                // KHÔNG gọi handleSmartRetry() nữa → tránh DOUBLE RETRY STORM
                log.error("❌ [SMS_RESULT] FINAL FAIL: {} -> {} | orderId={}",
                        sim.getPhoneNumber(), to, orderId);

                // Update local database
                updateLocalSmsStatus(orderId, false, "SMS failed after all retries");
                saveMongoSms(sim, to, content, "OUTBOUND", "FAILED", orderId);

            }

        } catch (Exception e) {
            log.error("❌ [SMS_RESULT] push failed: {}", e.getMessage(), e);
        }
    }

    private void saveMongoSms(Sim sim, String phoneNumber, String content,
            String direction, String status, String referenceId) {
        try {
            String fingerprint = referenceId == null || referenceId.isBlank()
                    ? null
                    : "outbound:" + referenceId;
            SmsDocument document = fingerprint == null
                    ? null
                    : mongoSmsRepository.findByFingerprint(fingerprint).orElse(null);
            if (document == null) {
                document = SmsDocument.builder()
                        .fingerprint(fingerprint)
                        .createdAt(Instant.now())
                        .build();
            }
            document.setComPort(sim != null ? sim.getComName() : null);
            document.setSimPhone(sim != null ? sim.getPhoneNumber() : null);
            document.setPhoneNumber(phoneNumber);
            document.setDirection(direction);
            document.setContent(content);
            document.setStatus(status);
            mongoSmsRepository.save(document);
        } catch (Exception e) {
            log.error("Không thể lưu SMS vào Mongo: {}", e.getMessage());
        }
    }

    /**
     * ✅ Update local SMS database status
     */
    private void updateLocalSmsStatus(String orderId, boolean success, String errorMessage) {
        if (orderId == null || orderId.isBlank())
            return;

        try {
            Long smsId = Long.parseLong(orderId);
            smsRepository.findById(smsId).ifPresent(sms -> {
                sms.setStatus(success ? "SENT" : "FAILED");
                sms.setType(success ? "SENT" : "OUTBOX");
                if (!success && errorMessage != null) {
                    sms.setErrorMessage(errorMessage);
                }
                smsRepository.save(sms);
                log.debug("💾 [DB] Updated SMS {} status to {}", smsId, sms.getStatus());

                // Push to WebSocket
                simpMessagingTemplate.convertAndSend("/topic/sms/status", sms);
            });
        } catch (NumberFormatException e) {
            log.debug("⚠️ [SMS_RESULT] orderId is not a local DB ID: {}", orderId);
        }
    }

    /**
     * ✅ Get message ID for callback (prefer original localMsgId)
     */
    private String getMsgId(String localMsgId, String orderId, RetryState state) {
        if (localMsgId != null && !localMsgId.isBlank())
            return localMsgId;
        if (state != null && state.localMsgId != null)
            return state.localMsgId;
        if (orderId != null)
            return orderId;
        return UUID.randomUUID().toString();
    }

    /** Ghi nhận kết quả gửi SMS tại máy; không callback tới web cũ. */
    private void recordSmsResult(Sim sim, String to, String content, String orderId,
            String serviceCode, String msgId, boolean success, List<String> triedSims) {
        log.info("📋 [SMS_RESULT] Local result={}: orderId={}, localMsgId={}, triedSims={}",
                success ? "SUCCESS" : "FINAL_FAIL", orderId, msgId,
                triedSims != null ? triedSims : "N/A");
    }

    /**
     * ✅ SMART RETRY with SIM switching
     * - Max 2 retries per SIM
     * - Switch to max 3 different SIMs
     * - Total 6 retries max
     * - Send final fail notification after exhausting all attempts
     */
    private void handleSmartRetry(Sim currentSim, String to, String content,
            String orderId, String serviceCode, String localMsgId) {

        RetryState state = retryStates.computeIfAbsent(orderId, k -> {
            RetryState newState = new RetryState();
            newState.currentSimPhone = currentSim.getPhoneNumber();
            newState.triedSims.add(currentSim.getPhoneNumber());
            newState.toNumber = to;
            newState.content = content;
            newState.serviceCode = serviceCode;
            newState.localMsgId = localMsgId;
            return newState;
        });

        state.totalAttempts++;
        state.currentSimAttempts++;

        log.info("🔄 [RETRY] Attempt {}/3 on SIM {} (attempt {} on this SIM)",
                state.totalAttempts, state.currentSimPhone, state.currentSimAttempts);

        // ✅ FIX: Giảm max retry từ 6 xuống 3 để tránh retry storm
        // 5000 SMS × 10% fail × 6 retry = 3000 extra tasks → tắc nghẽn queue
        if (state.totalAttempts >= 3) {
            handleFinalFail(orderId, state);
            retryStates.remove(orderId);
            return;
        }

        // ✅ FIX: Switch SIM ngay sau lần fail đầu tiên (thay vì 2 lần)
        // Nếu SIM đã fail 1 lần, khả năng cao fail tiếp → chuyển SIM ngay
        if (state.currentSimAttempts >= 1 && state.triedSims.size() < 2) {
            Sim nextSim = getNextAvailableSim(state.triedSims);

            if (nextSim == null) {
                log.warn("⚠️ [RETRY] No more SIMs available after trying: {}", state.triedSims);
                handleFinalFail(orderId, state);
                retryStates.remove(orderId);
                return;
            }

            state.currentSimPhone = nextSim.getPhoneNumber();
            state.triedSims.add(nextSim.getPhoneNumber());
            state.currentSimAttempts = 0;

            log.info("🔄 [RETRY] Switching SIM: {} ({}/{} SIMs tried)",
                    nextSim.getPhoneNumber(), state.triedSims.size(), 2);

            scheduleRetry(nextSim, state, orderId);
        } else {
            scheduleRetry(currentSim, state, orderId);
        }
    }

    /**
     * Schedule retry with exponential backoff
     */
    private void scheduleRetry(Sim sim, RetryState state, String orderId) {
        // ✅ FIX: Tăng backoff delay để tránh overwhelm SIM
        // Backoff: 5s, 10s (thay vì 0s, 2s, 4s)
        long delaySeconds = Math.max(5, state.currentSimAttempts * 5);

        log.info("🕒 [RETRY] Scheduling retry in {}s on SIM {}",
                delaySeconds, sim.getPhoneNumber());

        scheduler.schedule(() -> {
            try {
                PortWorker worker = comManager.getWorker(sim.getComName());
                if (worker != null && worker.isOpen()) {
                    // ✅ FIX: Check queue size trước khi retry
                    // Nếu queue quá đầy, skip retry để tránh tắc nghẽn thêm
                    int queueSize = worker.getQueueSize();
                    if (queueSize > 20) {
                        log.warn("⚠️ [RETRY] Worker queue too full ({} tasks), skipping retry for order {}",
                                queueSize, orderId);
                        handleFinalFail(orderId, state);
                        retryStates.remove(orderId);
                        return;
                    }
                    worker.enqueue(PortWorker.TaskType.SEND, state.toNumber,
                            state.content, state.serviceCode, orderId);
                    log.info("📨 [RETRY] Queued retry on SIM {}", sim.getPhoneNumber());
                } else {
                    log.error("❌ [RETRY] Worker not available for SIM {}", sim.getPhoneNumber());
                    // ✅ FIX: Fail ngay thay vì gọi đệ quy handleSmartRetry()
                    // Gọi đệ quy gây potential infinite loop và thêm pressure
                    handleFinalFail(orderId, state);
                    retryStates.remove(orderId);
                }
            } catch (Exception e) {
                log.error("❌ [RETRY] Error scheduling retry: {}", e.getMessage(), e);
            }
        }, delaySeconds, TimeUnit.SECONDS);
    }

    /**
     * Get next available SIM (not in triedSims list)
     * ✅ FIX: Use "ACTIVE" status (correct enum) and check worker availability
     */
    private Sim getNextAvailableSim(List<String> triedSims) {
        try {
            List<Sim> allSims = simRepository.findAll();

            // Debug: count available SIMs
            long activeCount = allSims.stream().filter(s -> "ACTIVE".equalsIgnoreCase(s.getStatus())).count();
            long withWorker = allSims.stream()
                    .filter(s -> "ACTIVE".equalsIgnoreCase(s.getStatus()))
                    .filter(s -> {
                        PortWorker w = comManager.getWorker(s.getComName());
                        return w != null && w.isOpen();
                    })
                    .count();
            log.debug("🔍 [RETRY] SIM pool: {} ACTIVE, {} with running workers, excluding {} already tried",
                    activeCount, withWorker, triedSims.size());

            return allSims.stream()
                    // ✅ FIX: Use "ACTIVE" status - "AVAILABLE" doesn't exist in SimStatus enum!
                    .filter(sim -> "ACTIVE".equalsIgnoreCase(sim.getStatus()))
                    .filter(sim -> sim.getPhoneNumber() != null && !sim.getPhoneNumber().isBlank())
                    .filter(sim -> !triedSims.contains(sim.getPhoneNumber()))
                    // ✅ Also verify the worker is running
                    .filter(sim -> {
                        PortWorker worker = comManager.getWorker(sim.getComName());
                        return worker != null && worker.isOpen();
                    })
                    .findFirst()
                    .orElse(null);

        } catch (Exception e) {
            log.error("❌ [RETRY] Error getting next SIM: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Handle final fail - send notification to backend
     * ✅ UPDATED: Now updates local DB and sends proper FAILED callback
     */
    private void handleFinalFail(String orderId, RetryState state) {
        log.error("❌ [RETRY] FINAL FAIL for order {} after {} attempts (tried SIMs: {})",
                orderId, state.totalAttempts, state.triedSims);

        try {
            // ✅ Update local database
            updateLocalSmsStatus(orderId, false,
                    "SMS failed after " + state.totalAttempts + " attempts on " + state.triedSims.size() + " SIMs");

            // ✅ Get the last SIM that tried (for callback)
            Sim lastSim = null;
            if (state.currentSimPhone != null) {
                lastSim = simRepository.findByPhoneNumber(state.currentSimPhone).orElse(null);
            }

            // ✅ Send FAILED callback to cloud using unified method
            if (lastSim != null) {
                String msgId = getMsgId(state.localMsgId, orderId, state);
                recordSmsResult(lastSim, state.toNumber, state.content, orderId,
                        state.serviceCode, msgId, false, state.triedSims);
            } else {
                log.warn("⚠️ [RETRY] Không tìm thấy SIM cuối để lưu kết quả cho order {}", orderId);
            }
        } catch (Exception e) {
            log.error("❌ [RETRY] Error sending final fail notification: {}", e.getMessage(), e);
        }
    }

    // ==========================================================
    // 📡 TRẠNG THÁI CUỘC GỌI - ĐÃ SỬA (QUAN TRỌNG)
    // ==========================================================
    public void onCallStatus(Sim sim, String targetPhone, String status, String orderId) {
        try {
            log.info("📡 [CALL_STATUS] [{}] SIM={} → {} | {} | orderId={}",
                    sim.getComName(), sim.getPhoneNumber(), targetPhone, status, orderId);

            // Đẩy trạng thái cuộc gọi qua WebSocket
            Map<String, Object> payload = new HashMap<>();
            payload.put("type", "CALL_STATUS");
            payload.put("sim", sim.getPhoneNumber());
            payload.put("com", sim.getComName());
            payload.put("target", targetPhone);
            payload.put("status", status);
            payload.put("orderId", orderId);
            payload.put("timestamp", Instant.now().toString());

            simpMessagingTemplate.convertAndSend("/topic/call-status", payload);

            log.debug("✅ [CALL_STATUS] Đã gửi WebSocket: /topic/call-status");

        } catch (Exception e) {
            log.error("❌ [CALL_STATUS] WebSocket send error: {}", e.getMessage(), e);
        }
    }

    // ==========================================================
    // 📞 CUỘC GỌI ĐẾN (CALL_IN) - ĐÃ SỬA
    // ==========================================================
    public void processIncomingCall(Sim sim, String fromNumber, RentSession session) {
        if (!session.isActive() || session.isCallHandled())
            return;

        try {
            log.info("📞 [CALL_IN] từ {} → SIM {}", fromNumber, sim.getPhoneNumber());

            // Sử dụng PortWorker để trả lời cuộc gọi
            PortWorker worker = comManager.getWorker(sim.getComName());
            if (worker != null) {
                // Gửi lệnh ATA để trả lời cuộc gọi
                worker.enqueue(PortWorker.TaskType.CMD, null, "ATA",
                        session.getServiceType(), session.getOrderId());

                session.setCallHandled(true);
                session.setCallStartTime(Instant.now());

                // Kết thúc sau 20s
                scheduler.schedule(() -> {
                    try {
                        // Gửi lệnh ATH để kết thúc cuộc gọi
                        worker.enqueue(PortWorker.TaskType.CMD, null, "ATH",
                                session.getServiceType(), session.getOrderId());

                        // Lưu record cuộc gọi
                        callRecordService.saveCallRecord(
                                session.getOrderId(), session.getAccountId(), sim,
                                fromNumber, "RECEIVED", null,
                                session.getCallStartTime(), Instant.now(),
                                session.getStartTime().plus(Duration.ofMinutes(session.getDurationMinutes())));

                        log.info("✅ [CALL_IN] {} → {} ended.", fromNumber, sim.getPhoneNumber());
                    } catch (Exception e) {
                        log.error("❌ [CALL_IN] Error ending call: {}", e.getMessage(), e);
                    } finally {
                        closeSession(sim, session);
                    }
                }, 20, TimeUnit.SECONDS);

            } else {
                log.error("❌ [CALL_IN] Không tìm thấy worker cho SIM {}", sim.getPhoneNumber());
            }

        } catch (Exception e) {
            log.error("❌ [CALL_IN] Error processing incoming call: {}", e.getMessage(), e);
        }
    }

    // ==========================================================
    // 🧭 QUẢN LÝ WORKER VÀ SESSION - ĐÃ SỬA
    // ==========================================================
    private void closeSession(Sim sim, RentSession session) {
        try {
            List<RentSession> sessions = activeSessions.get(sim.getId());
            if (sessions != null) {
                sessions.remove(session);
                log.info("✅ [SESSION] Đã đóng session cho orderId={} SIM={}",
                        session.getOrderId(), sim.getPhoneNumber());
            }

            if (sessions == null || sessions.isEmpty()) {
                activeSessions.remove(sim.getId());
                stopWorkerIfNoActiveSession(sim);
            }
        } catch (Exception e) {
            log.error("❌ [SESSION] Lỗi khi đóng session: {}", e.getMessage(), e);
        }
    }

    private void startWorkerForSim(Sim sim) {
        try {
            PortWorker worker = comManager.getWorker(sim.getComName());
            if (worker == null) {
                comManager.startWorker(sim);
                worker = comManager.getWorker(sim.getComName());
            }

            if (worker != null) {
                workers.put(sim.getComName().trim().toUpperCase(Locale.ROOT), worker);
                log.info("✅ [WORKER] Đã khởi động worker cho SIM {}", sim.getPhoneNumber());
            } else {
                log.error("❌ [WORKER] Không thể khởi động worker cho SIM {}", sim.getPhoneNumber());
            }
        } catch (Exception e) {
            log.error("❌ [WORKER] Lỗi khi khởi động worker: {}", e.getMessage(), e);
        }
    }

    private void stopWorkerIfNoActiveSession(Sim sim) {
        try {
            List<RentSession> sessions = activeSessions.getOrDefault(sim.getId(), List.of());
            boolean hasActive = sessions.stream().anyMatch(RentSession::isActive);

            if (!hasActive) {
                PortWorker worker = workers.remove(sim.getComName().trim().toUpperCase(Locale.ROOT));
                if (worker != null) {
                    worker.stop();
                    log.info("🛑 [WORKER] Đã dừng worker cho SIM={}", sim.getPhoneNumber());
                }
            }
        } catch (Exception e) {
            log.error("❌ [WORKER] Lỗi khi dừng worker: {}", e.getMessage(), e);
        }
    }

    // ==========================================================
    // 🛠️ TIỆN ÍCH - ĐÃ SỬA
    // ==========================================================
    private void sendAtCommand(Sim sim, String cmd) {
        try {
            if (sim == null || sim.getComName() == null) {
                log.warn("⚠️ [AT_CMD] SIM hoặc COM name null, bỏ qua lệnh: {}", cmd);
                return;
            }

            PortWorker worker = comManager.getWorker(sim.getComName());
            if (worker != null) {
                worker.enqueue(PortWorker.TaskType.CMD, null, cmd, "MANUAL",
                        "manual-" + System.currentTimeMillis());
                log.info("📡 [AT_CMD] Đã đưa vào queue: '{}' cho {}", cmd, sim.getComName());
            } else {
                log.warn("⚠️ [AT_CMD] Không tìm thấy worker cho SIM {}", sim.getPhoneNumber());
            }
        } catch (Exception e) {
            log.error("❌ [AT_CMD] Lỗi khi gửi lệnh: {}", e.getMessage(), e);
        }
    }

    // ==========================================================
    // 📊 LẤY THÔNG TIN TRẠNG THÁI
    // ==========================================================
    public Map<String, Object> getWorkerStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("activeWorkers", workers.size());
        status.put("activeSessions", activeSessions.size());
        status.put("workerDetails", workers.keySet());
        return status;
    }

    public boolean isSimActive(String simId) {
        List<RentSession> sessions = activeSessions.get(simId);
        return sessions != null && sessions.stream().anyMatch(RentSession::isActive);
    }

    // ==========================================================
    // 🔌 CONNECTION HANDLING
    // ==========================================================
    public void onConnectionLost(Sim sim) {
        try {
            log.warn("🔌 [CONNECTION] Connection lost for SIM {} (COM={})", sim.getPhoneNumber(), sim.getComName());

            // Update status to INACTIVE
            sim.setStatus(String.valueOf(SimStatus.INACTIVE));
            sim.setLastUpdated(Instant.now());
            simRepository.save(sim);

            log.info("✅ [CONNECTION] SIM {} status updated to INACTIVE", sim.getPhoneNumber());
        } catch (Exception e) {
            log.error("❌ [CONNECTION] Error handling connection loss for {}: {}", sim.getComName(), e.getMessage());
        }
    }

    public void onConnectionRestored(Sim sim) {
        try {
            log.info("🔌 [CONNECTION] Connection restored for SIM {} (COM={})", sim.getPhoneNumber(), sim.getComName());

            // Update status to ACTIVE if it has a phone number
            if (sim.getPhoneNumber() != null && !sim.getPhoneNumber().isBlank()) {
                sim.setStatus(String.valueOf(SimStatus.ACTIVE));
                sim.setLastUpdated(Instant.now());
                simRepository.save(sim);
                log.info("✅ [CONNECTION] SIM {} status updated to ACTIVE", sim.getPhoneNumber());
            }
        } catch (Exception e) {
            log.error("❌ [CONNECTION] Error handling connection restore for {}: {}", sim.getComName(), e.getMessage());
        }
    }

    @Data
    @AllArgsConstructor
    public static class RentSession {
        private Long accountId;
        private List<String> services;
        private Instant startTime;
        private int durationMinutes;
        private Country country;
        private String orderId;
        private OtpSessionType type;
        private boolean otpReceived;
        private String serviceType;
        private boolean record;
        private boolean callHandled;
        private Instant callStartTime;
        private String recordFilePath;

        public boolean isActive() {
            return Instant.now().isBefore(startTime.plus(Duration.ofMinutes(durationMinutes)));
        }
    }

    // ==========================================================
    // ♻️ CLEANUP - Prevent thread leaks
    // ==========================================================
    @jakarta.annotation.PreDestroy
    public void cleanup() {
        try {
            log.info("🧹 Shutting down GsmListenerService...");

            // Shutdown scheduler
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.warn("⚠️ Scheduler did not terminate in time, forcing shutdown");
                    List<Runnable> pending = scheduler.shutdownNow();
                    log.warn("⚠️ Cancelled {} pending tasks", pending.size());
                }
            }

            // Clear state maps
            int workerCount = workers.size();
            int sessionCount = activeSessions.size();
            int retryCount = retryStates.size();

            workers.clear();
            activeSessions.clear();
            retryStates.clear();

            log.info("✅ GsmListenerService cleanup complete (cleared {} workers, {} sessions, {} retry states)",
                    workerCount, sessionCount, retryCount);
        } catch (Exception e) {
            log.error("❌ Error shutting down GsmListenerService", e);
        }
    }
}
