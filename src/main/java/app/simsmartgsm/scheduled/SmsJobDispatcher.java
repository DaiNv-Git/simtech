package app.simsmartgsm.scheduled;

import app.simsmartgsm.config.RemoteStompClientConfig;
import app.simsmartgsm.entity.SmsMessage;
import app.simsmartgsm.repository.SmsMessageRepository;
import app.simsmartgsm.service.PortManager;
import app.simsmartgsm.uitils.AtCommandHelper;
import app.simsmartgsm.uitils.DeviceIdProvider;
import app.simsmartgsm.uitils.MarketingSessionRegistry;
import ch.qos.logback.classic.net.SimpleSocketServer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.*;

// @Service // DISABLED to avoid concurrent dispatch with RemoteSmsJobSubscriberConfig
@RequiredArgsConstructor
@Slf4j
public class SmsJobDispatcher {

    private final RemoteStompClientConfig stompClientConfig;
    private final SmsMessageRepository smsMessageRepository;
    private final PortManager portManager;
    private final MarketingSessionRegistry marketingRegistry;

    private String localDeviceName;

    /** Mỗi COM có 1 queue riêng (để xử lý tuần tự) */
    private final Map<String, BlockingQueue<JSONObject>> comQueues = new ConcurrentHashMap<>();
    private final Map<String, Thread> comWorkers = new ConcurrentHashMap<>();

    /** Khởi tạo khi app start */
    @PostConstruct
    public void init() {
        try {
            this.localDeviceName = DeviceIdProvider.getDeviceId() ;
        } catch (Exception e) {
            this.localDeviceName = "UNKNOWN_HOST";
            log.error("❌ Không lấy được hostname", e);
        }
        log.info("💻 Local deviceName: {}", localDeviceName);

        // 👉 Đăng ký callback để subscribe topic sau khi connect WS thành công
        stompClientConfig.addOnConnectedCallback(this::subscribeSmsJobs);
    }

    /** Gọi khi StompSession connect thành công */
    private void subscribeSmsJobs(StompSession session) {
        session.subscribe("/topic/sms-job-topic", new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
                return byte[].class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
                try {
                    String body = (payload instanceof byte[]) ? new String((byte[]) payload) : payload.toString();
                    log.info("📩 Nhận job từ topic: {}", body);

                    JSONObject job = new JSONObject(body);

                    // ✅ Lọc theo deviceName
                    if (!localDeviceName.equalsIgnoreCase(job.optString("deviceName"))) {
                        log.debug("🚫 Bỏ qua job không khớp deviceName (local={}, msg={})",
                                localDeviceName, job.optString("deviceName"));
                        return;
                    }

                    String comName = job.getString("comName");

                    // ✅ Gán job vào hàng đợi COM tương ứng
                    comQueues
                            .computeIfAbsent(comName, k -> {
                                BlockingQueue<JSONObject> q = new LinkedBlockingQueue<>();
                                startComWorker(comName, q, session);
                                return q;
                            })
                            .offer(job);

                } catch (Exception e) {
                    log.error("❌ Lỗi khi parse job", e);
                }
            }
        });

        log.info("✅ Đã subscribe topic /topic/sms-job-topic");
    }

    /** Worker riêng cho từng COM */
    private void startComWorker(String comName, BlockingQueue<JSONObject> queue, StompSession session) {
        Thread worker = new Thread(() -> {
            log.info("🚀 Worker chạy cho COM {}", comName);
            while (true) {
                try {
                    JSONObject job = queue.take(); // lấy tuần tự
                    processSmsJob(job, comName, session);
                } catch (Exception e) {
                    log.error("❌ Lỗi worker COM {}", comName, e);
                }
            }
        }, "Worker-" + comName);

        worker.setDaemon(true);
        worker.start();
        comWorkers.put(comName, worker);
        log.info("✅ Khởi tạo worker thread cho COM {}", comName);
    }

    /** Xử lý logic gửi SMS */
    private void processSmsJob(JSONObject job, String comName, StompSession session) {
        String localMsgId = job.optString("localMsgId", "");
        String simId      = job.optString("simId", "");
        String simPhone   = job.optString("simPhoneNumber", "");
        String toNumber   = job.optString("phoneNumber", "");
        String content    = job.optString("content", "");
        String campaignId = job.optString("campaignId", null);
        String sessionId  = job.optString("sessionId", null);

        String mode = job.optString("smsType", "ONE_WAY");
        int replyWindowMinutes = job.optInt("timeDuration", 0);

        JSONObject response = new JSONObject();
        response.put("localMsgId", localMsgId);
        response.put("simId", simId);
        response.put("campaignId", campaignId);
        response.put("sessionId", sessionId);
        response.put("phoneNumber", toNumber);
        response.put("comName", comName);

        boolean success = false;
        String errorMsg = null;

        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                log.info("📤 [Attempt {}] Gửi SMS qua {} (SIM={}) → {}: {}", attempt, comName, simPhone, toNumber, content);
                success = sendSmsViaCom(comName, toNumber, content);
                if (success) break;

                // ✅ FIX #2: Exponential backoff — 1s, 2s, 4s (cap 10s)
                // Thay vì fixed 1200ms, mỗi lần fail sẽ chờ lâu hơn
                // Tránh retry flood gây tắc nghẽn khi queue đầy
                long backoffMs = Math.min(1000L * (1L << (attempt - 1)), 10_000L);
                log.debug("⏳ [{}] Retry attempt {}: chờ {}ms", comName, attempt, backoffMs);
                Thread.sleep(backoffMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("⚠️ [{}] Retry bị interrupt, dừng", comName);
                break;
            } catch (Exception e) {
                errorMsg = e.getMessage();
                log.warn("⚠️ Attempt {} thất bại COM {}: {}", attempt, comName, errorMsg);
                // ✅ FIX #2: Cũng dùng exponential backoff khi exception
                long backoffMs = Math.min(1000L * (1L << (attempt - 1)), 10_000L);
                try { Thread.sleep(backoffMs); } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        if (success) {
            response.put("status", "SENT");
            response.put("errorMsg", JSONObject.NULL);
            if ("TWO_WAY".equalsIgnoreCase(mode) && replyWindowMinutes > 0) {
                Instant expiresAt = Instant.now().plus(replyWindowMinutes, ChronoUnit.MINUTES);
                marketingRegistry.register(
                        simPhone,      // số SIM trong GSM
                        toNumber,      // số KH
                        campaignId,    // mã campaign
                        sessionId,     // sessionId
                        localMsgId,    // ID tin gửi (BE sinh ra)
                        simId,         // ID SIM
                        expiresAt      // thời gian hết hạn
                );

                log.info("🕒 Đăng ký TWO_WAY session sim={} ↔ cus={} hết hạn {}", simPhone, toNumber, expiresAt);
            }

        } else {
            response.put("status", "FAILED");
            response.put("errorMsg", errorMsg != null ? errorMsg : "Unknown error");
            log.error("❌ Gửi SMS thất bại sau 3 lần qua COM {} → {}", comName, toNumber);
        }

        session.send("/topic/sms-response", response.toString().getBytes());
        log.info("📩 Pushed WS result: {}", response);

        // ✅ Lưu vào MongoDB
        try {
            SmsMessage sms = new SmsMessage();
            sms.setOrderId(campaignId);
            sms.setDeviceName(localDeviceName);
            sms.setComPort(comName);
            sms.setSimPhone(simPhone);
            sms.setFromNumber(simPhone);
            sms.setToNumber(toNumber);
            sms.setContent(content);
            sms.setModemResponse(success ? "OK" : (errorMsg != null ? errorMsg : "ERROR"));
            sms.setType("OUTBOX");
            sms.setTimestamp(Instant.now());

            smsMessageRepository.save(sms);
            log.info("💾 Đã lưu OUTBOX SMS vào DB: {}", sms.getId());
        } catch (Exception e) {
            log.error("❌ Lỗi khi lưu SMS OUTBOX", e);
        }
    }

    /** Thực thi gửi SMS qua cổng COM thật sự */
    private boolean sendSmsViaCom(String comName, String phoneNumber, String content) {
        Boolean ok = portManager.withPort(comName, (AtCommandHelper helper) -> {
            try {
                // Bước 1: gửi lệnh CMGS
                String resp = helper.sendAndRead("AT+CMGS=\"" + phoneNumber + "\"", 2000);
                if (resp == null || !resp.contains(">")) {
                    log.error("❌ Không nhận được prompt '>' từ modem {}", comName);
                    return false;
                }

                // Bước 2: gửi nội dung tin nhắn
                helper.writeRaw(content.getBytes());
                helper.writeCtrlZ();

                // Bước 3: đọc phản hồi OK
                String finalResp = helper.sendAndRead("", 5000);
                log.info("📩 Phản hồi từ {}: {}", comName, finalResp);
                return finalResp != null && finalResp.contains("OK");

            } catch (Exception e) {
                log.error("❌ Lỗi khi gửi SMS qua {}: {}", comName, e.getMessage(), e);
                return false;
            }
        }, 5000);

        return ok != null && ok;
    }

}
