package app.simsmartgsm.baseGateway;

import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.service.UploadService;
import app.simsmartgsm.uitils.SmsDecoder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 🔹 Gửi dữ liệu SMS / CALL / RECORD lên Cloud API.
 * 🔹 Hỗ trợ upload file ghi âm và gửi callback sau khi upload thành công.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CloudGateway {
    private final GsmProperties props;

    private final RestTemplate restTemplate = createRestTemplate();

    private RestTemplate createRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(3000);
        factory.setReadTimeout(5000);
        return new RestTemplate(factory);
    }

    public void forwardSms(Sim sim, String sender, String message) {
        if (sim == null || sim.getPhoneNumber() == null) {
            log.warn("⚠️ SIM hoặc số điện thoại null, bỏ qua forwardSms.");
            return;
        }

        try {
            // ✅ DECODE SMS CONTENT BEFORE SENDING TO CLOUD (safety layer)
            String decodedMessage = SmsDecoder.decode(message);

            if (decodedMessage == null || decodedMessage.isBlank()) {
                log.warn("⚠️ SMS message is empty after decoding, skipping forward to cloud");
                return;
            }

            Map<String, Object> payload = Map.of(
                    "sender", sender,
                    "receiver", sim.getPhoneNumber(),
                    "message", decodedMessage); // ✅ Use decoded message

            // ✅ Ghép URL an toàn
            String baseUrl = props.getApi().getBaseUrl();
            if (!baseUrl.endsWith("/"))
                baseUrl += "/";
            String url = baseUrl + "api/seller/receive-sms";

            ResponseEntity<String> response = restTemplate.postForEntity(url, payload, String.class);
            log.info("📡 [SMS→Cloud] {} | status={} | payload={}", url, response.getStatusCodeValue(), payload);
        } catch (Exception e) {
            log.error("❌ Error forward SMS to Cloud: {}", e.getMessage(), e);
        }
    }

    // ================================================================
    // ☎️ FORWARD INCOMING CALL (báo cuộc gọi đến)
    // ================================================================
    public void forwardCall(Sim sim, String from, String recordFile) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("caller", from);
            payload.put("receiver", sim != null ? sim.getPhoneNumber() : null);
            if (recordFile != null)
                payload.put("recordFile", recordFile);

            String baseUrl = props.getApi().getBaseUrl();
            if (!baseUrl.endsWith("/"))
                baseUrl += "/";
            String url = baseUrl + "api/gsm/call/receive";

            ResponseEntity<String> res = restTemplate.postForEntity(url, payload, String.class);
            log.info("📡 [CALL→Cloud] {} | status={} | payload={}", url, res.getStatusCodeValue(), payload);
        } catch (Exception e) {
            log.error("❌ Error forward CALL: {}", e.getMessage(), e);
        }
    }

    // ================================================================
    // 📞 CALL EVENTS (CALL_OUT trạng thái START / END / FAILED)
    // ================================================================
    public void sendCallEvent(String event,
            Sim sim,
            String to,
            Instant start,
            Instant end,
            String orderId,
            boolean record,
            String errorMsg) {
        try {
            String baseUrl = props.getApi().getBaseUrl();
            if (!baseUrl.endsWith("/"))
                baseUrl += "/";
            String url = baseUrl + "api/gsm/call/receive";

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("event", event);
            body.put("orderId", orderId);
            body.put("deviceName", sim != null ? sim.getDeviceName() : null);
            body.put("comPort", sim != null ? sim.getComName() : null);
            body.put("simPhone", sim != null ? sim.getPhoneNumber() : null);
            body.put("targetPhone", to);
            body.put("record", record);
            body.put("error", errorMsg);
            body.put("callStartTime", start != null ? start.toString() : null);
            body.put("callEndTime", end != null ? end.toString() : null);

            ResponseEntity<String> res = restTemplate.postForEntity(url, body, String.class);
            log.info("📡 [CALL_EVENT:{}] {} | status={} | {}", event, url, res.getStatusCodeValue(), body);
        } catch (Exception e) {
            log.error("❌ sendCallEvent error: {}", e.getMessage(), e);
        }
    }

    public void sendCallRecord(
            Sim sim,
            String to,
            Instant start,
            Instant end,
            String uploadedUrl, // ← đã là URL public
            String orderId,
            long connectedDuration, // ← thêm: thời gian đàm thoại thực tế (giây)
            boolean callConnected // ← thêm: cuộc gọi có được bắt máy không
    ) {
        // ✅ Retry configuration
        final int MAX_RETRY_ATTEMPTS = 3;
        final int[] RETRY_DELAYS_MS = { 1000, 2000, 4000 }; // Exponential backoff

        for (int attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                String callbackUrl = props.getRecord().getCallbackUrl();
                if (callbackUrl == null || callbackUrl.isBlank()) {
                    log.warn("⚠️ Callback URL chưa được cấu hình, bỏ qua gửi record!");
                    return;
                }

                // ✅ Không upload lại, chỉ gán trực tiếp link từ server
                String publicUrl = uploadedUrl;

                // ✅ Chuẩn hóa định dạng thời gian ISO-8601 (UTC, không nano)
                DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
                        .withZone(ZoneOffset.UTC);

                String startTime = start != null ? fmt.format(start) : fmt.format(Instant.now());
                String endTime = end != null ? fmt.format(end) : fmt.format(Instant.now());

                // ✅ Xác định trạng thái cuộc gọi chi tiết
                String callStatus;
                String callResult;

                if (callConnected) {
                    callStatus = "CALL_OUT_CONNECTED"; // Đã kết nối
                    callResult = "SUCCESS";
                } else if (connectedDuration > 0) {
                    callStatus = "CALL_OUT_NO_ANSWER"; // Đổ chuông nhưng không bắt máy
                    callResult = "NO_ANSWER";
                } else {
                    callStatus = "CALL_OUT_FAILED"; // Gọi thất bại
                    callResult = "FAILED";
                }

                // ✅ Tính tổng thời gian cuộc gọi
                long totalDuration = 0;
                if (start != null && end != null) {
                    totalDuration = Duration.between(start, end).getSeconds();
                }

                // ✅ JSON body
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("orderId", orderId);
                body.put("fromNumber", sim != null ? sim.getPhoneNumber() : null);
                body.put("toNumber", to);
                body.put("comPort", sim != null ? sim.getComName() : null);
                body.put("deviceName", sim != null ? sim.getDeviceName() : null);
                body.put("status", callStatus); // ✅ Trạng thái chi tiết
                body.put("callResult", callResult); // ✅ Kết quả cuộc gọi
                body.put("recordFile", publicUrl); // ✅ Gán thẳng URL
                body.put("callStartTime", startTime);
                body.put("callEndTime", endTime);
                body.put("totalDuration", totalDuration); // ✅ Tổng thời gian cuộc gọi
                body.put("connectedDuration", connectedDuration); // ✅ Thời gian đàm thoại thực tế
                body.put("expireAt", null);

                // ✅ Thêm thông tin lỗi nếu có
                if (!callConnected) {
                    if (connectedDuration > 0) {
                        body.put("errorCode", "NO_ANSWER");
                        body.put("errorMessage", "Thuê bao không bắt máy sau " + connectedDuration + " giây");
                    } else {
                        body.put("errorCode", "CALL_FAILED");
                        body.put("errorMessage", "Cuộc gọi thất bại hoặc bị từ chối");
                    }
                }

                // ✅ Headers
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                HttpEntity<Map<String, Object>> entity = new HttpEntity<>(body, headers);

                // ✅ Gửi callback
                ResponseEntity<String> res = restTemplate.postForEntity(callbackUrl, entity, String.class);

                log.info(
                        "📡 [CALL_RECORD] ✅ Sent to {} | status={} | callResult={} | total={}s | connected={}s | orderId={} | record={} | attempt={}",
                        callbackUrl, res.getStatusCodeValue(), callResult, totalDuration, connectedDuration, orderId,
                        publicUrl, attempt + 1);

                // ✅ Success - exit retry loop
                return;

            } catch (HttpServerErrorException e) {
                // ✅ Server error (5xx) - retry with backoff
                log.warn("⚠️ [CALL_RECORD] Server error (attempt {}/{}): {} | response={} | orderId={}",
                        attempt + 1, MAX_RETRY_ATTEMPTS, e.getStatusCode(), e.getResponseBodyAsString(), orderId);

                if (attempt < MAX_RETRY_ATTEMPTS - 1) {
                    try {
                        Thread.sleep(RETRY_DELAYS_MS[attempt]);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("❌ [CALL_RECORD] Retry interrupted for orderId={}", orderId);
                        return;
                    }
                } else {
                    log.error("❌ [CALL_RECORD] Max retries reached for orderId={} | final error: {} | response={}",
                            orderId, e.getStatusCode(), e.getResponseBodyAsString());
                }
            } catch (HttpClientErrorException e) {
                // ✅ Client error (4xx) - don't retry
                log.error("❌ [CALL_RECORD] Client error (no retry): {} | response={} | orderId={}",
                        e.getStatusCode(), e.getResponseBodyAsString(), orderId, e);
                return;
            } catch (Exception e) {
                // ✅ Other errors - log and retry
                log.warn("⚠️ [CALL_RECORD] Error (attempt {}/{}): {} | orderId={}",
                        attempt + 1, MAX_RETRY_ATTEMPTS, e.getMessage(), orderId);

                if (attempt < MAX_RETRY_ATTEMPTS - 1) {
                    try {
                        Thread.sleep(RETRY_DELAYS_MS[attempt]);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("❌ [CALL_RECORD] Retry interrupted for orderId={}", orderId);
                        return;
                    }
                } else {
                    log.error("❌ [CALL_RECORD] Max retries reached for orderId={} | final error: {}",
                            orderId, e.getMessage(), e);
                }
            }
        }
    }

    // ✅ Overload method cũ để tương thích ngược
    public void sendCallRecord(
            Sim sim,
            String to,
            Instant start,
            Instant end,
            String uploadedUrl,
            String orderId,
            String serviceCode, // 🟢 THÊM
            long connectedDuration,
            boolean callConnected) {
        try {
            String callbackUrl = props.getRecord().getCallbackUrl();
            if (callbackUrl == null || callbackUrl.isBlank()) {
                log.warn("⚠️ Callback URL chưa được cấu hình, bỏ qua!");
                return;
            }

            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
                    .withZone(ZoneOffset.UTC);

            String startTime = start != null ? fmt.format(start) : fmt.format(Instant.now());
            String endTime = end != null ? fmt.format(end) : fmt.format(Instant.now());

            String callStatus;
            String callResult;

            // 🔥 HAPPYMAIL LOGIC: Nếu là Happymail, chỉ cần gọi được (dial thành công) thì
            // coi là SUCCESS
            // Chỉ báo FAILED khi có lỗi hệ thống (COM bận, không dial được)
            boolean isHappymail = "Happymail".equalsIgnoreCase(serviceCode);

            if (isHappymail) {
                // ✅ Happymail: Luôn SUCCESS nếu đã thực hiện gọi (dù reject hay không nghe máy)
                callStatus = "SUCCESS";
                callResult = "SUCCESS";
            } else {
                // ✅ Các service khác: Phân biệt SUCCESS/FAILED dựa vào callConnected
                if (callConnected) {
                    callStatus = "SUCCESS";
                    callResult = "SUCCESS";
                } else {
                    callStatus = "FAILED";
                    callResult = "FAILED";
                }
            }

            long totalDuration = 0;
            if (start != null && end != null) {
                totalDuration = Duration.between(start, end).getSeconds();
            }

            // ============================================================
            // 🔥 BODY gửi về backend - BỔ SUNG serviceCode !!!
            // ============================================================
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("orderId", orderId);
            body.put("serviceCode", serviceCode); // 🟢 FIX QUAN TRỌNG
            body.put("fromNumber", sim != null ? sim.getPhoneNumber() : null);
            body.put("toNumber", to);
            body.put("deviceName", sim != null ? sim.getDeviceName() : null);
            body.put("comPort", sim != null ? sim.getComName() : null);
            body.put("recordFile", uploadedUrl);
            body.put("status", callStatus);
            body.put("callResult", callResult);
            body.put("callStartTime", startTime);
            body.put("callEndTime", endTime);
            body.put("totalDuration", totalDuration);
            body.put("connectedDuration", connectedDuration);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(body, headers);

            ResponseEntity<String> res = restTemplate.postForEntity(callbackUrl, entity, String.class);

            log.info("📡 [CALL RECORD] → {} | status={} | body={}",
                    callbackUrl, res.getStatusCodeValue(), body);

        } catch (HttpClientErrorException | HttpServerErrorException e) {
            log.error("❌ sendCallRecord error: {} | {}",
                    e.getStatusCode(), e.getResponseBodyAsString());
        } catch (Exception e) {
            log.error("❌ sendCallRecord exception: {}", e.getMessage());
        }
    }

    // Giữ overload cũ để không lỗi compile
    public void sendCallRecord(
            Sim sim,
            String to,
            Instant start,
            Instant end,
            String uploadedUrl,
            String orderId) {
        sendCallRecord(sim, to, start, end, uploadedUrl, orderId, null, 0, false);
    }

}
