package app.simsmartgsm.controller;

import app.simsmartgsm.dto.request.*;
import app.simsmartgsm.dto.response.ApiResponse;
import app.simsmartgsm.dto.response.SimResponse;
import app.simsmartgsm.entity.*;
import app.simsmartgsm.service.GsmService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Controller chính cho GSM App
 * Bao gồm tất cả APIs: SIM, SMS, Call, Recording, Settings
 */
@RestController
@RequestMapping("/api/gsm")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "GSM API", description = "API cho ứng dụng GSM Smart")
public class GsmController {

    private final GsmService gsmService;
    private final app.simsmartgsm.service.SmsDailyLimitService smsDailyLimitService;

    // ==================== SIM APIS ====================

    @GetMapping("/sim/scan")
    @Operation(summary = "Scan tất cả SIM", description = "Quét tất cả COM ports và lấy thông tin SIM. Detect số chạy background.")
    public ResponseEntity<ApiResponse<List<SimResponse>>> scanSims() {
        try {
            log.info("🔍 Bắt đầu scan SIM...");
            List<SimResponse> sims = gsmService.scanAllSims();
            return ResponseEntity.ok(ApiResponse.success("Scan hoàn tất. Detect số đang chạy background.", sims));
        } catch (Exception e) {
            log.error("❌ Lỗi scan SIM: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/sim/detect-numbers")
    @Operation(summary = "Detect số điện thoại (DEPRECATED)", description = "Tính năng đã bị loại bỏ. Số điện thoại được detect tự động khi scan.")
    public ResponseEntity<ApiResponse<String>> detectNumbers() {
        log.info("🔢 /sim/detect-numbers called - Feature has been removed");
        return ResponseEntity.ok(ApiResponse.success("Tính năng detect số đã được tích hợp vào scan. Vui lòng dùng API /api/gsm/sim/scan", "DEPRECATED"));
    }

    @GetMapping("/sim/list")
    @Operation(summary = "Danh sách SIM online", description = "Lấy danh sách SIM đang hoạt động")
    public ResponseEntity<ApiResponse<List<SimResponse>>> getSimList() {
        try {
            List<SimResponse> sims = gsmService.getOnlineSims();
            return ResponseEntity.ok(ApiResponse.success(sims));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/sim/{comPort}")
    @Operation(summary = "Thông tin SIM", description = "Lấy thông tin chi tiết của 1 SIM")
    public ResponseEntity<ApiResponse<SimResponse>> getSimInfo(@PathVariable String comPort) {
        try {
            SimResponse sim = gsmService.getSimInfo(comPort);
            return ResponseEntity.ok(ApiResponse.success(sim));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/sim/scan/{comPort}")
    @Operation(summary = "Reload 1 COM port", description = "Quét lại một COM port cụ thể")
    public ResponseEntity<ApiResponse<SimResponse>> rescanPort(@PathVariable String comPort) {
        try {
            log.info("🔄 Rescanning COM port: {}", comPort);
            SimResponse sim = gsmService.rescanSinglePort(comPort);
            return ResponseEntity.ok(ApiResponse.success("Đã quét lại " + comPort, sim));
        } catch (Exception e) {
            log.error("❌ Lỗi rescan {}: {}", comPort, e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    // ==================== SMS APIS ====================

    @PostMapping("/sms/send")
    @Operation(summary = "Gửi SMS đơn", description = "Gửi 1 tin nhắn SMS")
    public ResponseEntity<ApiResponse<SmsMessageEntity>> sendSms(@RequestBody SendSmsRequest request) {
        try {
            log.info("📤 Gửi SMS tới {} qua {}", request.getPhoneNumber(), request.getComPort());
            SmsMessageEntity result = gsmService.sendSms(request);
            return ResponseEntity.ok(ApiResponse.success("Đã thêm vào hàng chờ gửi", result));
        } catch (Exception e) {
            log.error("❌ Lỗi gửi SMS: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/sms/bulk-send")
    @Operation(summary = "Gửi SMS hàng loạt", description = "Gửi tin nhắn đến nhiều số qua nhiều COM port")
    public ResponseEntity<ApiResponse<List<SmsMessageEntity>>> sendBulkSms(@RequestBody BulkSmsRequest request) {
        try {
            log.info("📤 Gửi Bulk SMS tới {} số qua {} COM ports",
                    request.getPhoneNumbers().size(), request.getComPorts().size());
            List<SmsMessageEntity> results = gsmService.sendBulkSms(request);
            return ResponseEntity.ok(ApiResponse.success("Đã gửi " + results.size() + " tin nhắn", results));
        } catch (Exception e) {
            log.error("❌ Lỗi gửi Bulk SMS: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/sms/read/{comPort}")
    @Operation(summary = "Đọc SMS từ modem", description = "Đọc tất cả SMS từ modem và lưu vào database")
    public ResponseEntity<ApiResponse<List<SmsMessageEntity>>> readSmsFromModem(@PathVariable String comPort) {
        try {
            log.info("📥 Đọc SMS từ {}", comPort);
            List<SmsMessageEntity> messages = gsmService.readSmsFromModem(comPort);
            return ResponseEntity.ok(ApiResponse.success("Đọc được " + messages.size() + " tin nhắn", messages));
        } catch (Exception e) {
            log.error("❌ Lỗi đọc SMS: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/sms/inbox")
    @Operation(summary = "Danh sách Inbox", description = "Lấy tin nhắn đã nhận")
    public ResponseEntity<ApiResponse<Page<SmsMessageEntity>>> getInbox(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String phoneNumber) {
        try {
            Page<SmsMessageEntity> messages;
            if (phoneNumber != null && !phoneNumber.isEmpty()) {
                messages = gsmService.getInboxByPhone(page, size, phoneNumber);
            } else if (startDate != null && endDate != null) {
                LocalDateTime start = LocalDate.parse(startDate).atStartOfDay();
                LocalDateTime end = LocalDate.parse(endDate).atTime(23, 59, 59);
                messages = gsmService.getInboxByDate(page, size, start, end);
            } else {
                messages = gsmService.getInbox(page, size);
            }
            return ResponseEntity.ok(ApiResponse.success(messages));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/sms/sent")
    @Operation(summary = "Danh sách Sent", description = "Lấy tin nhắn đã gửi thành công")
    public ResponseEntity<ApiResponse<Page<SmsMessageEntity>>> getSent(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String phoneNumber) {
        try {
            Page<SmsMessageEntity> messages;
            if (phoneNumber != null && !phoneNumber.isEmpty()) {
                messages = gsmService.getSentByPhone(page, size, phoneNumber);
            } else if (startDate != null && endDate != null) {
                LocalDateTime start = LocalDate.parse(startDate).atStartOfDay();
                LocalDateTime end = LocalDate.parse(endDate).atTime(23, 59, 59);
                messages = gsmService.getSentByDate(page, size, start, end);
            } else {
                messages = gsmService.getSent(page, size);
            }
            return ResponseEntity.ok(ApiResponse.success(messages));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/sms/outbox")
    @Operation(summary = "Danh sách Outbox", description = "Lấy tin nhắn gửi lỗi")
    public ResponseEntity<ApiResponse<Page<SmsMessageEntity>>> getOutbox(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String phoneNumber) {
        try {
            Page<SmsMessageEntity> messages;
            if (phoneNumber != null && !phoneNumber.isEmpty()) {
                messages = gsmService.getOutboxByPhone(page, size, phoneNumber);
            } else if (startDate != null && endDate != null) {
                LocalDateTime start = LocalDate.parse(startDate).atStartOfDay();
                LocalDateTime end = LocalDate.parse(endDate).atTime(23, 59, 59);
                messages = gsmService.getOutboxByDate(page, size, start, end);
            } else {
                messages = gsmService.getOutbox(page, size);
            }
            return ResponseEntity.ok(ApiResponse.success(messages));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/sms/unread-count")
    @Operation(summary = "Số tin chưa đọc", description = "Đếm tin nhắn inbox chưa đọc")
    public ResponseEntity<ApiResponse<Long>> getUnreadCount() {
        try {
            long count = gsmService.getUnreadCount();
            return ResponseEntity.ok(ApiResponse.success(count));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PutMapping("/sms/{id}/read")
    @Operation(summary = "Đánh dấu đã đọc", description = "Đánh dấu tin nhắn đã đọc")
    public ResponseEntity<ApiResponse<Void>> markAsRead(@PathVariable Long id) {
        try {
            gsmService.markAsRead(id);
            return ResponseEntity.ok(ApiResponse.success("Đã đánh dấu đọc", null));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PutMapping("/sms/mark-all-read")
    @Operation(summary = "Đánh dấu tất cả đã đọc", description = "Đánh dấu tất cả tin nhắn inbox là đã đọc")
    public ResponseEntity<ApiResponse<Integer>> markAllAsRead() {
        try {
            int count = gsmService.markAllAsRead();
            return ResponseEntity.ok(ApiResponse.success("Đã đánh dấu " + count + " tin nhắn đã đọc", count));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/sms/decode-all")
    @Operation(summary = "Decode tất cả SMS", description = "Decode lại toàn bộ SMS trong database (chạy 1 lần để fix old data)")
    public ResponseEntity<ApiResponse<Map<String, Object>>> decodeAllSms() {
        try {
            log.info("🔄 Starting SMS decoding migration via API...");
            int decodedCount = gsmService.decodeAllSmsInDatabase();

            Map<String, Object> result = Map.of(
                    "decodedCount", decodedCount,
                    "message", "Đã decode " + decodedCount + " tin nhắn");

            return ResponseEntity.ok(ApiResponse.success("Migration hoàn thành", result));
        } catch (Exception e) {
            log.error("❌ SMS decoding migration error: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error("Lỗi migration: " + e.getMessage()));
        }
    }

    @PostMapping("/sms/{id}/resend")
    @Operation(summary = "Gửi lại tin lỗi", description = "Gửi lại tin nhắn thất bại")
    public ResponseEntity<ApiResponse<SmsMessageEntity>> resendSms(@PathVariable Long id) {
        try {
            SmsMessageEntity result = gsmService.resendSms(id);
            return ResponseEntity.ok(ApiResponse.success("Đã gửi lại", result));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @DeleteMapping("/sms/{id}")
    @Operation(summary = "Xóa tin nhắn", description = "Xóa tin nhắn khỏi database")
    public ResponseEntity<ApiResponse<Void>> deleteSms(@PathVariable Long id) {
        try {
            gsmService.deleteSms(id);
            return ResponseEntity.ok(ApiResponse.success("Đã xóa", null));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @DeleteMapping("/sms/delete-all")
    @Operation(summary = "Xóa toàn bộ tin nhắn local", description = "⚠️ Xóa tất cả tin nhắn trong database local (không thể hoàn tác)")
    public ResponseEntity<ApiResponse<Map<String, Object>>> deleteAllSmsMessages() {
        try {
            log.warn("🗑️ API called: DELETE ALL SMS MESSAGES");
            int deletedCount = gsmService.deleteAllSmsMessages();

            Map<String, Object> result = Map.of(
                    "deletedCount", deletedCount,
                    "message", "Đã xóa " + deletedCount + " tin nhắn",
                    "timestamp", java.time.Instant.now().toString());

            return ResponseEntity.ok(ApiResponse.success("Đã xóa toàn bộ tin nhắn", result));
        } catch (Exception e) {
            log.error("❌ Lỗi xóa tin nhắn: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error("Lỗi xóa tin nhắn: " + e.getMessage()));
        }
    }

    // ==================== CALL APIS ====================

    @PostMapping("/call/make")
    @Operation(summary = "Thực hiện cuộc gọi", description = "Gọi điện với setting thời gian và ghi âm")
    public ResponseEntity<ApiResponse<CallRecordEntity>> makeCall(@RequestBody MakeCallRequest request) {
        try {
            log.info("📞 Gọi tới {} qua {}, duration={}s, record={}",
                    request.getTargetPhone(), request.getComPort(),
                    request.getCallDuration(), request.getRecord());
            CallRecordEntity result = gsmService.makeCall(request);
            return ResponseEntity.ok(ApiResponse.success("Đang thực hiện cuộc gọi", result));
        } catch (Exception e) {
            log.error("❌ Lỗi gọi điện: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/call/answer")
    @Operation(summary = "Nhận cuộc gọi đến", description = "Đợi và nhận cuộc gọi đến với setting ghi âm. Có thể filter theo số gọi đến.")
    public ResponseEntity<ApiResponse<CallRecordEntity>> answerCall(@RequestBody AnswerCallRequest request) {
        try {
            log.info(
                    "📞 Đợi cuộc gọi đến qua {}, expectedCaller={}, wait={}s, duration={}s, record={}, acceptHidden={}",
                    request.getComPort(), request.getExpectedCaller(),
                    request.getWaitTimeout(), request.getCallDuration(),
                    request.getRecord(), request.getAcceptHiddenCaller());
            CallRecordEntity result = gsmService.answerCall(request);
            return ResponseEntity.ok(ApiResponse.success("Đang đợi cuộc gọi đến", result));
        } catch (Exception e) {
            log.error("❌ Lỗi nhận cuộc gọi: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/call/hangup/{comPort}")
    @Operation(summary = "Kết thúc cuộc gọi", description = "Tắt cuộc gọi đang thực hiện")
    public ResponseEntity<ApiResponse<Void>> hangupCall(@PathVariable String comPort) {
        try {
            gsmService.hangupCall(comPort);
            return ResponseEntity.ok(ApiResponse.success("Đã tắt cuộc gọi", null));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    // ==================== CALL WITH AUDIO APIS ====================

    @PostMapping("/call/with-audio")
    @Operation(summary = "Gọi và phát audio", description = "Gọi điện tới 1 số và phát file audio ghi sẵn cho đối phương nghe. " +
            "Sử dụng AT+QPSND để phát audio vào uplink. Chỉ hỗ trợ module EC25/EC21.")
    public ResponseEntity<ApiResponse<CallRecordEntity>> makeCallWithAudio(@RequestBody CallWithAudioRequest request) {
        try {
            log.info("📞🔊 Gọi với audio tới {} qua {}, audioFile={}",
                    request.getTargetPhone(), request.getComPort(), request.getAudioFileName());
            CallRecordEntity result = gsmService.makeCallWithAudio(request);
            return ResponseEntity.ok(ApiResponse.success("Đang thực hiện cuộc gọi với audio", result));
        } catch (Exception e) {
            log.error("❌ Lỗi gọi với audio: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/call/upload-audio")
    @Operation(summary = "Upload audio lên modem", description = "Upload file audio lên bộ nhớ modem (UFS) để sử dụng với /call/with-audio. " +
            "File phải là PCM 8KHz 16bit Mono (.wav) hoặc AMR (.amr)")
    public ResponseEntity<ApiResponse<Map<String, Object>>> uploadAudioToModem(
            @RequestParam String comPort,
            @RequestParam String filePath) {
        try {
            log.info("📤 Upload audio tới modem {} từ {}", comPort, filePath);
            Map<String, Object> result = gsmService.uploadAudioToModem(comPort, filePath);
            return ResponseEntity.ok(ApiResponse.success("Upload hoàn tất", result));
        } catch (Exception e) {
            log.error("❌ Lỗi upload audio: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/call/modem-files/{comPort}")
    @Operation(summary = "Liệt kê file trên modem", description = "Xem danh sách file đã upload trên bộ nhớ modem (UFS)")
    public ResponseEntity<ApiResponse<Map<String, Object>>> listModemFiles(@PathVariable String comPort) {
        try {
            Map<String, Object> result = gsmService.listAudioFilesOnModem(comPort);
            return ResponseEntity.ok(ApiResponse.success(result));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/call/history")
    @Operation(summary = "Lịch sử cuộc gọi", description = "Xem lịch sử cuộc gọi")
    public ResponseEntity<ApiResponse<Page<CallRecordEntity>>> getCallHistory(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String comPort,
            @RequestParam(required = false) String phoneNumber) {
        try {
            Page<CallRecordEntity> calls;
            if (phoneNumber != null && !phoneNumber.isEmpty()) {
                calls = gsmService.getCallHistoryByPhone(page, size, phoneNumber);
            } else {
                calls = gsmService.getCallHistory(page, size, comPort);
            }
            return ResponseEntity.ok(ApiResponse.success(calls));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/call/{id}")
    @Operation(summary = "Chi tiết cuộc gọi", description = "Xem chi tiết 1 cuộc gọi")
    public ResponseEntity<ApiResponse<CallRecordEntity>> getCallDetail(@PathVariable Long id) {
        try {
            CallRecordEntity call = gsmService.getCallById(id);
            return ResponseEntity.ok(ApiResponse.success(call));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    // ==================== RECORDING APIS ====================

    @GetMapping("/recording/list")
    @Operation(summary = "Danh sách file ghi âm", description = "Lấy danh sách file ghi âm trong thư mục")
    public ResponseEntity<ApiResponse<List<Map<String, Object>>>> getRecordings() {
        try {
            List<Map<String, Object>> recordings = gsmService.getRecordingFiles();
            return ResponseEntity.ok(ApiResponse.success(recordings));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/recording/download/{fileName}")
    @Operation(summary = "Download file ghi âm", description = "Tải file ghi âm WAV")
    public ResponseEntity<?> downloadRecording(@PathVariable String fileName) {
        try {
            return gsmService.downloadRecording(fileName);
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/recording/open/{fileName}")
    @Operation(summary = "Mở file ghi âm", description = "Mở file ghi âm bằng ứng dụng mặc định")
    public ResponseEntity<ApiResponse<String>> openRecording(@PathVariable String fileName) {
        try {
            String path = gsmService.openRecordingFile(fileName);
            return ResponseEntity.ok(ApiResponse.success("Đã mở file", path));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/recording/open-folder/{fileName}")
    @Operation(summary = "Mở thư mục chứa file ghi âm", description = "Mở folder chứa file ghi âm (thay vì mở file)")
    public ResponseEntity<ApiResponse<String>> openRecordingFolder(@PathVariable String fileName) {
        try {
            String folderPath = gsmService.openRecordingFolder(fileName);
            return ResponseEntity.ok(ApiResponse.success("Đã mở thư mục", folderPath));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/recording/open-folder")
    @Operation(summary = "Mở thư mục ghi âm", description = "Mở folder chứa tất cả file ghi âm")
    public ResponseEntity<ApiResponse<String>> openRecordingFolderDirect() {
        try {
            String folderPath = gsmService.openRecordingFolderDirect();
            return ResponseEntity.ok(ApiResponse.success("Đã mở thư mục", folderPath));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @DeleteMapping("/recording/{fileName}")
    @Operation(summary = "Xóa file ghi âm", description = "Xóa file ghi âm")
    public ResponseEntity<ApiResponse<Void>> deleteRecording(@PathVariable String fileName) {
        try {
            gsmService.deleteRecording(fileName);
            return ResponseEntity.ok(ApiResponse.success("Đã xóa file", null));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/recording/config")
    @Operation(summary = "Cấu hình recording", description = "Lấy thư mục lưu file ghi âm")
    public ResponseEntity<ApiResponse<String>> getRecordingConfig() {
        try {
            String path = gsmService.getRecordingPath();
            return ResponseEntity.ok(ApiResponse.success(path));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    // ==================== STATS APIS ====================

    @GetMapping("/stats")
    @Operation(summary = "Thống kê", description = "Lấy thống kê hệ thống")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getStats() {
        try {
            Map<String, Object> stats = gsmService.getStats();
            return ResponseEntity.ok(ApiResponse.success(stats));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/stats/fail-stats")
    @Operation(summary = "Thống kê lỗi SIM", description = "Lấy thống kê fail liên tiếp và blacklist của SIM (reset hàng ngày)")
    public ResponseEntity<ApiResponse<Map<String, Map<String, Object>>>> getFailStats() {
        try {
            Map<String, Map<String, Object>> stats = smsDailyLimitService.getFailStats();
            return ResponseEntity.ok(ApiResponse.success(stats));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/sim/{comPort}/unblacklist")
    @Operation(summary = "Gỡ blacklist SIM", description = "Reset manual một SIM đã bị blacklist")
    public ResponseEntity<ApiResponse<String>> unblacklistSim(@PathVariable String comPort) {
        try {
            smsDailyLimitService.unblacklist(comPort);
            return ResponseEntity.ok(ApiResponse.success("Đã gỡ blacklist cho SIM " + comPort, "OK"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    // ==================== CLEANUP APIS ====================

    @PostMapping("/sim/cleanup-duplicates")
    @Operation(summary = "Cleanup duplicate SIM", description = "Xóa các SIM duplicate trong database, giữ lại SIM mới nhất")
    public ResponseEntity<ApiResponse<Map<String, Object>>> cleanupDuplicateSims() {
        try {
            log.info("🧹 Bắt đầu cleanup duplicate SIM...");
            int deletedCount = gsmService.cleanupDuplicateSims();

            Map<String, Object> result = Map.of(
                    "deletedCount", deletedCount,
                    "message", "Đã xóa " + deletedCount + " SIM duplicate");

            return ResponseEntity.ok(ApiResponse.success("Cleanup hoàn tất", result));
        } catch (Exception e) {
            log.error("❌ Lỗi cleanup duplicate: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/sim/cleanup-replaced")
    @Operation(summary = "Cleanup SIM REPLACED cũ", description = "Xóa các SIM REPLACED cũ hơn 30 ngày")
    public ResponseEntity<ApiResponse<Map<String, Object>>> cleanupOldReplacedSims() {
        try {
            log.info("🗑️ Bắt đầu cleanup SIM REPLACED cũ...");
            int deletedCount = gsmService.cleanupOldReplacedSims();

            Map<String, Object> result = Map.of(
                    "deletedCount", deletedCount,
                    "message", "Đã xóa " + deletedCount + " SIM REPLACED cũ");

            return ResponseEntity.ok(ApiResponse.success("Cleanup hoàn tất", result));
        } catch (Exception e) {
            log.error("❌ Lỗi cleanup REPLACED: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/sim/statistics")
    @Operation(summary = "Thống kê SIM", description = "Lấy thống kê chi tiết về SIM trong database")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getSimStatistics() {
        try {
            Map<String, Object> stats = gsmService.getSimStatistics();
            return ResponseEntity.ok(ApiResponse.success(stats));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/sim/find-duplicates")
    @Operation(summary = "Tìm SIM duplicate theo CCID", description = "Tìm tất cả SIM duplicate trong database theo CCID")
    public ResponseEntity<ApiResponse<Map<String, List<Sim>>>> findDuplicateSims() {
        try {
            Map<String, List<Sim>> duplicates = gsmService.findDuplicateSims();
            return ResponseEntity.ok(ApiResponse.success(duplicates));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/sim/cleanup-duplicate-phones")
    @Operation(summary = "Cleanup SIM duplicate theo phoneNumber", description = "Xóa các SIM duplicate theo số điện thoại (cùng số khác CCID). Giữ lại SIM ACTIVE hoặc mới nhất.")
    public ResponseEntity<ApiResponse<Map<String, Object>>> cleanupDuplicateByPhoneNumber() {
        try {
            log.info("🧹 Bắt đầu cleanup duplicate SIM theo phoneNumber...");
            int deletedCount = gsmService.cleanupDuplicateByPhoneNumber();

            Map<String, Object> result = Map.of(
                    "deletedCount", deletedCount,
                    "message", "Đã xóa " + deletedCount + " SIM duplicate theo phoneNumber");

            return ResponseEntity.ok(ApiResponse.success("Cleanup hoàn tất", result));
        } catch (Exception e) {
            log.error("❌ Lỗi cleanup duplicate phone: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/sim/find-duplicate-phones")
    @Operation(summary = "Tìm SIM duplicate theo phoneNumber", description = "Tìm tất cả SIM có cùng số điện thoại nhưng khác CCID")
    public ResponseEntity<ApiResponse<Map<String, List<Sim>>>> findDuplicateByPhoneNumber() {
        try {
            Map<String, List<Sim>> duplicates = gsmService.findDuplicateByPhoneNumber();
            return ResponseEntity.ok(ApiResponse.success(duplicates));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error(e.getMessage()));
        }
    }
}
