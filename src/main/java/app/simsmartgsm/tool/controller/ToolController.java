package app.simsmartgsm.tool.controller;

import app.simsmartgsm.tool.dto.SendSmsRequest;
import app.simsmartgsm.tool.dto.WebhookSettingsRequest;
import app.simsmartgsm.tool.dto.SimImportResult;
import app.simsmartgsm.tool.dto.SimTextImportRequest;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.entity.SmsMessageEntity;
import app.simsmartgsm.tool.model.SmsDocument;
import app.simsmartgsm.tool.service.GsmToolService;
import app.simsmartgsm.tool.service.TelegramService;
import app.simsmartgsm.tool.service.WebhookService;
import app.simsmartgsm.tool.service.SimImportService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/tool")
@RequiredArgsConstructor
public class ToolController {
    private final GsmToolService gsmToolService;
    private final TelegramService telegramService;
    private final WebhookService webhookService;
    private final SimImportService simImportService;

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException exception) {
        return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
    }

    @GetMapping("/sims")
    public List<Sim> sims() {
        return gsmToolService.listSims();
    }

    @PostMapping("/sims/scan")
    public List<Sim> scan() {
        return gsmToolService.scanAll();
    }

    @PostMapping(value = "/sims/import/excel", consumes = "multipart/form-data")
    public SimImportResult importSimsExcel(@RequestPart("file") MultipartFile file) {
        return simImportService.importExcel(file);
    }

    @PostMapping("/sims/import/text")
    public SimImportResult importSimsText(@RequestBody SimTextImportRequest request) {
        return simImportService.importText(request.text());
    }

    @GetMapping("/sms")
    public Page<SmsDocument> sms(
            @RequestParam(required = false) String direction,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {
        return gsmToolService.listSms(direction, page, size);
    }

    @DeleteMapping("/sms")
    public Map<String, Object> deleteAllSms() {
        int deletedCount = gsmToolService.deleteAllSms();
        return Map.of("deletedCount", deletedCount, "message", "Đã xóa toàn bộ tin nhắn");
    }

    @PostMapping("/sms/send")
    public ResponseEntity<SmsMessageEntity> send(@Valid @RequestBody SendSmsRequest request) {
        SmsMessageEntity result = gsmToolService.sendSms(request.comPort(), request.toNumber(), request.message());
        return ResponseEntity.ok(result);
    }

    @GetMapping("/telegram/status")
    public Map<String, Boolean> telegramStatus() {
        return Map.of("configured", telegramService.isConfigured());
    }

    @PostMapping("/telegram/test")
    public ResponseEntity<Map<String, String>> telegramTest() {
        if (!telegramService.isConfigured()) {
            return ResponseEntity.badRequest().body(Map.of("message", "Telegram chưa được cấu hình"));
        }
        telegramService.sendMessage("✅ simTech đã kết nối Telegram thành công.");
        return ResponseEntity.ok(Map.of("message", "Đã gửi tin nhắn thử"));
    }

    @GetMapping("/settings/webhook")
    public Map<String, Object> webhookSettings() {
        return webhookService.getPublicSettings();
    }

    @PutMapping("/settings/webhook")
    public Map<String, Object> updateWebhookSettings(
            @Valid @RequestBody WebhookSettingsRequest request) {
        return webhookService.update(request);
    }

    @PostMapping("/settings/webhook/test")
    public ResponseEntity<Map<String, String>> webhookTest() {
        try {
            webhookService.sendTest();
            return ResponseEntity.ok(Map.of("message", "Webhook phản hồi thành công"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("message", e.getMessage()));
        }
    }
}
