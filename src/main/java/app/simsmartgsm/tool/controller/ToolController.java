package app.simsmartgsm.tool.controller;

import app.simsmartgsm.tool.dto.SendSmsRequest;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.entity.SmsMessageEntity;
import app.simsmartgsm.tool.model.SmsDocument;
import app.simsmartgsm.tool.service.GsmToolService;
import app.simsmartgsm.tool.service.TelegramService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/tool")
@RequiredArgsConstructor
public class ToolController {
    private final GsmToolService gsmToolService;
    private final TelegramService telegramService;

    @GetMapping("/sims")
    public List<Sim> sims() {
        return gsmToolService.listSims();
    }

    @PostMapping("/sims/scan")
    public List<Sim> scan() {
        return gsmToolService.scanAll();
    }

    @GetMapping("/sms")
    public Page<SmsDocument> sms(
            @RequestParam(required = false) String direction,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {
        return gsmToolService.listSms(direction, page, size);
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
}
