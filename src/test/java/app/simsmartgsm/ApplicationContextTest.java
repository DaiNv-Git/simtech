package app.simsmartgsm;

import app.simsmartgsm.controller.GsmController;
import app.simsmartgsm.service.CallService;
import app.simsmartgsm.tool.controller.ToolController;
import app.simsmartgsm.tool.service.TelegramService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.data.mongodb.uri=mongodb://127.0.0.1:1/simtech?connectTimeoutMS=50&serverSelectionTimeoutMS=50",
                "spring.data.mongodb.auto-index-creation=false",
                "gsm.auto-scan-on-startup=false",
                "gsm.recording-path=target/test-recordings",
                "recording.save.path=target/test-recordings"
        })
class ApplicationContextTest {
    @Autowired private GsmController dashboardController;
    @Autowired private CallService callService;
    @Autowired private ToolController toolController;
    @Autowired private TelegramService telegramService;

    @Test
    void dashboardCallAndSmsToolAreAvailableTogether() {
        assertThat(dashboardController).isNotNull();
        assertThat(callService).isNotNull();
        assertThat(toolController).isNotNull();
        assertThat(telegramService).isNotNull();
    }
}
