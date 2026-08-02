package app.simsmartgsm.config;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Auto-open browser window khi app start
 * Giống WinForms desktop app
 */
// @Component // Disabled: Comment to stop auto browser launch
@Slf4j
public class AutoBrowserLauncher {

    @Value("${server.port:8080}")
    private int serverPort;

    @PostConstruct
    public void openBrowser() {
        // Delay 3 giây để Spring Boot khởi động xong
        new Thread(() -> {
            try {
                Thread.sleep(3000);
                String url = "http://localhost:" + serverPort + "/index.html";

                log.info("🚀 Opening GSM Manager UI: {}", url);

                String os = System.getProperty("os.name").toLowerCase();
                Runtime runtime = Runtime.getRuntime();

                // Chỉ dùng 1 phương thức mở browser dựa trên OS
                if (os.contains("mac")) {
                    // Mac: mở bằng lệnh open
                    runtime.exec(new String[] { "open", url });
                    log.info("✅ Browser opened on Mac");
                } else if (os.contains("win")) {
                    // Windows: mở bằng cmd
                    runtime.exec(new String[] { "cmd", "/c", "start", url });
                    log.info("✅ Browser opened on Windows");
                } else {
                    // Linux: dùng xdg-open
                    runtime.exec(new String[] { "xdg-open", url });
                    log.info("✅ Browser opened on Linux");
                }
            } catch (Exception e) {
                log.error("Cannot open browser automatically", e);
            }
        }).start();
    }
}
