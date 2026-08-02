package app.simsmartgsm.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.awt.Desktop;
import java.net.URI;

/**
 * Auto-open browser khi app start
 * DISABLED: Using JavaFX desktop wrapper instead
 */
// @Component // Disabled - JavaFX handles UI
@Slf4j
public class BrowserLauncher {

    @EventListener(ApplicationReadyEvent.class)
    public void openBrowser() {
        try {
            // Đợi 2 giây cho server khởi động hoàn toàn
            Thread.sleep(2000);

            String url = "http://localhost:8080";
            log.info("🌐 Opening browser at: {}", url);

            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().browse(new URI(url));
            } else {
                // Fallback for systems without Desktop support
                String os = System.getProperty("os.name").toLowerCase();
                if (os.contains("win")) {
                    Runtime.getRuntime().exec("rundll32 url.dll,FileProtocolHandler " + url);
                } else if (os.contains("mac")) {
                    Runtime.getRuntime().exec("open " + url);
                } else if (os.contains("nix") || os.contains("nux")) {
                    Runtime.getRuntime().exec("xdg-open " + url);
                }
            }

            log.info("✅ Browser opened successfully");
        } catch (Exception e) {
            log.warn("Cannot auto-open browser: {}", e.getMessage());
        }
    }
}
