package app.simsmartgsm.scheduled;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DailyRestartJob {

    // ✅ DISABLED - App will no longer restart every night at midnight
    // Uncomment if you need automatic daily restarts
    // @Scheduled(cron = "0 0 0 * * ?")
    public void restart() {
        log.info("🕛 Daily restart is DISABLED - app will continue running");
    }
}
