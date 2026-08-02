package app.simsmartgsm.scheduled;

import app.simsmartgsm.repository.SmsMessageJpaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * ✅ Scheduled job to delete old SMS messages from H2 database
 *
 * Runs every 5 minutes to delete messages older than 15 minutes
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SmsCleanupJob {

    private final SmsMessageJpaRepository smsMessageJpaRepository;

    /**
     * Delete SMS messages older than 15 minutes
     * Runs every 5 minutes (300,000 ms)
     */
    @Scheduled(initialDelay = 60_000, fixedRate = 300_000)
    @Transactional
    public void cleanupOldSmsMessages() {
        try {
            // Calculate cutoff time: start of today (midnight)
            // Keeps SMS for the current day only (24h of today)
            LocalDateTime cutoffTime = LocalDate.now().atStartOfDay();

            int deletedCount = smsMessageJpaRepository.deleteOldMessages(cutoffTime);

            if (deletedCount > 0) {
                log.info("🧹 Cleaned up {} old SMS messages (older than today)", deletedCount);
            }
        } catch (Exception e) {
            log.error("❌ Error cleaning up old SMS messages: {}", e.getMessage(), e);
        }
    }
}
