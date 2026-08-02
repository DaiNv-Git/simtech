package app.simsmartgsm.uitils;

import app.simsmartgsm.dto.session.MarketingSession;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class MarketingSessionRegistry {

    private final Map<String, MarketingSession> activeSessions = new ConcurrentHashMap<>();

    // ✅ ObjectMapper hỗ trợ Instant
    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final File persistFile = new File("session-store.json");

    public MarketingSessionRegistry() {
        loadSessions();
    }

    private String key(String simNumber, String customerPhone) {
        return simNumber + ":" + customerPhone;
    }

    /** ✅ Đăng ký session mới với thời gian hết hạn */
    /** Đăng ký session mới với timeDuration phút — chỉ tạo mới nếu chưa tồn tại */
    public void register(String simNumber, String customerPhone,
                         String campaignId, String sessionId,
                         String localMsgId, String simId,
                         Instant expiresAt) {
        try {
            String k = key(simNumber, customerPhone);

            MarketingSession existing = activeSessions.get(k);
            if (existing != null) {
                log.info("⚠️ Session {} → {} đã tồn tại (campaignId={}), bỏ qua không ghi đè.",
                        simNumber, customerPhone, existing.getCampaignId());
                return;
            }

            MarketingSession session = new MarketingSession(
                    simNumber, customerPhone, campaignId,
                    sessionId, localMsgId, simId, expiresAt
            );

            activeSessions.put(k, session);
            log.info("🕒 Registered TWO_WAY session {} → {} until {}", simNumber, customerPhone, expiresAt);
            saveSessions();

        } catch (Exception e) {
            log.error("❌ Error registering session {} → {}", simNumber, customerPhone, e);
        }
    }


    /** ✅ Tìm session còn hạn (và xóa nếu hết hạn) */
    public MarketingSession findActiveSession(String simNumber, String customerPhone) {
        String k = key(simNumber, customerPhone);
        MarketingSession session = activeSessions.get(k);
        if (session == null) return null;

        Instant expiresAt = session.getExpiresAt();
        if (expiresAt == null || Instant.now().isAfter(expiresAt)) {
            activeSessions.remove(k);
            return null;
        }
        return session;
    }

    /** 🧹 Dọn session hết hạn mỗi 1 phút */
    @Scheduled(fixedRate = 60000)
    public void cleanupExpired() {
        Instant now = Instant.now();
        int before = activeSessions.size();
        activeSessions.entrySet().removeIf(e -> {
            Instant exp = e.getValue().getExpiresAt();
            return exp == null || now.isAfter(exp);
        });
        int after = activeSessions.size();
        if (before != after) {
            log.info("🧹 Cleaned {} expired sessions", (before - after));
            saveSessions();
        }
    }

    /** 💾 Lưu session ra file */
    private synchronized void saveSessions() {
        try {
            mapper.writerWithDefaultPrettyPrinter().writeValue(persistFile, activeSessions);
            log.debug("💾 Saved {} sessions to file {}", activeSessions.size(), persistFile.getAbsolutePath());
        } catch (Exception e) {
            log.error("❌ Failed to persist sessions", e);
        }
    }

    /** ♻️ Load session từ file khi app start */
    private void loadSessions() {
        try {
            if (persistFile.exists() && persistFile.length() > 0) {
                Map<String, MarketingSession> loaded = mapper.readValue(
                        persistFile, new TypeReference<Map<String, MarketingSession>>() {}
                );
                activeSessions.putAll(loaded);
                log.info("🔁 Restored {} sessions from file", loaded.size());
            } else {
                log.info("ℹ️ No previous session-store.json found, starting fresh");
            }
        } catch (Exception e) {
            log.error("❌ Failed to load sessions from file {}", persistFile.getAbsolutePath(), e);
        }
    }
}
