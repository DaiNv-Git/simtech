package app.simsmartgsm.service;

import app.simsmartgsm.entity.SmsDailyCounter;
import app.simsmartgsm.repository.SmsDailyCounterRepository;
import app.simsmartgsm.uitils.DeviceIdProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 📊 SMS Daily Limit Service
 * 
 * Quản lý giới hạn số lượng SMS gửi đi mỗi SIM mỗi ngày.
 * - Mỗi SIM chỉ được gửi tối đa 150 SEGMENTS/ngày (không phải 150 tin nhắn)
 * - Đếm theo comName (mỗi SIM gắn với 1 COM port)
 * - Tự động reset vào cuối ngày (00:00)
 * - ✅ LƯU VÀO MONGODB theo ngày + deviceId + comName → restart app không mất data
 * - In-memory cache (ConcurrentHashMap) cho performance, sync với MongoDB
 * 
 * ⚠️ QUAN TRỌNG: Nhà mạng đếm theo SEGMENT, không phải theo lần gửi!
 * - GSM 7-bit (ASCII): 1 segment = 160 ký tự, multipart = 153 ký tự/segment
 * - UCS2 (Unicode/Tiếng Nhật/Việt): 1 segment = 70 ký tự, multipart = 67 ký tự/segment
 * - VD: 1 tin nhắn 140 ký tự Unicode = 3 segments trên mạng nhà mạng!
 */
@Service
@Slf4j
public class SmsDailyLimitService {

    /** Giới hạn SEGMENTS mỗi SIM mỗi ngày (fix cứng 150) */
    private static final int DAILY_SEGMENT_LIMIT = 150;

    /** Ngưỡng fail liên tiếp trước khi blacklist SIM - SIM fail >= 5 lần liên tiếp sẽ bị đánh dấu đỏ */
    private static final int FAIL_THRESHOLD = 5;

    /** Đếm số SEGMENTS đã gửi theo comName → count (IN-MEMORY CACHE) */
    private final ConcurrentHashMap<String, AtomicInteger> dailyCounts = new ConcurrentHashMap<>();

    /** ✅ Đếm số lần FAIL liên tiếp theo comName → count */
    private final ConcurrentHashMap<String, AtomicInteger> consecutiveFailCounts = new ConcurrentHashMap<>();

    /** ✅ Đếm tổng fail trong ngày theo comName → count */
    private final ConcurrentHashMap<String, AtomicInteger> dailyFailCounts = new ConcurrentHashMap<>();

    /** ✅ SIM bị blacklist (fail quá nhiều) - tự động loại khỏi pool gửi */
    private final java.util.Set<String> blacklistedComs = ConcurrentHashMap.newKeySet();

    /** Ngày hiện tại để phát hiện chuyển ngày */
    private volatile LocalDate currentDay;

    /** ✅ Device ID (lấy từ DeviceIdProvider) */
    private String deviceId;

    /** ✅ MongoDB Repository - persistent storage */
    private final SmsDailyCounterRepository counterRepository;

    public SmsDailyLimitService(SmsDailyCounterRepository counterRepository) {
        this.counterRepository = counterRepository;
    }

    @PostConstruct
    public void init() {
        currentDay = LocalDate.now();
        try {
            this.deviceId = DeviceIdProvider.getDeviceId();
        } catch (Exception e) {
            this.deviceId = "UNKNOWN_DEVICE";
        }

        // ✅ Load counters từ MongoDB khi khởi động
        loadCountersFromMongo();

        log.info("✅ SmsDailyLimitService initialized - limit: {} SEGMENTS/SIM/day, fail threshold: {}, deviceId: {}",
                DAILY_SEGMENT_LIMIT, FAIL_THRESHOLD, deviceId);
    }

    // ======================================================================
    // MONGODB PERSISTENCE
    // ======================================================================

    /**
     * ✅ Load tất cả counters từ MongoDB cho ngày hôm nay + deviceId hiện tại
     * Gọi khi khởi động hoặc khi chuyển ngày
     */
    private void loadCountersFromMongo() {
        try {
            List<SmsDailyCounter> counters = counterRepository.findByDateAndDeviceId(currentDay, deviceId);

            int loaded = 0;
            for (SmsDailyCounter counter : counters) {
                String comName = counter.getComName();

                // Load segments sent
                if (counter.getSegmentsSent() > 0) {
                    dailyCounts.put(comName, new AtomicInteger(counter.getSegmentsSent()));
                }

                // Load consecutive fails
                if (counter.getConsecutiveFails() > 0) {
                    consecutiveFailCounts.put(comName, new AtomicInteger(counter.getConsecutiveFails()));
                }

                // Load daily fails
                if (counter.getDailyFails() > 0) {
                    dailyFailCounts.put(comName, new AtomicInteger(counter.getDailyFails()));
                }

                // Load blacklist status
                if (counter.isBlacklisted()) {
                    blacklistedComs.add(comName);
                }

                loaded++;
            }

            if (loaded > 0) {
                int totalSegments = dailyCounts.values().stream().mapToInt(AtomicInteger::get).sum();
                log.info("📊 Loaded {} SIM counters from MongoDB (date={}, deviceId={}, totalSegments={})",
                        loaded, currentDay, deviceId, totalSegments);
            } else {
                log.info("📊 No existing counters for today (date={}, deviceId={}) - starting fresh",
                        currentDay, deviceId);
            }
        } catch (Exception e) {
            log.error("❌ Failed to load counters from MongoDB: {} - starting with empty counters", e.getMessage());
        }
    }

    /**
     * ✅ Persist counter cho 1 SIM vào MongoDB (upsert) — ASYNC
     * Gọi mỗi khi có thay đổi counter (sent, fail, blacklist)
     * ✅ ASYNC để không block SMS sending thread (MongoDB round-trip ~5-20ms)
     */
    private void persistCounterToMongo(String comName) {
        // Snapshot giá trị hiện tại trước khi chuyển sang thread khác
        final LocalDate snapshotDate = currentDay;
        final String snapshotDeviceId = deviceId;
        final int snapshotSegments;
        final int snapshotConsecutive;
        final int snapshotDailyFails;
        final boolean snapshotBlacklisted;

        AtomicInteger segments = dailyCounts.get(comName);
        snapshotSegments = segments != null ? segments.get() : 0;

        AtomicInteger consecutive = consecutiveFailCounts.get(comName);
        snapshotConsecutive = consecutive != null ? consecutive.get() : 0;

        AtomicInteger dailyFail = dailyFailCounts.get(comName);
        snapshotDailyFails = dailyFail != null ? dailyFail.get() : 0;

        snapshotBlacklisted = blacklistedComs.contains(comName);

        java.util.concurrent.CompletableFuture.runAsync(() -> {
            try {
                SmsDailyCounter counter = counterRepository
                        .findByDateAndDeviceIdAndComName(snapshotDate, snapshotDeviceId, comName)
                        .orElse(SmsDailyCounter.builder()
                                .date(snapshotDate)
                                .deviceId(snapshotDeviceId)
                                .comName(comName)
                                .build());

                counter.setSegmentsSent(snapshotSegments);
                counter.setConsecutiveFails(snapshotConsecutive);
                counter.setDailyFails(snapshotDailyFails);
                counter.setBlacklisted(snapshotBlacklisted);

                counterRepository.save(counter);
            } catch (Exception e) {
                // ✅ Log nhưng KHÔNG throw - tránh ảnh hưởng SMS sending flow
                log.warn("⚠️ [{}] Failed to persist counter to MongoDB: {}", comName, e.getMessage());
            }
        });
    }

    // ======================================================================
    // SEGMENT COUNTING LOGIC
    // ======================================================================

    /**
     * ✅ Tính số segments thực tế mà nhà mạng sẽ đếm cho tin nhắn này.
     * 
     * GSM 7-bit (ASCII only):
     *   - <= 160 chars → 1 segment
     *   - > 160 chars → ceil(length / 153) segments (7 bytes UDH header mỗi segment)
     * 
     * UCS2 (Unicode - tiếng Nhật, Việt, emoji...):
     *   - <= 70 chars → 1 segment
     *   - > 70 chars → ceil(length / 67) segments (6 bytes UDH header mỗi segment)
     * 
     * @param messageContent Nội dung tin nhắn
     * @return Số segments (tối thiểu 1)
     */
    public static int countSegments(String messageContent) {
        if (messageContent == null || messageContent.isEmpty()) {
            return 1; // Empty message vẫn tính 1 segment
        }

        boolean isGsm7Bit = isGsm7BitCompatible(messageContent);
        int length = messageContent.length();

        if (isGsm7Bit) {
            // GSM 7-bit encoding
            if (length <= 160) {
                return 1;
            }
            return (int) Math.ceil(length / 153.0);
        } else {
            // UCS2 encoding (Unicode)
            if (length <= 70) {
                return 1;
            }
            return (int) Math.ceil(length / 67.0);
        }
    }

    /**
     * ✅ Kiểm tra xem nội dung có thuộc bảng mã GSM 7-bit không.
     * Nếu có bất kỳ ký tự nào ngoài GSM 7-bit → phải dùng UCS2.
     * 
     * GSM 7-bit Basic Character Set (3GPP TS 23.038):
     * Bao gồm ASCII cơ bản + một số ký tự mở rộng châu Âu
     */
    private static boolean isGsm7BitCompatible(String text) {
        if (text == null) return true;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c > 127) {
                return false; // Ký tự ngoài ASCII → cần UCS2
            }
        }
        return true;
    }

    // ======================================================================
    // CORE API
    // ======================================================================

    /**
     * ✅ Kiểm tra SIM còn quota gửi SMS không (check cơ bản, ít nhất 1 segment)
     * 
     * @param comName COM port name (e.g. "COM3")
     * @return true nếu còn quota, false nếu đã hết
     */
    public boolean canSend(String comName) {
        checkDayRollover();
        AtomicInteger count = dailyCounts.computeIfAbsent(comName, k -> new AtomicInteger(0));
        return count.get() < DAILY_SEGMENT_LIMIT;
    }

    /**
     * ✅ Kiểm tra SIM còn đủ quota cho tin nhắn cụ thể không
     * Tính toán số segments cần thiết và so sánh với quota còn lại.
     * 
     * @param comName COM port name
     * @param messageContent Nội dung tin nhắn sẽ gửi
     * @return true nếu còn đủ quota cho tin nhắn này, false nếu không đủ
     */
    public boolean canSend(String comName, String messageContent) {
        checkDayRollover();
        int segmentsNeeded = countSegments(messageContent);
        AtomicInteger count = dailyCounts.computeIfAbsent(comName, k -> new AtomicInteger(0));
        int remaining = DAILY_SEGMENT_LIMIT - count.get();

        if (remaining < segmentsNeeded) {
            log.warn("🛑 [{}] Không đủ quota: cần {} segments nhưng chỉ còn {}/{} segments",
                    comName, segmentsNeeded, remaining, DAILY_SEGMENT_LIMIT);
            return false;
        }
        return true;
    }

    /**
     * ✅ Ghi nhận SMS đã gửi thành công - ĐẾM THEO SEGMENTS
     * ✅ PERSIST vào MongoDB
     * 
     * @param comName COM port name
     * @param messageContent Nội dung tin nhắn đã gửi (để tính segments)
     * @return số SEGMENTS đã gửi hôm nay (sau khi increment)
     */
    public int recordSent(String comName, String messageContent) {
        checkDayRollover();
        int segments = countSegments(messageContent);
        AtomicInteger count = dailyCounts.computeIfAbsent(comName, k -> new AtomicInteger(0));
        int newCount = count.addAndGet(segments);

        if (segments > 1) {
            log.info("📊 [{}] Tin nhắn {} ký tự = {} segments | Tổng hôm nay: {}/{} segments",
                    comName, messageContent != null ? messageContent.length() : 0,
                    segments, newCount, DAILY_SEGMENT_LIMIT);
        }

        if (newCount >= DAILY_SEGMENT_LIMIT) {
            log.warn("🔴 [{}] ĐÃ ĐẠT GIỚI HẠN {} segments/ngày! (hiện tại: {})",
                    comName, DAILY_SEGMENT_LIMIT, newCount);
        } else if (newCount % 50 == 0 || (DAILY_SEGMENT_LIMIT - newCount) <= 10) {
            log.info("📊 [{}] Đã gửi {}/{} segments hôm nay (còn lại: {})",
                    comName, newCount, DAILY_SEGMENT_LIMIT, DAILY_SEGMENT_LIMIT - newCount);
        }

        // ✅ Persist to MongoDB
        persistCounterToMongo(comName);

        return newCount;
    }

    /**
     * ✅ Backward-compatible: Ghi nhận 1 segment (cho các caller cũ không truyền content)
     * @deprecated Dùng {@link #recordSent(String, String)} để đếm chính xác segments
     */
    @Deprecated
    public int recordSent(String comName) {
        return recordSent(comName, null);
    }

    /**
     * ✅ Lấy số segments còn lại trong ngày cho SIM
     */
    public int getRemainingQuota(String comName) {
        checkDayRollover();
        AtomicInteger count = dailyCounts.get(comName);
        if (count == null) return DAILY_SEGMENT_LIMIT;
        return Math.max(0, DAILY_SEGMENT_LIMIT - count.get());
    }

    /**
     * ✅ Lấy số segments đã gửi hôm nay cho SIM
     */
    public int getSentToday(String comName) {
        checkDayRollover();
        AtomicInteger count = dailyCounts.get(comName);
        return count != null ? count.get() : 0;
    }

    /**
     * ✅ Lấy tất cả daily counts (cho dashboard monitoring)
     */
    public Map<String, Integer> getAllDailyCounts() {
        checkDayRollover();
        Map<String, Integer> result = new ConcurrentHashMap<>();
        dailyCounts.forEach((com, count) -> result.put(com, count.get()));
        return result;
    }

    // ======================================================================
    // FAIL TRACKING & BLACKLIST
    // ======================================================================

    /**
     * ✅ Ghi nhận SMS gửi FAIL - tăng consecutive fail count
     * Khi đạt FAIL_THRESHOLD → tự động blacklist SIM
     * ✅ PERSIST vào MongoDB
     */
    public void recordFail(String comName) {
        checkDayRollover();
        AtomicInteger consecutive = consecutiveFailCounts.computeIfAbsent(comName, k -> new AtomicInteger(0));
        AtomicInteger dailyFail = dailyFailCounts.computeIfAbsent(comName, k -> new AtomicInteger(0));

        int fails = consecutive.incrementAndGet();
        dailyFail.incrementAndGet();

        if (fails >= FAIL_THRESHOLD && !blacklistedComs.contains(comName)) {
            blacklistedComs.add(comName);
            log.error("🚨🔴 [{}] SIM BLACKLISTED! {} lần fail liên tiếp - tự động loại khỏi pool gửi SMS!",
                    comName, fails);
        } else if (fails >= 3) {
            log.warn("⚠️ [{}] SIM sắp bị blacklist: {}/{} fails liên tiếp", comName, fails, FAIL_THRESHOLD);
        }

        // ✅ Persist to MongoDB
        persistCounterToMongo(comName);
    }

    /**
     * ✅ Ghi nhận SMS gửi THÀNH CÔNG - reset consecutive fail count
     * SIM gửi OK → reset fail counter (hồi phục niềm tin)
     * ✅ PERSIST vào MongoDB
     */
    public void recordSuccess(String comName) {
        AtomicInteger consecutive = consecutiveFailCounts.get(comName);
        if (consecutive != null && consecutive.get() > 0) {
            int oldCount = consecutive.getAndSet(0);
            log.info("✅ [{}] SMS success - reset fail counter (was {})", comName, oldCount);

            // ✅ Persist to MongoDB
            persistCounterToMongo(comName);
        }
    }

    /**
     * ✅ Check SIM có bị blacklist không
     */
    public boolean isBlacklisted(String comName) {
        checkDayRollover();
        return blacklistedComs.contains(comName);
    }

    /**
     * ✅ Xóa blacklist cho SIM (manual recovery từ dashboard)
     * ✅ PERSIST vào MongoDB
     */
    public void unblacklist(String comName) {
        blacklistedComs.remove(comName);
        AtomicInteger consecutive = consecutiveFailCounts.get(comName);
        if (consecutive != null) consecutive.set(0);
        log.info("🔓 [{}] SIM đã được xóa blacklist thủ công", comName);

        // ✅ Persist to MongoDB
        persistCounterToMongo(comName);
    }

    /**
     * ✅ Lấy danh sách fail counts cho dashboard
     * Return: Map<comName, {consecutive, dailyTotal, blacklisted}>
     */
    public Map<String, Map<String, Object>> getFailStats() {
        checkDayRollover();
        Map<String, Map<String, Object>> result = new java.util.HashMap<>();

        // Gom tất cả COM có fail hoặc blacklist
        java.util.Set<String> allComs = new java.util.HashSet<>();
        allComs.addAll(consecutiveFailCounts.keySet());
        allComs.addAll(dailyFailCounts.keySet());
        allComs.addAll(blacklistedComs);

        for (String com : allComs) {
            Map<String, Object> stats = new java.util.HashMap<>();
            AtomicInteger consecutive = consecutiveFailCounts.get(com);
            AtomicInteger dailyFail = dailyFailCounts.get(com);

            stats.put("consecutiveFails", consecutive != null ? consecutive.get() : 0);
            stats.put("dailyFails", dailyFail != null ? dailyFail.get() : 0);
            stats.put("blacklisted", blacklistedComs.contains(com));
            result.put(com, stats);
        }
        return result;
    }

    /**
     * ✅ Lấy danh sách SIM bị blacklist
     */
    public java.util.Set<String> getBlacklistedComs() {
        checkDayRollover();
        return java.util.Collections.unmodifiableSet(blacklistedComs);
    }

    /**
     * ✅ Reset tất cả counters - dùng khi chuyển ngày
     * ✅ KHÔNG xóa MongoDB data cũ (giữ cho history/reporting)
     */
    private void resetAllCounters() {
        int totalSent = dailyCounts.values().stream().mapToInt(AtomicInteger::get).sum();
        int simsUsed = (int) dailyCounts.values().stream().filter(c -> c.get() > 0).count();
        int totalFails = dailyFailCounts.values().stream().mapToInt(AtomicInteger::get).sum();
        int blacklisted = blacklistedComs.size();

        log.info("🔄 Daily SMS reset - Tổng kết ngày {}: {} SEGMENTS gửi từ {} SIM | {} fails | {} blacklisted",
                currentDay, totalSent, simsUsed, totalFails, blacklisted);

        // Clear in-memory caches
        dailyCounts.clear();
        consecutiveFailCounts.clear();
        dailyFailCounts.clear();
        blacklistedComs.clear();
        currentDay = LocalDate.now();

        // ✅ Load counters cho ngày mới từ MongoDB (nếu có - VD: app restart giữa ngày)
        loadCountersFromMongo();

        log.info("✅ Daily counters + fail tracking + blacklist reset - Ngày mới: {}", currentDay);
    }

    /**
     * ✅ Kiểm tra chuyển ngày (thread-safe)
     * Nếu đã sang ngày mới → reset all counters
     */
    private void checkDayRollover() {
        LocalDate today = LocalDate.now();
        if (!today.equals(currentDay)) {
            synchronized (this) {
                // Double-check trong synchronized block
                if (!today.equals(currentDay)) {
                    resetAllCounters();
                }
            }
        }
    }

    /**
     * ✅ Scheduled reset lúc 00:00 mỗi ngày (backup cho checkDayRollover)
     */
    @Scheduled(cron = "0 0 0 * * *")
    public void scheduledDailyReset() {
        log.info("⏰ Scheduled daily SMS counter reset triggered");
        resetAllCounters();
    }

    /**
     * ✅ Scheduled cleanup: Xóa dữ liệu MongoDB cũ hơn 30 ngày
     * Chạy mỗi ngày lúc 01:00
     */
    @Scheduled(cron = "0 0 1 * * *")
    public void scheduledMongoCleanup() {
        try {
            LocalDate cutoff = LocalDate.now().minusDays(30);
            counterRepository.deleteByDateBefore(cutoff);
            log.info("🧹 Cleaned up SMS daily counter records older than {}", cutoff);
        } catch (Exception e) {
            log.error("❌ Failed to cleanup old SMS counters: {}", e.getMessage());
        }
    }

    /**
     * ✅ Log daily summary mỗi giờ
     */
    @Scheduled(cron = "0 0 * * * *")
    public void hourlyReport() {
        if (dailyCounts.isEmpty()) return;

        int totalSent = dailyCounts.values().stream().mapToInt(AtomicInteger::get).sum();
        int simsUsed = (int) dailyCounts.values().stream().filter(c -> c.get() > 0).count();
        int simsAtLimit = (int) dailyCounts.values().stream()
                .filter(c -> c.get() >= DAILY_SEGMENT_LIMIT).count();

        log.info("📊 [HOURLY] SMS stats - Total segments: {} | Active SIMs: {} | At limit: {} | Time: {}",
                totalSent, simsUsed, simsAtLimit, LocalTime.now());

        // Cảnh báo nếu nhiều SIM đạt limit
        if (simsAtLimit > 0) {
            dailyCounts.forEach((com, count) -> {
                if (count.get() >= DAILY_SEGMENT_LIMIT) {
                    log.warn("🔴 [{}] Đã đạt limit {}/{} segments", com, count.get(), DAILY_SEGMENT_LIMIT);
                }
            });
        }
    }

    /** Getter cho limit value (cho API/dashboard) */
    public int getDailyLimit() {
        return DAILY_SEGMENT_LIMIT;
    }
}
