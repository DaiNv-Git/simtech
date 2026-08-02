package app.simsmartgsm.repository;

import app.simsmartgsm.entity.SmsDailyCounter;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * 📊 MongoDB Repository cho SMS Daily Counters
 * 
 * Lưu persistent counter theo ngày + deviceId + comName
 * → Restart app không mất dữ liệu
 */
@Repository
public interface SmsDailyCounterRepository extends MongoRepository<SmsDailyCounter, String> {

    /**
     * Tìm counter cho 1 SIM cụ thể trong ngày
     */
    Optional<SmsDailyCounter> findByDateAndDeviceIdAndComName(LocalDate date, String deviceId, String comName);

    /**
     * Tìm tất cả counters của 1 device trong ngày
     */
    List<SmsDailyCounter> findByDateAndDeviceId(LocalDate date, String deviceId);

    /**
     * Tìm tất cả counters trong ngày (cho dashboard)
     */
    List<SmsDailyCounter> findByDate(LocalDate date);

    /**
     * Tìm tất cả counters bị blacklist trong ngày
     */
    List<SmsDailyCounter> findByDateAndDeviceIdAndBlacklistedTrue(LocalDate date, String deviceId);

    /**
     * Xóa dữ liệu cũ (cleanup) - giữ 7 ngày gần nhất
     */
    void deleteByDateBefore(LocalDate date);
}
