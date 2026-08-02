package app.simsmartgsm.repository;

import app.simsmartgsm.entity.SmsMessageEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface SmsMessageJpaRepository extends JpaRepository<SmsMessageEntity, Long> {

    /** Lấy inbox (tin nhận) */
    Page<SmsMessageEntity> findByTypeOrderByCreatedAtDesc(String type, Pageable pageable);

    /** Lấy theo type và status */
    Page<SmsMessageEntity> findByTypeAndStatusOrderByCreatedAtDesc(String type, String status, Pageable pageable);

    /** Lấy theo COM port */
    Page<SmsMessageEntity> findByComPortOrderByCreatedAtDesc(String comPort, Pageable pageable);

    /** Filter theo ngày */
    Page<SmsMessageEntity> findByTypeAndCreatedAtBetweenOrderByCreatedAtDesc(
            String type, LocalDateTime startDate, LocalDateTime endDate, Pageable pageable);

    /** Đếm tin chưa đọc */
    long countByTypeAndIsReadFalse(String type);

    /** Đếm tin theo type và COM port */
    long countByTypeAndComPort(String type, String comPort);

    /** Đếm theo type */
    long countByType(String type);

    /** Lấy tin nhắn gần đây */
    List<SmsMessageEntity> findTop10ByOrderByCreatedAtDesc();

    /** Lấy tin nhắn gần đây theo COM port */
    List<SmsMessageEntity> findTop50ByTypeAndComPortOrderByCreatedAtDesc(String type, String comPort);

    /** Lấy tin chưa đọc */
    List<SmsMessageEntity> findByTypeAndIsReadFalseOrderByCreatedAtDesc(String type);

    /** Đánh dấu tất cả tin nhắn INBOX là đã đọc */
    @Modifying
    @Query("UPDATE SmsMessageEntity s SET s.isRead = true WHERE s.type = :type AND s.isRead = false")
    int markAllAsReadByType(@Param("type") String type);

    /** Tìm theo type và số điện thoại (search) */
    Page<SmsMessageEntity> findByTypeAndPhoneNumberContainingOrderByCreatedAtDesc(
            String type, String phoneNumber, Pageable pageable);

    /** ✅ Xoá tin nhắn cũ hơn thời gian chỉ định */
    @Modifying
    @Query("DELETE FROM SmsMessageEntity s WHERE s.createdAt < :cutoffTime")
    int deleteOldMessages(@Param("cutoffTime") LocalDateTime cutoffTime);
}
