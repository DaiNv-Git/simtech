package app.simsmartgsm.repository;

import app.simsmartgsm.entity.SmsMessageEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.LocalDateTime;
import java.util.List;

public interface SmsHistoryRepository extends MongoRepository<SmsMessageEntity, String> {
    Page<SmsMessageEntity> findByTypeOrderByCreatedAtDesc(String type, Pageable pageable);
    Page<SmsMessageEntity> findByTypeAndCreatedAtBetweenOrderByCreatedAtDesc(
            String type, LocalDateTime startDate, LocalDateTime endDate, Pageable pageable);
    long countByTypeAndIsReadFalse(String type);
    long countByTypeAndComPort(String type, String comPort);
    long countByType(String type);
    List<SmsMessageEntity> findTop50ByTypeAndComPortOrderByCreatedAtDesc(String type, String comPort);
    List<SmsMessageEntity> findByTypeAndIsReadFalseOrderByCreatedAtDesc(String type);
    Page<SmsMessageEntity> findByTypeAndPhoneNumberContainingOrderByCreatedAtDesc(
            String type, String phoneNumber, Pageable pageable);
    long deleteByCreatedAtBefore(LocalDateTime cutoffTime);
}
