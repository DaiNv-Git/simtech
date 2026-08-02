package app.simsmartgsm.repository;

import app.simsmartgsm.entity.CallRecordEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface DashboardCallRepository extends MongoRepository<CallRecordEntity, String> {
    Page<CallRecordEntity> findAllByOrderByCreatedAtDesc(Pageable pageable);
    Page<CallRecordEntity> findByComPortOrderByCreatedAtDesc(String comPort, Pageable pageable);
    long countByComPortAndCreatedAtGreaterThanEqual(String comPort, java.time.LocalDateTime startOfDay);
    Page<CallRecordEntity> findByTargetPhoneContainingIgnoreCaseOrSimPhoneContainingIgnoreCaseOrderByCreatedAtDesc(
            String targetPhone, String simPhone, Pageable pageable);
}
