package app.simsmartgsm.repository;

import app.simsmartgsm.entity.CallRecordEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CallRecordJpaRepository extends JpaRepository<CallRecordEntity, Long> {

    /** Lịch sử cuộc gọi theo thời gian */
    Page<CallRecordEntity> findAllByOrderByCreatedAtDesc(Pageable pageable);

    /** Lọc theo COM port */
    Page<CallRecordEntity> findByComPortOrderByCreatedAtDesc(String comPort, Pageable pageable);

    /** Lọc theo SIM phone */
    Page<CallRecordEntity> findBySimPhoneContainingIgnoreCaseOrderByCreatedAtDesc(String simPhone, Pageable pageable);

    /** Lọc theo status */
    Page<CallRecordEntity> findByStatusOrderByCreatedAtDesc(String status, Pageable pageable);

    /** Cuộc gọi có ghi âm */
    List<CallRecordEntity> findByIsRecordedTrueOrderByCreatedAtDesc();

    /** Đếm cuộc gọi */
    long countByStatus(String status);

    /** Tìm theo số điện thoại đích (targetPhone) */
    Page<CallRecordEntity> findByTargetPhoneContainingIgnoreCaseOrderByCreatedAtDesc(String targetPhone,
            Pageable pageable);
            
    /** Đếm cuộc gọi trong ngày theo COM port */
    long countByComPortAndCreatedAtGreaterThanEqual(String comPort, java.time.LocalDateTime startOfDay);

    /** Tìm theo số gọi đi hoặc số nhận */
    Page<CallRecordEntity> findByTargetPhoneContainingIgnoreCaseOrSimPhoneContainingIgnoreCaseOrderByCreatedAtDesc(
            String targetPhone, String simPhone, Pageable pageable);
}
