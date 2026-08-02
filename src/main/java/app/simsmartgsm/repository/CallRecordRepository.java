package app.simsmartgsm.repository;

import app.simsmartgsm.entity.CallRecord;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CallRecordRepository extends MongoRepository<CallRecord, String> {
    Page<CallRecord> findBySimPhoneContainingIgnoreCase(String simPhone, Pageable pageable);

    // Mới thêm cho ModemCallController
    Page<CallRecord> findByComPort(String comPort, Pageable pageable);

    Page<CallRecord> findByServiceCode(String serviceCode, Pageable pageable);

    Page<CallRecord> findBySimPhoneContainingIgnoreCaseAndComPort(
            String simPhone, String comPort, Pageable pageable);
}
