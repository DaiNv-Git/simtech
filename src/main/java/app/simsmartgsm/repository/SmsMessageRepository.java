package app.simsmartgsm.repository;
import app.simsmartgsm.entity.SmsMessage;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SmsMessageRepository extends MongoRepository<SmsMessage, String> {


    Page<SmsMessage> findByToNumberContainingIgnoreCase(String toNumber, Pageable pageable);

    Page<SmsMessage> findByTypeIgnoreCase(String type, Pageable pageable);

    Page<SmsMessage> findByToNumberContainingIgnoreCaseAndTypeIgnoreCase(
            String toNumber, String type, Pageable pageable);
}
