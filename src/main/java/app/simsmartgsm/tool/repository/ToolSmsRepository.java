package app.simsmartgsm.tool.repository;

import app.simsmartgsm.tool.model.SmsDocument;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface ToolSmsRepository extends MongoRepository<SmsDocument, String> {
    Optional<SmsDocument> findByFingerprint(String fingerprint);
    boolean existsByFingerprint(String fingerprint);
    Page<SmsDocument> findByDirectionOrderByCreatedAtDesc(String direction, Pageable pageable);
}
