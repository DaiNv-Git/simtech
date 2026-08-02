package app.simsmartgsm.repository;

import app.simsmartgsm.entity.SmsSession;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface SmsSessionRepository extends MongoRepository<SmsSession, String> {
    Optional<SmsSession> findByCampaignIdAndPhoneNumber(String campaignId, String phoneNumber);

    List<SmsSession> findAllByExpiredAtAfter(LocalDateTime now);

    @Query("{ 'phoneNumber': ?0, 'active': true, 'expiredAt': { $gt: ?1 } }")
    Optional<SmsSession> findByPhoneNumberAndActiveTrueAndExpiredAtAfter(String phoneNumber, LocalDateTime now);


    @Query("{ 'active': true, 'expiredAt': { $lt: ?0 } }")
    List<SmsSession> findAllByActiveTrueAndExpiredAtBefore(LocalDateTime now);
}