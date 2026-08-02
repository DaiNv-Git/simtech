package app.simsmartgsm.repository;
// SimRepository.java

import app.simsmartgsm.entity.Sim;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import java.util.List;
import java.util.Optional;

public interface SimRepository extends MongoRepository<Sim, String> {
    Optional<Sim> findByPhoneNumber(String phoneNumber);

    Optional<Sim> findFirstByPhoneNumber(String phoneNumber);

    Optional<Sim> findFirstByComName(String comName);

    List<Sim> findAllByDeviceNameAndComName(String deviceName, String comName);

    List<Sim> findByDeviceName(String deviceName);

    // 🆕 Đếm SIM theo deviceName và status
    long countByDeviceNameAndStatus(String deviceName, String status);

    // 🆕 Đếm SIM có CCID (có thông tin thực sự)
    long countByDeviceNameAndCcidNotNull(String deviceName);

    // 🆕 Tìm SIM theo CCID exact match
    Optional<Sim> findByCcid(String ccid);

    // 🆕 Tìm SIM theo CCID bằng regex (fuzzy match 18 số liên tục)
    @Query("{ 'ccid': { $regex: ?0 } }")
    List<Sim> findByCcidRegex(String ccidRegex);

    // 🆕 Tìm SIM theo IMSI (để check số đã có trong DB trước khi scan)
    Optional<Sim> findByImsi(String imsi);

    // 🆕 Tìm SIM theo deviceName và comName (cho multi-device environment)
    Optional<Sim> findFirstByDeviceNameAndComName(String deviceName, String comName);

    // 🆕 Tìm SIM theo deviceName và status (cho Proxy module)
    List<Sim> findByDeviceNameAndStatus(String deviceName, String status);

    // 🆕 Tìm các SIM đang bị khoá không cho gửi SMS
    List<Sim> findByAllowSmsFalse();
}
