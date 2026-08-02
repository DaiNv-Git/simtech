package app.simsmartgsm.config;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;

/**
 * ✅ Tự động tạo unique index cho CCID và IMSI để tránh duplicate SIM
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class MongoIndexConfig {

    private final MongoTemplate mongoTemplate;

    @PostConstruct
    public void initIndexes() {
        try {
            IndexOperations indexOps = mongoTemplate.indexOps("sims");

            // ✅ Unique index cho CCID (chỉ apply cho document có CCID không null)
            // sparse = true: chỉ index các document có field này
            Index ccidIndex = new Index()
                    .on("ccid", Sort.Direction.ASC)
                    .unique()
                    .sparse(); // Quan trọng: cho phép nhiều document có ccid = null
            
            indexOps.ensureIndex(ccidIndex);
            log.info("✅ Created unique index on 'ccid' field (sparse)");

            // ✅ Unique index cho IMSI (backup nếu CCID bị lỗi)
            Index imsiIndex = new Index()
                    .on("imsi", Sort.Direction.ASC)
                    .unique()
                    .sparse();
            
            indexOps.ensureIndex(imsiIndex);
            log.info("✅ Created unique index on 'imsi' field (sparse)");

            // ✅ Compound index cho deviceName + comName (tìm kiếm nhanh)
            Index deviceComIndex = new Index()
                    .on("deviceName", Sort.Direction.ASC)
                    .on("comName", Sort.Direction.ASC);
            
            indexOps.ensureIndex(deviceComIndex);
            log.info("✅ Created compound index on 'deviceName' + 'comName'");

            // ✅ Index cho phoneNumber (tìm kiếm nhanh)
            Index phoneIndex = new Index()
                    .on("phoneNumber", Sort.Direction.ASC)
                    .sparse();
            
            indexOps.ensureIndex(phoneIndex);
            log.info("✅ Created index on 'phoneNumber' field");

            // ✅ Index cho status (filter nhanh)
            Index statusIndex = new Index()
                    .on("status", Sort.Direction.ASC);
            
            indexOps.ensureIndex(statusIndex);
            log.info("✅ Created index on 'status' field");

            // ======================================================================
            // ✅ SMS Daily Counters indexes
            // ======================================================================
            IndexOperations counterOps = mongoTemplate.indexOps("sms_daily_counters");

            // Compound unique index: (date, deviceId, comName) — mỗi SIM chỉ có 1 record/ngày
            Index dateDeviceComIndex = new Index()
                    .on("date", Sort.Direction.ASC)
                    .on("deviceId", Sort.Direction.ASC)
                    .on("comName", Sort.Direction.ASC)
                    .unique();
            counterOps.ensureIndex(dateDeviceComIndex);
            log.info("✅ Created unique compound index on sms_daily_counters (date, deviceId, comName)");

            // Index cho date + deviceId (load all counters for a device on startup)
            Index dateDeviceIndex = new Index()
                    .on("date", Sort.Direction.ASC)
                    .on("deviceId", Sort.Direction.ASC);
            counterOps.ensureIndex(dateDeviceIndex);
            log.info("✅ Created index on sms_daily_counters (date, deviceId)");

        } catch (Exception e) {
            log.error("❌ Failed to create indexes: {}", e.getMessage(), e);
        }
    }
}
