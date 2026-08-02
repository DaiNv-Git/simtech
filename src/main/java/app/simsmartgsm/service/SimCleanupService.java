package app.simsmartgsm.service;

import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.uitils.SimStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ✅ Service để cleanup duplicate SIM và SIM REPLACED cũ
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SimCleanupService {

    private final SimRepository simRepository;

    /**
     * 🧹 Cleanup duplicate SIM trong database
     * - Giữ lại SIM mới nhất (lastUpdated)
     * - Xóa các SIM duplicate cũ hơn
     * 
     * @return Số lượng SIM đã xóa
     */
    public int cleanupDuplicateSims() {
        log.info("=== 🧹 BẮT ĐẦU CLEANUP DUPLICATE SIM ===");

        List<Sim> allSims = simRepository.findAll();
        log.info("📊 Tổng số SIM trong database: {}", allSims.size());

        int deletedCount = 0;

        // Group by CCID
        Map<String, List<Sim>> byCcid = allSims.stream()
                .filter(s -> s.getCcid() != null && !s.getCcid().isBlank())
                .collect(Collectors.groupingBy(Sim::getCcid));

        log.info("📊 Tìm thấy {} CCID unique", byCcid.size());

        // Xử lý duplicate theo CCID
        for (Map.Entry<String, List<Sim>> entry : byCcid.entrySet()) {
            String ccid = entry.getKey();
            List<Sim> sims = entry.getValue();

            if (sims.size() > 1) {
                log.warn("⚠️ Tìm thấy {} SIM duplicate với CCID: {}", sims.size(), ccid);

                // Sắp xếp theo lastUpdated (mới nhất lên đầu)
                sims.sort((a, b) -> {
                    Instant timeA = a.getLastUpdated() != null ? a.getLastUpdated() : Instant.MIN;
                    Instant timeB = b.getLastUpdated() != null ? b.getLastUpdated() : Instant.MIN;
                    return timeB.compareTo(timeA);
                });

                // Giữ lại SIM đầu tiên (mới nhất)
                Sim keepSim = sims.get(0);
                log.info("✅ Giữ lại SIM: id={}, phone={}, status={}, lastUpdated={}",
                        keepSim.getId(), keepSim.getPhoneNumber(), keepSim.getStatus(), keepSim.getLastUpdated());

                // Xóa các SIM còn lại
                for (int i = 1; i < sims.size(); i++) {
                    Sim deleteSim = sims.get(i);
                    log.warn("🗑️ Xóa SIM duplicate: id={}, phone={}, status={}, lastUpdated={}",
                            deleteSim.getId(), deleteSim.getPhoneNumber(), deleteSim.getStatus(),
                            deleteSim.getLastUpdated());

                    simRepository.delete(deleteSim);
                    deletedCount++;
                }
            }
        }

        // Group by IMSI (cho các SIM không có CCID hoặc CCID bị lỗi)
        Map<String, List<Sim>> byImsi = allSims.stream()
                .filter(s -> s.getImsi() != null && !s.getImsi().isBlank())
                .filter(s -> s.getCcid() == null || s.getCcid().isBlank()) // Chỉ xử lý SIM không có CCID
                .collect(Collectors.groupingBy(Sim::getImsi));

        log.info("📊 Tìm thấy {} IMSI unique (không có CCID)", byImsi.size());

        // Xử lý duplicate theo IMSI
        for (Map.Entry<String, List<Sim>> entry : byImsi.entrySet()) {
            String imsi = entry.getKey();
            List<Sim> sims = entry.getValue();

            if (sims.size() > 1) {
                log.warn("⚠️ Tìm thấy {} SIM duplicate với IMSI: {}", sims.size(), imsi);

                sims.sort((a, b) -> {
                    Instant timeA = a.getLastUpdated() != null ? a.getLastUpdated() : Instant.MIN;
                    Instant timeB = b.getLastUpdated() != null ? b.getLastUpdated() : Instant.MIN;
                    return timeB.compareTo(timeA);
                });

                Sim keepSim = sims.get(0);
                log.info("✅ Giữ lại SIM (IMSI): id={}, phone={}, status={}", keepSim.getId(),
                        keepSim.getPhoneNumber(), keepSim.getStatus());

                for (int i = 1; i < sims.size(); i++) {
                    Sim deleteSim = sims.get(i);
                    log.warn("🗑️ Xóa SIM duplicate (IMSI): id={}, phone={}, status={}", deleteSim.getId(),
                            deleteSim.getPhoneNumber(), deleteSim.getStatus());

                    simRepository.delete(deleteSim);
                    deletedCount++;
                }
            }
        }

        log.info("=== ✅ CLEANUP HOÀN TẤT: Đã xóa {} SIM duplicate ===", deletedCount);
        return deletedCount;
    }

    /**
     * 🗑️ Xóa các SIM REPLACED cũ (quá 30 ngày)
     * 
     * @return Số lượng SIM đã xóa
     */
    public int cleanupOldReplacedSims() {
        log.info("=== 🗑️ BẮT ĐẦU CLEANUP SIM REPLACED CỦ ===");

        List<Sim> allSims = simRepository.findAll();
        Instant threshold = Instant.now().minusSeconds(30L * 24 * 60 * 60); // 30 ngày trước

        List<Sim> toDelete = allSims.stream()
                .filter(s -> String.valueOf(SimStatus.REPLACED).equals(s.getStatus()))
                .filter(s -> s.getLastUpdated() != null && s.getLastUpdated().isBefore(threshold))
                .collect(Collectors.toList());

        log.info("📊 Tìm thấy {} SIM REPLACED cũ hơn 30 ngày", toDelete.size());

        for (Sim sim : toDelete) {
            log.info("🗑️ Xóa SIM REPLACED cũ: ccid={}, phone={}, lastUpdated={}",
                    sim.getCcid(), sim.getPhoneNumber(), sim.getLastUpdated());
            simRepository.delete(sim);
        }

        log.info("=== ✅ CLEANUP HOÀN TẤT: Đã xóa {} SIM REPLACED cũ ===", toDelete.size());
        return toDelete.size();
    }

    /**
     * 🧹 Cleanup duplicate SIM theo PHONE NUMBER
     * - Nếu cùng số điện thoại xuất hiện ở nhiều CCID khác nhau (do thay SIM)
     * - Giữ lại SIM có ACTIVE status, hoặc mới nhất nếu không có ACTIVE
     * - Xóa các SIM duplicate cũ hơn
     * 
     * @return Số lượng SIM đã xóa
     */
    public int cleanupDuplicateByPhoneNumber() {
        log.info("=== 🧹 BẮT ĐẦU CLEANUP DUPLICATE SIM THEO PHONE NUMBER ===");

        List<Sim> allSims = simRepository.findAll();
        log.info("📊 Tổng số SIM trong database: {}", allSims.size());

        int deletedCount = 0;

        // Group by phoneNumber (chỉ xử lý SIM có số điện thoại)
        Map<String, List<Sim>> byPhone = allSims.stream()
                .filter(s -> s.getPhoneNumber() != null && !s.getPhoneNumber().isBlank())
                .collect(Collectors.groupingBy(Sim::getPhoneNumber));

        log.info("📊 Tìm thấy {} số điện thoại unique", byPhone.size());

        for (Map.Entry<String, List<Sim>> entry : byPhone.entrySet()) {
            String phoneNumber = entry.getKey();
            List<Sim> sims = entry.getValue();

            if (sims.size() > 1) {
                log.warn("⚠️ Tìm thấy {} SIM duplicate với phoneNumber: {}", sims.size(), phoneNumber);

                // Log tất cả các SIM duplicate
                for (Sim sim : sims) {
                    log.info("   📱 id={}, ccid={}, status={}, device={}, com={}, lastUpdated={}",
                            sim.getId(), sim.getCcid(), sim.getStatus(),
                            sim.getDeviceName(), sim.getComName(), sim.getLastUpdated());
                }

                // Sắp xếp: ACTIVE lên đầu, rồi theo lastUpdated (mới nhất lên đầu)
                sims.sort((a, b) -> {
                    // Ưu tiên ACTIVE
                    boolean aActive = "ACTIVE".equals(a.getStatus());
                    boolean bActive = "ACTIVE".equals(b.getStatus());

                    if (aActive && !bActive)
                        return -1;
                    if (!aActive && bActive)
                        return 1;

                    // Nếu cùng status, so sánh lastUpdated (mới nhất lên đầu)
                    Instant timeA = a.getLastUpdated() != null ? a.getLastUpdated() : Instant.MIN;
                    Instant timeB = b.getLastUpdated() != null ? b.getLastUpdated() : Instant.MIN;
                    return timeB.compareTo(timeA);
                });

                // Giữ lại SIM đầu tiên (ACTIVE hoặc mới nhất)
                Sim keepSim = sims.get(0);
                log.info("✅ Giữ lại SIM: id={}, ccid={}, status={}, lastUpdated={}",
                        keepSim.getId(), keepSim.getCcid(), keepSim.getStatus(), keepSim.getLastUpdated());

                // Xóa các SIM còn lại
                for (int i = 1; i < sims.size(); i++) {
                    Sim deleteSim = sims.get(i);
                    log.warn("🗑️ Xóa SIM duplicate (phoneNumber): id={}, ccid={}, status={}, lastUpdated={}",
                            deleteSim.getId(), deleteSim.getCcid(), deleteSim.getStatus(),
                            deleteSim.getLastUpdated());

                    simRepository.delete(deleteSim);
                    deletedCount++;
                }
            }
        }

        log.info("=== ✅ CLEANUP HOÀN TẤT: Đã xóa {} SIM duplicate theo phoneNumber ===", deletedCount);
        return deletedCount;
    }

    /**
     * 🔍 Tìm tất cả SIM duplicate theo phoneNumber
     */
    public Map<String, List<Sim>> findDuplicateByPhoneNumber() {
        List<Sim> allSims = simRepository.findAll();

        Map<String, List<Sim>> byPhone = allSims.stream()
                .filter(s -> s.getPhoneNumber() != null && !s.getPhoneNumber().isBlank())
                .collect(Collectors.groupingBy(Sim::getPhoneNumber));

        // Chỉ giữ lại các phoneNumber có duplicate
        return byPhone.entrySet().stream()
                .filter(e -> e.getValue().size() > 1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * 📊 Thống kê SIM trong database
     */
    public Map<String, Object> getSimStatistics() {
        List<Sim> allSims = simRepository.findAll();

        Map<String, Long> statusCount = allSims.stream()
                .collect(Collectors.groupingBy(
                        s -> s.getStatus() != null ? s.getStatus() : "NULL",
                        Collectors.counting()));

        long withCcid = allSims.stream().filter(s -> s.getCcid() != null && !s.getCcid().isBlank()).count();
        long withImsi = allSims.stream().filter(s -> s.getImsi() != null && !s.getImsi().isBlank()).count();
        long withPhone = allSims.stream().filter(s -> s.getPhoneNumber() != null && !s.getPhoneNumber().isBlank())
                .count();

        // Đếm duplicate
        Map<String, Long> ccidCount = allSims.stream()
                .filter(s -> s.getCcid() != null && !s.getCcid().isBlank())
                .collect(Collectors.groupingBy(Sim::getCcid, Collectors.counting()));

        long duplicateCcids = ccidCount.values().stream().filter(count -> count > 1).count();

        Map<String, Object> stats = new HashMap<>();
        stats.put("total", allSims.size());
        stats.put("withCcid", withCcid);
        stats.put("withImsi", withImsi);
        stats.put("withPhone", withPhone);
        stats.put("statusBreakdown", statusCount);
        stats.put("duplicateCcids", duplicateCcids);

        return stats;
    }

    /**
     * 🔍 Tìm tất cả SIM duplicate
     */
    public Map<String, List<Sim>> findDuplicateSims() {
        List<Sim> allSims = simRepository.findAll();

        Map<String, List<Sim>> byCcid = allSims.stream()
                .filter(s -> s.getCcid() != null && !s.getCcid().isBlank())
                .collect(Collectors.groupingBy(Sim::getCcid));

        // Chỉ giữ lại các CCID có duplicate
        return byCcid.entrySet().stream()
                .filter(e -> e.getValue().size() > 1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
