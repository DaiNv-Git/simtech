package app.simsmartgsm.tool.service;

import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.entity.SmsMessageEntity;
import app.simsmartgsm.service.GsmService;
import app.simsmartgsm.service.SimSyncService;
import app.simsmartgsm.tool.model.SmsDocument;
import app.simsmartgsm.tool.repository.ToolSmsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;

@Service
@RequiredArgsConstructor
public class GsmToolService {
    private final app.simsmartgsm.repository.SimRepository simRepository;
    private final ToolSmsRepository smsRepository;
    private final SimSyncService simSyncService;
    private final GsmService gsmService;

    /**
     * Dùng đúng scanner/PortWorker của dashboard để không tranh chấp cổng COM
     * với chức năng gọi điện.
     */
    public List<Sim> scanAll() {
        try {
            return simSyncService.scanSimsOnly();
        } catch (Exception e) {
            throw new IllegalStateException("Không thể quét SIM: " + e.getMessage(), e);
        }
    }

    public SmsMessageEntity sendSms(String comPort, String toNumber, String message) {
        return gsmService.sendSms(app.simsmartgsm.dto.request.SendSmsRequest.builder()
                .comPort(comPort)
                .phoneNumber(toNumber)
                .content(message)
                .build());
    }

    public List<Sim> listSims() {
        List<Sim> sims = simRepository.findAll();
        sims.sort(Comparator.comparing(Sim::getComName,
                Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)));
        return sims;
    }

    public Page<SmsDocument> listSms(String direction, int page, int size) {
        PageRequest pageable = PageRequest.of(page, Math.min(Math.max(size, 1), 200),
                Sort.by(Sort.Direction.DESC, "createdAt"));
        if (direction == null || direction.isBlank()) {
            return smsRepository.findAll(pageable);
        }
        return smsRepository.findByDirectionOrderByCreatedAtDesc(direction.toUpperCase(), pageable);
    }
}

