package app.simsmartgsm.service;

import app.simsmartgsm.dto.response.SimResponse;
import app.simsmartgsm.uitils.AtCommandHelper;
import com.fazecast.jSerialComm.SerialPort;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class SimScanService {

    private final SimpMessagingTemplate messagingTemplate;

    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    /** Quét toàn bộ COM ports song song */
    public List<SimResponse> scanAllSims() {
        SerialPort[] ports = SerialPort.getCommPorts();

        List<Callable<SimResponse>> tasks = new ArrayList<>();
        for (SerialPort port : ports) {
            String comPort = port.getSystemPortName();
            tasks.add(() -> scanSimByCom(comPort));
        }

        List<SimResponse> results = new ArrayList<>();
        try {
            List<Future<SimResponse>> futures = executor.invokeAll(tasks);
            for (Future<SimResponse> f : futures) {
                try {
                    results.add(f.get());
                } catch (Exception e) {
                    log.error("❌ Lỗi khi scan SIM", e);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Push lên WebSocket
        messagingTemplate.convertAndSend("/topic/sims", results);

        return results;
    }

    /** Quét 1 COM port cụ thể */
    public SimResponse scanSimByCom(String comPort) {
        SimResponse response;
        try (AtCommandHelper helper = AtCommandHelper.open(comPort, 115200, 2000, 2000)) {
            String iccid = helper.getCcid();
            String phoneNumber = helper.getCnum();
            String provider = helper.queryOperator();

            String status = (phoneNumber == null ? "ERROR" : "OK");

            response = SimResponse.builder()
                    .comPort(comPort)
                    .status("ONLINE")
                    .carrier(provider)
                    .phoneNumber(phoneNumber)
                    .iccid(iccid)
                    .message(status)
                    .build();
        } catch (Exception e) {
            log.warn("❌ Không đọc được SIM ở {}", comPort, e);
            response = SimResponse.builder()
                    .comPort(comPort)
                    .status("OFFLINE")
                    .message("ERROR")
                    .build();
        }

        messagingTemplate.convertAndSend("/topic/sims/" + comPort, response);
        return response;
    }
}
