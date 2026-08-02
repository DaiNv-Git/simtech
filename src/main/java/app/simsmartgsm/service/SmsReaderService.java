package app.simsmartgsm.service;

import app.simsmartgsm.config.SmsParser;
import app.simsmartgsm.dto.response.SmsMessageUser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class SmsReaderService {

    private final PortManager portManager;

    /**
     * Đọc toàn bộ SMS trong SIM/Modem (SM, ME, MT).
     */
    public List<SmsMessageUser> readAllSms(String comPort) {
        return portManager.withPort(comPort, helper -> {
            List<SmsMessageUser> result = new ArrayList<>();
            try {
                helper.sendAndRead("AT+CMGF=1", 2000);          // text mode
                helper.sendAndRead("AT+CSCS=\"GSM\"", 2000);    // charset

                String[] stores = {"SM", "ME", "MT"};
                for (String store : stores) {
                    helper.sendAndRead("AT+CPMS=\"" + store + "\",\"" + store + "\",\"" + store + "\"", 2000);

                    String resp = helper.sendAndRead("AT+CMGL=\"ALL\"", 10_000);
                    if (resp != null && resp.contains("+CMGL:")) {
                        result.addAll(SmsParser.parseMulti(resp));
                    }
                }

                log.info("📩 Đọc {} SMS từ {}", result.size(), comPort);
            } catch (Exception e) {
                log.error("❌ Lỗi đọc SMS từ {}: {}", comPort, e.getMessage());
            }
            return result;
        }, 15_000L);
    }

}
