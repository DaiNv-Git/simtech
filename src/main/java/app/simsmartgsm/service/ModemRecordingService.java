package app.simsmartgsm.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service để xử lý ghi âm từ MODEM (tương tự code C#)
 * - Nhận WAV bytes từ modem qua serial port
 * - Detect RIFF header
 * - Lưu file khi nhận được +QFDWL: (download complete)
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ModemRecordingService {

    // Tương tự C# line 74-76
    private final ConcurrentHashMap<String, String> portWriteFile = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ByteArrayOutputStream> portFileBytes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> wavDownloading = new ConcurrentHashMap<>();

    @Value("${recording.save.path:recordings}")
    private String recordingSavePath;

    /**
     * Khởi tạo folder lưu recordings (giống C# CreateCallRecordFolder)
     */
    public void initializeRecordingFolder() {
        try {
            Path path = Paths.get(recordingSavePath);
            if (!Files.exists(path)) {
                Files.createDirectories(path);
                log.info("Created recording folder: {}", recordingSavePath);
            }
        } catch (IOException e) {
            log.error("Failed to create recording folder", e);
        }
    }

    /**
     * Bắt đầu tracking WAV download cho một COM port
     * Tương tự C# line 366-367
     */
    public void startWavDownload(String comPort, String fileName) {
        wavDownloading.put(comPort, true);
        portWriteFile.put(comPort, fileName);
        portFileBytes.put(comPort, new ByteArrayOutputStream());
        log.info("Started WAV download for port: {}, fileName: {}", comPort, fileName);
    }

    /**
     * Xử lý data nhận được từ serial port
     * Tương tự C# HandleReceivedLine (line 358-406)
     */
    public void handleSerialData(String comPort, byte[] data, String textData) {
        // Detect RIFF header (WAV file header) - giống C# line 366
        if (textData.contains("RIFF")) {
            if (!wavDownloading.getOrDefault(comPort, false)) {
                String fileName = generateFileName();
                startWavDownload(comPort, fileName);
            }
        }

        // Nếu đang download WAV file - giống C# line 369-406
        if (Boolean.TRUE.equals(wavDownloading.get(comPort))) {
            // Nếu chưa phải kết thúc (+QFDWL:)
            if (!textData.contains("+QFDWL:")) {
                // Append bytes vào buffer - giống C# line 373-387
                ByteArrayOutputStream buffer = portFileBytes.get(comPort);
                if (buffer != null) {
                    try {
                        buffer.write(data);
                    } catch (IOException e) {
                        log.error("Error writing WAV data for port: {}", comPort, e);
                    }
                }
            } else {
                // Download complete - giống C# line 392-405
                completeWavDownload(comPort);
            }
        }
    }

    /**
     * Hoàn thành WAV download và lưu file
     * Tương tự C# line 392-405
     */
    private void completeWavDownload(String comPort) {
        wavDownloading.put(comPort, false);

        String fileName = portWriteFile.get(comPort);
        ByteArrayOutputStream buffer = portFileBytes.get(comPort);

        if (fileName != null && buffer != null) {
            try {
                // Lưu file WAV - giống C# line 398-399
                Path filePath = Paths.get(recordingSavePath, fileName + ".wav");
                Files.write(filePath, buffer.toByteArray());

                log.info("Saved WAV file: {}", filePath);

                // TODO: Upload to server nếu cần - giống C# line 401
                // uploadToServer(buffer.toByteArray());

                // Cleanup
                portWriteFile.remove(comPort);
                portFileBytes.remove(comPort);

            } catch (IOException e) {
                log.error("Error saving WAV file for port: {}", comPort, e);
            }
        }
    }

    /**
     * Generate file name với timestamp
     */
    private String generateFileName() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
        return "call_" + LocalDateTime.now().format(formatter);
    }

    /**
     * Lấy đường dẫn file recording
     */
    public String getRecordingPath(String fileName) {
        return Paths.get(recordingSavePath, fileName + ".wav").toString();
    }

    /**
     * Lấy recording folder path
     */
    public String getRecordingSavePath() {
        return recordingSavePath;
    }

    /**
     * Set recording folder path
     */
    public void setRecordingSavePath(String path) {
        this.recordingSavePath = path;
        initializeRecordingFolder();
    }

    /**
     * Cleanup resources cho một port
     */
    public void cleanupPort(String comPort) {
        wavDownloading.remove(comPort);
        portWriteFile.remove(comPort);

        ByteArrayOutputStream buffer = portFileBytes.remove(comPort);
        if (buffer != null) {
            try {
                buffer.close();
            } catch (IOException e) {
                log.error("Error closing buffer for port: {}", comPort, e);
            }
        }
    }

    /**
     * Check xem port có đang download WAV không
     */
    public boolean isDownloading(String comPort) {
        return Boolean.TRUE.equals(wavDownloading.get(comPort));
    }
}
