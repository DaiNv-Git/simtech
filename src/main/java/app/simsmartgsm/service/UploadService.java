package app.simsmartgsm.service;

import app.simsmartgsm.baseGateway.GsmProperties;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

@Slf4j
@Service
@RequiredArgsConstructor
public class UploadService {

    private final GsmProperties props;

    private static final int SESSION_TIMEOUT_MS = 10000;
    private static final int CHANNEL_TIMEOUT_MS = 5000;

    /**
     * Upload file ghi âm qua SFTP → trả public URL
     */
    public String uploadRecording(File localFile) {
        if (localFile == null || !localFile.exists()) {
            log.warn("⚠️ File không tồn tại: {}", localFile);
            return null;
        }

        if (!localFile.canRead()) {
            log.warn("⚠️ Không có quyền đọc file: {}", localFile.getAbsolutePath());
            return null;
        }

        Session session = null;
        ChannelSftp sftp = null;

        try {
            var ssh = props.getSsh();
            var recordConfig = props.getRecord();

            JSch jsch = new JSch();
            session = jsch.getSession(ssh.getUser(), ssh.getHost(), ssh.getPort());
            session.setPassword(ssh.getPassword());

            // 🔒 Cấu hình an toàn
            session.setConfig("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "password");
            session.connect(SESSION_TIMEOUT_MS);

            sftp = (ChannelSftp) session.openChannel("sftp");
            sftp.connect(CHANNEL_TIMEOUT_MS);

            // ✅ Đường dẫn upload thực tế (VD: /var/www/html/recordings)
            String remoteDir = recordConfig.getUploadDir();
            if (remoteDir == null || remoteDir.isBlank()) {
                log.error("❌ Upload dir not configured in gsm.record.upload-dir");
                return null;
            }

            // Tạo thư mục nếu chưa tồn tại
            try {
                sftp.cd(remoteDir);
            } catch (Exception e) {
                log.warn("📁 Remote dir not found, creating: {}", remoteDir);
                sftp.mkdir(remoteDir);
                sftp.cd(remoteDir);
            }

            String remoteFileName = localFile.getName();
            try (FileInputStream fis = new FileInputStream(localFile)) {
                sftp.put(fis, remoteFileName);
            }

            String publicUrl = recordConfig.getPublicUrl();
            if (!publicUrl.endsWith("/")) publicUrl += "/";
            publicUrl += remoteFileName;

            log.info("📤 Upload thành công: {} → {}", localFile.getName(), publicUrl);
            return publicUrl;

        } catch (Exception e) {
            log.error("❌ Upload recording failed: {}", e.getMessage(), e);
            return null;
        } finally {
            closeQuietly(sftp);
            closeQuietly(session);
        }
    }

    private void closeQuietly(ChannelSftp channel) {
        if (channel != null && channel.isConnected()) {
            try {
                channel.disconnect();
            } catch (Exception e) {
                log.debug("Lỗi khi đóng SFTP channel: {}", e.getMessage());
            }
        }
    }

    private void closeQuietly(Session session) {
        if (session != null && session.isConnected()) {
            try {
                session.disconnect();
            } catch (Exception e) {
                log.debug("Lỗi khi đóng SSH session: {}", e.getMessage());
            }
        }
    }

    /**
     * Kiểm tra kết nối SFTP
     */
    public boolean testConnection() {
        Session session = null;
        ChannelSftp sftp = null;

        try {
            var ssh = props.getSsh();
            JSch jsch = new JSch();
            session = jsch.getSession(ssh.getUser(), ssh.getHost(), ssh.getPort());
            session.setPassword(ssh.getPassword());
            session.setConfig("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "password");
            session.connect(SESSION_TIMEOUT_MS);

            sftp = (ChannelSftp) session.openChannel("sftp");
            sftp.connect(CHANNEL_TIMEOUT_MS);

            log.info("✅ SFTP connection test successful");
            return true;

        } catch (Exception e) {
            log.error("❌ SFTP connection test failed: {}", e.getMessage());
            return false;
        } finally {
            closeQuietly(sftp);
            closeQuietly(session);
        }
    }
}
