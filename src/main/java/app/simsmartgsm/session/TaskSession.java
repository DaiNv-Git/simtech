package app.simsmartgsm.session;

/**
 * ✅ Interface cho session
 * - AutoCloseable → tự động cleanup
 */
public interface TaskSession extends AutoCloseable {

    /**
     * ✅ Mở port và khởi tạo modem
     */
    void openPort() throws Exception;

    /**
     * ✅ Thực thi task
     */
    SessionResult execute() throws Exception;

    /**
     * ✅ Đóng port (auto-called by try-with-resources)
     */
    @Override
    void close() throws Exception;
}
