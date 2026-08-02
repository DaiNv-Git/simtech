# simTech

Ứng dụng desktop quản lý thiết bị GSM, giữ nguyên dashboard và chức năng gọi điện. Phần tích hợp `sms-global-hub` đã được loại bỏ; SMS nhận được lưu vào MongoDB và chuyển tiếp tới Telegram, đồng thời ứng dụng vẫn hỗ trợ gửi SMS qua modem.

Ứng dụng dùng cùng máy chủ MongoDB hiện có nhưng tách riêng database `simtech`. Hai collection chính:

- `sims`: số SIM, cổng COM, nhà mạng, ICCID, IMSI và trạng thái.
- `sms`: lịch sử tin nhận/gửi, trạng thái và phản hồi modem.

## Cấu hình

Thiết lập các biến môi trường trước khi chạy:

```text
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=<token do BotFather cấp>
TELEGRAM_CHAT_ID=<chat hoặc group id nhận SMS>
MONGODB_URI=mongodb://<username>:<password>@72.60.41.168:27017/simtech?authSource=admin
```

Sao chép `.env.example` thành `.env`, giữ nguyên tài khoản MongoDB cũ nhưng dùng database `simtech`. File `.env` đã được ignore và không được commit.

## Kiểm tra Telegram

Sau khi khởi động ứng dụng:

```bash
curl -X POST http://localhost:8080/api/tool/telegram/test
```

Kiểm tra trạng thái cấu hình:

```bash
curl http://localhost:8080/api/tool/telegram/status
```

## API chính

- `GET /api/tool/sims`: danh sách SIM trong MongoDB.
- `POST /api/tool/sims/scan`: quét modem ngay.
- `GET /api/tool/sms`: lịch sử SMS.
- `POST /api/tool/sms/send`: gửi SMS với JSON `comPort`, `toNumber`, `message`.

Việc quét SIM và thao tác modem dùng chung scanner/PortWorker của dashboard để không tranh chấp cổng COM với chức năng gọi điện. SMS nhận được lưu vào collection `sms` rồi gửi Telegram khi cấu hình hợp lệ.
