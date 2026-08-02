# simTech

Ứng dụng desktop quản lý thiết bị GSM với dashboard mới, chức năng gửi/nhận SMS và gọi điện. Phần tích hợp `sms-global-hub` đã được loại bỏ; SMS nhận được lưu vào MongoDB và chuyển tiếp song song tới Telegram cùng webhook của khách hàng.

Ứng dụng dùng cùng máy chủ MongoDB hiện có nhưng tách riêng database `simtech`. Hai collection chính:

- `sims`: số SIM, cổng COM, nhà mạng, ICCID, IMSI và trạng thái.
- `sms`: lịch sử tin nhận/gửi, trạng thái và phản hồi modem.
- `settings`: cấu hình webhook khách hàng, bearer token và signing secret.

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

## Webhook khách hàng

Cấu hình trực tiếp tại tab **Settings** trên dashboard. Webhook nhận HTTP
`POST` với event `sms.received`. Nếu có signing secret, ứng dụng thêm header
`X-SimTech-Signature: sha256=<HMAC>`; nếu có bearer token, ứng dụng thêm header
`Authorization: Bearer <token>`.

- `GET /api/tool/settings/webhook`: đọc trạng thái cấu hình đã được che bí mật.
- `PUT /api/tool/settings/webhook`: lưu cấu hình.
- `POST /api/tool/settings/webhook/test`: gửi payload kiểm tra.

## API chính

- `GET /api/tool/sims`: danh sách SIM trong MongoDB.
- `POST /api/tool/sims/scan`: quét modem ngay.
- `GET /api/tool/sms`: lịch sử SMS.
- `POST /api/tool/sms/send`: gửi SMS với JSON `comPort`, `toNumber`, `message`.

Việc quét SIM và thao tác modem dùng chung scanner/PortWorker của dashboard để không tranh chấp cổng COM với chức năng gọi điện. Khi nhận SMS, Telegram và webhook được gọi độc lập nên một kênh lỗi không làm chặn kênh còn lại.
