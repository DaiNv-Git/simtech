package app.simsmartgsm.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
  public class SmsMessageUser {
  private Integer index;

  /** Số điện thoại gửi (sender) */
  private String sender;

  /** Ngày giờ (timestamp) modem ghi nhận, dạng "24/01/13,12:03:05+28" */
  private String timestamp;

  /** Nội dung SMS (body) */
  private String body;
}