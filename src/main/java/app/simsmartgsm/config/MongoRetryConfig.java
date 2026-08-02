package app.simsmartgsm.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.ClientSessionException;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * 🔄 MongoDB Retry Configuration
 * 
 * Tự động retry khi gặp lỗi:
 * - ClientSessionException (state should be: open)
 * - DataAccessException (connection timeout)
 */
@Configuration
@EnableRetry
@Slf4j
public class MongoRetryConfig {

    @Bean
    public RetryTemplate mongoRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        // ✅ Retry policy: retry 3 lần cho các exception cụ thể
        Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        retryableExceptions.put(ClientSessionException.class, true);
        retryableExceptions.put(DataAccessException.class, true);
        retryableExceptions.put(com.mongodb.MongoException.class, true);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(3, retryableExceptions);
        retryTemplate.setRetryPolicy(retryPolicy);

        // ✅ Backoff policy: chờ 500ms giữa các lần retry
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(500);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        log.info("✅ MongoDB RetryTemplate configured (3 retries, 500ms backoff)");

        return retryTemplate;
    }
}
