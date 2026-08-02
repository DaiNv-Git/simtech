package app.simsmartgsm.tool.repository;

import app.simsmartgsm.tool.model.WebhookSettingsDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface WebhookSettingsRepository extends MongoRepository<WebhookSettingsDocument, String> {
}

