package app.simsmartgsm.tool.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TelegramServiceTest {
    @Test
    void configurationRequiresEnabledTokenAndChatId() {
        assertFalse(new TelegramService(false, "token", "123").isConfigured());
        assertFalse(new TelegramService(true, "", "123").isConfigured());
        assertFalse(new TelegramService(true, "token", " ").isConfigured());
        assertTrue(new TelegramService(true, "token", "123").isConfigured());
    }
}
