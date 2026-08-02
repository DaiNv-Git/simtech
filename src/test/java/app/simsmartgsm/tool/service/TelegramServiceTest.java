package app.simsmartgsm.tool.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TelegramServiceTest {

    @Test
    void formatsIncomingSmsForTelegram() {
        assertEquals("DESKTOP-V819GJI\n"
                        + "COM: COM57\n"
                        + "FROM: 0032069605\n"
                        + "TO: 08088867528\n"
                        + "MSG: 【LINE】認証番号「604127」",
                TelegramService.formatIncomingSms(
                        "DESKTOP-V819GJI", "COM57", "08088867528", "0032069605",
                        "【LINE】認証番号「604127」"));
    }
    @Test
    void configurationRequiresEnabledTokenAndChatId() {
        assertFalse(new TelegramService(false, "token", "123").isConfigured());
        assertFalse(new TelegramService(true, "", "123").isConfigured());
        assertFalse(new TelegramService(true, "token", " ").isConfigured());
        assertTrue(new TelegramService(true, "token", "123").isConfigured());
    }
}
