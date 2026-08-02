package app.simsmartgsm.uitils;
import com.github.pemistahl.lingua.api.Language;
import com.github.pemistahl.lingua.api.LanguageDetector;
import com.github.pemistahl.lingua.api.LanguageDetectorBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LanguageUtil {
    private static final LanguageDetector detector =
            LanguageDetectorBuilder.fromAllLanguages().build();

    public static Language detect(String text) {
        if (text == null || text.isBlank()) return Language.UNKNOWN;
        try {
            Language lang = detector.detectLanguageOf(text);
            log.debug("🌐 Detected language: {} for text: {}", lang, text);
            return lang;
        } catch (Exception e) {
            log.warn("⚠️ Language detection failed: {}", e.getMessage());
            return Language.UNKNOWN;
        }
    }
}
