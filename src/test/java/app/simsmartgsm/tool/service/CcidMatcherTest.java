package app.simsmartgsm.tool.service;

import app.simsmartgsm.entity.Sim;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CcidMatcherTest {
    private final CcidMatcher matcher = new CcidMatcher();

    @Test
    void normalizesModemDecorationAndTrailingPadding() {
        assertThat(matcher.normalize("+CCID: 8981 0410-1234-5678-901F"))
                .isEqualTo("8981041012345678901");
    }

    @Test
    void matchesOneWrongDigitAndOneExtraDigit() {
        assertThat(matcher.score("8981041012345678901", "8981041012345678909")).isGreaterThanOrEqualTo(80);
        assertThat(matcher.score("8981041012345678901", "89810410123456789010")).isGreaterThanOrEqualTo(90);
    }

    @Test
    void refusesAmbiguousBestMatch() {
        Sim first = Sim.builder().id("a").ccid("8981041012345678901").build();
        Sim second = Sim.builder().id("b").ccid("8981041012345678901").build();
        assertThat(matcher.findBest("8981041012345678901F", List.of(first, second))).isEmpty();
    }
}
