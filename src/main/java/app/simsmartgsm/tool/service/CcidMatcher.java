package app.simsmartgsm.tool.service;

import app.simsmartgsm.entity.Sim;
import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Objects;

/**
 * Chuẩn hóa và đối chiếu ICCID đọc từ modem với ICCID từ file import.
 *
 * Modem thường trả thêm F, dấu cách, prefix hoặc lệch một ký tự kiểm tra.
 * Chỉ tự ghép khi có một ứng viên tốt nhất duy nhất để tránh gán nhầm số.
 */
@Service
public class CcidMatcher {
    private static final int MIN_CCID_LENGTH = 15;

    public String normalize(String raw) {
        if (raw == null) {
            return "";
        }
        String normalized = Normalizer.normalize(raw, Normalizer.Form.NFKC)
                .toUpperCase()
                .replaceFirst("^\\s*ICCID\\s*[:=]\\s*", "")
                .replaceFirst("^\\s*CCID\\s*[:=]\\s*", "")
                .replaceAll("[^0-9]", "");
        return normalized;
    }

    public boolean isValid(String raw) {
        int length = normalize(raw).length();
        return length >= MIN_CCID_LENGTH && length <= 24;
    }

    public int score(String leftRaw, String rightRaw) {
        String left = normalize(leftRaw);
        String right = normalize(rightRaw);
        if (left.length() < MIN_CCID_LENGTH || right.length() < MIN_CCID_LENGTH) {
            return 0;
        }
        if (left.equals(right)) {
            return 100;
        }

        int minLength = Math.min(left.length(), right.length());
        if (minLength >= 17 && (left.startsWith(right) || right.startsWith(left))) {
            return 96 - Math.abs(left.length() - right.length());
        }

        int distance = levenshtein(left, right, 2);
        if (distance <= 2) {
            return 92 - distance * 6 - Math.abs(left.length() - right.length());
        }

        int prefix = commonPrefix(left, right);
        int suffix = commonSuffix(left, right);
        int substring = longestCommonSubstring(left, right);
        if (prefix >= 18 || suffix >= 18) {
            return 82;
        }
        if (substring >= 18) {
            return 78;
        }
        if (substring >= 17 && minLength <= 20) {
            return 70;
        }
        return 0;
    }

    public Optional<Match> findBest(String scannedCcid, List<Sim> candidates) {
        List<Match> matches = candidates.stream()
                .map(sim -> {
                    int direct = score(scannedCcid, sim.getCcid());
                    int imported = score(scannedCcid, sim.getImportedCcid());
                    return new Match(sim, Math.max(direct, imported),
                            imported > direct ? "IMPORTED_CCID" : "CCID");
                })
                .filter(match -> match.score() >= 70)
                .sorted(Comparator.comparingInt(Match::score).reversed()
                        .thenComparing(match -> match.sim().getPhoneNumber() == null ? 1 : 0))
                .toList();
        if (matches.isEmpty()) {
            return Optional.empty();
        }
        Match best = matches.get(0);
        if (matches.size() > 1 && matches.get(1).score() == best.score()
                && !Objects.equals(matches.get(1).sim().getId(), best.sim().getId())) {
            return Optional.empty();
        }
        return Optional.of(best);
    }

    private int levenshtein(String left, String right, int limit) {
        if (Math.abs(left.length() - right.length()) > limit) {
            return limit + 1;
        }
        int[] previous = new int[right.length() + 1];
        for (int j = 0; j <= right.length(); j++) previous[j] = j;
        for (int i = 1; i <= left.length(); i++) {
            int[] current = new int[right.length() + 1];
            current[0] = i;
            int rowMin = current[0];
            for (int j = 1; j <= right.length(); j++) {
                int cost = left.charAt(i - 1) == right.charAt(j - 1) ? 0 : 1;
                current[j] = Math.min(Math.min(current[j - 1] + 1, previous[j] + 1), previous[j - 1] + cost);
                rowMin = Math.min(rowMin, current[j]);
            }
            if (rowMin > limit) return limit + 1;
            previous = current;
        }
        return previous[right.length()];
    }

    private int commonPrefix(String left, String right) {
        int i = 0;
        while (i < Math.min(left.length(), right.length()) && left.charAt(i) == right.charAt(i)) i++;
        return i;
    }

    private int commonSuffix(String left, String right) {
        int i = 0;
        while (i < Math.min(left.length(), right.length())
                && left.charAt(left.length() - 1 - i) == right.charAt(right.length() - 1 - i)) i++;
        return i;
    }

    private int longestCommonSubstring(String left, String right) {
        int[] previous = new int[right.length() + 1];
        int best = 0;
        for (int i = 1; i <= left.length(); i++) {
            int[] current = new int[right.length() + 1];
            for (int j = 1; j <= right.length(); j++) {
                if (left.charAt(i - 1) == right.charAt(j - 1)) {
                    current[j] = previous[j - 1] + 1;
                    best = Math.max(best, current[j]);
                }
            }
            previous = current;
        }
        return best;
    }

    public record Match(Sim sim, int score, String matchedField) {}
}
