package app.simsmartgsm.tool.service;

import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.tool.dto.SimImportResult;
import lombok.RequiredArgsConstructor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.math.BigDecimal;
import java.text.Normalizer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class SimImportService {
    private static final int MAX_ROWS = 20_000;

    private final SimRepository simRepository;
    private final CcidMatcher ccidMatcher;

    public SimImportResult importExcel(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("File Excel đang trống");
        }
        List<ImportRow> rows = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        try (InputStream input = file.getInputStream(); Workbook workbook = WorkbookFactory.create(input)) {
            Sheet sheet = workbook.getSheetAt(0);
            DataFormatter formatter = new DataFormatter(Locale.ROOT);
            Header header = detectHeader(sheet, formatter);
            for (int rowIndex = header.dataStartRow(); rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                if (rows.size() >= MAX_ROWS) {
                    warnings.add("Chỉ xử lý " + MAX_ROWS + " dòng đầu tiên");
                    break;
                }
                Row row = sheet.getRow(rowIndex);
                if (row == null) continue;
                String sequence = cellValue(row.getCell(header.sequenceColumn()), formatter, warnings, rowIndex);
                String phone = cellValue(row.getCell(header.phoneColumn()), formatter, warnings, rowIndex);
                String ccid = cellValue(row.getCell(header.ccidColumn()), formatter, warnings, rowIndex);
                if (sequence.isBlank() && phone.isBlank() && ccid.isBlank()) continue;
                rows.add(new ImportRow(parseSequence(sequence, rowIndex + 1), phone, ccid, rowIndex + 1));
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Không đọc được file Excel: " + e.getMessage(), e);
        }
        return persist(rows, warnings);
    }

    public SimImportResult importText(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("Nội dung import đang trống");
        }
        List<ImportRow> rows = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        String[] lines = text.split("\\R");
        int start = looksLikeHeader(lines[0]) ? 1 : 0;
        for (int i = start; i < lines.length && rows.size() < MAX_ROWS; i++) {
            String line = lines[i].trim();
            if (line.isBlank()) continue;
            String[] columns = splitLine(line);
            if (columns.length < 3) {
                warnings.add("Dòng " + (i + 1) + ": cần đủ STT, SĐT và CCID");
                continue;
            }
            rows.add(new ImportRow(parseSequence(columns[0], i + 1), columns[1], columns[2], i + 1));
        }
        return persist(rows, warnings);
    }

    private SimImportResult persist(List<ImportRow> rows, List<String> warnings) {
        List<Sim> existing = new ArrayList<>(simRepository.findAll());
        List<Sim> changed = new ArrayList<>();
        Set<String> seenCcids = new HashSet<>();
        Set<String> seenPhones = new HashSet<>();
        int created = 0;
        int updated = 0;
        int fuzzyMatched = 0;
        int skipped = 0;

        for (ImportRow row : rows) {
            String phone = normalizePhone(row.phone());
            String normalizedPhone = digits(phone);
            String normalizedCcid = ccidMatcher.normalize(row.ccid());

            if (normalizedPhone.length() < 7) {
                warnings.add("Dòng " + row.sourceRow() + ": SĐT không hợp lệ");
                skipped++;
                continue;
            }
            if (!ccidMatcher.isValid(row.ccid())) {
                warnings.add("Dòng " + row.sourceRow() + ": CCID không hợp lệ");
                skipped++;
                continue;
            }
            if (!seenPhones.add(normalizedPhone) || !seenCcids.add(normalizedCcid)) {
                warnings.add("Dòng " + row.sourceRow() + ": trùng SĐT hoặc CCID trong file");
                skipped++;
                continue;
            }

            Sim target = existing.stream()
                    .filter(sim -> normalizedPhone.equals(digits(sim.getPhoneNumber())))
                    .findFirst()
                    .orElse(null);
            CcidMatcher.Match match = null;
            if (target == null) {
                match = ccidMatcher.findBest(normalizedCcid, existing).orElse(null);
                if (match != null) {
                    target = match.sim();
                    if (match.score() < 100) fuzzyMatched++;
                }
            }

            boolean isNew = target == null;
            if (isNew) {
                target = Sim.builder()
                        .phoneNumber(phone)
                        .ccid(normalizedCcid)
                        .status("IMPORTED")
                        .countryCode("JP")
                        .dataSource("IMPORT")
                        .lastUpdated(Instant.now())
                        .build();
                existing.add(target);
                created++;
            } else {
                target.setPhoneNumber(phone);
                target.setLastUpdated(Instant.now());
                if (target.getCcid() == null || target.getCcid().isBlank()) {
                    target.setCcid(normalizedCcid);
                }
                if (target.getDataSource() == null) {
                    target.setDataSource("IMPORT");
                }
                updated++;
            }

            target.setImportedCcid(row.ccid().trim());
            target.setImportedCcidNormalized(normalizedCcid);
            target.setImportSequence(row.sequence());
            Integer matchScore = null;
            if (match != null) {
                matchScore = match.score();
            } else if (!isNew) {
                matchScore = ccidMatcher.score(normalizedCcid, target.getCcid());
            }
            target.setCcidMatchScore(matchScore);
            changed.add(target);
        }

        if (!changed.isEmpty()) {
            simRepository.saveAll(changed);
        }
        return SimImportResult.builder()
                .totalRows(rows.size())
                .created(created)
                .updated(updated)
                .fuzzyMatched(fuzzyMatched)
                .skipped(skipped)
                .warnings(warnings.stream().limit(100).toList())
                .build();
    }

    private Header detectHeader(Sheet sheet, DataFormatter formatter) {
        int last = Math.min(sheet.getLastRowNum(), 10);
        for (int rowIndex = sheet.getFirstRowNum(); rowIndex <= last; rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            if (row == null) continue;
            int sequence = -1, phone = -1, ccid = -1;
            for (int col = 0; col < Math.min(Math.max(row.getLastCellNum(), 0), 20); col++) {
                String name = normalizeHeader(formatter.formatCellValue(row.getCell(col)));
                if (name.equals("stt") || name.equals("so thu tu") || name.equals("no")) sequence = col;
                if (name.equals("sdt") || name.contains("so dien thoai") || name.equals("phone")
                        || name.equals("phonenumber")) phone = col;
                if (name.equals("ccid") || name.equals("iccid")) ccid = col;
            }
            if (phone >= 0 && ccid >= 0) {
                return new Header(sequence >= 0 ? sequence : 0, phone, ccid, rowIndex + 1);
            }
        }
        return new Header(0, 1, 2, sheet.getFirstRowNum());
    }

    private String cellValue(Cell cell, DataFormatter formatter, List<String> warnings, int rowIndex) {
        if (cell == null) return "";
        if (cell.getCellType() == CellType.NUMERIC) {
            String plain = BigDecimal.valueOf(cell.getNumericCellValue()).stripTrailingZeros().toPlainString();
            if (plain.length() >= 15) {
                warnings.add("Dòng " + (rowIndex + 1)
                        + ": CCID dạng số trong Excel có thể mất chữ số; nên định dạng cột CCID là Text");
            }
            return plain;
        }
        return formatter.formatCellValue(cell).trim();
    }

    private String[] splitLine(String line) {
        String[] separated = line.split("\\s*[\\t,;|]\\s*", -1);
        if (separated.length >= 3) return separated;
        separated = line.split("\\s{2,}", -1);
        return separated.length >= 3 ? separated : line.split("\\s+", -1);
    }

    private boolean looksLikeHeader(String line) {
        String normalized = normalizeHeader(line);
        return normalized.contains("ccid") && (normalized.contains("sdt") || normalized.contains("dien thoai"));
    }

    private String normalizeHeader(String value) {
        return Normalizer.normalize(value == null ? "" : value, Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "")
                .toLowerCase()
                .replaceAll("[^a-z0-9]+", " ")
                .trim();
    }

    private int parseSequence(String value, int fallback) {
        try {
            String digits = value == null ? "" : value.replaceAll("[^0-9]", "");
            return digits.isBlank() ? fallback : Integer.parseInt(digits);
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private String normalizePhone(String value) {
        if (value == null) return "";
        String trimmed = value.trim();
        boolean plus = trimmed.startsWith("+");
        String digits = digits(trimmed);
        return plus ? "+" + digits : digits;
    }

    private String digits(String value) {
        return value == null ? "" : value.replaceAll("[^0-9]", "");
    }

    private record ImportRow(int sequence, String phone, String ccid, int sourceRow) {}
    private record Header(int sequenceColumn, int phoneColumn, int ccidColumn, int dataStartRow) {}
}
