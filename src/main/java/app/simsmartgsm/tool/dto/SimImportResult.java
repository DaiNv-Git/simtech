package app.simsmartgsm.tool.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class SimImportResult {
    private int totalRows;
    private int created;
    private int updated;
    private int fuzzyMatched;
    private int skipped;
    private List<String> warnings;
}
