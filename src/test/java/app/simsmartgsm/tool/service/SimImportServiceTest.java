package app.simsmartgsm.tool.service;

import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.SimRepository;
import app.simsmartgsm.tool.dto.SimImportResult;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.mock.web.MockMultipartFile;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SimImportServiceTest {
    @Test
    void importsTextAndUpdatesFuzzyCcidMatch() {
        SimRepository repository = mock(SimRepository.class);
        Sim existing = Sim.builder()
                .id("sim-1")
                .ccid("8981041012345678909")
                .status("INACTIVE")
                .build();
        when(repository.findAll()).thenReturn(List.of(existing));
        when(repository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        SimImportService service = new SimImportService(repository, new CcidMatcher());
        SimImportResult result = service.importText(
                "STT, SĐT, CCID\n1, 0901234567, 8981041012345678901");

        assertThat(result.getUpdated()).isEqualTo(1);
        assertThat(result.getFuzzyMatched()).isEqualTo(1);
        assertThat(existing.getPhoneNumber()).isEqualTo("0901234567");
        assertThat(existing.getImportedCcidNormalized()).isEqualTo("8981041012345678901");

        ArgumentCaptor<Iterable<Sim>> captor = ArgumentCaptor.forClass(Iterable.class);
        verify(repository).saveAll(captor.capture());
        assertThat(captor.getValue()).hasSize(1);
    }

    @Test
    void importsExcelWithVietnameseHeaders() throws Exception {
        SimRepository repository = mock(SimRepository.class);
        when(repository.findAll()).thenReturn(List.of());
        when(repository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        byte[] workbookBytes;
        try (XSSFWorkbook workbook = new XSSFWorkbook(); ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            var sheet = workbook.createSheet("SIM");
            var header = sheet.createRow(0);
            header.createCell(0).setCellValue("STT");
            header.createCell(1).setCellValue("SĐT");
            header.createCell(2).setCellValue("CCID");
            var row = sheet.createRow(1);
            row.createCell(0).setCellValue(1);
            row.createCell(1).setCellValue("0909876543");
            row.createCell(2).setCellValue("8981041012345678901");
            workbook.write(output);
            workbookBytes = output.toByteArray();
        }

        MockMultipartFile file = new MockMultipartFile(
                "file", "sims.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", workbookBytes);
        SimImportResult result = new SimImportService(repository, new CcidMatcher()).importExcel(file);

        assertThat(result.getCreated()).isEqualTo(1);
        ArgumentCaptor<Iterable<Sim>> captor = ArgumentCaptor.forClass(Iterable.class);
        verify(repository).saveAll(captor.capture());
        Sim imported = captor.getValue().iterator().next();
        assertThat(imported.getPhoneNumber()).isEqualTo("0909876543");
        assertThat(imported.getImportedCcidNormalized()).isEqualTo("8981041012345678901");
        assertThat(imported.getStatus()).isEqualTo("IMPORTED");
    }
}
