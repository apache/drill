package org.apache.drill.exec.store.pdf;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.junit.Test;
import technology.tabula.Table;
import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class testPDFUtils {

  private static final String DATA_PATH = "src/test/resources/pdf/";

  @Test
  public void testTableExtractor() throws Exception {
    PDDocument document = getDocument("argentina_diputados_voting_record.pdf");
    List<Table> tableList = Utils.extractTablesFromPDF(document);
    document.close();
    assertEquals(tableList.size(), 1);

    PDDocument document2 = getDocument("twotables.pdf");
    List<Table> tableList2 = Utils.extractTablesFromPDF(document2);
    document2.close();
    assertEquals(tableList2.size(), 2);
  }

  @Test
  public void testTableExtractorWithNoBoundingFrame() throws Exception {
    PDDocument document = getDocument("spreadsheet_no_bounding_frame.pdf");
    List<Table> tableList = Utils.extractTablesFromPDF(document);
    document.close();
    assertEquals(tableList.size(), 1);
  }

  @Test
  public void testTableExtractorWitMultipage() throws Exception {
    PDDocument document = getDocument("us-020.pdf");
    List<Table> tableList = Utils.extractTablesFromPDF(document);
    document.close();
    assertEquals(tableList.size(), 4);
  }


  private PDDocument getDocument(String fileName) throws Exception {
    return PDDocument.load(new File(DATA_PATH + fileName));
  }

}
