package org.apache.drill.exec.store.pdf;

import org.apache.pdfbox.pdmodel.PDDocument;
import technology.tabula.ObjectExtractor;
import technology.tabula.Page;
import technology.tabula.PageIterator;
import technology.tabula.Rectangle;
import technology.tabula.RectangularTextContainer;
import technology.tabula.Table;
import technology.tabula.detectors.NurminenDetectionAlgorithm;
import technology.tabula.extractors.BasicExtractionAlgorithm;
import technology.tabula.extractors.ExtractionAlgorithm;
import technology.tabula.extractors.SpreadsheetExtractionAlgorithm;

import java.util.ArrayList;
import java.util.List;

public class Utils {

  public static final ExtractionAlgorithm DEFAULT_ALGORITHM = new BasicExtractionAlgorithm();

  /**
   * Returns a list of tables found in a given PDF document.  There are several extraction algorithms
   * available and this function uses the default Basic Extraction Algorithm.
   * @param document The input PDF document to search for tables
   * @return A list of tables found in the document.
   */
  public static List<Table> extractTablesFromPDF(PDDocument document) {
    return extractTablesFromPDF(document, DEFAULT_ALGORITHM);
  }

  public static List<Table> extractTablesFromPDF(PDDocument document, ExtractionAlgorithm algorithm) {
    NurminenDetectionAlgorithm detectionAlgorithm = new NurminenDetectionAlgorithm();

    ExtractionAlgorithm algExtractor;

    SpreadsheetExtractionAlgorithm extractor=new SpreadsheetExtractionAlgorithm();

    ObjectExtractor objectExtractor = new ObjectExtractor(document);
    PageIterator pages = objectExtractor.extract();
    List<Table> tables= new ArrayList<>();
    while (pages.hasNext()) {
      Page page = pages.next();

      algExtractor = algorithm;
      /*if (extractor.isTabular(page)) {
        algExtractor=new SpreadsheetExtractionAlgorithm();
      }
      else {
        algExtractor = new BasicExtractionAlgorithm();
      }*/

      List<Rectangle> tablesOnPage = detectionAlgorithm.detect(page);

      for (Rectangle guessRect : tablesOnPage) {
        Page guess = page.getArea(guessRect);
        tables.addAll(algExtractor.extract(guess));
      }
    }
    return tables;
  }

  public static List<String> extractRowValues(Table table) {
    List<RectangularTextContainer> firstRow = table.getRows().get(0);
    List<String> values = new ArrayList<>();

    if (firstRow != null) {
      for (int i =0; i < firstRow.size(); i++) {
        values.add(firstRow.get(i).getText());
      }
    }
    return values;
  }

}
