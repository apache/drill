package org.apache.drill.exec.store.pdf;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;

public class PdfBatchReader implements ManagedReader<FileScanFramework.FileSchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(PdfBatchReader.class);
  private final int maxRecords;
  private final PdfReaderConfig readerConfig;
  private FileSplit split;
  private CustomErrorContext errorContext;
  private RowSetLoader rowWriter;
  private InputStream fsStream;
  private PDDocument document;
  private PDDocumentInformation info;

  // Document Metadata Fields
  private int pageCount;
  private String title;
  private String author;
  private String subject;
  private String keywords;
  private String creator;
  private String producer;
  private Calendar creationDate;
  private Calendar modificationDate;
  private String trapped;


  static class PdfReaderConfig {
    final PdfFormatPlugin plugin;

    PdfReaderConfig(PdfFormatPlugin plugin) {
      this.plugin = plugin;
    }
  }

  public PdfBatchReader(PdfReaderConfig readerConfig, int maxRecords) {
    this.readerConfig = readerConfig;
    this.maxRecords = maxRecords;
  }

  @Override
  public boolean open(FileScanFramework.FileSchemaNegotiator negotiator) {
    split = negotiator.split();
    errorContext = negotiator.parentErrorContext();
    ResultSetLoader loader = negotiator.build();
    rowWriter = loader.writer();
    openFile(negotiator);

    return true;
  }

  @Override
  public boolean next() {
    return false;
  }

  @Override
  public void close() {
    if (fsStream != null) {
      try {
        fsStream.close();
      } catch (IOException e) {
        logger.warn("Error when closing Excel File Stream resource: {}", e.getMessage());
      }
      fsStream = null;
    }
  }

  /**
   * This method opens the PDF file, and finds the tables
   * @param negotiator The Drill file negotiator object that represents the file system
   */
  private void openFile(FileScanFramework.FileSchemaNegotiator negotiator) {
    try {
      fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
      document = PDDocument.load(fsStream);
      info = document.getDocumentInformation();
      populateMetadata();
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to open open input file: %s", split.getPath().toString())
        .addContext(e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }
  }

  private void populateMetadata() {
    pageCount = document.getNumberOfPages();
    title = info.getTitle();
    author = info.getAuthor();
    subject = info.getSubject();
    keywords = info.getKeywords();
    creator = info.getCreator();
    producer = info.getProducer();
    creationDate = info.getCreationDate();
    modificationDate = info.getModificationDate();
    trapped = info.getTrapped();
  }
}
