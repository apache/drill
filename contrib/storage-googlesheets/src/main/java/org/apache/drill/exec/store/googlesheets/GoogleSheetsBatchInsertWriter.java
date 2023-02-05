package org.apache.drill.exec.store.googlesheets;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GoogleSheetsBatchInsertWriter extends GoogleSheetsBatchWriter {
  private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsBatchInsertWriter.class);

  public GoogleSheetsBatchInsertWriter(OperatorContext context, String name, GoogleSheetsWriter config) {
    super(context, name, config);
  }

  @Override
  public void updateSchema(VectorAccessible batch) {
    // no-op
  }

  @Override
  public void cleanup() {
    try {
      GoogleSheetsUtils.appendDataToGoogleSheet(service, spreadsheetID, tabName, values);
    } catch (IOException e) {
      throw UserException.dataWriteError(e)
          .message("Error writing to GoogleSheets " + e.getMessage())
          .build(logger);
    }
  }
}
