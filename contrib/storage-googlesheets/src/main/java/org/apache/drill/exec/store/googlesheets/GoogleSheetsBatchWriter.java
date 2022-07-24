/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.googlesheets;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.NullableDateHolder;
import org.apache.drill.exec.expr.holders.NullableFloat4Holder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableSmallIntHolder;
import org.apache.drill.exec.expr.holders.NullableTimeHolder;
import org.apache.drill.exec.expr.holders.NullableTimeStampHolder;
import org.apache.drill.exec.expr.holders.NullableTinyIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.NullableVarDecimalHolder;
import org.apache.drill.exec.expr.holders.SmallIntHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.TinyIntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.expr.holders.VarDecimalHolder;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.AbstractRecordWriter;
import org.apache.drill.exec.store.EventBasedRecordWriter;
import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsUtils;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GoogleSheetsBatchWriter extends AbstractRecordWriter {
  private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsBatchWriter.class);

  private final Sheets service;
  private final String tabName;
  private final String sheetName;
  private final List<List<Object>> values;
  private List<Object> rowList;
  private String spreadsheetID;

  public GoogleSheetsBatchWriter(OperatorContext context, String name, GoogleSheetsWriter config) {
    GoogleSheetsStoragePlugin plugin = config.getPlugin();

    this.service = plugin.getSheetsService(config.getQueryUser());

    // GoogleSheets has three different identifiers to track:
    // 1.  The spreadsheetID is a non-human readable ID for the actual document which can contain
    //     one or more tabs of data.  This ID can be found in the URL when viewing a GoogleSheet.
    // 2.  The sheetName is the human readable name of the document.  When you have the spreadsheetID,
    //      you can obtain the sheetName, however Google did not provide any obvious way to list available
    //      GoogleSheet documents, nor to look up a sheetID from a title.
    // 3.  The tabName refers to the tab within a GoogleSheet document.
    this.tabName = name;
    this.sheetName = config.getSheetName();
    values = new ArrayList<>();
  }

  @Override
  public void init(Map<String, String> writerOptions) {
    // Do nothing
  }

  @Override
  public void updateSchema(VectorAccessible batch) throws IOException {
    // Create the new GoogleSheet doc
    Spreadsheet spreadsheet = new Spreadsheet()
      .setProperties(new SpreadsheetProperties().setTitle(sheetName));

    spreadsheet = service.spreadsheets().create(spreadsheet)
      .setFields("spreadsheetId")
      .execute();

    this.spreadsheetID = spreadsheet.getSpreadsheetId();
    // Now add the tab
    GoogleSheetsUtils.addTabToGoogleSheet(service, spreadsheetID, tabName);

    // Add the column names to the values list.  GoogleSheets does not have any concept
    // of column names, so we just insert the column names as the first row of data.
    BatchSchema schema = batch.getSchema();
    List<Object> columnNames = new ArrayList<>();
    for (MaterializedField field : schema) {
      columnNames.add(field.getName());
    }
    values.add(columnNames);
  }

  @Override
  public void startRecord() {
    rowList = new ArrayList<>();
  }

  @Override
  public void endRecord() {
    values.add(rowList);
  }

  @Override
  public void abort() {
    // Do nothing
  }

  @Override
  public void cleanup() {
    try {
      GoogleSheetsUtils.writeDataToGoogleSheet(service, spreadsheetID, tabName, values);
    } catch (IOException e) {
      throw UserException.dataWriteError(e)
        .message("Error writing to GoogleSheets " + e.getMessage())
        .build(logger);
    }
  }

  @Override
  public EventBasedRecordWriter.FieldConverter getNewNullableBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableBigIntGSConverter(fieldId, fieldName, reader);
  }

  public class NullableBigIntGSConverter extends EventBasedRecordWriter.FieldConverter {
    private final NullableBigIntHolder holder = new NullableBigIntHolder();

    public NullableBigIntGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add(null);
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public EventBasedRecordWriter.FieldConverter getNewBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new BigIntGSConverter(fieldId, fieldName, reader);
  }

  public class BigIntGSConverter extends EventBasedRecordWriter.FieldConverter {
    private final BigIntHolder holder = new BigIntHolder();

    public BigIntGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add(null);
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public EventBasedRecordWriter.FieldConverter getNewNullableIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableIntGSConverter(fieldId, fieldName, reader);
  }
  public class NullableIntGSConverter extends EventBasedRecordWriter.FieldConverter {
    private final NullableIntHolder holder = new NullableIntHolder();

    public NullableIntGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add(null);
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public EventBasedRecordWriter.FieldConverter getNewIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new IntGSConverter(fieldId, fieldName, reader);
  }

  public class IntGSConverter extends EventBasedRecordWriter.FieldConverter {
    private final IntHolder holder = new IntHolder();

    public IntGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add(null);
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }
  @Override
  public EventBasedRecordWriter.FieldConverter getNewNullableSmallIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableSmallIntGSConverter(fieldId, fieldName, reader);
  }
  public class NullableSmallIntGSConverter extends EventBasedRecordWriter.FieldConverter {
    private final NullableSmallIntHolder holder = new NullableSmallIntHolder();

    public NullableSmallIntGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add(null);
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public EventBasedRecordWriter.FieldConverter getNewSmallIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new SmallIntGSConverter(fieldId, fieldName, reader);
  }

  public class SmallIntGSConverter extends EventBasedRecordWriter.FieldConverter {
    private final SmallIntHolder holder = new SmallIntHolder();

    public SmallIntGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add(null);
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }
  @Override
  public EventBasedRecordWriter.FieldConverter getNewNullableTinyIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableTinyIntGSConverter(fieldId, fieldName, reader);
  }
  public class NullableTinyIntGSConverter extends EventBasedRecordWriter.FieldConverter {
    private final NullableTinyIntHolder holder = new NullableTinyIntHolder();

    public NullableTinyIntGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add(null);
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public EventBasedRecordWriter.FieldConverter getNewTinyIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new TinyIntGSConverter(fieldId, fieldName, reader);
  }

  public class TinyIntGSConverter extends EventBasedRecordWriter.FieldConverter {
    private final TinyIntHolder holder = new TinyIntHolder();

    public TinyIntGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add(null);
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public EventBasedRecordWriter.FieldConverter getNewNullableFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableFloat8GSConverter(fieldId, fieldName, reader);
  }
  public class NullableFloat8GSConverter extends EventBasedRecordWriter.FieldConverter {
    private final NullableFloat8Holder holder = new NullableFloat8Holder();

    public NullableFloat8GSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add(null);
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }
  @Override
  public EventBasedRecordWriter.FieldConverter getNewFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
    return new Float8GSConverter(fieldId, fieldName, reader);
  }

  public class Float8GSConverter extends EventBasedRecordWriter.FieldConverter {
    private final Float8Holder holder = new Float8Holder();

    public Float8GSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add(null);
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public EventBasedRecordWriter.FieldConverter getNewNullableFloat4Converter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableFloat4GSConverter(fieldId, fieldName, reader);
  }
  public class NullableFloat4GSConverter extends EventBasedRecordWriter.FieldConverter {
    private final NullableFloat4Holder holder = new NullableFloat4Holder();

    public NullableFloat4GSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add(null);
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public EventBasedRecordWriter.FieldConverter getNewFloat4Converter(int fieldId, String fieldName, FieldReader reader) {
    return new Float4GSConverter(fieldId, fieldName, reader);
  }

  public class Float4GSConverter extends EventBasedRecordWriter.FieldConverter {
    private final Float4Holder holder = new Float4Holder();

    public Float4GSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add(null);
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableVarDecimalConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableVardecimalGSConverter(fieldId, fieldName, reader);
  }

  public class NullableVardecimalGSConverter extends FieldConverter {
    private final NullableVarDecimalHolder holder = new NullableVarDecimalHolder();

    public NullableVardecimalGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add("null");
        return;
      }
      reader.read(holder);
      BigDecimal value = DecimalUtility.getBigDecimalFromDrillBuf(holder.buffer,
        holder.start, holder.end - holder.start, holder.scale);
      rowList.add(value);
    }
  }

  @Override
  public FieldConverter getNewVarDecimalConverter(int fieldId, String fieldName, FieldReader reader) {
    return new VardecimalGSConverter(fieldId, fieldName, reader);
  }

  public class VardecimalGSConverter extends FieldConverter {
    private final VarDecimalHolder holder = new VarDecimalHolder();

    public VardecimalGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      reader.read(holder);
      BigDecimal value = DecimalUtility.getBigDecimalFromDrillBuf(holder.buffer,
        holder.start, holder.end - holder.start, holder.scale);
      rowList.add(value);
    }
  }
  @Override
  public FieldConverter getNewNullableVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableVarCharGSConverter(fieldId, fieldName, reader);
  }

  public class NullableVarCharGSConverter extends FieldConverter {
    private final NullableVarCharHolder holder = new NullableVarCharHolder();

    public NullableVarCharGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      reader.read(holder);
      if (reader.isSet()) {
        String input = StringFunctionHelpers.toStringFromUTF8(holder.start, holder.end, holder.buffer);
        rowList.add(input);
      }
    }
  }


  @Override
  public FieldConverter getNewVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
    return new VarCharGSConverter(fieldId, fieldName, reader);
  }

  public class VarCharGSConverter extends FieldConverter {
    private final VarCharHolder holder = new VarCharHolder();

    public VarCharGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      reader.read(holder);
      if (reader.isSet()) {
        String input = StringFunctionHelpers.toStringFromUTF8(holder.start, holder.end, holder.buffer);
        rowList.add(input);
      }
    }
  }

  @Override
  public FieldConverter getNewNullableDateConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableDateGSConverter(fieldId, fieldName, reader);
  }

  public class NullableDateGSConverter extends FieldConverter {
    private final NullableDateHolder holder = new NullableDateHolder();

    public NullableDateGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add("null");
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewDateConverter(int fieldId, String fieldName, FieldReader reader) {
    return new DateGSConverter(fieldId, fieldName, reader);
  }

  public class DateGSConverter extends FieldConverter {
    private final DateHolder holder = new DateHolder();

    public DateGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableTimeConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableTimeGSConverter(fieldId, fieldName, reader);
  }

  public class NullableTimeGSConverter extends FieldConverter {
    private final NullableTimeHolder holder = new NullableTimeHolder();

    public NullableTimeGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add("null");
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewTimeConverter(int fieldId, String fieldName, FieldReader reader) {
    return new TimeGSConverter(fieldId, fieldName, reader);
  }

  public class TimeGSConverter extends FieldConverter {
    private final TimeHolder holder = new TimeHolder();

    public TimeGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableTimeStampConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableTimeStampGSConverter(fieldId, fieldName, reader);
  }

  public class NullableTimeStampGSConverter extends FieldConverter {
    private final NullableTimeStampHolder holder = new NullableTimeStampHolder();

    public NullableTimeStampGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add("null");
        return;
      }
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewTimeStampConverter(int fieldId, String fieldName, FieldReader reader) {
    return new TimeStampGSConverter(fieldId, fieldName, reader);
  }

  public class TimeStampGSConverter extends FieldConverter {
    private final TimeStampHolder holder = new TimeStampHolder();

    public TimeStampGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableBitConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableBitGSConverter(fieldId, fieldName, reader);
  }

  public class NullableBitGSConverter extends FieldConverter {
    private final NullableBitHolder holder = new NullableBitHolder();

    public NullableBitGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        rowList.add("null");
        return;
      }
      reader.read(holder);
      String booleanValue = "false";
      if (holder.value == 1) {
        booleanValue = "true";
      }
      rowList.add(booleanValue);
    }
  }
  @Override
  public FieldConverter getNewBitConverter(int fieldId, String fieldName, FieldReader reader) {
    return new BitGSConverter(fieldId, fieldName, reader);
  }

  public class BitGSConverter extends FieldConverter {
    private final BitHolder holder = new BitHolder();

    public BitGSConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      reader.read(holder);
      String booleanValue = "false";
      if (holder.value == 1) {
        booleanValue = "true";
      }
      rowList.add(booleanValue);
    }
  }
}
