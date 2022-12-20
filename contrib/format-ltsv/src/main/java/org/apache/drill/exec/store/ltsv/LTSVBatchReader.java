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

package org.apache.drill.exec.store.ltsv;

import com.github.lolo.ltsv.LtsvParser;
import com.github.lolo.ltsv.LtsvParser.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileDescrip;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class LTSVBatchReader implements ManagedReader {

  private static final Logger logger = LoggerFactory.getLogger(LTSVBatchReader.class);
  private final LTSVFormatPluginConfig config;
  private final FileDescrip file;
  private final CustomErrorContext errorContext;
  private final LtsvParser ltsvParser;
  private final RowSetLoader rowWriter;
  private final FileSchemaNegotiator negotiator;
  private InputStream fsStream;
  private Iterator<Map<String, String>> rowIterator;


  public LTSVBatchReader(LTSVFormatPluginConfig config, FileSchemaNegotiator negotiator) {
    this.config = config;
    this.negotiator = negotiator;
    file = negotiator.file();
    errorContext = negotiator.parentErrorContext();
    ltsvParser = buildParser();

    openFile();

    // If there is a provided schema, import it
    if (negotiator.providedSchema() != null) {
      TupleMetadata schema = negotiator.providedSchema();
      negotiator.tableSchema(schema, false);
    }
    ResultSetLoader loader = negotiator.build();
    rowWriter = loader.writer();

  }

  private void openFile() {
    try {
      fsStream = file.fileSystem().openPossiblyCompressedStream(file.split().getPath());
    } catch (IOException e) {
      throw UserException
          .dataReadError(e)
          .message("Unable to open LTSV File %s", file.split().getPath() + " " + e.getMessage())
          .addContext(errorContext)
          .build(logger);
    }
    rowIterator = ltsvParser.parse(fsStream);
  }

  @Override
  public boolean next() {
    while (!rowWriter.isFull()) {
      if (!processNextRow()) {
        return false;
      }
    }
    return true;
  }

  private LtsvParser buildParser() {
    Builder builder = LtsvParser.builder();
    builder.trimKeys();
    builder.trimValues();
    builder.skipNullValues();

    if (config.getParseMode().contentEquals("strict")) {
      builder.strict();
    } else {
      builder.lenient();
    }

    if (StringUtils.isNotEmpty(config.getEscapeCharacter())) {
      builder.withEscapeChar(config.getEscapeCharacter().charAt(0));
    }

    if (StringUtils.isNotEmpty(config.getKvDelimiter())) {
      builder.withKvDelimiter(config.getKvDelimiter().charAt(0));
    }

    if (StringUtils.isNotEmpty(config.getEntryDelimiter())) {
      builder.withEntryDelimiter(config.getEntryDelimiter().charAt(0));
    }

    if (StringUtils.isNotEmpty(config.getLineEnding())) {
      builder.withLineEnding(config.getLineEnding().charAt(0));
    }

    if (StringUtils.isNotEmpty(config.getQuoteChar())) {
      builder.withQuoteChar(config.getQuoteChar().charAt(0));
    }

    return builder.build();
  }

  private boolean processNextRow() {
    if (!rowIterator.hasNext()) {
      return false;
    }
    // Start the row
    String key;
    String value;
    int columnIndex;
    ScalarWriter columnWriter;
    Map<String, String> row = rowIterator.next();

    // Skip empty lines
    if (row.isEmpty()) {
      return true;
    }
    rowWriter.start();
    for (Map.Entry<String,String> field: row.entrySet()) {
      key = field.getKey();
      value = field.getValue();
      columnIndex = getColumnIndex(key);
      columnWriter = getColumnWriter(key);


      if (negotiator.providedSchema() != null) {
        // Check the type. LTSV will only read other data types if a schema is provided.
        ColumnMetadata columnMetadata = rowWriter.tupleSchema().metadata(columnIndex);
        MinorType dataType = columnMetadata.type();
        LocalTime localTime;
        LocalDate localDate;

        switch (dataType) {
          case BIT:
            columnWriter.setBoolean(Boolean.parseBoolean(value));
            break;
          case INT:
          case SMALLINT:
          case TINYINT:
            columnWriter.setInt(Integer.parseInt(value));
            break;
          case BIGINT:
            columnWriter.setLong(Long.parseLong(value));
            break;
          case FLOAT8:
          case FLOAT4:
            columnWriter.setDouble(Double.parseDouble(value));
            break;
          case TIME:
            columnMetadata = rowWriter.tupleSchema().metadata(key);
            String dateFormat = columnMetadata.property("drill.format");

            if (Strings.isNullOrEmpty(dateFormat)) {
              localTime = LocalTime.parse(value);
            } else {
              localTime = LocalTime.parse(value, DateTimeFormatter.ofPattern(dateFormat));
            }
            columnWriter.setTime(localTime);
            break;
          case DATE:
            dateFormat = columnMetadata.property("drill.format");

            if (Strings.isNullOrEmpty(dateFormat)) {
              localDate = LocalDate.parse(value);
            } else {
              localDate = LocalDate.parse(value, DateTimeFormatter.ofPattern(dateFormat));
            }
            columnWriter.setDate(localDate);
            break;
          case TIMESTAMP:
            dateFormat = columnMetadata.property("drill.format");
            Instant timestamp;
            if (Strings.isNullOrEmpty(dateFormat)) {
              timestamp = Instant.parse(value);
            } else {
              try {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
                Date parsedDate = simpleDateFormat.parse(value);
                timestamp = Instant.ofEpochMilli(parsedDate.getTime());
              } catch (ParseException e) {
                throw UserException.parseError(e)
                    .message("Cannot parse " + value + " as a timestamp. You can specify a format string in the provided schema to correct this.")
                    .addContext(errorContext)
                    .build(logger);
              }
            }
            columnWriter.setTimestamp(timestamp);
            break;
          default:
            columnWriter.setString(value);
        }
      } else {
        columnWriter.setString(value);
      }
    }
    // Finish the row
    rowWriter.save();
    return true;
  }

  @Override
  public void close() {
    logger.debug("Closing input stream for LTSV reader.");
    AutoCloseables.closeSilently(fsStream);
  }

  private int getColumnIndex(String fieldName) {
    // Find the TupleWriter object
    int index = rowWriter.tupleSchema().index(fieldName);

    // Unknown columns are always strings.
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(fieldName, TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    return index;
  }

  private ScalarWriter getColumnWriter(String fieldName){
    // Find the TupleWriter object
    int index = getColumnIndex(fieldName);
    return rowWriter.scalar(index);
  }
}
