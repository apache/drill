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

package org.apache.drill.exec.store.msaccess;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileDescrip;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class MSAccessBatchReader implements ManagedReader {

  private static final Logger logger = LoggerFactory.getLogger(MSAccessBatchReader.class);

  private final FileDescrip file;
  private final CustomErrorContext errorContext;
  private final Database db;
  private final Table table;
  private final Iterator<Row> rowIterator;
  private final RowSetLoader rowWriter;
  private final File tempDir;
  private final List<MSAccessColumn> columnList;
  private final MSAccessFormatConfig config;
  private final InputStream fsStream;

  public MSAccessBatchReader(FileSchemaNegotiator negotiator, File tempDir, MSAccessFormatConfig config) {
    this.tempDir = tempDir;
    this.columnList = new ArrayList<>();
    this.config = config;

    file = negotiator.file();
    errorContext = negotiator.parentErrorContext();

    try {
      fsStream = file.fileSystem().openPossiblyCompressedStream(file.split().getPath());
      db = DatabaseBuilder.open(convertInputStreamToFile(fsStream));
      Set<String> x = db.getTableNames();
    } catch (IOException e) {
      throw UserException.dataReadError(e)
          .message("Error reading MS Access file: " + e.getMessage())
          .addContext(errorContext)
          .build(logger);
    }

    // This is broken out into a separate try/catch block, so we get a better error message in the event the table
    // is not found or otherwise unreadable.
    try {
      table = db.getTable(config.getTableName());
    } catch (IOException e) {
      throw UserException.dataReadError(e)
          .message("Table " + config.getTableName() + " not found. " + e.getMessage())
          .addContext(errorContext)
          .build(logger);
    }


    // Now build the schema
    SchemaBuilder schemaBuilder = new SchemaBuilder();

    // Add schema if provided. Users probably shouldn't use this.
    TupleMetadata schema;
    if (negotiator.providedSchema() != null) {
      schema = negotiator.providedSchema();
      negotiator.tableSchema(schema, false);
    } else {
      schema = buildSchema(schemaBuilder);
      negotiator.tableSchema(schema, true);
    }

    rowIterator = table.iterator();

    ResultSetLoader loader = negotiator.build();
    rowWriter = loader.writer();
  }

  private TupleMetadata buildSchema(SchemaBuilder builder) {
    List<? extends Column> columns = table.getColumns();

    for (Column column : columns) {
      MinorType drillDataType;
      String columnName = column.getName();
      DataType dataType = column.getType();

      switch (dataType) {
        case BOOLEAN:
          builder.addNullable(columnName, MinorType.BIT);
          drillDataType = MinorType.BIT;
          break;
        case BYTE:
        case INT:
          builder.addNullable(columnName, MinorType.SMALLINT);
          drillDataType = MinorType.SMALLINT;
          break;
        case LONG:
          builder.addNullable(columnName, MinorType.INT);
          drillDataType = MinorType.INT;
          break;
        case BIG_INT:
        case COMPLEX_TYPE:
          builder.addNullable(columnName, MinorType.BIGINT);
          drillDataType = MinorType.BIGINT;
          break;
        case FLOAT:
          builder.addNullable(columnName, MinorType.FLOAT4);
          drillDataType = MinorType.FLOAT4;
          break;
        case MEMO:
        case TEXT:
        case GUID:
          builder.addNullable(columnName, MinorType.VARCHAR);
          drillDataType = MinorType.VARCHAR;
          break;
        case MONEY:
        case DOUBLE:
        case NUMERIC:
          builder.addNullable(columnName, MinorType.FLOAT8);
          drillDataType = MinorType.FLOAT8;
          break;
        case OLE:
        case BINARY:
        case UNSUPPORTED_VARLEN:
        case UNSUPPORTED_FIXEDLEN:
        case UNKNOWN_0D:
        case UNKNOWN_11:
          builder.addNullable(columnName, MinorType.VARBINARY);
          drillDataType = MinorType.VARBINARY;
          break;
        case EXT_DATE_TIME:
        case SHORT_DATE_TIME:
          builder.addNullable(columnName, MinorType.TIMESTAMP);
          drillDataType = MinorType.TIMESTAMP;
          break;
        default:
          throw UserException.dataReadError()
              .message(dataType.name() + " is not supported.")
              .build(logger);
      }
      columnList.add(new MSAccessColumn(columnName, drillDataType));
    }
    return builder.buildSchema();
  }

  @Override
  public boolean next() {
    while (!rowWriter.isFull()) {
      if (rowIterator.hasNext()) {
        processRow(rowIterator.next());
      } else {
        return false;
      }
    }
    return true;
  }

  private void processRow(Row next) {
    rowWriter.start();
    for (MSAccessColumn col : columnList) {
      switch (col.dataType) {
        case BIT:
          Boolean boolValue = next.getBoolean(col.columnName);
          rowWriter.scalar(col.columnName).setBoolean(boolValue);
          break;
        case SMALLINT:
          Short shortValue = next.getShort(col.columnName);
          rowWriter.scalar(col.columnName).setInt(shortValue);
          break;
        case BIGINT:
        case INT:
          Integer intValue = next.getInt(col.columnName);
          if (intValue != null) {
            rowWriter.scalar(col.columnName).setInt(intValue);
          }
          break;
        case FLOAT4:
          Float floatValue = next.getFloat(col.columnName);
          if (floatValue != null) {
            rowWriter.scalar(col.columnName).setFloat(floatValue);
          }
          break;
        case FLOAT8:
          Double doubleValue = next.getDouble(col.columnName);
          rowWriter.scalar(col.columnName).setDouble(doubleValue);
          break;
        case VARCHAR:
          String stringValue = next.getString(col.columnName);
          if (StringUtils.isNotEmpty(stringValue)) {
            rowWriter.scalar(col.columnName).setString(stringValue);
          }
          break;
        case TIMESTAMP:
          LocalDateTime tsValue = next.getLocalDateTime(col.columnName);
          if (tsValue != null) {
            rowWriter.scalar(col.columnName).setTimestamp(tsValue.toInstant(ZoneOffset.UTC));
          }
          break;
        case VARBINARY:
          byte[] byteValue = next.getBytes(col.columnName);
          rowWriter.scalar(col.columnName).setBytes(byteValue, byteValue.length);
          break;
      }
    }
    rowWriter.save();
  }

  @Override
  public void close() {
    AutoCloseables.closeSilently(db);
    AutoCloseables.closeSilently(fsStream);
  }

  /**
   * This function converts the Drill InputStream into a File object for the Jackcess library. This function
   * exists due to a known limitation in the Jacksess library which cannot parse MS Access directly from an input stream.
   *
   * @param stream The {@link InputStream} to be converted to a File
   * @return {@link File} The file which was converted from an {@link InputStream}
   */
  private File convertInputStreamToFile(InputStream stream) {
    String tempFileName = tempDir.getPath() + "/~" + file.filePath().getName();
    File targetFile = new File(tempFileName);

    try {
      Files.copy(stream, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      if (targetFile.exists()) {
        if (!targetFile.delete()) {
          logger.warn("{} not deleted.", targetFile.getName());
        }
      }
      throw UserException
          .dataWriteError(e)
          .message("Failed to create temp HDF5 file: %s", file.filePath())
          .addContext(e.getMessage())
          .build(logger);
    }

    AutoCloseables.closeSilently(stream);
    return targetFile;
  }

  private static class MSAccessColumn {
    private final String columnName;
    private final MinorType dataType;

    public MSAccessColumn(String columnName, MinorType dataType) {
      this.columnName = columnName;
      this.dataType = dataType;
    }
  }
}
