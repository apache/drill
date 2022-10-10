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

package org.apache.drill.exec.store.jdbc;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.AbstractRecordWriter;
import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.store.jdbc.utils.CreateTableStmtBuilder;
import org.apache.drill.exec.store.jdbc.utils.InsertStatementBuilder;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JdbcRecordWriter extends AbstractRecordWriter {

  private static final Logger logger = LoggerFactory.getLogger(JdbcRecordWriter.class);

  private final List<String> tableIdentifier;
  private final Connection connection;
  protected final SqlDialect dialect;
  private final InsertStatementBuilder insertStatementBuilder;
  private final JdbcWriter config;
  private int recordCount;

  public JdbcRecordWriter(UserCredentials userCredentials, List<String> tableIdentifier, JdbcWriter config) {
    this.tableIdentifier = tableIdentifier;
    this.dialect = config.getPlugin().getDialect(userCredentials);
    this.config = config;
    this.recordCount = 0;
    this.insertStatementBuilder = getInsertStatementBuilder(tableIdentifier);

    try {
      this.connection = config.getPlugin().getDataSource(userCredentials).get().getConnection();
    } catch (SQLException e) {
      throw UserException.connectionError()
        .message("Unable to open JDBC connection for writing.")
        .addContext(e.getSQLState())
        .build(logger);
    }
  }

  protected InsertStatementBuilder getInsertStatementBuilder(List<String> tableIdentifier) {
    return new InsertStatementBuilder(tableIdentifier, dialect);
  }

  @Override
  public void init(Map<String, String> writerOptions) {
    // Nothing to see here...
  }

  @Override
  public void updateSchema(VectorAccessible batch) {
    BatchSchema schema = batch.getSchema();
    CreateTableStmtBuilder queryBuilder = new CreateTableStmtBuilder(tableIdentifier, dialect);
    for (MaterializedField field : schema) {
      logger.debug("Adding column {} of type {}.", field.getName(), field.getType().getMinorType());

      if (field.getType().getMode() == DataMode.REPEATED) {
        throw UserException.dataWriteError()
          .message("Drill does not yet support writing arrays to JDBC. " + field.getName() + " is an array.")
          .build(logger);
      }

      queryBuilder.addColumn(field);
    }

    String sql = queryBuilder.build();
    logger.debug("Final query: {}", sql);

    // Execute the query to build the schema
    try (Statement statement = connection.createStatement()) {
      logger.debug("Executing CREATE query: {}", sql);
      statement.execute(sql);
    } catch (SQLException e) {
      throw UserException.dataReadError(e)
        .message("The JDBC storage plugin failed while trying to create the schema. ")
        .addContext("Sql", sql)
        .build(logger);
    }
  }

  @Override
  public void startRecord() {
    insertStatementBuilder.resetRow();

    logger.debug("Start record");
  }

  @Override
  public void endRecord() throws IOException {
    logger.debug("Ending record");
    insertStatementBuilder.endRecord();

    recordCount++;

    if (recordCount >= config.getPlugin().getConfig().getWriterBatchSize()) {
      executeInsert(insertStatementBuilder.buildInsertQuery());

      // Reset the batch
      recordCount = 0;
    }

    insertStatementBuilder.resetRow();
  }

  @Override
  public void abort() {
    logger.debug("Abort insert.");
  }

  @Override
  public void cleanup() throws IOException {
    logger.debug("Cleanup record");
    if (recordCount != 0) {
      executeInsert(insertStatementBuilder.buildInsertQuery());
    }
    AutoCloseables.closeSilently(connection);
  }

  private void executeInsert(String insertQuery) throws IOException {
    try (Statement stmt = connection.createStatement()) {
      logger.debug("Executing insert query: {}", insertQuery);
      stmt.execute(insertQuery);
      logger.debug("Query complete");
      // Close connection
      AutoCloseables.closeSilently(stmt);
    } catch (SQLException e) {
      logger.error("Error: {} {} {}", e.getMessage(), e.getSQLState(), e.getErrorCode());
      AutoCloseables.closeSilently(connection);
      throw new IOException(e.getMessage() + " " + e.getSQLState() + "\n" + insertQuery);
    }
  }

  public class NullableJdbcConverter extends FieldConverter {
    private final FieldConverter delegate;

    public NullableJdbcConverter(int fieldId, String fieldName, FieldReader reader, FieldConverter delegate) {
      super(fieldId, fieldName, reader);
      this.delegate = delegate;
    }

    @Override
    public void writeField() throws IOException {
      if (reader.isSet()) {
        delegate.writeField();
      } else {
        insertStatementBuilder.addRowValue(SqlLiteral.createNull(SqlParserPos.ZERO));
      }
    }
  }

  public class ExactNumericJdbcConverter extends FieldConverter {

    public ExactNumericJdbcConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void writeField() {
      insertStatementBuilder.addRowValue(
        SqlLiteral.createExactNumeric(String.valueOf(reader.readObject()),
          SqlParserPos.ZERO));
    }
  }

  public class ApproxNumericJdbcConverter extends FieldConverter {

    public ApproxNumericJdbcConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void writeField() {
      insertStatementBuilder.addRowValue(
        SqlLiteral.createApproxNumeric(String.valueOf(reader.readObject()),
          SqlParserPos.ZERO));
    }
  }

  @Override
  public FieldConverter getNewNullableIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableJdbcConverter(fieldId, fieldName, reader,
      new ExactNumericJdbcConverter(fieldId, fieldName, reader));
  }

  @Override
  public FieldConverter getNewIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ExactNumericJdbcConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableJdbcConverter(fieldId, fieldName, reader,
      new ExactNumericJdbcConverter(fieldId, fieldName, reader));
  }

  @Override
  public FieldConverter getNewBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ExactNumericJdbcConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableSmallIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableJdbcConverter(fieldId, fieldName, reader,
      new ExactNumericJdbcConverter(fieldId, fieldName, reader));
  }

  @Override
  public FieldConverter getNewSmallIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ExactNumericJdbcConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableTinyIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableJdbcConverter(fieldId, fieldName, reader,
      new ExactNumericJdbcConverter(fieldId, fieldName, reader));
  }

  @Override
  public FieldConverter getNewTinyIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ExactNumericJdbcConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableFloat4Converter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableJdbcConverter(fieldId, fieldName, reader,
      new ApproxNumericJdbcConverter(fieldId, fieldName, reader));
  }

  @Override
  public FieldConverter getNewFloat4Converter(int fieldId, String fieldName, FieldReader reader) {
    return new ApproxNumericJdbcConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableJdbcConverter(fieldId, fieldName, reader,
      new ApproxNumericJdbcConverter(fieldId, fieldName, reader));
  }

  @Override
  public FieldConverter getNewFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
    return new ApproxNumericJdbcConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableVarDecimalConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableJdbcConverter(fieldId, fieldName, reader,
      new ExactNumericJdbcConverter(fieldId, fieldName, reader));
  }

  @Override
  public FieldConverter getNewVarDecimalConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ExactNumericJdbcConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableJdbcConverter(fieldId, fieldName, reader,
      new VarCharJDBCConverter(fieldId, fieldName, reader));
  }

  @Override
  public FieldConverter getNewVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
    return new VarCharJDBCConverter(fieldId, fieldName, reader);
  }

  public class VarCharJDBCConverter extends FieldConverter {

    public VarCharJDBCConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      byte[] bytes = reader.readText().copyBytes();
      insertStatementBuilder.addRowValue(
        SqlLiteral.createCharString(new String(bytes),
          SqlParserPos.ZERO));
    }
  }

  @Override
  public FieldConverter getNewNullableDateConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableJdbcConverter(fieldId, fieldName, reader,
      new DateJDBCConverter(fieldId, fieldName, reader));
  }

  @Override
  public FieldConverter getNewDateConverter(int fieldId, String fieldName, FieldReader reader) {
    return new DateJDBCConverter(fieldId, fieldName, reader);
  }

  public class DateJDBCConverter extends FieldConverter {

    public DateJDBCConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      insertStatementBuilder.addRowValue(
        SqlLiteral.createDate(DateString.fromDaysSinceEpoch((int) reader.readLocalDate().toEpochDay()),
          SqlParserPos.ZERO));
    }
  }

  @Override
  public FieldConverter getNewNullableTimeConverter(int fieldId, String fieldName, FieldReader reader) {
      return new NullableJdbcConverter(fieldId, fieldName, reader,
        new TimeJDBCConverter(fieldId, fieldName, reader));
    }

  @Override
  public FieldConverter getNewTimeConverter(int fieldId, String fieldName, FieldReader reader) {
    return new TimeJDBCConverter(fieldId, fieldName, reader);
  }

  public class TimeJDBCConverter extends FieldConverter {

    public TimeJDBCConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      insertStatementBuilder.addRowValue(
        SqlLiteral.createTime(TimeString.fromMillisOfDay(
            (int) (reader.readLocalTime().toNanoOfDay() / TimeUnit.MILLISECONDS.toNanos(1))),
          Types.DEFAULT_TIMESTAMP_PRECISION,
          SqlParserPos.ZERO));
    }
  }

  @Override
  public FieldConverter getNewNullableTimeStampConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableJdbcConverter(fieldId, fieldName, reader,
      new TimeStampJDBCConverter(fieldId, fieldName, reader));
  }

  @Override
  public FieldConverter getNewTimeStampConverter(int fieldId, String fieldName, FieldReader reader) {
    return new TimeStampJDBCConverter(fieldId, fieldName, reader);
  }

  public class TimeStampJDBCConverter extends FieldConverter {

    public TimeStampJDBCConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      insertStatementBuilder.addRowValue(
        SqlLiteral.createTimestamp(
          TimestampString.fromMillisSinceEpoch(reader.readLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli()),
          Types.DEFAULT_TIMESTAMP_PRECISION,
          SqlParserPos.ZERO));
    }
  }

  @Override
  public FieldConverter getNewNullableBitConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableJdbcConverter(fieldId, fieldName, reader,
      new BitJDBCConverter(fieldId, fieldName, reader));
  }

  @Override
  public FieldConverter getNewBitConverter(int fieldId, String fieldName, FieldReader reader) {
    return new BitJDBCConverter(fieldId, fieldName, reader);
  }

  public class BitJDBCConverter extends FieldConverter {

    public BitJDBCConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      insertStatementBuilder.addRowValue(SqlLiteral.createBoolean(reader.readBoolean(), SqlParserPos.ZERO));
    }
  }
}
