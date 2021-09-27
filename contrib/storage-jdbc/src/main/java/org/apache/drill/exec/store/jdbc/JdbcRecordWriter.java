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
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableDateHolder;
import org.apache.drill.exec.expr.holders.NullableFloat4Holder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableSmallIntHolder;
import org.apache.drill.exec.expr.holders.NullableTimeHolder;
import org.apache.drill.exec.expr.holders.NullableTimeStampHolder;
import org.apache.drill.exec.expr.holders.NullableTinyIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.SmallIntHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.TinyIntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.AbstractRecordWriter;
import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.store.jdbc.utils.JdbcQueryBuilder;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JdbcRecordWriter extends AbstractRecordWriter {

  private static final Logger logger = LoggerFactory.getLogger(JdbcRecordWriter.class);
  public static final ImmutableMap<MinorType, Integer> JDBC_TYPE_MAPPINGS;

  private static final String INSERT_QUERY_TEMPLATE = "INSERT INTO %s VALUES\n%s";
  private final String tableName;
  private final Connection connection;
  private final JdbcWriter config;
  private final SqlDialect dialect;
  private StringBuilder rowString;
  private final List<Object> rowList;
  private final List<String> insertRows;


  // TODO Wrap inserts in transaction?
  // TODO Config option for CREATE or CREATE IF NOT EXISTS
  // TODO Add config option for max packet size

  /*
   * This map maps JDBC data types to their Drill equivalents.  The basic strategy is that if there
   * is a Drill equivalent, then do the mapping as expected.  All flavors of INT (SMALLINT, TINYINT etc)
   * are mapped to INT in Drill, with the exception of BIGINT.
   *
   * All flavors of character fields are mapped to VARCHAR in Drill. All versions of binary fields are
   * mapped to VARBINARY.
   *
   */
  static {
    JDBC_TYPE_MAPPINGS = ImmutableMap.<MinorType, Integer>builder()
      .put(MinorType.FLOAT8, java.sql.Types.DOUBLE)
      .put(MinorType.FLOAT4, java.sql.Types.FLOAT)
      .put(MinorType.TINYINT, java.sql.Types.TINYINT)
      .put(MinorType.SMALLINT, java.sql.Types.SMALLINT)
      .put(MinorType.INT, java.sql.Types.INTEGER)
      .put(MinorType.BIGINT, java.sql.Types.BIGINT)
      .put(MinorType.VARCHAR, java.sql.Types.VARCHAR)
      .put(MinorType.VARBINARY, java.sql.Types.VARBINARY)
      .put(MinorType.VARDECIMAL, java.sql.Types.DECIMAL)
      .put(MinorType.DATE, java.sql.Types.DATE)
      .put(MinorType.TIME, java.sql.Types.TIME)
      .put(MinorType.TIMESTAMP, java.sql.Types.TIMESTAMP)
      .put(MinorType.BIT, java.sql.Types.BOOLEAN)
      .build();
  }

  public JdbcRecordWriter(DataSource source, OperatorContext context, String name, JdbcWriter config) {
    this.tableName = name;
    this.config = config;
    this.rowList = new ArrayList<>();
    this.insertRows = new ArrayList<>();
    this.dialect = config.getPlugin().getDialect();

    try {
      this.connection = source.getConnection();
    } catch (SQLException e) {
      throw UserException.connectionError()
        .message("Unable to open JDBC connection for writing.")
        .addContext(e.getSQLState())
        .build(logger);
    }
  }

  @Override
  public void init(Map<String, String> writerOptions) {

  }

  @Override
  public void updateSchema(VectorAccessible batch) throws IOException {
    BatchSchema schema = batch.getSchema();
    String columnName;
    MinorType type;
    String sql;
    Statement statement;
    boolean nullable = false;
    JdbcQueryBuilder queryBuilder = new JdbcQueryBuilder(tableName, dialect);

    for (Iterator<MaterializedField> it = schema.iterator(); it.hasNext(); ) {
      MaterializedField field = it.next();
      columnName = field.getName();
      type = field.getType().getMinorType();
      logger.debug("Adding column {} of type {}.", columnName, type);

      if (field.getType().getMode() == DataMode.OPTIONAL) {
        nullable = true;
      }

      int precision = field.getPrecision();
      int scale = field.getScale();

      queryBuilder.addColumn(columnName, field.getType().getMinorType(), nullable, precision, scale);
    }
    sql = queryBuilder.getCreateTableQuery();
    //sql = JdbcQueryBuilder.convertToDestinationDialect(sql, dialect);
    logger.debug("Final query: {}", sql);

    // Execute the query to build the schema
    try {
      statement = connection.createStatement();
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
  public void startRecord() throws IOException {
    rowString = new StringBuilder();
    rowString.append("(");
    logger.debug("Start record");
  }

  @Override
  public void endRecord() throws IOException {
    // Add values to rowString
    for (int i = 0; i < this.rowList.size(); i++) {
      if (i > 0) {
        rowString.append(", ");
      }
      // TODO Check if the holder is a VarChar
      rowString.append(this.rowList.get(i));
    }

    rowString.append(")");
    logger.debug("End record: {}", rowString);
    this.rowList.clear();
    insertRows.add(rowString.toString());
  }

  @Override
  public void abort() throws IOException {
    logger.debug("Abort record");
  }

  @Override
  public void cleanup() throws IOException {
    logger.debug("Cleanup record");
    // Execute query
    String insertQuery = buildInsertQuery();

    try {
      logger.debug("Executing insert query: {}", insertQuery);
      Statement stmt = connection.createStatement();
      stmt.execute(insertQuery);
      logger.debug("Query complete");

      // Close connection
      AutoCloseables.closeSilently(stmt, connection);

    } catch (SQLException e) {
      throw new IOException();
    }
  }

  private String buildInsertQuery() {
    StringBuilder values = new StringBuilder();
    for (int i = 0; i < insertRows.size(); i++) {
      if (i > 0) {
        values.append(",\n");
      }
      values.append(insertRows.get(i));
    }

    String sql = String.format(INSERT_QUERY_TEMPLATE, tableName, values);
    //return JdbcQueryBuilder.convertToDestinationDialect(sql, dialect);
    return sql;
  }


  @Override
  public FieldConverter getNewNullableIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableIntJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class NullableIntJDBCConverter extends FieldConverter {
    private final NullableIntHolder holder = new NullableIntHolder();
    private final List<Object> rowList;

    public NullableIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new IntJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class IntJDBCConverter extends FieldConverter {
    private final IntHolder holder = new IntHolder();

    private final List<Object> rowList;

    public IntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableBigIntJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class NullableBigIntJDBCConverter extends FieldConverter {
    private final NullableBigIntHolder holder = new NullableBigIntHolder();

    private final List<Object> rowList;

    public NullableBigIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new BigIntJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class BigIntJDBCConverter extends FieldConverter {
    private final BigIntHolder holder = new BigIntHolder();

    private final List<Object> rowList;

    public BigIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableSmallIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableSmallIntJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class NullableSmallIntJDBCConverter extends FieldConverter {
    private final NullableSmallIntHolder holder = new NullableSmallIntHolder();

    private final List<Object> rowList;

    public NullableSmallIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewSmallIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new SmallIntJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class SmallIntJDBCConverter extends FieldConverter {
    private final SmallIntHolder holder = new SmallIntHolder();

    private final List<Object> rowList;

    public SmallIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableTinyIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableTinyIntJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class NullableTinyIntJDBCConverter extends FieldConverter {
    private final NullableTinyIntHolder holder = new NullableTinyIntHolder();

    private final List<Object> rowList;

    public NullableTinyIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewTinyIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new TinyIntJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class TinyIntJDBCConverter extends FieldConverter {
    private final TinyIntHolder holder = new TinyIntHolder();

    private final List<Object> rowList;

    public TinyIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableFloat4Converter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableFloat4JDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class NullableFloat4JDBCConverter extends FieldConverter {
    private final NullableFloat4Holder holder = new NullableFloat4Holder();

    private final List<Object> rowList;

    public NullableFloat4JDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewFloat4Converter(int fieldId, String fieldName, FieldReader reader) {
    return new Float4JDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class Float4JDBCConverter extends FieldConverter {
    private final Float4Holder holder = new Float4Holder();

    private final List<Object> rowList;

    public Float4JDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableFloat8JDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class NullableFloat8JDBCConverter extends FieldConverter {
    private final NullableFloat4Holder holder = new NullableFloat4Holder();

    private final List<Object> rowList;

    public NullableFloat8JDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
    return new Float8JDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class Float8JDBCConverter extends FieldConverter {
    private final Float8Holder holder = new Float8Holder();

    private final List<Object> rowList;

    public Float8JDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableVarCharJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class NullableVarCharJDBCConverter extends FieldConverter {
    private final NullableVarCharHolder holder = new NullableVarCharHolder();

    private final List<Object> rowList;

    public NullableVarCharJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      reader.read(holder);
      if (reader.isSet()) {
        byte[] bytes = new byte[holder.end - holder.start];
        holder.buffer.getBytes(holder.start, bytes);
        this.rowList.add(holder);
      }
    }
  }

  @Override
  public FieldConverter getNewVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
    return new VarCharJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class VarCharJDBCConverter extends FieldConverter {
    private final VarCharHolder holder = new VarCharHolder();

    private final List<Object> rowList;

    public VarCharJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      reader.read(holder);
      if (reader.isSet()) {
        byte[] bytes = new byte[holder.end - holder.start];
        holder.buffer.getBytes(holder.start, bytes);
        this.rowList.add(holder);
      }
    }
  }

  @Override
  public FieldConverter getNewNullableDateConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableDateJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class NullableDateJDBCConverter extends FieldConverter {
    private final NullableDateHolder holder = new NullableDateHolder();

    private final List<Object> rowList;

    public NullableDateJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewDateConverter(int fieldId, String fieldName, FieldReader reader) {
    return new DateJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class DateJDBCConverter extends FieldConverter {
    private final DateHolder holder = new DateHolder();

    private final List<Object> rowList;

    public DateJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableTimeConverter(int fieldId, String fieldName, FieldReader reader) {
      return new NullableTimeJDBCConverter(fieldId, fieldName, reader, this.rowList);
    }

    public static class NullableTimeJDBCConverter extends FieldConverter {
    private final NullableTimeHolder holder = new NullableTimeHolder();

    private final List<Object> rowList;

    public NullableTimeJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewTimeConverter(int fieldId, String fieldName, FieldReader reader) {
    return new TimeJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class TimeJDBCConverter extends FieldConverter {
    private final TimeHolder holder = new TimeHolder();

    private final List<Object> rowList;

    public TimeJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableTimeStampConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableTimeStampJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class NullableTimeStampJDBCConverter extends FieldConverter {
    private final NullableTimeStampHolder holder = new NullableTimeStampHolder();

    private final List<Object> rowList;

    public NullableTimeStampJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }
  @Override
  public FieldConverter getNewTimeStampConverter(int fieldId, String fieldName, FieldReader reader) {
    return new TimeStampJDBCConverter(fieldId, fieldName, reader, this.rowList);
  }

  public static class TimeStampJDBCConverter extends FieldConverter {
    private final TimeStampHolder holder = new TimeStampHolder();

    private final List<Object> rowList;

    public TimeStampJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<Object> rowList) {
      super(fieldID, fieldName, reader);
      this.rowList = rowList;
    }

    @Override
    public void writeField() {
      if (!reader.isSet()) {
        this.rowList.add("null");
      }
      reader.read(holder);
      this.rowList.add(holder.value);
    }
  }
}
