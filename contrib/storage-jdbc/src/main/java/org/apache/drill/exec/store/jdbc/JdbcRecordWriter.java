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
import org.apache.commons.text.StringEscapeUtils;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
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
import org.apache.drill.exec.store.jdbc.utils.JdbcDDLQueryUtils;
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
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JdbcRecordWriter extends AbstractRecordWriter {

  private static final Logger logger = LoggerFactory.getLogger(JdbcRecordWriter.class);
  public static final ImmutableMap<MinorType, Integer> JDBC_TYPE_MAPPINGS;

  private static final String INSERT_QUERY_TEMPLATE = "INSERT INTO %s VALUES\n%s";
  private final String tableName;
  private final Connection connection;
  private final JdbcWriter config;
  private final SqlDialect dialect;
  private final List<Object> rowList;
  private final List<String> insertRows;
  private final List<JdbcWriterField> fields;
  private StringBuilder rowString;


  // TODO Wrap inserts in transaction?
  // TODO Config option for CREATE or CREATE IF NOT EXISTS
  // TODO Add config option for max packet size ?

  /*
   * This map maps JDBC data types to their Drill equivalents.  The basic strategy is that if there
   * is a Drill equivalent, then do the mapping as expected.
   *
   * All flavors of character fields are mapped to VARCHAR in Drill. All versions of binary fields are
   * mapped to VARBINARY.
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
    rowList = new ArrayList<>();
    insertRows = new ArrayList<>();
    this.dialect = config.getPlugin().getDialect();

    this.fields = new ArrayList<>();

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
  public void updateSchema(VectorAccessible batch) {
    BatchSchema schema = batch.getSchema();
    String columnName;
    MinorType type;
    String sql;
    Statement statement;
    boolean nullable = false;
    JdbcQueryBuilder queryBuilder = new JdbcQueryBuilder(tableName, dialect);

    for (MaterializedField field : schema) {
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
    sql = JdbcDDLQueryUtils.cleanDDLQuery(sql, dialect);
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
    rowList.clear();
    rowString.append("(");
    logger.debug("Start record");
  }

  @Override
  public void endRecord() throws IOException {
    logger.debug("Ending record");

    // Add values to rowString
    for (int i = 0; i < rowList.size(); i++) {
      if (i > 0) {
        rowString.append(", ");
      }

      // Add null value to rowstring
      if (rowList.get(i) instanceof String && ((String) rowList.get(i)).equalsIgnoreCase("null")) {
        rowString.append("null");
        continue;
      }

      JdbcWriterField currentField = fields.get(i);
      if (currentField.getDataType() == MinorType.VARCHAR) {
        String value = null;
        // Get the string value
        if (currentField.getMode() == DataMode.REQUIRED) {
          VarCharHolder varCharHolder = (VarCharHolder) rowList.get(i);
          value = StringFunctionHelpers.getStringFromVarCharHolder(varCharHolder);
          // Escape any naughty characters
          value = StringEscapeUtils.escapeJava(value);
        } else {
          try {
            NullableVarCharHolder nullableVarCharHolder = (NullableVarCharHolder) rowList.get(i);
            value = StringFunctionHelpers.getStringFromVarCharHolder(nullableVarCharHolder);
          } catch (ClassCastException e) {
            logger.debug("Uh oh... {}",  rowList.get(i));
          }
        }

        // Add to value string
        rowString.append("'").append(value).append("'");
      } else if (currentField.getDataType() == MinorType.DATE) {
        String dateString = formatDateForInsertQuery((Long) rowList.get(i));
        rowString.append("'").append(dateString).append("'");
      } else if (currentField.getDataType() == MinorType.TIME) {
        String timeString = formatTimeForInsertQuery((Integer) rowList.get(i));
        rowString.append("'").append(timeString).append("'");
      } else if (currentField.getDataType() == MinorType.TIMESTAMP) {
        String timeString = formatTimeStampForInsertQuery((Long) rowList.get(i));
        rowString.append("'").append(timeString).append("'");
      } else {
        rowString.append(rowList.get(i));
      }
    }

    rowString.append(")");
    rowList.clear();
    insertRows.add(rowString.toString());
    logger.debug("End record: {}", rowString.toString());
  }

  @Override
  public void abort() throws IOException {
    logger.debug("Abort insert.");
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
      logger.error("Error: {} ", e.getMessage());
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
    return JdbcDDLQueryUtils.cleanDDLQuery(sql, dialect);
  }

  private String formatDateForInsertQuery(Long dateVal) {
    Date date=new Date(dateVal);
    SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
    return df2.format(date);
  }

  private String formatTimeForInsertQuery(Integer millis) {
    return String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millis),
      TimeUnit.MILLISECONDS.toMinutes(millis) % TimeUnit.HOURS.toMinutes(1),
      TimeUnit.MILLISECONDS.toSeconds(millis) % TimeUnit.MINUTES.toSeconds(1));
  }

  private String formatTimeStampForInsertQuery(Long time) {
    Date date = new Date(time);
    Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    return format.format(date);
  }


  @Override
  public FieldConverter getNewNullableIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableIntJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class NullableIntJDBCConverter extends FieldConverter {
    private final NullableIntHolder holder = new NullableIntHolder();

    public NullableIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.INT, DataMode.OPTIONAL));
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
  public FieldConverter getNewIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new IntJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class IntJDBCConverter extends FieldConverter {
    private final IntHolder holder = new IntHolder();

    public IntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.INT, DataMode.REQUIRED));
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableBigIntJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class NullableBigIntJDBCConverter extends FieldConverter {
    private final NullableBigIntHolder holder = new NullableBigIntHolder();

    public NullableBigIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.BIGINT, DataMode.OPTIONAL));
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
  public FieldConverter getNewBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new BigIntJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class BigIntJDBCConverter extends FieldConverter {
    private final BigIntHolder holder = new BigIntHolder();

    public BigIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.BIGINT, DataMode.REQUIRED));
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
  public FieldConverter getNewNullableSmallIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableSmallIntJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class NullableSmallIntJDBCConverter extends FieldConverter {
    private final NullableSmallIntHolder holder = new NullableSmallIntHolder();

    public NullableSmallIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.SMALLINT, DataMode.OPTIONAL));
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
  public FieldConverter getNewSmallIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new SmallIntJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class SmallIntJDBCConverter extends FieldConverter {
    private final SmallIntHolder holder = new SmallIntHolder();

    public SmallIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.SMALLINT, DataMode.REQUIRED));
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableTinyIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableTinyIntJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class NullableTinyIntJDBCConverter extends FieldConverter {
    private final NullableTinyIntHolder holder = new NullableTinyIntHolder();

    public NullableTinyIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.TINYINT, DataMode.OPTIONAL));
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
  public FieldConverter getNewTinyIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new TinyIntJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class TinyIntJDBCConverter extends FieldConverter {
    private final TinyIntHolder holder = new TinyIntHolder();

    public TinyIntJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.TINYINT, DataMode.REQUIRED));
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableFloat4Converter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableFloat4JDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class NullableFloat4JDBCConverter extends FieldConverter {
    private final NullableFloat4Holder holder = new NullableFloat4Holder();

    public NullableFloat4JDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.FLOAT4, DataMode.OPTIONAL));
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
  public FieldConverter getNewFloat4Converter(int fieldId, String fieldName, FieldReader reader) {
    return new Float4JDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class Float4JDBCConverter extends FieldConverter {
    private final Float4Holder holder = new Float4Holder();

    public Float4JDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.FLOAT4, DataMode.REQUIRED));
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableFloat8JDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class NullableFloat8JDBCConverter extends FieldConverter {
    private final NullableFloat8Holder holder = new NullableFloat8Holder();

    public NullableFloat8JDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.FLOAT8, DataMode.OPTIONAL));
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
  public FieldConverter getNewFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
    return new Float8JDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class Float8JDBCConverter extends FieldConverter {
    private final Float8Holder holder = new Float8Holder();

    public Float8JDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.FLOAT8, DataMode.REQUIRED));
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableVarCharJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class NullableVarCharJDBCConverter extends FieldConverter {
    private final NullableVarCharHolder holder = new NullableVarCharHolder();

    public NullableVarCharJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.VARCHAR, DataMode.OPTIONAL));
    }

    @Override
    public void writeField() {
      reader.read(holder);
      if (reader.isSet()) {
        byte[] bytes = new byte[holder.end - holder.start];
        holder.buffer.getBytes(holder.start, bytes);
      }
      rowList.add(holder);
    }
  }

  @Override
  public FieldConverter getNewVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
    return new VarCharJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class VarCharJDBCConverter extends FieldConverter {
    private final VarCharHolder holder = new VarCharHolder();

    public VarCharJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.VARCHAR, DataMode.REQUIRED));
    }

    @Override
    public void writeField() {
      reader.read(holder);
      if (reader.isSet()) {
        byte[] bytes = new byte[holder.end - holder.start];
        holder.buffer.getBytes(holder.start, bytes);
        rowList.add(holder);
      }
    }
  }

  @Override
  public FieldConverter getNewNullableDateConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableDateJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class NullableDateJDBCConverter extends FieldConverter {
    private final NullableDateHolder holder = new NullableDateHolder();

    public NullableDateJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.DATE, DataMode.OPTIONAL));
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
    return new DateJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class DateJDBCConverter extends FieldConverter {
    private final DateHolder holder = new DateHolder();

    public DateJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.DATE, DataMode.REQUIRED));
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableTimeConverter(int fieldId, String fieldName, FieldReader reader) {
      return new NullableTimeJDBCConverter(fieldId, fieldName, reader, fields);
    }

    public class NullableTimeJDBCConverter extends FieldConverter {
    private final NullableTimeHolder holder = new NullableTimeHolder();

    public NullableTimeJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.TIME, DataMode.OPTIONAL));
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
    return new TimeJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class TimeJDBCConverter extends FieldConverter {
    private final TimeHolder holder = new TimeHolder();

    public TimeJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.TIME, DataMode.REQUIRED));
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableTimeStampConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableTimeStampJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class NullableTimeStampJDBCConverter extends FieldConverter {
    private final NullableTimeStampHolder holder = new NullableTimeStampHolder();

    public NullableTimeStampJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.TIMESTAMP, DataMode.OPTIONAL));
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
    return new TimeStampJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class TimeStampJDBCConverter extends FieldConverter {
    private final TimeStampHolder holder = new TimeStampHolder();

    public TimeStampJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.TIMESTAMP, DataMode.REQUIRED));
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }

  @Override
  public FieldConverter getNewNullableBitConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableBitJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class NullableBitJDBCConverter extends FieldConverter {
    private final NullableBitHolder holder = new NullableBitHolder();

    public NullableBitJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.BIT, DataMode.OPTIONAL));
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
  public FieldConverter getNewBitConverter(int fieldId, String fieldName, FieldReader reader) {
    return new BitJDBCConverter(fieldId, fieldName, reader, fields);
  }

  public class BitJDBCConverter extends FieldConverter {
    private final BitHolder holder = new BitHolder();

    public BitJDBCConverter(int fieldID, String fieldName, FieldReader reader, List<JdbcWriterField> fields) {
      super(fieldID, fieldName, reader);
      fields.add(new JdbcWriterField(fieldName, MinorType.BIT, DataMode.REQUIRED));
    }

    @Override
    public void writeField() {
      reader.read(holder);
      rowList.add(holder.value);
    }
  }
}
