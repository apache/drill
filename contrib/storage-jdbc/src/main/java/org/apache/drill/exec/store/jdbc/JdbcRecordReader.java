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

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import javax.sql.DataSource;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableTimeVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.NullableVarDecimalVector;
import org.apache.drill.exec.vector.ValueVector;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(JdbcRecordReader.class);

  private static final ImmutableMap<Integer, MinorType> JDBC_TYPE_MAPPINGS;
  private final DataSource source;
  private ResultSet resultSet;
  private final String storagePluginName;
  private Connection connection;
  private PreparedStatement statement;
  private final String sql;
  private ImmutableList<ValueVector> vectors;
  private ImmutableList<Copier<?>> copiers;
  private final List<SchemaPath> columns;

  public JdbcRecordReader(DataSource source, String sql, String storagePluginName, List<SchemaPath> columns) {
    this.source = source;
    this.sql = sql;
    this.storagePluginName = storagePluginName;
    this.columns = columns;
  }

  static {
    JDBC_TYPE_MAPPINGS = ImmutableMap.<Integer, MinorType>builder()
        .put(java.sql.Types.DOUBLE, MinorType.FLOAT8)
        .put(java.sql.Types.FLOAT, MinorType.FLOAT4)
        .put(java.sql.Types.TINYINT, MinorType.INT)
        .put(java.sql.Types.SMALLINT, MinorType.INT)
        .put(java.sql.Types.INTEGER, MinorType.INT)
        .put(java.sql.Types.BIGINT, MinorType.BIGINT)

        .put(java.sql.Types.CHAR, MinorType.VARCHAR)
        .put(java.sql.Types.VARCHAR, MinorType.VARCHAR)
        .put(java.sql.Types.LONGVARCHAR, MinorType.VARCHAR)
        .put(java.sql.Types.CLOB, MinorType.VARCHAR)

        .put(java.sql.Types.NCHAR, MinorType.VARCHAR)
        .put(java.sql.Types.NVARCHAR, MinorType.VARCHAR)
        .put(java.sql.Types.LONGNVARCHAR, MinorType.VARCHAR)

        .put(java.sql.Types.VARBINARY, MinorType.VARBINARY)
        .put(java.sql.Types.LONGVARBINARY, MinorType.VARBINARY)
        .put(java.sql.Types.BLOB, MinorType.VARBINARY)

        .put(java.sql.Types.NUMERIC, MinorType.FLOAT8)
        .put(java.sql.Types.DECIMAL, MinorType.VARDECIMAL)
        .put(java.sql.Types.REAL, MinorType.FLOAT8)

        .put(java.sql.Types.DATE, MinorType.DATE)
        .put(java.sql.Types.TIME, MinorType.TIME)
        .put(java.sql.Types.TIMESTAMP, MinorType.TIMESTAMP)

        .put(java.sql.Types.BOOLEAN, MinorType.BIT)

        .put(java.sql.Types.BIT, MinorType.BIT)

        .build();
  }

  private static String nameFromType(int javaSqlType) {
    try {
      for (Field f : java.sql.Types.class.getFields()) {
        if (java.lang.reflect.Modifier.isStatic(f.getModifiers()) &&
            f.getType() == int.class &&
            f.getInt(null) == javaSqlType) {
          return f.getName();
        }
      }
    } catch (IllegalArgumentException | IllegalAccessException e) {
      logger.trace("Unable to SQL type {} into String: {}", javaSqlType, e.getMessage());
    }

    return Integer.toString(javaSqlType);

  }

  private Copier<?> getCopier(int jdbcType, int offset, ResultSet result, ValueVector v) {
    switch (jdbcType) {
      case java.sql.Types.BIGINT:
        return new BigIntCopier(offset, result, (NullableBigIntVector.Mutator) v.getMutator());
      case java.sql.Types.FLOAT:
        return new Float4Copier(offset, result, (NullableFloat4Vector.Mutator) v.getMutator());
      case java.sql.Types.DOUBLE:
      case java.sql.Types.NUMERIC:
      case java.sql.Types.REAL:
        return new Float8Copier(offset, result, (NullableFloat8Vector.Mutator) v.getMutator());
      case java.sql.Types.TINYINT:
      case java.sql.Types.SMALLINT:
      case java.sql.Types.INTEGER:
        return new IntCopier(offset, result, (NullableIntVector.Mutator) v.getMutator());
      case java.sql.Types.CHAR:
      case java.sql.Types.VARCHAR:
      case java.sql.Types.LONGVARCHAR:
      case java.sql.Types.CLOB:
      case java.sql.Types.NCHAR:
      case java.sql.Types.NVARCHAR:
      case java.sql.Types.LONGNVARCHAR:
        return new VarCharCopier(offset, result, (NullableVarCharVector.Mutator) v.getMutator());
      case java.sql.Types.VARBINARY:
      case java.sql.Types.LONGVARBINARY:
      case java.sql.Types.BLOB:
        return new VarBinaryCopier(offset, result, (NullableVarBinaryVector.Mutator) v.getMutator());
      case java.sql.Types.DATE:
        return new DateCopier(offset, result, (NullableDateVector.Mutator) v.getMutator());
      case java.sql.Types.TIME:
        return new TimeCopier(offset, result, (NullableTimeVector.Mutator) v.getMutator());
      case java.sql.Types.TIMESTAMP:
        return new TimeStampCopier(offset, result, (NullableTimeStampVector.Mutator) v.getMutator());
      case java.sql.Types.BOOLEAN:
      case java.sql.Types.BIT:
        return new BitCopier(offset, result, (NullableBitVector.Mutator) v.getMutator());
      case java.sql.Types.DECIMAL:
        return new DecimalCopier(offset, result, (NullableVarDecimalVector.Mutator) v.getMutator());
      default:
        throw new IllegalArgumentException("Unknown how to handle vector.");
    }
  }

  @Override
  public void setup(OperatorContext operatorContext, OutputMutator output) {
    try {
      connection = source.getConnection();
      statement = connection.prepareStatement(sql);
      resultSet = statement.executeQuery();

      ResultSetMetaData meta = resultSet.getMetaData();
      int columnsCount = meta.getColumnCount();
      if (columns.size() != columnsCount) {
        throw UserException
            .validationError()
            .message(
              "Expected columns count differs from the returned one.\n" +
                  "Expected columns: %s\n" +
                  "Returned columns count: %s",
              columns, columnsCount)
            .addContext("Sql", sql)
            .addContext("Plugin", storagePluginName)
            .build(logger);
      }
      ImmutableList.Builder<ValueVector> vectorBuilder = ImmutableList.builder();
      ImmutableList.Builder<Copier<?>> copierBuilder = ImmutableList.builder();

      for (int i = 1; i <= columnsCount; i++) {
        String name = columns.get(i - 1).getRootSegmentPath();
        // column index in ResultSetMetaData starts from 1
        int jdbcType = meta.getColumnType(i);
        int width = meta.getPrecision(i);
        int scale = meta.getScale(i);
        MinorType minorType = JDBC_TYPE_MAPPINGS.get(jdbcType);
        if (minorType == null) {
          logger.warn("Ignoring column that is unsupported.", UserException
              .unsupportedError()
              .message(
                  "A column you queried has a data type that is not currently supported by the JDBC storage plugin. "
                      + "The column's name was %s and its JDBC data type was %s. ",
                  name,
                  nameFromType(jdbcType))
              .addContext("Sql", sql)
              .addContext("Column Name", name)
              .addContext("Plugin", storagePluginName)
              .build(logger));
          continue;
        }

        MajorType type = MajorType.newBuilder()
            .setMode(TypeProtos.DataMode.OPTIONAL)
            .setMinorType(minorType)
            .setScale(scale)
            .setPrecision(width)
            .build();
        MaterializedField field = MaterializedField.create(name, type);
        Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(
            minorType, type.getMode());
        ValueVector vector = output.addField(field, clazz);
        vectorBuilder.add(vector);
        copierBuilder.add(getCopier(jdbcType, i, resultSet, vector));
      }

      vectors = vectorBuilder.build();
      copiers = copierBuilder.build();

    } catch (SQLException | SchemaChangeException e) {
      throw UserException.dataReadError(e)
          .message("The JDBC storage plugin failed while trying setup the SQL query. ")
          .addContext("Sql", sql)
          .addContext("Plugin", storagePluginName)
          .build(logger);
    }
  }

  @Override
  public int next() {
    int counter = 0;
    try {
      while (counter < 4095) { // loop at 4095 since nullables use one more than record count and we
                                            // allocate on powers of two.
        if (!resultSet.next()) {
          break;
        }
        for (Copier<?> c : copiers) {
          c.copy(counter);
        }
        counter++;
      }
    } catch (SQLException e) {
      throw UserException
          .dataReadError(e)
          .message("Failure while attempting to read from database.")
          .addContext("Sql", sql)
          .addContext("Plugin", storagePluginName)
          .build(logger);
    }

    int valueCount = Math.max(counter, 0);
    for (ValueVector vv : vectors) {
      vv.getMutator().setValueCount(valueCount);
    }

    return valueCount;
  }

  @Override
  public void close() {
    AutoCloseables.closeSilently(resultSet, statement, connection);
  }

  @Override
  public String toString() {
    return "JdbcRecordReader[sql=" + sql
        + ", Plugin=" + storagePluginName
        + "]";
  }

  private abstract static class Copier<T extends ValueVector.Mutator> {
    final int columnIndex;
    final ResultSet result;
    final T mutator;

    Copier(int columnIndex, ResultSet result, T mutator) {
      this.columnIndex = columnIndex;
      this.result = result;
      this.mutator = mutator;
    }

    abstract void copy(int index) throws SQLException;
  }

  private static class IntCopier extends Copier<NullableIntVector.Mutator> {

    IntCopier(int offset, ResultSet set, NullableIntVector.Mutator mutator) {
      super(offset, set, mutator);
    }

    @Override
    void copy(int index) throws SQLException {
      mutator.setSafe(index, result.getInt(columnIndex));
      if (result.wasNull()) {
        mutator.setNull(index);
      }
    }
  }

  private static class BigIntCopier extends Copier<NullableBigIntVector.Mutator> {

    BigIntCopier(int offset, ResultSet set, NullableBigIntVector.Mutator mutator) {
      super(offset, set, mutator);
    }

    @Override
    void copy(int index) throws SQLException {
      mutator.setSafe(index, result.getLong(columnIndex));
      if (result.wasNull()) {
        mutator.setNull(index);
      }
    }
  }

  private static class Float4Copier extends Copier<NullableFloat4Vector.Mutator> {

    Float4Copier(int columnIndex, ResultSet result, NullableFloat4Vector.Mutator mutator) {
      super(columnIndex, result, mutator);
    }

    @Override
    void copy(int index) throws SQLException {
      mutator.setSafe(index, result.getFloat(columnIndex));
      if (result.wasNull()) {
        mutator.setNull(index);
      }
    }
  }

  private static class Float8Copier extends Copier<NullableFloat8Vector.Mutator> {

    Float8Copier(int columnIndex, ResultSet result, NullableFloat8Vector.Mutator mutator) {
      super(columnIndex, result, mutator);
    }

    @Override
    void copy(int index) throws SQLException {
      mutator.setSafe(index, result.getDouble(columnIndex));
      if (result.wasNull()) {
        mutator.setNull(index);
      }
    }
  }

  private static class DecimalCopier extends Copier<NullableVarDecimalVector.Mutator> {

    DecimalCopier(int columnIndex, ResultSet result, NullableVarDecimalVector.Mutator mutator) {
      super(columnIndex, result, mutator);
    }

    @Override
    void copy(int index) throws SQLException {
      BigDecimal decimal = result.getBigDecimal(columnIndex);
      if (decimal != null) {
        mutator.setSafe(index, decimal);
      }
    }
  }

  private static class VarCharCopier extends Copier<NullableVarCharVector.Mutator> {

    VarCharCopier(int columnIndex, ResultSet result, NullableVarCharVector.Mutator mutator) {
      super(columnIndex, result, mutator);
    }

    @Override
    void copy(int index) throws SQLException {
      String val = result.getString(columnIndex);
      if (val != null) {
        byte[] record = val.getBytes(Charsets.UTF_8);
        mutator.setSafe(index, record, 0, record.length);
      }
    }
  }

  private static class VarBinaryCopier extends Copier<NullableVarBinaryVector.Mutator> {

    VarBinaryCopier(int columnIndex, ResultSet result, NullableVarBinaryVector.Mutator mutator) {
      super(columnIndex, result, mutator);
    }

    @Override
    void copy(int index) throws SQLException {
      byte[] record = result.getBytes(columnIndex);
      if (record != null) {
        mutator.setSafe(index, record, 0, record.length);
      }
    }
  }

  private static class DateCopier extends Copier<NullableDateVector.Mutator> {

    private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    DateCopier(int columnIndex, ResultSet result, NullableDateVector.Mutator mutator) {
      super(columnIndex, result, mutator);
    }

    @Override
    void copy(int index) throws SQLException {
      Date date = result.getDate(columnIndex, calendar);
      if (date != null) {
        mutator.setSafe(index, date.getTime());
      }
    }
  }

  private static class TimeCopier extends Copier<NullableTimeVector.Mutator> {

    private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    TimeCopier(int columnIndex, ResultSet result, NullableTimeVector.Mutator mutator) {
      super(columnIndex, result, mutator);
    }

    @Override
    void copy(int index) throws SQLException {
      Time time = result.getTime(columnIndex, calendar);
      if (time != null) {
        mutator.setSafe(index, (int) time.getTime());
      }
    }
  }

  private static class TimeStampCopier extends Copier<NullableTimeStampVector.Mutator> {

    private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    TimeStampCopier(int columnIndex, ResultSet result, NullableTimeStampVector.Mutator mutator) {
      super(columnIndex, result, mutator);
    }

    @Override
    void copy(int index) throws SQLException {
      Timestamp stamp = result.getTimestamp(columnIndex, calendar);
      if (stamp != null) {
        mutator.setSafe(index, stamp.getTime());
      }
    }
  }

  private static class BitCopier extends Copier<NullableBitVector.Mutator> {

    BitCopier(int columnIndex, ResultSet result, NullableBitVector.Mutator mutator) {
      super(columnIndex, result, mutator);
    }

    @Override
    void copy(int index) throws SQLException {
      mutator.setSafe(index, result.getBoolean(columnIndex) ? 1 : 0);
      if (result.wasNull()) {
        mutator.setNull(index);
      }
    }
  }
}
