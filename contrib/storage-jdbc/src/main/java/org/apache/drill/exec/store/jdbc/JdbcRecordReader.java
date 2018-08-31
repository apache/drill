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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import javax.sql.DataSource;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
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

@SuppressWarnings("unchecked")
class JdbcRecordReader extends AbstractRecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(JdbcRecordReader.class);

  private static final ImmutableMap<Integer, MinorType> JDBC_TYPE_MAPPINGS;
  private final DataSource source;
  private ResultSet resultSet;
  private final String storagePluginName;
  private Connection connection;
  private Statement statement;
  private final String sql;
  private ImmutableList<ValueVector> vectors;
  private ImmutableList<Copier<?>> copiers;

  public JdbcRecordReader(DataSource source, String sql, String storagePluginName) {
    this.source = source;
    this.sql = sql;
    this.storagePluginName = storagePluginName;
  }

  static {
    JDBC_TYPE_MAPPINGS = (ImmutableMap<Integer, MinorType>) (Object) ImmutableMap.builder()
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
  public void setup(OperatorContext operatorContext, OutputMutator output) throws ExecutionSetupException {
    try {
      connection = source.getConnection();
      statement = connection.createStatement();
      resultSet = statement.executeQuery(sql);

      final ResultSetMetaData meta = resultSet.getMetaData();
      final int columns = meta.getColumnCount();
      ImmutableList.Builder<ValueVector> vectorBuilder = ImmutableList.builder();
      ImmutableList.Builder<Copier<?>> copierBuilder = ImmutableList.builder();

      for (int i = 1; i <= columns; i++) {
        final String name = meta.getColumnLabel(i);
        final int jdbcType = meta.getColumnType(i);
        final int width = meta.getPrecision(i);
        final int scale = meta.getScale(i);
        MinorType minorType = JDBC_TYPE_MAPPINGS.get(jdbcType);
        if (minorType == null) {

          logger.warn("Ignoring column that is unsupported.", UserException
              .unsupportedError()
              .message(
                  "A column you queried has a data type that is not currently supported by the JDBC storage plugin. "
                      + "The column's name was %s and its JDBC data type was %s. ",
                  name,
                  nameFromType(jdbcType))
              .addContext("sql", sql)
              .addContext("column Name", name)
              .addContext("plugin", storagePluginName)
              .build(logger));

          continue;
        }

        final MajorType type = MajorType.newBuilder()
            .setMode(TypeProtos.DataMode.OPTIONAL)
            .setMinorType(minorType)
            .setScale(scale)
            .setPrecision(width)
            .build();
        final MaterializedField field = MaterializedField.create(name, type);
        final Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(
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
          .addContext("sql", sql)
          .addContext("plugin", storagePluginName)
          .build(logger);
    }
  }


  @Override
  public int next() {
    int counter = 0;
    Boolean b = true;
    try {
      while (counter < 4095 && b) { // loop at 4095 since nullables use one more than record count and we
                                            // allocate on powers of two.
        b = resultSet.next();
        if (!b) {
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
          .addContext("sql", sql)
          .addContext("plugin", storagePluginName)
          .build(logger);
    }

    for (ValueVector vv : vectors) {
      vv.getMutator().setValueCount(counter > 0 ? counter : 0);
    }

    return counter>0 ? counter : 0;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(resultSet, statement, connection);
  }

  @Override
  public String toString() {
    return "JdbcRecordReader[sql=" + sql
        + ", Plugin=" + storagePluginName
        + "]";
  }

  private abstract class Copier<T extends ValueVector.Mutator> {
    protected final int columnIndex;
    protected final ResultSet result;
    protected final T mutator;

    public Copier(int columnIndex, ResultSet result, T mutator) {
      super();
      this.columnIndex = columnIndex;
      this.result = result;
      this.mutator = mutator;
    }

    abstract void copy(int index) throws SQLException;
  }

  private class IntCopier extends Copier<NullableIntVector.Mutator> {
    public IntCopier(int offset, ResultSet set, NullableIntVector.Mutator mutator) {
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

  private class BigIntCopier extends Copier<NullableBigIntVector.Mutator> {
    public BigIntCopier(int offset, ResultSet set, NullableBigIntVector.Mutator mutator) {
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

  private class Float4Copier extends Copier<NullableFloat4Vector.Mutator> {

    public Float4Copier(int columnIndex, ResultSet result, NullableFloat4Vector.Mutator mutator) {
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


  private class Float8Copier extends Copier<NullableFloat8Vector.Mutator> {

    public Float8Copier(int columnIndex, ResultSet result, NullableFloat8Vector.Mutator mutator) {
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

  private class DecimalCopier extends Copier<NullableVarDecimalVector.Mutator> {

    public DecimalCopier(int columnIndex, ResultSet result, NullableVarDecimalVector.Mutator mutator) {
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

  private class VarCharCopier extends Copier<NullableVarCharVector.Mutator> {

    public VarCharCopier(int columnIndex, ResultSet result, NullableVarCharVector.Mutator mutator) {
      super(columnIndex, result, mutator);
    }

    @Override
    void copy(int index) throws SQLException {
      String val = resultSet.getString(columnIndex);
      if (val != null) {
        byte[] record = val.getBytes(Charsets.UTF_8);
        mutator.setSafe(index, record, 0, record.length);
      }
    }

  }

  private class VarBinaryCopier extends Copier<NullableVarBinaryVector.Mutator> {

    public VarBinaryCopier(int columnIndex, ResultSet result, NullableVarBinaryVector.Mutator mutator) {
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

  private class DateCopier extends Copier<NullableDateVector.Mutator> {

    private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    public DateCopier(int columnIndex, ResultSet result, NullableDateVector.Mutator mutator) {
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

  private class TimeCopier extends Copier<NullableTimeVector.Mutator> {

    private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    public TimeCopier(int columnIndex, ResultSet result, NullableTimeVector.Mutator mutator) {
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


  private class TimeStampCopier extends Copier<NullableTimeStampVector.Mutator> {

    private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    public TimeStampCopier(int columnIndex, ResultSet result, NullableTimeStampVector.Mutator mutator) {
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

  private class BitCopier extends Copier<NullableBitVector.Mutator> {

    public BitCopier(int columnIndex, ResultSet result, NullableBitVector.Mutator mutator) {
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
