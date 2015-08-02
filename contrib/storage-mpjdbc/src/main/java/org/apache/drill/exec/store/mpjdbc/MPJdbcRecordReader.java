/**
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
package org.apache.drill.exec.store.mpjdbc;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Decimal38DenseHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.MaterializedField.Key;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.Decimal38DenseVector;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableDecimal38DenseVector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableTimeVector;
import org.apache.drill.exec.vector.NullableVar16CharVector.Mutator;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.TimeVector;
import org.apache.drill.exec.vector.ValueHolderHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MPJdbcRecordReader extends AbstractRecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(MPJdbcRecordReader.class);

  private ResultSet rec;
  private VectorContainerWriter writer;
  private FragmentContext fc;
  private MPJdbcSubScan scanSpec;
  private MPJdbcFormatPlugin plugin;
  private List<MPJdbcScanSpec> scanList;
  private MPJdbcFormatConfig config;
  private Connection conn;
  private Statement statement;
  private String table;
  private String database;
  protected List<ValueVector> vectors = Lists.newArrayList();
  private int col_cnt = 0;
  private MajorType.Builder t;
  private OutputMutator outputMutator;
  private ResultSetMetaData meta;
  private OperatorContext operatorContext;
  private String columns;
  private List<String> filters;

  public MPJdbcRecordReader(FragmentContext fragmentContext, MPJdbcSubScan scan) {
    fc = fragmentContext;
    scanSpec = scan;
    // TODO Auto-generated constructor stub
    this.plugin = scanSpec.getPlugin();
    this.scanList = scanSpec.getScanList();
    this.config = scanSpec.getConfig();
    MPJdbcClientOptions options = new MPJdbcClientOptions(config);
    MPJdbcClient client = MPJdbcCnxnManager.getClient(config.getUri(), options,
        this.plugin);
    conn = client.getConnection();
    Iterator<MPJdbcScanSpec> iter = scanList.iterator();
    while (iter.hasNext()) {
      MPJdbcScanSpec o = iter.next();
      table = o.getTable();
      database = o.getDatabase();
      List<SchemaPath> ColList = scan.getColumns();
      Iterator<SchemaPath> collist_iter = ColList.iterator();
      StringBuilder b = new StringBuilder();
      while(collist_iter.hasNext()) {
          SchemaPath val = collist_iter.next();
          b.append(val.getAsUnescapedPath().trim());
          if(collist_iter.hasNext()) {
              b.append(",");
          }
      }
      columns = b.toString();
      filters = o.getFilters();
    }
    try {
      statement = conn.createStatement();
      rec = statement.executeQuery("SELECT " + this.columns + " FROM " + database.trim() + "." + table.trim());
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      meta = rec.getMetaData();
      col_cnt = meta.getColumnCount();
      Class cls = null;
      for (int i = 1; i <= col_cnt; i++) {
        String column_label = meta.getColumnLabel(i);
        int types = meta.getColumnType(i);
        int isnullable = meta.isNullable(i);
        int width = meta.getPrecision(i);
        int scale = meta.getScale(i);
        MaterializedField field = null;
        switch (types) {
        case java.sql.Types.BIGINT:
          t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.BIGINT);
          if (isnullable == 1) {
            t.setMode(DataMode.OPTIONAL);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, NullableBigIntVector.class));
          } else {
            t.setMode(DataMode.REQUIRED);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, BigIntVector.class));
          }
          break;
        case Types.DATE:
          t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.DATE);
          cls = org.apache.drill.exec.vector.DateVector.class;
          if (isnullable == 1) {
            t.setMode(DataMode.OPTIONAL);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, NullableDateVector.class));
          } else {
            t.setMode(DataMode.REQUIRED);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, DateVector.class));
          }
          break;
        case Types.DECIMAL:
          t = MajorType.newBuilder().setMinorType(
              TypeProtos.MinorType.DECIMAL38DENSE);
          t.setMode(DataMode.OPTIONAL);
          field = MaterializedField.create(column_label, t.build());
          vectors.add(output.addField(field, Decimal38DenseVector.class));
          break;
        case Types.DOUBLE:
          t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.BIGINT);
          if (isnullable == 1) {
            t.setMode(DataMode.OPTIONAL);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, NullableBigIntVector.class));
          } else {
            t.setMode(DataMode.REQUIRED);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, BigIntVector.class));
          }
          break;
        case Types.FLOAT:
          t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FLOAT8);
          if (isnullable == 1) {
            t.setMode(DataMode.OPTIONAL);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, NullableFloat8Vector.class));
          } else {
            t.setMode(DataMode.REQUIRED);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, Float8Vector.class));
          }
          break;
        case Types.INTEGER:
        case Types.SMALLINT:
          t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.INT);
          if (isnullable == 1) {
            t.setMode(DataMode.OPTIONAL);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, NullableIntVector.class));
          } else {
            t.setMode(DataMode.REQUIRED);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, IntVector.class));
          }
          break;
        case Types.LONGNVARCHAR:
        case Types.LONGVARCHAR:
          t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.VARCHAR);
          t.setWidth(width);
          if (isnullable == 1) {
            t.setMode(DataMode.OPTIONAL);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, NullableVarCharVector.class));
          } else {
            t.setMode(DataMode.REQUIRED);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, VarCharVector.class));
          }
          break;
        case Types.LONGVARBINARY:
          break;
        case Types.CHAR:
        case Types.NCHAR:
          t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.VARCHAR);
          t.setWidth(width);
          if (isnullable == 1) {
            t.setMode(DataMode.OPTIONAL);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, NullableVarCharVector.class));
          } else {
            t.setMode(DataMode.REQUIRED);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, VarCharVector.class));
          }
          break;
        case Types.NUMERIC:
          t = MajorType.newBuilder().setMinorType(
              TypeProtos.MinorType.DECIMAL38DENSE);
          if (isnullable == 1) {
            t.setMode(DataMode.OPTIONAL);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field,
                NullableDecimal38DenseVector.class));
          } else {
            t.setMode(DataMode.REQUIRED);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, Decimal38DenseVector.class));
          }
          break;
        case Types.NVARCHAR:
        case Types.VARCHAR:
          t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.VARCHAR);
          t.setWidth(width);
          if (isnullable == 1) {
            t.setMode(DataMode.OPTIONAL);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, NullableVarCharVector.class));
          } else {
            t.setMode(DataMode.REQUIRED);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, VarCharVector.class));
          }
          break;
        case Types.TIME:
          t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.TIME);
          if (isnullable == 1) {
            t.setMode(DataMode.OPTIONAL);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, NullableTimeVector.class));
          } else {
            t.setMode(DataMode.REQUIRED);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, TimeVector.class));
          }
          break;
        case Types.TIMESTAMP:
          t = MajorType.newBuilder().setMinorType(
              TypeProtos.MinorType.TIMESTAMP);
          if (isnullable == 1) {
            t.setMode(DataMode.OPTIONAL);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, NullableTimeStampVector.class));
          } else {
            t.setMode(DataMode.REQUIRED);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, TimeStampVector.class));
          }
          break;
        default:
          t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.VARCHAR);
          t.setWidth(width);
          if (isnullable == 1) {
            t.setMode(DataMode.OPTIONAL);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, NullableVarCharVector.class));
          } else {
            t.setMode(DataMode.REQUIRED);
            field = MaterializedField.create(column_label, t.build());
            vectors.add(output.addField(field, VarCharVector.class));
          }
          break;
        }
      }
      this.outputMutator = output;

    } catch (SQLException | SchemaChangeException e) {
      // TODO Auto-generated catch block
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public void setOperatorContext(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
  }

  @Override
  public int next() {
    // TODO Auto-generated method stub
    int counter = 0;
    int pos = 1;
    int prec = 0;
    Boolean b = true;
    try {
      while (counter < 65536 && b == true) {
        b = rec.next();
        if(b == false) {
            break;
        }
        for (ValueVector vv : vectors) {
          String val = rec.getString(pos);
          byte[] record = val.getBytes(Charsets.UTF_8);
          if (vv.getClass().equals(NullableVarCharVector.class)) {
            NullableVarCharVector v = (NullableVarCharVector) vv;
            v.getMutator().setSafe(counter, record, 0, record.length);
            v.getMutator().setValueLengthSafe(counter, record.length);
          } else if (vv.getClass().equals(VarCharVector.class)) {
            VarCharVector v = (VarCharVector) vv;
            v.getMutator().setSafe(counter, record, 0, record.length);
            v.getMutator().setValueLengthSafe(counter, record.length);
          } else if (vv.getClass().equals(BigIntVector.class)) {
            BigIntVector v = (BigIntVector) vv;
            v.getMutator().setSafe(counter, rec.getLong(pos));
          } else if (vv.getClass().equals(NullableBigIntVector.class)) {
            NullableBigIntVector v = (NullableBigIntVector) vv;
            v.getMutator().setSafe(counter, rec.getLong(pos));
          } else if (vv.getClass().equals(IntVector.class)) {
            IntVector v = (IntVector) vv;
            v.getMutator().setSafe(counter, rec.getInt(pos));
          } else if (vv.getClass().equals(NullableIntVector.class)) {
            NullableIntVector v = (NullableIntVector) vv;
            v.getMutator().setSafe(counter, rec.getInt(pos));
          } else if (vv.getClass().equals(DateVector.class)) {
            DateVector v = (DateVector) vv;
            long dtime = DateTime.parse(val).toDate().getTime(); // DateTime.parse(val).toDateTime().getMillis();
            v.getMutator().setSafe(counter, dtime);
          } else if (vv.getClass().equals(NullableDateVector.class)) {
            NullableDateVector v = (NullableDateVector) vv;
            if (rec.wasNull()) {
              v.getMutator().setNull(counter);
            } else {
              long dtime = DateTime.parse(val).toDate().getTime();
              v.getMutator().setSafe(counter, dtime);
            }
          } else if (vv.getClass().equals(Decimal38DenseVector.class)) {
            Decimal38DenseVector v = (Decimal38DenseVector) vv;
            java.math.BigDecimal d = rec.getBigDecimal(pos);
          } else if (vv.getClass().equals(NullableDecimal38DenseVector.class)) {
            NullableDecimal38DenseVector v = (NullableDecimal38DenseVector) vv;
            java.math.BigDecimal d = rec.getBigDecimal(pos);
          } else {
            NullableVarCharVector v = (NullableVarCharVector) vv;
            v.getMutator().setSafe(counter, record, 0, record.length);
            v.getMutator().setValueLengthSafe(counter, record.length);
          }
          pos++;
        }
        pos = 1;
        counter++;
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      throw new DrillRuntimeException(e);
    }
    //logger.info("Number of rows returned from JDBC: " + counter);
    for (ValueVector vv : vectors) {
      vv.getMutator().setValueCount(counter > 0 ? counter : 0);
    }
    return counter>0 ? counter : 0;
  }

  @Override
  public void allocate(Map<Key, ValueVector> vectorMap)
      throws OutOfMemoryException {
    int prec = 0;
    try {
      for (ValueVector vv : vectorMap.values()) {
        if (vv.getClass().equals(NullableVarCharVector.class)) {
                NullableVarCharVector v = (NullableVarCharVector) vv;
                prec = v.getField().getWidth();
                if(prec > 0) {
                   AllocationHelper.allocate(v, 65536, prec);
                } else {
                   AllocationHelper.allocate(v, 65536, 2000);
                }
              } else if (vv.getClass().equals(VarCharVector.class)) {
                VarCharVector v = (VarCharVector) vv;
                prec = v.getField().getWidth();
                if(prec > 0) {
                    AllocationHelper.allocate(v, 65536, prec);
                 } else {
                    AllocationHelper.allocate(v, 65536, 2000);
                 }
              } else if (vv.getClass().equals(BigIntVector.class)) {
                BigIntVector v = (BigIntVector) vv;
                v.allocateNew(65536);
              } else if (vv.getClass().equals(NullableBigIntVector.class)) {
                NullableBigIntVector v = (NullableBigIntVector) vv;
                v.allocateNew(65536);
              } else if (vv.getClass().equals(DateVector.class)) {
                DateVector v = (DateVector) vv;
                v.allocateNew(65536);
              } else if (vv.getClass().equals(NullableDateVector.class)) {
                NullableDateVector v = (NullableDateVector) vv;
                v.allocateNew(65536);
              } else if (vv.getClass().equals(Decimal38DenseVector.class)) {
                Decimal38DenseVector v = (Decimal38DenseVector) vv;
                v.allocateNew(65536);
              } else if (vv.getClass().equals(NullableDecimal38DenseVector.class)) {
                NullableDecimal38DenseVector v = (NullableDecimal38DenseVector) vv;
                v.allocateNew(65536);
              } else if (vv.getClass().equals(IntVector.class)) {
                IntVector v = (IntVector) vv;
                v.allocateNew(65536);
              } else if (vv.getClass().equals(NullableIntVector.class)) {
                NullableIntVector v = (NullableIntVector) vv;
                v.allocateNew(65536);
              }
      }
    } catch (NullPointerException e) {
      throw new OutOfMemoryException();
    }
  }

  @Override
  public void cleanup() {
    // TODO Auto-generated method stub
  }
}
