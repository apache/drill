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
package org.apache.drill.exec.store.kudu;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.kudu.KuduSubScan.KuduSubScanSpec;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduScanner.KuduScannerBuilder;
import org.kududb.client.KuduTable;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;
import org.kududb.client.shaded.com.google.common.collect.ImmutableMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class KuduRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KuduRecordReader.class);

  private static final int TARGET_RECORD_COUNT = 4000;

  private final KuduClient client;
  private final KuduSubScanSpec scanSpec;
  private KuduScanner scanner;
  private RowResultIterator iterator;
  
  
  private OutputMutator output;
  private ImmutableList<ValueVector> vectors;
  private ImmutableList<Copier<?>> copiers;

  public KuduRecordReader(KuduClient client, KuduSubScan.KuduSubScanSpec subScanSpec,
      List<SchemaPath> projectedColumns, FragmentContext context) {
    setColumns(projectedColumns);
    this.client = client;
    scanSpec = subScanSpec;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.output = output;
    try {
      KuduTable table = client.openTable(scanSpec.getTableName());
      
      KuduScannerBuilder builder = client.newScannerBuilder(table);
      if (!isStarQuery()) {
        List<String> colNames = Lists.newArrayList();
        for (SchemaPath p : this.getColumns()) {
          colNames.add(p.getAsUnescapedPath());
        }
        builder.setProjectedColumnNames(colNames);
      }
      scanner = builder.build();
    } catch (Exception e) {
      throw new ExecutionSetupException(e);
    }
  }

  static final Map<Type, MajorType> TYPES;

  static {
    TYPES = ImmutableMap.<Type, MajorType> builder()
        .put(Type.BINARY, Types.optional(MinorType.VARBINARY))
        .put(Type.BOOL, Types.optional(MinorType.BIT))
        .put(Type.DOUBLE, Types.optional(MinorType.FLOAT8))
        .put(Type.FLOAT, Types.optional(MinorType.FLOAT4))
        .put(Type.INT8, Types.optional(MinorType.TINYINT))
        .put(Type.INT16, Types.optional(MinorType.SMALLINT))
        .put(Type.INT32, Types.optional(MinorType.INT))
        .put(Type.INT64, Types.optional(MinorType.BIGINT))
        .put(Type.STRING, Types.optional(MinorType.VARCHAR))
        .put(Type.TIMESTAMP, Types.optional(MinorType.TIMESTAMP))
        .build();
  }

  @Override
  public int next() {
    int rowCount = 0;
    try {
      while (iterator == null || !iterator.hasNext()) {
        if (!scanner.hasMoreRows()) {
          iterator = null;
          return 0;
        }
        iterator = scanner.nextRows();
      }
      for (; rowCount < TARGET_RECORD_COUNT && iterator.hasNext(); rowCount++) {
        addRowResult(iterator.next(), rowCount);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    for (ValueVector vv : vectors) {
      vv.getMutator().setValueCount(rowCount);
    }
    return rowCount;
  }
  
  @SuppressWarnings("unchecked")
  private void initCopiers(Schema schema) throws SchemaChangeException {
    ImmutableList.Builder<ValueVector> vectorBuilder = ImmutableList.builder();
    ImmutableList.Builder<Copier<?>> copierBuilder = ImmutableList.builder();
    
    for (int i = 0; i < schema.getColumnCount(); i++) {
      ColumnSchema col = schema.getColumnByIndex(i);  
      
      final String name = col.getName();
      final Type kuduType = col.getType();
      MajorType majorType = TYPES.get(kuduType);
      if (majorType == null) {
        logger.warn("Ignoring column that is unsupported.", UserException
            .unsupportedError()
            .message(
                "A column you queried has a data type that is not currently supported by the JDBC storage plugin. "
                    + "The column's name was %s and its Kudu data type was %s. ",
                name, kuduType.toString())
            .addContext("column Name", name)
            .addContext("plugin", "kudu")
            .build(logger));

        continue;
      }
      MinorType minorType = majorType.getMinorType();
      MaterializedField field = MaterializedField.create(name, majorType);
      final Class<? extends ValueVector> clazz = (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(
          minorType, majorType.getMode());
      ValueVector vector = output.addField(field, clazz);
      vector.allocateNew();
      vectorBuilder.add(vector);
      copierBuilder.add(getCopier(kuduType, i, vector));
    }

    vectors = vectorBuilder.build();
    copiers = copierBuilder.build();
  }

  private void addRowResult(RowResult result, int rowIndex) throws SchemaChangeException {
    if (copiers == null) {
      initCopiers(result.getColumnProjection());
    }
    
    for (Copier<?> c : copiers) {
      c.copy(result, rowIndex);
    }
  }


  @Override
  public void close() {
  }
  

  private Copier<?> getCopier(Type kuduType, int offset, ValueVector v) {

    if (v instanceof NullableBigIntVector) {
      return new BigIntCopier(offset, (NullableBigIntVector.Mutator) v.getMutator());
    } else if (v instanceof NullableFloat4Vector) {
      return new Float4Copier(offset, (NullableFloat4Vector.Mutator) v.getMutator());
    } else if (v instanceof NullableFloat8Vector) {
      return new Float8Copier(offset, (NullableFloat8Vector.Mutator) v.getMutator());
    } else if (v instanceof NullableIntVector) {
      return new IntCopier(offset, (NullableIntVector.Mutator) v.getMutator());
    } else if (v instanceof NullableVarCharVector) {
      return new VarCharCopier(offset, (NullableVarCharVector.Mutator) v.getMutator());
    } else if (v instanceof NullableVarBinaryVector) {
      return new VarBinaryCopier(offset, (NullableVarBinaryVector.Mutator) v.getMutator());
    } else if (v instanceof NullableTimeStampVector) {
      return new TimeStampCopier(offset, (NullableTimeStampVector.Mutator) v.getMutator());
    } else if (v instanceof NullableBitVector) {
      return new BitCopier(offset, (NullableBitVector.Mutator) v.getMutator());
    }

    throw new IllegalArgumentException("Unknown how to handle vector.");
  }
  
  private abstract class Copier<T extends ValueVector.Mutator> {
    protected final int columnIndex;
    protected final T mutator;

    public Copier(int columnIndex, T mutator) {
      this.columnIndex = columnIndex;
      this.mutator = mutator;
    }

    abstract void copy(RowResult result, int index);
  }

  private class IntCopier extends Copier<NullableIntVector.Mutator> {
    public IntCopier(int offset, NullableIntVector.Mutator mutator) {
      super(offset, mutator);
    }

    @Override
    void copy(RowResult result, int index) {
      if (result.isNull(columnIndex)) {
        mutator.setNull(index);
      } else {
        mutator.setSafe(index, result.getInt(columnIndex));
      }
    }
  }

  private class BigIntCopier extends Copier<NullableBigIntVector.Mutator> {
    public BigIntCopier(int offset, NullableBigIntVector.Mutator mutator) {
      super(offset, mutator);
    }

    @Override
    void copy(RowResult result, int index) {
      if (result.isNull(columnIndex)) {
        mutator.setNull(index);
      } else {
        mutator.setSafe(index, result.getLong(columnIndex));
      }
    }
  }

  private class Float4Copier extends Copier<NullableFloat4Vector.Mutator> {

    public Float4Copier(int columnIndex, NullableFloat4Vector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(RowResult result, int index) {
      if (result.isNull(columnIndex)) {
        mutator.setNull(index);
      } else {
        mutator.setSafe(index, result.getFloat(columnIndex));
      }
    }

  }


  private class Float8Copier extends Copier<NullableFloat8Vector.Mutator> {

    public Float8Copier(int columnIndex, NullableFloat8Vector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(RowResult result, int index) {
      if (result.isNull(columnIndex)) {
        mutator.setNull(index);
      } else {
        mutator.setSafe(index, result.getDouble(columnIndex));
      }
    }
  }

  // TODO: decimal copier
  
  private class VarCharCopier extends Copier<NullableVarCharVector.Mutator> {

    public VarCharCopier(int columnIndex, NullableVarCharVector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(RowResult result, int index) {
      if (result.isNull(columnIndex)) {
        mutator.setNull(index);
      } else {
        ByteBuffer value = result.getBinary(columnIndex);
        mutator.setSafe(index, value, 0, value.remaining());
      }
    }
  }

  private class VarBinaryCopier extends Copier<NullableVarBinaryVector.Mutator> {

    public VarBinaryCopier(int columnIndex, NullableVarBinaryVector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(RowResult result, int index) {
      if (result.isNull(columnIndex)) {
        mutator.setNull(index);
      } else {
        ByteBuffer value = result.getBinary(columnIndex);
        mutator.setSafe(index, value, 0, value.remaining());
      }
    }
  }

  // TODO: DateCopier
  // TODO: TimeCopier
  
  private class TimeStampCopier extends Copier<NullableTimeStampVector.Mutator> {

    public TimeStampCopier(int columnIndex, NullableTimeStampVector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(RowResult result, int index) {
      if (result.isNull(columnIndex)) {
        mutator.setNull(index);
      } else {
        long ts = result.getLong(columnIndex);
        mutator.setSafe(index, ts / 1000);
      }
    }
  }

  private class BitCopier extends Copier<NullableBitVector.Mutator> {
    public BitCopier(int columnIndex, NullableBitVector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(RowResult result, int index) {
      if (result.isNull(columnIndex)) {
        mutator.setNull(index);
      } else {
        mutator.setSafe(index, result.getBoolean(columnIndex) ? 1 : 0);
      }
    }
  }

}
