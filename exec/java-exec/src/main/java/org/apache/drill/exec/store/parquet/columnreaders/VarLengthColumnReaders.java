/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.store.parquet.columnreaders;

import io.netty.buffer.DrillBuf;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.Decimal28SparseVector;
import org.apache.drill.exec.vector.Decimal38SparseVector;
import org.apache.drill.exec.vector.NullableDecimal28SparseVector;
import org.apache.drill.exec.vector.NullableDecimal38SparseVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

public class VarLengthColumnReaders {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VarLengthColumnReaders.class);

  public static class Decimal28Column extends VarLengthValuesColumn<Decimal28SparseVector> {

    protected Decimal28SparseVector decimal28Vector;

    Decimal28Column(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Decimal28SparseVector v,
                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      this.decimal28Vector = v;
    }

    @Override
    public boolean setSafe(int index, DrillBuf bytebuf, int start, int length) {
      int width = Decimal28SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromDrillBuf(bytebuf, start, length,
          schemaElement.getScale());
      if (index >= decimal28Vector.getValueCapacity()) {
        return false;
      }
      DecimalUtility.getSparseFromBigDecimal(intermediate, decimal28Vector.getBuffer(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal28SparseHolder.nDecimalDigits);
      return true;
    }

    @Override
    public int capacity() {
      return decimal28Vector.getBuffer().capacity();
    }
  }

  public static class NullableDecimal28Column extends NullableVarLengthValuesColumn<NullableDecimal28SparseVector> {

    protected NullableDecimal28SparseVector nullableDecimal28Vector;

    NullableDecimal28Column(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                    ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableDecimal28SparseVector v,
                    SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      nullableDecimal28Vector = v;
    }

    @Override
    public boolean setSafe(int index, DrillBuf bytebuf, int start, int length) {
      int width = Decimal28SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromDrillBuf(bytebuf, start, length,
          schemaElement.getScale());
      if (index >= nullableDecimal28Vector.getValueCapacity()) {
        return false;
      }
      DecimalUtility.getSparseFromBigDecimal(intermediate, nullableDecimal28Vector.getBuffer(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal28SparseHolder.nDecimalDigits);
      nullableDecimal28Vector.getMutator().setIndexDefined(index);
      return true;
    }

    @Override
    public int capacity() {
      return nullableDecimal28Vector.getBuffer().capacity();
    }
  }

  public static class Decimal38Column extends VarLengthValuesColumn<Decimal38SparseVector> {

    protected Decimal38SparseVector decimal28Vector;

    Decimal38Column(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                    ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Decimal38SparseVector v,
                    SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      decimal28Vector = v;
    }

    @Override
    public boolean setSafe(int index, DrillBuf bytebuf, int start, int length) {
      int width = Decimal38SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromDrillBuf(bytebuf, start, length,
          schemaElement.getScale());
      if (index >= decimal28Vector.getValueCapacity()) {
        return false;
      }
      DecimalUtility.getSparseFromBigDecimal(intermediate, decimal28Vector.getBuffer(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal38SparseHolder.nDecimalDigits);
      return true;
    }

    @Override
    public int capacity() {
      return decimal28Vector.getBuffer().capacity();
    }
  }

  public static class NullableDecimal38Column extends NullableVarLengthValuesColumn<NullableDecimal38SparseVector> {

    private final NullableDecimal38SparseVector nullableDecimal38Vector;

    NullableDecimal38Column(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableDecimal38SparseVector v,
                            SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      nullableDecimal38Vector = v;
    }

    @Override
    public boolean setSafe(int index, DrillBuf bytebuf, int start, int length) {
      int width = Decimal38SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromDrillBuf(bytebuf, start, length,
          schemaElement.getScale());
      if (index >= nullableDecimal38Vector.getValueCapacity()) {
        return false;
      }

      DecimalUtility.getSparseFromBigDecimal(intermediate, nullableDecimal38Vector.getBuffer(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal38SparseHolder.nDecimalDigits);
      nullableDecimal38Vector.getMutator().setIndexDefined(index);
      return true;
    }

    @Override
    public int capacity() {
      return nullableDecimal38Vector.getBuffer().capacity();
    }
  }

  public static class VarCharColumn extends VarLengthValuesColumn<VarCharVector> {

    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    protected final VarCharVector.Mutator mutator;
    protected final VarCharVector varCharVector;

    VarCharColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                  ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarCharVector v,
                  SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      varCharVector = v;
      mutator = v.getMutator();
    }

    @Override
    public boolean setSafe(int index, DrillBuf bytebuf, int start, int length) {
      if (index >= varCharVector.getValueCapacity()) {
        return false;
      }

      if (usingDictionary) {
        currDictValToWrite = pageReader.dictionaryValueReader.readBytes();
        ByteBuffer buf = currDictValToWrite.toByteBuffer();
        mutator.setSafe(index, buf, buf.position(), currDictValToWrite.length());
      } else {
        mutator.setSafe(index, start, start + length, bytebuf);
      }
      return true;
    }

    @Override
    public int capacity() {
      return varCharVector.getBuffer().capacity();
    }
  }

  public static class NullableVarCharColumn extends NullableVarLengthValuesColumn<NullableVarCharVector> {

    int nullsRead;
    boolean currentValNull = false;
    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    protected final NullableVarCharVector.Mutator mutator;
    private final NullableVarCharVector vector;

    NullableVarCharColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                          ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableVarCharVector v,
                          SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      vector = v;
      this.mutator = vector.getMutator();
    }

    @Override
    public boolean setSafe(int index, DrillBuf value, int start, int length) {
      if (index >= vector.getValueCapacity()) {
        return false;
      }

      if (usingDictionary) {
        ByteBuffer buf = currDictValToWrite.toByteBuffer();
        mutator.setSafe(index, buf, buf.position(), currDictValToWrite.length());
      } else {
        mutator.setSafe(index, 1, start, start + length, value);
      }
      return true;
    }

    @Override
    public int capacity() {
      return vector.getBuffer().capacity();
    }
  }

  public static class VarBinaryColumn extends VarLengthValuesColumn<VarBinaryVector> {

    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    private final VarBinaryVector varBinaryVector;
    private final VarBinaryVector.Mutator mutator;

    VarBinaryColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                    ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarBinaryVector v,
                    SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      varBinaryVector = v;
      mutator = v.getMutator();
    }

    @Override
    public boolean setSafe(int index, DrillBuf value, int start, int length) {
      if (index >= varBinaryVector.getValueCapacity()) {
        return false;
      }

      if (usingDictionary) {
        currDictValToWrite = pageReader.dictionaryValueReader.readBytes();
        ByteBuffer buf = currDictValToWrite.toByteBuffer();
        mutator.setSafe(index, buf, buf.position(), currDictValToWrite.length());
      } else {
        mutator.setSafe(index, start, start + length, value);
      }
      return true;
    }

    @Override
    public int capacity() {
      return varBinaryVector.getBuffer().capacity();
    }
  }

  public static class NullableVarBinaryColumn extends NullableVarLengthValuesColumn<NullableVarBinaryVector> {

    int nullsRead;
    boolean currentValNull = false;
    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    private final NullableVarBinaryVector nullableVarBinaryVector;
    private final NullableVarBinaryVector.Mutator mutator;

    NullableVarBinaryColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableVarBinaryVector v,
                            SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      nullableVarBinaryVector = v;
      mutator = v.getMutator();
    }


    @Override
    public boolean setSafe(int index, DrillBuf value, int start, int length) {
      if (index >= nullableVarBinaryVector.getValueCapacity()) {
        return false;
      }

      if (usingDictionary) {
        ByteBuffer buf = currDictValToWrite.toByteBuffer();
        mutator.setSafe(index, buf, buf.position(), currDictValToWrite.length());
      } else {
        mutator.setSafe(index, 1, start, start + length, value);
      }
      return true;
    }

    @Override
    public int capacity() {
      return nullableVarBinaryVector.getBuffer().capacity();
    }

  }

}
