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
package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Writer for an array-valued column. This writer appends values: once a value
 * is written, it cannot be changed. As a result, writer methods have no item
 * index; each set advances the array to the next position.
 * <p>
 * This class represents the array as a whole. In practice that means building
 * the offset vector. The array is associated with an element object that
 * manages writing to the scalar, array or tuple that is the array element. Note
 * that this representation makes little use of the methods in the "Repeated"
 * vector class: instead it works directly with the offset and element vectors.
 */

public abstract class AbstractArrayWriter implements ArrayWriter, WriterEvents {

  /**
   * Object representation of an array writer.
   */

  public static class ArrayObjectWriter extends AbstractObjectWriter {

    private AbstractArrayWriter arrayWriter;

    public ArrayObjectWriter(AbstractArrayWriter arrayWriter) {
      this.arrayWriter = arrayWriter;
    }

    @Override
    public void bindIndex(ColumnWriterIndex index) {
      arrayWriter.bindIndex(index);
    }

    @Override
    public ObjectType type() {
      return ObjectType.ARRAY;
    }

    @Override
    public void set(Object value) throws VectorOverflowException {
      arrayWriter.setArray(value);
    }

    public void start() {
      arrayWriter.startWrite();
    }

    @Override
    public ArrayWriter array() {
      return arrayWriter;
    }

    @Override
    public void startWrite() {
      arrayWriter.startWrite();
    }

    @Override
    public void startValue() {
      arrayWriter.startValue();
    }

    @Override
    public void endValue() {
      arrayWriter.endValue();
    }

    @Override
    public void endWrite() {
      arrayWriter.endWrite();
    }
  }

  protected final AbstractObjectWriter elementObjWriter;
  private final UInt4Vector.Mutator mutator;
  private ColumnWriterIndex baseIndex;
  protected FixedWidthElementWriterIndex elementIndex;
  private int lastWritePosn = 0;

  public AbstractArrayWriter(RepeatedValueVector vector, AbstractObjectWriter elementObjWriter) {
    this.elementObjWriter = elementObjWriter;
    mutator = vector.getOffsetVector().getMutator();
  }

  public void bindIndex(ColumnWriterIndex index) {
    baseIndex = index;
    elementIndex = new FixedWidthElementWriterIndex(baseIndex);
    elementObjWriter.bindIndex(elementIndex);
  }

  protected ElementWriterIndex elementIndex() { return elementIndex; }

  @Override
  public int size() {
    return elementIndex.arraySize();
  }

  private void setOffset(int posn, int offset) {
    mutator.setSafe(posn, offset);
  }

  @Override
  public ObjectWriter entry() {
    return elementObjWriter;
  }

  @Override
  public void startWrite() {
    elementIndex.reset();
    setOffset(0, 0);
    elementObjWriter.startWrite();
  }

  @Override
  public void startValue() { fillEmpties(); }

  private void fillEmpties() {
    final int curPosn = elementIndex.vectorIndex();
    while (lastWritePosn < baseIndex.vectorIndex()) {
      lastWritePosn++;
      setOffset(lastWritePosn, curPosn);
    }
  }

  @Override
  public void endValue() {
    assert lastWritePosn == baseIndex.vectorIndex();
    setOffset(lastWritePosn + 1, elementIndex.vectorIndex());
  }

  @Override
  public void endWrite() {
    fillEmpties();
    mutator.setValueCount(elementIndex.vectorIndex());
    elementObjWriter.endWrite();
  }

  @Override
  public ObjectType entryType() {
    return elementObjWriter.type();
  }

  @Override
  public ScalarWriter scalar() {
    return elementObjWriter.scalar();
  }

  @Override
  public TupleWriter tuple() {
    return elementObjWriter.tuple();
  }

  @Override
  public ArrayWriter array() {
    return elementObjWriter.array();
  }
}
