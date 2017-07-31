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
    public void set(Object value) {
      arrayWriter.setObject(value);
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

  /**
   * Index into the vector of elements for a repeated vector.
   * Keeps track of the current offset in terms of value positions.
   * Forwards overflow events to the base index.
   */

  public class ArrayElementWriterIndex implements ColumnWriterIndex {

    private final ColumnWriterIndex baseIndex;
    private int startOffset = 0;
    private int offset = 0;

    public ArrayElementWriterIndex(ColumnWriterIndex baseIndex) {
      this.baseIndex = baseIndex;
    }

    public ColumnWriterIndex baseIndex() { return baseIndex; }

    public void reset() {
      offset = 0;
      startOffset = 0;
    }

    public int endValue() {
      startOffset = offset;
      return offset;
    }

    @Override
    public int vectorIndex() { return offset; }

    @Override
    public void overflowed() {
      baseIndex.overflowed();
    }

    public int arraySize() {
      return offset - startOffset;
    }

    @Override
    public void nextElement() { offset++; }

    @Override
    public boolean legal() {
      return true;
    }
  }

  protected final AbstractObjectWriter elementObjWriter;
  private final OffsetVectorWriter offsetsWriter = new OffsetVectorWriter();
  private ColumnWriterIndex baseIndex;
  protected ArrayElementWriterIndex elementIndex;

  public AbstractArrayWriter(RepeatedValueVector vector, AbstractObjectWriter elementObjWriter) {
    this.elementObjWriter = elementObjWriter;
    offsetsWriter.bindVector(vector.getOffsetVector());
  }

  public void bindIndex(ColumnWriterIndex index) {
    baseIndex = index;
    offsetsWriter.bindIndex(index);
    elementIndex = new ArrayElementWriterIndex(baseIndex);
    elementObjWriter.bindIndex(elementIndex);
  }

  protected ColumnWriterIndex elementIndex() { return elementIndex; }

  @Override
  public int size() {
    return elementIndex.arraySize();
  }

  @Override
  public ObjectWriter entry() {
    return elementObjWriter;
  }

  @Override
  public void startWrite() {
    elementIndex.reset();
    elementObjWriter.startWrite();
  }

  @Override
  public void startValue() { }

  @Override
  public void endValue() {
    offsetsWriter.setOffset(elementIndex.endValue());
  }

  @Override
  public void endWrite() {
    offsetsWriter.finish();
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
