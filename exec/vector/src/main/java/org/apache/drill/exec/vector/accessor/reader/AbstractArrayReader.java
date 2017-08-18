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
package org.apache.drill.exec.vector.accessor.reader;

import org.apache.drill.exec.vector.UInt4Vector.Accessor;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Reader for an array-valued column. This reader provides access to specific
 * array members via an array index. This is an abstract base class;
 * subclasses are generated for each repeated value vector type.
 */

public abstract class AbstractArrayReader implements ArrayReader {

  /**
   * Object representation of an array reader.
   */

  public static class ArrayObjectReader extends AbstractObjectReader {

    private AbstractArrayReader arrayReader;

    public ArrayObjectReader(AbstractArrayReader arrayReader) {
      this.arrayReader = arrayReader;
    }

    @Override
    public void bindIndex(ColumnReaderIndex index) {
      arrayReader.bindIndex(index);
    }

    @Override
    public ObjectType type() {
      return ObjectType.ARRAY;
    }

    @Override
    public ArrayReader array() {
      return arrayReader;
    }

    @Override
    public ScalarElementReader elements() {
      return arrayReader.elements();
    }

    @Override
    public Object getObject() {
      return arrayReader.getObject();
    }

    @Override
    public String getAsString() {
      return arrayReader.getAsString();
    }

    @Override
    public void reposition() {
      arrayReader.reposition();
    }
  }

  public static class BaseElementIndex {
    private final ColumnReaderIndex base;
    protected int startOffset;
    protected int length;

    public BaseElementIndex(ColumnReaderIndex base) {
      this.base = base;
    }

    public int batchIndex() {
      return base.batchIndex();
    }

    public void reset(int startOffset, int length) {
      assert length >= 0;
      assert startOffset >= 0;
      this.startOffset = startOffset;
      this.length = length;
    }

    public int size() { return length; }

    public int elementIndex(int index) {
      if (index < 0 || length <= index) {
        throw new IndexOutOfBoundsException("Index = " + index + ", length = " + length);
      }
      return startOffset + index;
    }
  }

  private final Accessor accessor;
  private final VectorAccessor vectorAccessor;
  protected ColumnReaderIndex baseIndex;
  protected BaseElementIndex elementIndex;

  public AbstractArrayReader(RepeatedValueVector vector) {
    accessor = vector.getOffsetVector().getAccessor();
    vectorAccessor = null;
  }

  public AbstractArrayReader(VectorAccessor vectorAccessor) {
    accessor = null;
    this.vectorAccessor = vectorAccessor;
  }

  public void bindIndex(ColumnReaderIndex index) {
    baseIndex = index;
    if (vectorAccessor != null) {
      vectorAccessor.bind(index);
    }
  }

  private Accessor accessor() {
    if (accessor != null) {
      return accessor;
    }
    return ((RepeatedValueVector) (vectorAccessor.vector())).getOffsetVector().getAccessor();
  }

  public void reposition() {
    final int index = baseIndex.vectorIndex();
    final Accessor curAccesssor = accessor();
    final int startPosn = curAccesssor.get(index);
    elementIndex.reset(startPosn, curAccesssor.get(index + 1) - startPosn);
  }

  @Override
  public int size() { return elementIndex.size(); }

  @Override
  public ScalarElementReader elements() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ObjectReader entry(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TupleReader tuple(int index) {
    return entry(index).tuple();
  }

  @Override
  public ArrayReader array(int index) {
    return entry(index).array();
  }

  @Override
  public ObjectReader entry() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TupleReader tuple() {
    return entry().tuple();
  }

  @Override
  public ArrayReader array() {
    return entry().array();
  }
}
