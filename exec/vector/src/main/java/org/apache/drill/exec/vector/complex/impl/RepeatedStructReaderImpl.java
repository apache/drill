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
package org.apache.drill.exec.vector.complex.impl;

import java.util.Map;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.holders.RepeatedStructHolder;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.RepeatedStructVector;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.StructWriter;

import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

@SuppressWarnings("unused")
public class RepeatedStructReaderImpl extends AbstractFieldReader{
  private static final int NO_VALUES = Integer.MAX_VALUE - 1;

  private final RepeatedStructVector vector;
  private final Map<String, FieldReader> fields = Maps.newHashMap();
  private int currentOffset;
  private int maxOffset;

  public RepeatedStructReaderImpl(RepeatedStructVector vector) {
    this.vector = vector;
  }

  @Override
  public FieldReader reader(String name) {
    FieldReader reader = fields.get(name);
    if (reader == null) {
      ValueVector child = vector.getChild(name);
      if (child == null) {
        reader = NullReader.INSTANCE;
      } else {
        reader = child.getReader();
      }
      fields.put(name, reader);
      reader.setPosition(currentOffset);
    }
    return reader;
  }

  @Override
  public FieldReader reader() {
    if (isNull()) {
      return NullReader.INSTANCE;
    }

    setChildrenPosition(currentOffset);
    return new SingleLikeRepeatedStructReaderImpl(vector, this);
  }

  @Override
  public void reset() {
    super.reset();
    currentOffset = 0;
    maxOffset = 0;
    for (FieldReader reader:fields.values()) {
      reader.reset();
    }
    fields.clear();
  }

  @Override
  public int size() {
    return isNull() ? 0 : maxOffset - currentOffset;
  }

  @Override
  public void setPosition(int index) {
    if (index < 0 || index == NO_VALUES) {
      currentOffset = NO_VALUES;
      return;
    }

    super.setPosition(index);
    RepeatedStructHolder h = new RepeatedStructHolder();
    vector.getAccessor().get(index, h);
    if (h.start == h.end) {
      currentOffset = NO_VALUES;
    } else {
      currentOffset = h.start - 1;
      maxOffset = h.end - 1;
      setChildrenPosition(currentOffset);
    }
  }

  public void setSinglePosition(int index, int childIndex) {
    super.setPosition(index);
    RepeatedStructHolder h = new RepeatedStructHolder();
    vector.getAccessor().get(index, h);
    if (h.start == h.end) {
      currentOffset = NO_VALUES;
    } else {
      int singleOffset = h.start + childIndex;
      assert singleOffset < h.end;
      currentOffset = singleOffset;
      maxOffset = singleOffset + 1;
      setChildrenPosition(singleOffset);
    }
  }

  @Override
  public boolean next() {
    if (currentOffset < maxOffset) {
      setChildrenPosition(++currentOffset);
      return true;
    } else {
      currentOffset = NO_VALUES;
      return false;
    }
  }

  public boolean isNull() {
    return currentOffset == NO_VALUES;
  }

  @Override
  public Object readObject() {
    return vector.getAccessor().getObject(idx());
  }

  @Override
  public MajorType getType() {
    return vector.getField().getType();
  }

  @Override
  public java.util.Iterator<String> iterator() {
    return vector.fieldNameIterator();
  }

  @Override
  public boolean isSet() {
    return true;
  }

  @Override
  public void copyAsValue(StructWriter writer) {
    if (isNull()) {
      return;
    }
    RepeatedStructWriter impl = (RepeatedStructWriter) writer;
    impl.container.copyFromSafe(idx(), impl.idx(), vector);
  }

  public void copyAsValueSingle(StructWriter writer) {
    if (isNull()) {
      return;
    }
    SingleStructWriter impl = (SingleStructWriter) writer;
    impl.container.copyFromSafe(currentOffset, impl.idx(), vector);
  }

  @Override
  public void copyAsField(String name, StructWriter writer) {
    if (isNull()) {
      return;
    }
    RepeatedStructWriter impl = (RepeatedStructWriter) writer.struct(name);
    impl.container.copyFromSafe(idx(), impl.idx(), vector);
  }

  private void setChildrenPosition(int index) {
    for (FieldReader r : fields.values()) {
      r.setPosition(index);
    }
  }
}
