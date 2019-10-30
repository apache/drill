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

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ColumnReader;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.exec.vector.accessor.DictReader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DictReaderImpl implements DictReader, ReaderEvents {

  public static class DictObjectReader extends AbstractObjectReader {

    private final DictReaderImpl dictReader;

    public DictObjectReader(DictReaderImpl dictReader) {
      this.dictReader = dictReader;
    }

    @Override
    public DictReader dict() {
      return dictReader;
    }

    @Override
    public Object getObject() {
      return dictReader.getObject();
    }

    @Override
    public String getAsString() {
      return dictReader.getAsString();
    }

    @Override
    public ReaderEvents events() {
      return dictReader;
    }

    @Override
    public ColumnReader reader() {
      return dictReader;
    }
  }

  private final ColumnMetadata metadata;
  private final VectorAccessor accessor;

  private final AbstractObjectReader keyReader;
  private final AbstractObjectReader valueReader;
  private NullStateReader nullStateReader;

  private ArrayReaderImpl.ElementReaderIndex elementIndex;
  private final OffsetVectorReader offsetReader;

  public DictReaderImpl(ColumnMetadata metadata, VectorAccessor va, List<AbstractObjectReader> readers) {
    this.metadata = metadata;
    this.accessor = va;
    this.keyReader = readers.get(0);
    this.valueReader = readers.get(1);
    this.offsetReader = new OffsetVectorReader(VectorAccessors.arrayOffsetVectorAccessor(va));
  }

  public static DictObjectReader build(ColumnMetadata schema, VectorAccessor dictAccessor,
                                       List<AbstractObjectReader> readers) {
    DictReaderImpl dictReader = new DictReaderImpl(schema, dictAccessor, readers);
    dictReader.bindNullState(NullStateReaders.REQUIRED_STATE_READER);
    return new DictObjectReader(dictReader);
  }

  @Override
  public ObjectReader getValueReader(Object key) {
    if (find(key)) {
      return valueReader;
    }
    return NullReader.instance();
  }

  @Override
  public Object get(Object key) {
    return getValueReader(key).getObject();
  }

  private boolean find(Object key) {
    if (key == null) {
      throw new IllegalArgumentException("Key in DICT can not be null.");
    }

    resetPosition();
    while (next()) {
      if (key.equals(keyReader.getObject())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int size() {
    return offsetReader.vectorIndex.size();
  }

  @Override
  public ValueType keyColumnType() {
    return keyReader.scalar().valueType();
  }

  @Override
  public ObjectType valueColumnType() {
    return valueReader.type();
  }

  private boolean next() {
    if (!elementIndex.next()) {
      return false;
    }
    keyReader.events().reposition();
    valueReader.events().reposition();
    return true;
  }

  /**
   * Reset entry position
   */
  private void resetPosition() {
    elementIndex.rewind();
  }

  @Override
  public ColumnMetadata schema() {
    return metadata;
  }

  @Override
  public ObjectType type() {
    return ObjectType.ARRAY;
  }

  @Override
  public boolean isNull() {
    return false;
  }

  @Override
  public Map<Object, Object> getObject() {
    resetPosition();
    Map<Object, Object> map = new HashMap<>();
    while (next()) {
      map.put(keyReader.getObject(), valueReader.getObject());
    }
    return map;
  }

  @Override
  public String getAsString() {
    resetPosition();
    StringBuilder buf = new StringBuilder();
    buf.append("{");
    boolean comma = false;
    while (next()) {
      if (comma) {
        buf.append(", ");
      }
      buf.append(keyReader.getAsString())
          .append(':')
          .append(valueReader.getAsString());
      comma = true;
    }
    buf.append("}");
    return buf.toString();
  }

  @Override
  public void bindIndex(ColumnReaderIndex index) {
    accessor.bind(index);
    offsetReader.bindIndex(index);
    nullStateReader.bindIndex(index);

    elementIndex = new ArrayReaderImpl.ElementReaderIndex(index);
    keyReader.events().bindIndex(elementIndex);
    valueReader.events().bindIndex(elementIndex);
  }

  @Override
  public void bindNullState(NullStateReader nullStateReader) {
    this.nullStateReader = nullStateReader;
  }

  @Override
  public NullStateReader nullStateReader() {
    return nullStateReader;
  }

  @Override
  public void reposition() {
    long entry = offsetReader.getEntry();
    elementIndex.reset((int) (entry >> 32), (int) (entry));
  }
}
