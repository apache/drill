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
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.exec.vector.accessor.DictReader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DictReaderImpl extends ArrayReaderImpl implements DictReader, ReaderEvents {

  public static class DictObjectReader extends ArrayObjectReader {

    private final DictReaderImpl dictReader;

    public DictObjectReader(DictReaderImpl dictReader) {
      super(dictReader);
      this.dictReader = dictReader;
    }

    @Override
    public DictReader dict() {
      return (DictReader) array();
    }

    @Override
    protected AbstractObjectReader createNullReader() {
      return new DictObjectReader(dictReader.getNullReader());
    }
  }

  private final AbstractObjectReader keyReader;
  private final AbstractObjectReader valueReader;

  /**
   * The value reader instance which will be returned in case of not found {@code value}
   */
  private AbstractObjectReader nullValueReader;

  public DictReaderImpl(ColumnMetadata metadata, VectorAccessor va, AbstractTupleReader.TupleObjectReader entryObjectReader) {
    super(metadata, va, entryObjectReader);
    DictEntryReader reader = (DictEntryReader) entryObjectReader.reader();
    this.keyReader = reader.keyReader();
    this.valueReader = reader.valueReader();
  }

  public static DictObjectReader build(ColumnMetadata schema, VectorAccessor dictAccessor,
                                       List<AbstractObjectReader> readers) {
    AbstractTupleReader.TupleObjectReader entryReader = DictEntryReader.build(schema, dictAccessor, readers);
    DictReaderImpl dictReader = new DictReaderImpl(schema, dictAccessor, entryReader);
    dictReader.bindNullState(NullStateReaders.REQUIRED_STATE_READER);
    return new DictObjectReader(dictReader);
  }

  @Override
  public ObjectReader getValueReader(Object key) {
    if (find(key)) {
      return valueReader;
    }

    if (nullValueReader == null) {
      nullValueReader = valueReader.createNullReader();
    }
    return nullValueReader;
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
  public ValueType keyColumnType() {
    return keyReader.scalar().valueType();
  }

  @Override
  public ObjectType valueColumnType() {
    return valueReader.type();
  }

  /**
   * Reset entry position
   */
  private void resetPosition() {
    rewind();
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

  private DictReaderImpl getNullReader() {
    return new NullDictReader(schema(), (AbstractTupleReader.TupleObjectReader) elementReader.createNullReader());
  }

  private static class NullDictReader extends DictReaderImpl {

    private NullDictReader(ColumnMetadata metadata, AbstractTupleReader.TupleObjectReader entryObjectReader) {
      super(metadata, null, entryObjectReader);
      this.nullStateReader = NullStateReaders.NULL_STATE_READER;
    }

    @Override
    public void bindIndex(ColumnReaderIndex index) {
    }

    @Override
    public void bindNullState(NullStateReader nullStateReader) {
    }

    @Override
    public void bindBuffer() {
    }

    @Override
    public boolean isNull() {
      return true;
    }

    @Override
    public void reposition() {
    }

    @Override
    public Map<Object, Object> getObject() {
      return null;
    }

    @Override
    public String getAsString() {
      return "null";
    }
  }
}
