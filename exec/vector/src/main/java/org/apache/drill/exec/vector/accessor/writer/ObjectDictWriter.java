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

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.accessor.DictWriter;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.complex.DictVector;
import org.apache.drill.exec.vector.complex.RepeatedDictVector;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

/**
 * The implementation represents the writer as an array writer
 * with special dict entry writer as its element writer.
 */
public class ObjectDictWriter extends ObjectArrayWriter implements DictWriter {

  public static class DictObjectWriter extends AbstractArrayWriter.ArrayObjectWriter {

    public DictObjectWriter(ObjectDictWriter dictWriter) {
      super(dictWriter);
    }

    @Override
    public DictWriter dict() {
      return (DictWriter) arrayWriter;
    }

    @Override
    public void dump(HierarchicalFormatter format) {
      format.startObject(this)
          .attribute("dictWriter");
      arrayWriter.dump(format);
      format.endObject();
    }
  }

  public static ObjectDictWriter.DictObjectWriter buildDict(ColumnMetadata metadata, DictVector vector,
                                                            List<AbstractObjectWriter> keyValueWriters) {
    DictEntryWriter.DictEntryObjectWriter entryObjectWriter = DictEntryWriter.buildDictEntryWriter(metadata, keyValueWriters);
    ObjectDictWriter objectDictWriter = new ObjectDictWriter(metadata, vector.getOffsetVector(), entryObjectWriter);
    return new ObjectDictWriter.DictObjectWriter(objectDictWriter);
  }

  public static ArrayObjectWriter buildDictArray(RepeatedDictVector vector, ColumnMetadata metadata,
                                                 List<AbstractObjectWriter> keyValueWriters) {
    final DictVector dataVector = (DictVector) vector.getDataVector();
    ObjectDictWriter.DictObjectWriter dictWriter = buildDict(metadata, dataVector, keyValueWriters);
    AbstractArrayWriter arrayWriter = new ObjectArrayWriter(metadata, vector.getOffsetVector(), dictWriter);
    return new ArrayObjectWriter(arrayWriter);
  }

  private static final int FIELD_KEY_ORDINAL = 0;
  private static final int FIELD_VALUE_ORDINAL = 1;

  private final DictEntryWriter.DictEntryObjectWriter entryObjectWriter;

  private ObjectDictWriter(ColumnMetadata schema, UInt4Vector offsetVector,
                           DictEntryWriter.DictEntryObjectWriter entryObjectWriter) {
    super(schema, offsetVector, entryObjectWriter);
    this.entryObjectWriter = entryObjectWriter;
  }

  @Override
  public ValueType keyType() {
    return tuple().scalar(FIELD_KEY_ORDINAL).valueType();
  }

  @Override
  public ObjectType valueType() {
    return tuple().type(FIELD_VALUE_ORDINAL);
  }

  @Override
  public ScalarWriter keyWriter() {
    return tuple().scalar(FIELD_KEY_ORDINAL);
  }

  @Override
  public ObjectWriter valueWriter() {
    return tuple().column(FIELD_VALUE_ORDINAL);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setObject(Object object) {
    if (object instanceof Map) {
      Map<Object, Object> map = (Map<Object, Object>) object;
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        entryObjectWriter.setObject(0, entry.getKey());
        entryObjectWriter.setObject(1, entry.getValue());
        save();
      }
    } else {
      if (object == null) {
        return;
      }
      int size = Array.getLength(object);
      for (int i = 0; i < size; i++) {
        Object value = Array.get(object, i);
        int col = i % 2; // 0 corresponds to key and 1 - to value
        entryObjectWriter.setObject(col, value);
        if (col == 1) { // save only after key-value pair is written
          save();
        }
      }
    }
  }
}
