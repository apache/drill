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

import java.util.List;
import java.util.Map;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;

/**
 * Writer for a Dict entry. The entry is a special kind of tuple.
 */
public class DictEntryWriter extends AbstractTupleWriter {

  public static class DictEntryObjectWriter extends TupleObjectWriter {

    public DictEntryObjectWriter(DictEntryWriter entryWriter) {
      super(entryWriter);
    }

    public void setObject(int col, Object value) {
      tupleWriter.set(col, value);
    }

    @Override
    public void dump(HierarchicalFormatter format) {
      format.startObject(this)
          .attribute("dictEntryWriter");
      tupleWriter.dump(format);
      format.endObject();
    }
  }

  public static DictEntryObjectWriter buildDictEntryWriter(ColumnMetadata schema,
                                                           List<AbstractObjectWriter> keyValueWriters) {
    assert keyValueWriters.size() == 2;
    DictEntryWriter dictEntryWriter = new DictEntryWriter(schema, keyValueWriters);
    return new DictEntryObjectWriter(dictEntryWriter);
  }

  private final ColumnMetadata dictColumnSchema;

  public DictEntryWriter(ColumnMetadata schema, List<AbstractObjectWriter> writers) {
    super(schema.tupleSchema(), writers);
    dictColumnSchema = schema;
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {

    // Similarly to a repeated map, the provided index is an array element
    // index. Convert this to an index that will not increment the element
    // index on each write so that a dict with key and value members won't
    // increment the index for each member. Rather, the index must be
    // incremented at the array level.

    bindIndex(index, new MemberWriterIndex(index));
  }

  @Override
  public boolean isProjected() {
    return true;
  }

  @Override
  public ColumnMetadata schema() {
    return dictColumnSchema;
  }

  @Override
  public void setObject(Object value) {
    if (value instanceof Map.Entry) {
      Map.Entry entry = (Map.Entry) value;
      set(0, entry.getKey());
      set(1, entry.getValue());
    } else {
      super.setObject(value);
    }
  }
}
