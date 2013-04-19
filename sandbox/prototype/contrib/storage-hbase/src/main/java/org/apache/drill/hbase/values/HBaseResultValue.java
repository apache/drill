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
package org.apache.drill.hbase.values;

import com.google.common.collect.Maps;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.drill.exec.ref.values.BytesValue;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.hbase.HbaseUtils;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

import static org.apache.drill.hbase.HbaseUtils.nameFromBytes;

/**
 * A DataValue corresponding the whole row.
 */
public class HBaseResultValue extends ImmutableHBaseMapValue {

  public static final String ROW_KEY = "rowKey";

  private final Result result;
  private final BytesValue rowKey;
  private final Map<String, HBaseFamilyValue> families = Maps.newLinkedHashMap();

  public HBaseResultValue(Result result) {
    this.result = result;
    this.rowKey = new BytesDataValue(this.result.getRow());
    for (NavigableMap.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> family : result.getMap().entrySet()) {
      this.families.put(HbaseUtils.nameFromBytes(family.getKey()), new HBaseFamilyValue(family.getValue()));
    }
  }

  @Override
  protected DataValue getByName(CharSequence name) {
    if (name.equals(ROW_KEY)) {
      return rowKey;
    }
    return families.get(name.toString());
  }

  @Override
  public DataValue copy() {
    Result newResult = new Result();
    newResult.copyFrom(result);
    return new HBaseResultValue(newResult);
  }

  @Override
  public void write(DataWriter writer) throws IOException {
    writer.writeMapStart();
    writer.writeMapKey(ROW_KEY);
    writer.writeMapValueStart();
    writer.writeBytes(rowKey.getAsArray());
    writer.writeMapValueEnd();
    for (Map.Entry<String, HBaseFamilyValue> familyEntry : families.entrySet()) {
      writer.writeMapKey(familyEntry.getKey());
      writer.writeMapValueStart();
      familyEntry.getValue().write(writer);
      writer.writeMapValueEnd();
    }
    writer.writeMapEnd();
  }

  @Override
  public String toString() {
    return "HBaseResultValue{" +
      "result=" + result +
      '}';
  }

  public boolean equals(DataValue o) {
    if (this == o) return true;
    if (!(o instanceof HBaseResultValue)) return false;
    HBaseResultValue entries = (HBaseResultValue) o;
    if (result != null ? !result.equals(entries.result) : entries.result != null) return false;
    return true;
  }

  @Override
  public int hashCode() {
    return result.hashCode();
  }

  @Override
  public Iterator<Map.Entry<CharSequence, DataValue>> iterator() {
    return new HBaseColumnFamilyIterator(result.getMap().entrySet().iterator());
  }

  private static class HBaseColumnFamilyIterator implements Iterator<Map.Entry<CharSequence, DataValue>> {


    private final Iterator<Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> original;

    HBaseColumnFamilyIterator(Iterator<Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> original) {
      this.original = original;
    }

    @Override
    public boolean hasNext() {
      return original.hasNext();
    }

    @Override
    public Map.Entry<CharSequence, DataValue> next() {
      final Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> originalEntry = original.next();
      return new Map.Entry<CharSequence, DataValue>() {
        @Override
        public CharSequence getKey() {
          return nameFromBytes(originalEntry.getKey());
        }

        @Override
        public DataValue getValue() {
          return new HBaseFamilyValue(originalEntry.getValue());
        }

        @Override
        public DataValue setValue(DataValue value) {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public void remove() {
      original.remove();
    }
  }
}
