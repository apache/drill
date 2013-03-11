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

import org.apache.drill.exec.ref.values.DataValue;
import org.apache.hadoop.hbase.client.Result;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

import static org.apache.drill.hbase.DrillHBaseUtils.nameFromBytes;
import static org.apache.drill.hbase.DrillHBaseUtils.nameToBytes;

/**
 * A DataValue corresponding the whole row.
 */
public class HBaseResultValue extends ImmutableHBaseMapValue {

  private final Result result;

  public HBaseResultValue(Result result) {
    this.result = result;
  }

  @Override
  protected DataValue getByName(CharSequence name) {
    return new HBaseFamilyValue(result.getMap().get(nameToBytes(name)));
  }

  @Override
  public boolean equals(DataValue v) {
    return false;
  }

  @Override
  public int hashCode() {
    return result.hashCode();
  }

  @Override
  public DataValue copy() {
    return null;
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

  @Override
  public String toString() {
    return "HBaseResultValue{" +
      "result=" + result +
      '}';
  }
}
