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
import org.apache.drill.exec.ref.values.DataValue;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

import static org.apache.drill.hbase.HbaseUtils.nameFromBytes;

/**
 * A DataValue corresponding to a single column family within a row.
 */
public class HBaseFamilyValue extends ImmutableHBaseMapValue {

  private final Map<String, HBaseColumnValue> columnsMap = Maps.newLinkedHashMap();

  public HBaseFamilyValue(NavigableMap<byte[], NavigableMap<Long, byte[]>> columnsMap) {
    for (NavigableMap.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : columnsMap.entrySet()) {
      this.columnsMap.put(nameFromBytes(columnEntry.getKey()), new HBaseColumnValue(columnEntry.getValue()));
    }
  }

  @Override
  protected DataValue getByName(CharSequence name) {
    return columnsMap.get(name.toString());
  }

  @Override
  public Iterator<Map.Entry<CharSequence, DataValue>> iterator() {
    return null;
  }

  @Override
  public void write(DataWriter writer) throws IOException {
    writer.writeMapStart();
    for (Map.Entry<String, HBaseColumnValue> column : columnsMap.entrySet()) {
      writer.writeMapKey(column.getKey());
      writer.writeMapValueStart();
      column.getValue().write(writer);
      writer.writeMapValueEnd();
    }
    writer.writeMapEnd();
  }

  @Override
  public boolean equals(DataValue v) {
    return false;
  }

  @Override
  public int hashCode() {
    return columnsMap.hashCode();
  }

  @Override
  public DataValue copy() {
    return null;
  }


}
