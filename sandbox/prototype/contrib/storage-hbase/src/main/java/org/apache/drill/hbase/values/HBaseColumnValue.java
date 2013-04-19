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

import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.drill.exec.ref.values.BaseDataValue;
import org.apache.drill.exec.ref.values.DataValue;

import java.io.IOException;
import java.util.NavigableMap;

/**
 * A DataValue corresponding to a single column within a column family.
 */
public class HBaseColumnValue extends BaseDataValue {

  private final NavigableMap<Long, byte[]> versions;

  public HBaseColumnValue(NavigableMap<Long, byte[]> versions) {
    this.versions = versions;
  }

  @Override
  public void write(DataWriter writer) throws IOException {
    if (versions.size() == 1) {
      writer.writeMapStart();
      writer.writeMapKey("timestamp");
      writer.writeMapValueStart();
      writer.writeSInt64(versions.lastEntry().getKey());
      writer.writeMapValueEnd();
      writer.writeMapKey("value");
      writer.writeMapValueStart();
      writer.writeBytes(versions.lastEntry().getValue());
      writer.writeMapValueEnd();
      writer.writeMapEnd();
      return;
    }
    throw new UnsupportedOperationException("Multi-version columns are not supported yet");
  }

  @Override
  public DataType getDataType() {
    return null;
  }

  @Override
  public boolean equals(DataValue v) {
    return false;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public DataValue copy() {
    return null;
  }
}
