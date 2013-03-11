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
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A DataValue corresponding to a single column within a column family.
 */
public class HBaseColumnValue extends BaseDataValue {

  private final byte[] value;
  private final byte[] name;
  private final Long version;

  public HBaseColumnValue(byte[] columnName, Map.Entry<Long, byte[]> bytes) {
    this.name = checkNotNull(columnName);
    this.version = checkNotNull(bytes.getKey());
    this.value = checkNotNull(bytes.getValue());
  }

  @Override
  public void write(DataWriter writer) throws IOException {
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
