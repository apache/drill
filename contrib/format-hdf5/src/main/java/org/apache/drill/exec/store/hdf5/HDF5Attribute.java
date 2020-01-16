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
package org.apache.drill.exec.store.hdf5;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;

/**
 * This class represents an HDF5 attribute and is used when the attributes are projected.
 */
public class HDF5Attribute {
  private final MinorType dataType;
  private final String key;
  private final Object value;

  public HDF5Attribute(TypeProtos.MinorType type, String key, Object value) {
    this.dataType = type;
    this.key = key;
    this.value = value;
  }

  public MinorType getDataType(){ return dataType; }

  public String getKey() { return key; }

  public Object getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("%s: %s type: %s", getKey(), getValue(), getDataType());
  }
}
