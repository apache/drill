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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.common.types.TypeProtos;

public class FieldInfo {
  String parquetType;
  String name;
  int bitLength;
  int numberOfPages;
  Object[] values;
  TypeProtos.MinorType type;

  FieldInfo(String parquetType, String name, int bitLength, Object[] values, TypeProtos.MinorType type, ParquetTestProperties props){
    this.parquetType = parquetType;
    this.name = name;
    this.bitLength  = bitLength;
    this.numberOfPages = Math.max(1, (int) Math.ceil( ((long) props.recordsPerRowGroup) * bitLength / 8.0 / props.bytesPerPage));
    this.values = values;
    // generator is designed to use 3 values
    assert values.length == 3;
    this.type = type;
  }
}
