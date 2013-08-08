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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StorageEngineConfigBase;

import java.util.HashMap;

@JsonTypeName("parquet")
public class ParquetStorageEngineConfig extends StorageEngineConfigBase {

  public String getDfsName() {
    return dfsName;
  }

  // information needed to identify an HDFS instance
  private String dfsName;
  private HashMap<String,String> map;

  @JsonCreator
  public ParquetStorageEngineConfig(@JsonProperty("dfsName") String dfsName) {
    this.dfsName = dfsName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ParquetStorageEngineConfig that = (ParquetStorageEngineConfig) o;

    if (dfsName != null ? !dfsName.equals(that.dfsName) : that.dfsName != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return dfsName != null ? dfsName.hashCode() : 0;
  }
  public void set(String key, String value) {
    map.put(key, value);
  }

  public String get(String key) {
    return map.get(key);
  }
}
