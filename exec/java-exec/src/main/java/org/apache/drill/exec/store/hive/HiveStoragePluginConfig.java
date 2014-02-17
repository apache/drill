/**
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
package org.apache.drill.exec.store.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;

@JsonTypeName("hive")
public class HiveStoragePluginConfig extends StoragePluginConfigBase {
  @JsonProperty
  public Map<String, String> configProps;
  @JsonIgnore
  private HiveConf hiveConf;

  @JsonIgnore
  public HiveConf getHiveConf() {
    if (hiveConf == null) {
      hiveConf = new HiveConf();
      if (configProps != null) {
        for (Map.Entry<String, String> entry : configProps.entrySet()) {
          hiveConf.set(entry.getKey(), entry.getValue());
        }
      }
    }

    return hiveConf;
  }

  @JsonCreator
  public HiveStoragePluginConfig(@JsonProperty("config") Map<String, String> props) {
    this.configProps = props;
  }

  @Override
  public int hashCode() {
    return configProps != null ? configProps.hashCode() : 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HiveStoragePluginConfig that = (HiveStoragePluginConfig) o;

    if (configProps != null ? !configProps.equals(that.configProps) : that.configProps != null) return false;

    return true;
  }
}
