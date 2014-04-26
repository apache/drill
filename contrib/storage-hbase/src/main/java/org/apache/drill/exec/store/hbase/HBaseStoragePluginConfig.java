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
package org.apache.drill.exec.store.hbase;

import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonIgnore;

@JsonTypeName("hbase")
public class HBaseStoragePluginConfig extends StoragePluginConfigBase {

  @JsonIgnore
  public Configuration conf;

  @JsonProperty
  public String zookeeperQuorum;
  @JsonProperty
  public int zookeeperPort;

  @JsonCreator
  public HBaseStoragePluginConfig(@JsonProperty("zookeeperQuorum") String zookeeperQuorum,
                                  @JsonProperty("zookeeperPort") int zookeeperPort) {
    this.zookeeperQuorum = zookeeperQuorum;
    this.zookeeperPort = zookeeperPort;
    conf = HBaseConfiguration.create();
    if (zookeeperQuorum != null && zookeeperQuorum.length() != 0) {
      conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
      conf.setInt("hbase.zookeeper.property.clientPort", zookeeperPort);
    }
  }

  /*
  @JsonIgnore
  public Configuration getConf() {
    return conf;
  }
  */

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HBaseStoragePluginConfig that = (HBaseStoragePluginConfig) o;

    if (conf != null ? !conf.equals(that.conf) : that.conf != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return conf != null ? conf.hashCode() : 0;
  }
}
