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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectionKey;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;

@JsonTypeName("hbase")
public class HBaseStoragePluginConfig extends StoragePluginConfigBase implements DrillHBaseConstants {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseStoragePluginConfig.class);

  @JsonProperty
  public String zookeeperQuorum;

  @JsonProperty
  public int zookeeperPort;

  private Configuration hbaseConf;
  private HConnectionKey hbaseConfKey;

  @JsonCreator
  public HBaseStoragePluginConfig(@JsonProperty("zookeeperQuorum") String zookeeperQuorum,
                                  @JsonProperty("zookeeperPort") int zookeeperPort) {
    this.zookeeperQuorum = zookeeperQuorum;
    this.zookeeperPort = zookeeperPort;

    this.hbaseConf = HBaseConfiguration.create();
    logger.debug("Configuring HBase StoragePlugin with zookeeper quorum '{}', port '{}' node '{}'.",
        zookeeperQuorum, zookeeperPort, hbaseConf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    if (zookeeperQuorum != null && zookeeperQuorum.length() != 0) {
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
      hbaseConf.setInt(HBASE_ZOOKEEPER_PORT, zookeeperPort);
    }
    this.hbaseConfKey = new HConnectionKey(hbaseConf);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HBaseStoragePluginConfig that = (HBaseStoragePluginConfig) o;
    return this.hbaseConfKey.equals(that.hbaseConfKey);
  }

  @Override
  public int hashCode() {
    return this.hbaseConfKey != null ? this.hbaseConfKey.hashCode() : 0;
  }

  @JsonIgnore
  public Configuration getHBaseConf() {
    return hbaseConf;
  }

  @JsonIgnore
  @VisibleForTesting
  public void setZookeeperPort(int zookeeperPort) {
    this.zookeeperPort = zookeeperPort;
    hbaseConf.setInt(HBASE_ZOOKEEPER_PORT, zookeeperPort);
  }

}
