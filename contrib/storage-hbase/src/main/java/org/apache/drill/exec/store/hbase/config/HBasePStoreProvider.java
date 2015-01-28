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
package org.apache.drill.exec.store.hbase.config;

import java.io.IOException;
import java.util.Map;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.store.hbase.DrillHBaseConstants;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.store.sys.PStoreRegistry;
import org.apache.drill.exec.store.sys.local.LocalEStoreProvider;
import org.apache.drill.exec.store.sys.zk.ZkEStoreProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.annotations.VisibleForTesting;

public class HBasePStoreProvider implements PStoreProvider {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBasePStoreProvider.class);

  static final byte[] FAMILY = Bytes.toBytes("s");

  static final byte[] QUALIFIER = Bytes.toBytes("d");

  private final String storeTableName;

  private Configuration hbaseConf;

  private HConnection connection;

  private HTableInterface table;

  private final boolean zkAvailable;
  private final LocalEStoreProvider localEStoreProvider;
  private final ZkEStoreProvider zkEStoreProvider;

  public HBasePStoreProvider(PStoreRegistry registry) {
    @SuppressWarnings("unchecked")
    Map<String, Object> config = (Map<String, Object>) registry.getConfig().getAnyRef(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_CONFIG);
    this.hbaseConf = HBaseConfiguration.create();
    this.hbaseConf.set(HConstants.HBASE_CLIENT_INSTANCE_ID, "drill-hbase-persistent-store-client");
    if (config != null) {
      for (Map.Entry<String, Object> entry : config.entrySet()) {
        this.hbaseConf.set(entry.getKey(), String.valueOf(entry.getValue()));
      }
    }
    this.storeTableName = registry.getConfig().getString(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_TABLE);

    ClusterCoordinator coord = registry.getClusterCoordinator();
    if ((coord instanceof ZKClusterCoordinator)) {
      this.localEStoreProvider = null;
      this.zkEStoreProvider = new ZkEStoreProvider(((ZKClusterCoordinator)registry.getClusterCoordinator()).getCurator());
      this.zkAvailable = true;
    } else {
      this.localEStoreProvider = new LocalEStoreProvider();
      this.zkEStoreProvider = null;
      this.zkAvailable = false;
    }

  }

  @VisibleForTesting
  public HBasePStoreProvider(Configuration conf, String storeTableName) {
    this.hbaseConf = conf;
    this.storeTableName = storeTableName;
    this.localEStoreProvider = new LocalEStoreProvider();
    this.zkEStoreProvider = null;
    this.zkAvailable = false;
  }



  @Override
  public <V> PStore<V> getStore(PStoreConfig<V> config) throws IOException {
    switch(config.getMode()){
    case EPHEMERAL:
      if (this.zkAvailable) {
        return zkEStoreProvider.getStore(config);
      } else {
        return localEStoreProvider.getStore(config);
      }

    case BLOB_PERSISTENT:
    case PERSISTENT:
      return new HBasePStore<V>(config, this.table);

    default:
      throw new IllegalStateException();
    }
  }


  @Override
  @SuppressWarnings("deprecation")
  public void start() throws IOException {
    this.connection = HConnectionManager.createConnection(hbaseConf);

    try(HBaseAdmin admin = new HBaseAdmin(connection)) {
      if (!admin.tableExists(storeTableName)) {
        HTableDescriptor desc = new HTableDescriptor(storeTableName);
        desc.addFamily(new HColumnDescriptor(FAMILY).setMaxVersions(1));
        admin.createTable(desc);
      } else {
        HTableDescriptor desc = admin.getTableDescriptor(Bytes.toBytes(storeTableName));
        if (!desc.hasFamily(FAMILY)) {
          throw new DrillRuntimeException("The HBase table " + storeTableName
              + " specified as persistent store exists but does not contain column family: "
              + (Bytes.toString(FAMILY)));
        }
      }
    }

    this.table = connection.getTable(storeTableName);
    this.table.setAutoFlush(true);
  }

  @Override
  public synchronized void close() {
    if (this.table != null) {
      try {
        this.table.close();
        this.table = null;
      } catch (IOException e) {
        logger.warn("Caught exception while closing HBase table.", e);
      }
    }
    if (this.connection != null && !this.connection.isClosed()) {
      try {
        this.connection.close();
      } catch (IOException e) {
        logger.warn("Caught exception while closing HBase connection.", e);
      }
      this.connection = null;
    }
  }

}
