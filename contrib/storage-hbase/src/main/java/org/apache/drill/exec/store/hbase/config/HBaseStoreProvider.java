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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.store.hbase.DrillHBaseConstants;
import org.apache.drill.exec.store.sys.Store;
import org.apache.drill.exec.store.sys.StoreConfig;
import org.apache.drill.exec.store.sys.StoreRegistry;
import org.apache.drill.exec.store.sys.store.provider.BaseStoreProvider;
import org.apache.drill.exec.store.sys.store.provider.LocalStoreProvider;
import org.apache.drill.exec.store.sys.store.provider.ZookeeperStoreProvider;
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

public class HBaseStoreProvider extends BaseStoreProvider {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseStoreProvider.class);

  static final byte[] FAMILY = Bytes.toBytes("s");

  static final byte[] QUALIFIER = Bytes.toBytes("d");

  private final String storeTableName;

  private Configuration hbaseConf;

  private HConnection connection;

  private HTableInterface table;

  private final boolean zkAvailable;
  private final LocalStoreProvider localStoreProvider;
  private final ZookeeperStoreProvider zkStoreProvider;

  public HBaseStoreProvider(StoreRegistry registry) throws StoreException {
    @SuppressWarnings("unchecked")
    final Map<String, Object> config = (Map<String, Object>) registry.getConfig().getAnyRef(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_CONFIG);
    this.hbaseConf = HBaseConfiguration.create();
    this.hbaseConf.set(HConstants.HBASE_CLIENT_INSTANCE_ID, "drill-hbase-persistent-store-client");
    if (config != null) {
      for (Map.Entry<String, Object> entry : config.entrySet()) {
        this.hbaseConf.set(entry.getKey(), String.valueOf(entry.getValue()));
      }
    }
    this.storeTableName = registry.getConfig().getString(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_TABLE);

    final ClusterCoordinator coord = registry.getCoordinator();
    if ((coord instanceof ZKClusterCoordinator)) {
      this.localStoreProvider = null;
      this.zkStoreProvider = new ZookeeperStoreProvider((StoreRegistry<ZKClusterCoordinator>)registry);
      this.zkAvailable = true;
    } else {
      this.localStoreProvider = new LocalStoreProvider(registry);
      this.zkStoreProvider = null;
      this.zkAvailable = false;
    }

  }

  @VisibleForTesting
  public HBaseStoreProvider(Configuration conf, String storeTableName) throws StoreException {
    this.hbaseConf = conf;
    this.storeTableName = storeTableName;
    this.localStoreProvider = new LocalStoreProvider(DrillConfig.create());
    this.zkStoreProvider = null;
    this.zkAvailable = false;
  }



  @Override
  public <V> Store<V> getStore(StoreConfig<V> config) throws StoreException {
    switch(config.getMode()){
    case EPHEMERAL:
      if (this.zkAvailable) {
        return zkStoreProvider.getStore(config);
      } else {
        return localStoreProvider.getStore(config);
      }

    case BLOB_PERSISTENT:
    case PERSISTENT:
      return new HBasePersistentStore<V>(config, this.table);

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
