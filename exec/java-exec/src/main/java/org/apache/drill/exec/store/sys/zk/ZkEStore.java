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
package org.apache.drill.exec.store.sys.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.drill.exec.store.sys.EStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;

/**
 * Implementation of EStore using Zookeeper's EPHEMERAL node.
 * @param <V>
 */
public class ZkEStore<V> extends ZkAbstractStore<V> implements EStore<V>{

  public ZkEStore(CuratorFramework framework, PStoreConfig<V> config) throws IOException{
    super(framework,config);
  }

  @Override
  public void delete(String key) {
    try {
      if (framework.checkExists().forPath(p(key)) != null) {
        framework.delete().forPath(p(key));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failure while accessing Zookeeper. " + e.getMessage(), e);
    }
  }

  @Override
  public void createNodeInZK(String key, V value) {
    try {
      framework.create().withMode(CreateMode.EPHEMERAL).forPath(p(key), config.getSerializer().serialize(value));
    } catch (Exception e) {
      throw new RuntimeException("Failure while accessing Zookeeper", e);
    }
  }
}
