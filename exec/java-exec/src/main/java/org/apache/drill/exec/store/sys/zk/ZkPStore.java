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

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.zookeeper.CreateMode;

/**
 * Implementation of PStore using Zookeeper's PERSISTENT node.
 * @param <V>
 */
public class ZkPStore<V> extends ZkAbstractStore<V> implements PStore<V> {

  public ZkPStore(CuratorFramework framework, PStoreConfig<V> config) throws IOException {
    super(framework, config);
  }

  @Override
  protected CreateMode getCreateMode() {
    return CreateMode.PERSISTENT;
  }
}
