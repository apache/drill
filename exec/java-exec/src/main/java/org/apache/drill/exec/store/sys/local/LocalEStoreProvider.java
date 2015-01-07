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

package org.apache.drill.exec.store.sys.local;

import java.io.IOException;

import org.apache.drill.exec.store.sys.EStore;
import org.apache.drill.exec.store.sys.EStoreProvider;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreConfig.Mode;

import com.google.common.base.Preconditions;

public class LocalEStoreProvider implements EStoreProvider{

  @Override
  public <V> EStore<V> getStore(PStoreConfig<V> storeConfig) throws IOException {
    Preconditions.checkArgument(storeConfig.getMode() == Mode.EPHEMERAL, "Estore configurations must be set ephemeral.");

    return new MapEStore<V>();
  }

  @Override
  public void start() throws IOException {
  }

  @Override
  public void close() {
  }

}
