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

import java.io.File;
import java.io.IOException;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreProvider;

import com.typesafe.config.Config;

/**
 * A really simple provider that stores data in the local file system, one value per file.
 */
public class LocalPStoreProvider implements PStoreProvider{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalPStoreProvider.class);

  private File path;
  private final boolean enableWrite;

  public LocalPStoreProvider(Config config) {
    path = new File(config.getString(ExecConstants.SYS_STORE_PROVIDER_LOCAL_PATH));
    enableWrite = config.getBoolean(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE);
  }

  @Override
  public void close() {
  }

  @Override
  public <V> PStore<V> getPStore(PStoreConfig<V> storeConfig) throws IOException {
    if(enableWrite){
      return new LocalPStore<V>(path, storeConfig);
    }else{
      return new NoWriteLocalPStore<V>();
    }
  }

  @Override
  public void start() {
  }

}
