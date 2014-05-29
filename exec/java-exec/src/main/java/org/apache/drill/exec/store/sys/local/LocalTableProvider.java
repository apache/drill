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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.sys.PTable;
import org.apache.drill.exec.store.sys.PTableConfig;
import org.apache.drill.exec.store.sys.TableProvider;

/**
 * A really simple provider that stores data in the local file system, one value per file.
 */
public class LocalTableProvider implements TableProvider{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalTableProvider.class);

  private File path;
  private final boolean enableWrite;

  public LocalTableProvider(DrillConfig config){
    path = new File(config.getString(ExecConstants.SYS_TABLES_LOCAL_PATH));
    enableWrite = config.getBoolean(ExecConstants.SYS_TABLES_LOCAL_ENABLE_WRITE);
  }

  @Override
  public void close() {
  }

  @Override
  public <V> PTable<V> getPTable(PTableConfig<V> table) throws IOException {
    if(enableWrite){
      return new LocalTable<V>(path, table);
    }else{
      return new NoWriteLocalTable<V>();
    }
  }

  @Override
  public void start() {
  }


}
