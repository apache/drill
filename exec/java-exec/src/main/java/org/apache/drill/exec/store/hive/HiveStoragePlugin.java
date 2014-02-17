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

import java.io.IOException;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.hive.schema.HiveSchemaFactory;
import org.apache.hadoop.hive.conf.HiveConf;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HiveStoragePlugin extends AbstractStoragePlugin {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveStoragePlugin.class);
  
  private final HiveStoragePluginConfig config;
  private final HiveConf hiveConf;
  private final HiveSchemaFactory schemaFactory;
  private final DrillbitContext context;
  private final String name;

  public HiveStoragePlugin(HiveStoragePluginConfig config, DrillbitContext context, String name) throws ExecutionSetupException {
    this.config = config;
    this.context = context;
    this.schemaFactory = new HiveSchemaFactory(config, name, config.getHiveConf());
    this.hiveConf = config.getHiveConf();
    this.name = name;
  }

  public HiveStoragePluginConfig getConfig() {
    return config;
  }

  public String getName(){
    return name;
  }
  
  public DrillbitContext getContext() {
    return context;
  }

  @Override
  public HiveScan getPhysicalScan(Scan scan) throws IOException {
    HiveReadEntry hiveReadEntry = scan.getSelection().getListWith(new ObjectMapper(), new TypeReference<HiveReadEntry>(){});
    try {
      return new HiveScan(hiveReadEntry, this, null);
    } catch (ExecutionSetupException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Schema createAndAddSchema(SchemaPlus parent) {
    return schemaFactory.add(parent);
  }


}
