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
package org.apache.drill.exec.store.mpjdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.linq4j.Extensions;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.mock.MockGroupScanPOP;
import org.apache.drill.exec.store.mock.MockStorageEngine;
import org.apache.drill.exec.store.mock.MockStorageEngineConfig;
import org.apache.drill.exec.store.mock.MockGroupScanPOP.MockScanEntry;
import org.apache.drill.exec.store.mpjdbc.MPJdbcClient.OdbcSchema;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class MPJdbcFormatPlugin extends AbstractStoragePlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(MPJdbcFormatPlugin.class);

  private final MPJdbcFormatConfig storageConfig;
  protected String name = "odbc";
  private final DrillbitContext context;

  public MPJdbcFormatPlugin(MPJdbcFormatConfig storageConfig,
      DrillbitContext context, String name) {
    this.context = context;
    this.storageConfig = storageConfig;
    ObjectMapper mapper = new ObjectMapper();
    try {
      String result = mapper.writeValueAsString(storageConfig);
      logger.info(result);
    } catch (JsonProcessingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    this.name = name;
  }

  @Override
  public void registerSchemas(SchemaConfig config, SchemaPlus parent) {
    if(storageConfig == null) {
       logger.info("StorageConfig is null");
    }
    MPJdbcClientOptions options = new MPJdbcClientOptions(storageConfig);
    MPJdbcClient client = MPJdbcCnxnManager.getClient(storageConfig.getUri(),
        options,this);
    Connection conn = (client == null) ? null : client.getConnection();
    Map<String, Integer> schemas;
    if(client == null) {
      logger.info("Could not create client...");
    }
    OdbcSchema o = client.getSchema();
    SchemaPlus tl = parent.add(this.name, o);
    try {
      schemas = client.getSchemas();
      Set<Entry<String, Integer>> a = schemas.entrySet();
      Iterator<Entry<String, Integer>> aiter = a.iterator();
      while (aiter.hasNext()) {
        Entry<String, Integer> val = aiter.next();
        String catalog = val.getKey();
        OdbcSchema sc = client.getSchema(catalog);
        tl.add(catalog, sc);
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public MPJdbcFormatConfig getConfig() {
    logger.info("MPJdbcFormatPlugin:getConfig called");
    logger.info(storageConfig.toString());
    return storageConfig;
  }

  public DrillbitContext getContext() {
    return this.context;
  }

  public String getName() {
      return this.name;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }
/*
  @Override
  public AbstractGroupScan getPhysicalScan(String userName,JSONOptions selection)
      throws IOException {
    MPJdbcScanSpec odbcScanSpec = selection.getListWith(new ObjectMapper(),
        new TypeReference<MPJdbcScanSpec>() {
        });
    return new MPJdbcGroupScan(userName,this, odbcScanSpec, null);
  }
  */
  @Override
  public AbstractGroupScan getPhysicalScan(String userName,JSONOptions selection,List<SchemaPath> columns)
      throws IOException {
    MPJdbcScanSpec mPJdbcScanSpec = selection.getListWith(new ObjectMapper(),
        new TypeReference<MPJdbcScanSpec>() {
        });
    return new MPJdbcGroupScan(userName,this, mPJdbcScanSpec, columns);
  }

  @Override
  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    // TODO Auto-generated method stub
    return ImmutableSet.of(MPJdbcFilterRule.INSTANCE);
  }
}
