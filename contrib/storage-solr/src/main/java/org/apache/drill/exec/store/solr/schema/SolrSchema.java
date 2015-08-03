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
package org.apache.drill.exec.store.solr.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.solr.SolrClientAPIExec;
import org.apache.drill.exec.store.solr.SolrScanSpec;
import org.apache.drill.exec.store.solr.SolrStoragePlugin;
import org.apache.drill.exec.store.solr.SolrStoragePluginConfig;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class SolrSchema extends AbstractSchema {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(SolrSchema.class);
  private final Set<String> availableSolrCores;
  private String currentSchema = "root";
  private final Map<String, DrillTable> drillTables = Maps.newHashMap();
  private final SolrStoragePlugin solrStoragePlugin;

  public SolrSchema(List<String> schemaPath,String currentSchema, SolrStoragePlugin solrStoragePlugin) {   
    super(schemaPath, currentSchema);
    this.solrStoragePlugin = solrStoragePlugin;
    availableSolrCores = solrStoragePlugin.getSolrClientApiExec().getSolrCoreList();
  }

  @Override
  public String getTypeName() {
    return SolrStoragePluginConfig.NAME;
  }

  void setHolder(SchemaPlus plusOfThis) {
    for (String solrCore : availableSolrCores) {
      plusOfThis.add("root", getSubSchema(solrCore));
    }

  }

  @Override
  public Table getTable(String coreName) {
    logger.info("SolrSchema :: getTable");
    if (!availableSolrCores.contains(coreName)) { // table does not exist
      return null;
    }

    if (!drillTables.containsKey(coreName)) {
      drillTables.put(coreName, this.getDrillTable(this.name, coreName));
    }

    return drillTables.get(coreName);
  }

  DrillTable getDrillTable(String dbName, String collectionName) {
    logger.info("SolrSchema :: getDrillTable");
    SolrScanSpec solrScanSpec = new SolrScanSpec(collectionName);
    return new DynamicDrillTable(solrStoragePlugin, SolrStoragePluginConfig.NAME, null, solrScanSpec);
  }

  @Override
  public Set<String> getTableNames() {
    logger.info("SolrSchema :: getTableNames");
    return availableSolrCores;
  }

}
