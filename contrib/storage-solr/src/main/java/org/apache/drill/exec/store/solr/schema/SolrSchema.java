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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.solr.SolrScanSpec;
import org.apache.drill.exec.store.solr.SolrStoragePlugin;
import org.apache.drill.exec.store.solr.SolrStoragePluginConfig;
import org.apache.drill.exec.store.solr.SolrStorageProperties;
import org.apache.drill.exec.store.solr.datatype.SolrDataType;
import org.apache.drill.exec.store.sys.StaticDrillTable;
import org.apache.http.client.ClientProtocolException;

import com.google.common.collect.Maps;

public class SolrSchema extends AbstractSchema {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(SolrSchema.class);
  private final Set<String> availableSolrCores;
  private String currentSchema = "root";
  private final Map<String, DrillTable> drillTables = Maps.newHashMap();
  private final Map<String, SolrSchemaPojo> schemaMap = Maps.newHashMap();
  private final SolrStoragePlugin solrStoragePlugin;
  private final List<String> schemaPath;

  public SolrSchema(List<String> schemaPath, String currentSchema,
      SolrStoragePlugin solrStoragePlugin) {
    super(schemaPath, currentSchema);
    this.schemaPath = schemaPath;
    this.currentSchema = currentSchema;
    this.solrStoragePlugin = solrStoragePlugin;
    availableSolrCores = solrStoragePlugin.getSolrClientApiExec()
        .getSolrCoreList();
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
    logger.debug("SolrSchema :: getTable");
    SolrSchemaPojo oCVSchema = null;
    DrillTable drillTable = null;
    if (!availableSolrCores.contains(coreName)) { // table does not exist
      return null;
    }
    if (!drillTables.containsKey(coreName)) {
      if (schemaMap.containsKey(coreName)) {
        logger.debug("schema found in cached map for core : " + coreName);
        oCVSchema = schemaMap.get(coreName);

      } else {
        String solrServerUrl = this.solrStoragePlugin.getSolrStorageConfig()
            .getSolrServer();
        String schemaUrl = this.solrStoragePlugin.getSolrStorageConfig()
            .getSolrStorageProperties().getSolrSchemaUrl();
        oCVSchema = this.solrStoragePlugin.getSolrClientApiExec()
            .getSchemaForCore(coreName, solrServerUrl, schemaUrl);

        if (!schemaMap.containsKey(coreName)) {
          schemaMap.put(coreName, oCVSchema);
        }
      }
      SolrScanSpec scanSpec = new SolrScanSpec(coreName, oCVSchema);
      drillTable = new StaticDrillTable(SolrStoragePluginConfig.NAME,
          solrStoragePlugin, scanSpec, new SolrDataType(scanSpec.getCvSchema()));

      drillTables.put(coreName, drillTable);

    } else {
      drillTable = drillTables.get(coreName);
    }
    return drillTable;
  }

  DrillTable getDrillTable(String dbName, String coreName) {
    logger.debug("SolrSchema :: getDrillTable");
    if (!drillTables.containsKey(coreName)) { // table does not exist
      return null;
    }
    return drillTables.get(coreName);

  }

  @Override
  public Set<String> getTableNames() {
    logger.debug("SolrSchema :: getTableNames");
    SolrStorageProperties solrStorageConfig = this.solrStoragePlugin
        .getSolrStorageConfig().getSolrStorageProperties();
    if (solrStorageConfig.isCreateViews())
      createORReplaceViews();
    return availableSolrCores;
  }

  @Override
  public boolean showInInformationSchema() {
    return true;
  }

  private void createORReplaceViews() {
    logger.debug("SolrStoragePlugin :: createORReplaceViews");

    String solrServerUrl = this.solrStoragePlugin.getSolrStorageConfig()
        .getSolrServer();
    String solrCoreViewWorkspace = this.solrStoragePlugin
        .getSolrStorageConfig().getSolrCoreViewWorkspace();
    String schemaUrl = this.solrStoragePlugin.getSolrStorageConfig()
        .getSolrStorageProperties().getSolrSchemaUrl();
    try {
      if (!availableSolrCores.isEmpty()) {
        for (String solrCoreName : availableSolrCores) {
          SolrSchemaPojo oCVSchema = this.solrStoragePlugin.getSolrClientApiExec()
              .getSchemaForCore(solrCoreName, solrServerUrl, schemaUrl);

          this.solrStoragePlugin.getSolrClientApiExec().createSolrView(
              solrCoreName, solrCoreViewWorkspace, oCVSchema);
          if (!schemaMap.containsKey(solrCoreName)) {
            schemaMap.put(solrCoreName, oCVSchema);
          }
        }

      } else {
        logger.debug("There is no cores in the current solr server : "
            + solrServerUrl);
      }

    } catch (ClientProtocolException e) {
      logger.debug("creating view failed : " + e.getMessage());
    } catch (IOException e) {
      logger.debug("creating view failed : " + e.getMessage());
    }

  }
}
