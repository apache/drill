/*
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
package org.apache.drill.exec.planner.logical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Pair;
import org.apache.drill.exec.dotdrill.MaterializedView;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.conversion.SqlConverter;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for materialized view query rewriting.
 * <p>
 * When enabled via planner.enable_materialized_view_rewrite, this class attempts
 * to rewrite queries to use materialized views when beneficial.
 * <p>
 * Uses Calcite's {@link RelOptMaterializations#useMaterializedViews} API which
 * normalizes both the query and MV definitions before performing structural
 * matching via {@link org.apache.calcite.plan.SubstitutionVisitor}.
 * <p>
 * Materialized views are discovered by iterating over enabled file-based
 * storage plugins (the only plugin type that supports MVs) and force-loading
 * their schemas to find .materialized_view.drill files.
 */
public class MaterializedViewRewriter {
  private static final Logger logger = LoggerFactory.getLogger(MaterializedViewRewriter.class);

  private final QueryContext context;
  private final SchemaPlus rootSchema;
  private final SqlConverter sqlConverter;

  public MaterializedViewRewriter(QueryContext context, SchemaPlus rootSchema, SqlConverter sqlConverter) {
    this.context = context;
    this.rootSchema = rootSchema;
    this.sqlConverter = sqlConverter;
  }

  /**
   * Attempts to rewrite the given RelNode to use a materialized view.
   *
   * @param queryRel the query plan to potentially rewrite
   * @return the rewritten plan using an MV, or the original plan if no rewrite is possible
   */
  public RelNode rewrite(RelNode queryRel) {
    if (!context.getPlannerSettings().isMaterializedViewRewriteEnabled()) {
      return queryRel;
    }

    // Find all available materialized views that have been refreshed
    List<MaterializedViewCandidate> candidates = findCandidateMaterializedViews();

    if (candidates.isEmpty()) {
      logger.debug("No refreshed materialized views available for rewriting");
      return queryRel;
    }

    logger.debug("Found {} materialized view candidates for potential rewriting", candidates.size());

    // Build Calcite RelOptMaterialization objects for each refreshed candidate
    List<RelOptMaterialization> materializations = new ArrayList<>();
    for (MaterializedViewCandidate candidate : candidates) {
      if (!candidate.isRefreshed()) {
        logger.debug("Skipping MV {} - not refreshed", candidate.getName());
        continue;
      }

      try {
        RelOptMaterialization mat = buildMaterialization(candidate);
        if (mat != null) {
          materializations.add(mat);
        }
      } catch (Exception e) {
        logger.debug("Failed to build materialization for MV {}: {}", candidate.getName(), e.getMessage());
      }
    }

    if (materializations.isEmpty()) {
      logger.debug("No valid materializations could be built");
      return queryRel;
    }

    // Use Calcite's materialized view matching API which normalizes both the
    // query and MV definitions (trimming unused fields, converting Filter/Project
    // to Calc, merging, etc.) before performing structural matching.
    try {
      List<Pair<RelNode, List<RelOptMaterialization>>> results =
          RelOptMaterializations.useMaterializedViews(queryRel, materializations);

      if (!results.isEmpty()) {
        RelNode rewritten = results.get(0).left;
        if (logger.isInfoEnabled()) {
          List<RelOptMaterialization> usedMVs = results.get(0).right;
          logger.info("Query rewritten to use materialized view(s): {}",
              !usedMVs.isEmpty() ? usedMVs.get(0).qualifiedTableName : "unknown");
        }
        return rewritten;
      }
    } catch (Exception e) {
      logger.debug("Materialized view rewriting failed: {}", e.getMessage());
    }

    logger.debug("No materialized view matched the query");
    return queryRel;
  }

  /**
   * Builds a Calcite {@link RelOptMaterialization} for a candidate MV.
   */
  private RelOptMaterialization buildMaterialization(MaterializedViewCandidate candidate) {
    RelNode mvQueryRel = parseMvSql(candidate);
    if (mvQueryRel == null) {
      return null;
    }

    RelNode mvTableRel = buildMvScanRel(candidate);
    if (mvTableRel == null) {
      return null;
    }

    List<String> qualifiedTableName = java.util.Arrays.asList(
        candidate.getSchemaPath().split("\\."));

    return new RelOptMaterialization(
        mvTableRel,
        mvQueryRel,
        null,
        qualifiedTableName
    );
  }

  /**
   * Parses the MV's SQL definition into a RelNode.
   */
  private RelNode parseMvSql(MaterializedViewCandidate candidate) {
    try {
      String mvSql = candidate.getSql();
      org.apache.calcite.sql.SqlNode parsedNode = sqlConverter.parse(mvSql);
      org.apache.calcite.sql.SqlNode validatedNode = sqlConverter.validate(parsedNode);
      RelRoot relRoot = sqlConverter.toRel(validatedNode);
      return relRoot.rel;
    } catch (Exception e) {
      logger.debug("Failed to parse MV SQL for {}: {}", candidate.getName(), e.getMessage());
      return null;
    }
  }

  /**
   * Builds a RelNode that scans the MV's pre-computed data.
   */
  private RelNode buildMvScanRel(MaterializedViewCandidate candidate) {
    try {
      String mvDataTable = candidate.getSchemaPath() + ".`" + candidate.getName() + "_mv_data`";

      // Build explicit column list from the MV's field definitions to avoid
      // DYNAMIC_STAR type issues with SELECT *
      List<org.apache.drill.exec.dotdrill.View.Field> fields = candidate.getMaterializedView().getFields();
      StringBuilder scanSql = new StringBuilder("SELECT ");
      if (fields != null && !fields.isEmpty()) {
        for (int i = 0; i < fields.size(); i++) {
          if (i > 0) {
            scanSql.append(", ");
          }
          scanSql.append("`").append(fields.get(i).getName()).append("`");
        }
      } else {
        scanSql.append("*");
      }
      scanSql.append(" FROM ").append(mvDataTable);

      org.apache.calcite.sql.SqlNode parsedNode = sqlConverter.parse(scanSql.toString());
      org.apache.calcite.sql.SqlNode validatedNode = sqlConverter.validate(parsedNode);
      RelRoot relRoot = sqlConverter.toRel(validatedNode);
      return relRoot.rel;
    } catch (Exception e) {
      logger.debug("Failed to build MV scan for {}: {}", candidate.getName(), e.getMessage());
      return null;
    }
  }

  /**
   * Finds all materialized views by iterating over enabled file-based storage
   * plugins. Only file-based plugins (FileSystemPlugin) support materialized views.
   * <p>
   * Because Drill's schema tree is lazily loaded, we cannot simply traverse
   * already-loaded schemas. Instead, we use the StoragePluginRegistry to
   * discover file-based plugins and force-load their workspace schemas.
   */
  private List<MaterializedViewCandidate> findCandidateMaterializedViews() {
    List<MaterializedViewCandidate> candidates = new ArrayList<>();
    StoragePluginRegistry pluginRegistry = context.getStorage();

    // Get all enabled storage plugin names
    Map<String, org.apache.drill.common.logical.StoragePluginConfig> enabledPlugins =
        pluginRegistry.storedConfigs(StoragePluginRegistry.PluginFilter.ENABLED);

    for (String pluginName : enabledPlugins.keySet()) {
      try {
        StoragePlugin plugin = pluginRegistry.getPlugin(pluginName);
        if (!(plugin instanceof FileSystemPlugin)) {
          continue;
        }

        // Force-load this plugin's schema into the root schema tree.
        // DynamicRootSchema lazily loads schemas only on getSubSchema() calls.
        SchemaPlus pluginSchema = rootSchema.getSubSchema(pluginName);
        if (pluginSchema == null) {
          continue;
        }

        // Iterate over workspaces (sub-schemas of the plugin)
        for (String workspaceName : pluginSchema.getSubSchemaNames()) {
          SchemaPlus workspaceSchema = pluginSchema.getSubSchema(workspaceName);
          if (workspaceSchema == null) {
            continue;
          }

          AbstractSchema abstractSchema = workspaceSchema.unwrap(AbstractSchema.class);
          if (abstractSchema == null) {
            continue;
          }

          collectMaterializedViewsFromSchema(abstractSchema, candidates);
        }
      } catch (PluginException e) {
        logger.debug("Error accessing plugin {}: {}", pluginName, e.getMessage());
      }
    }

    return candidates;
  }

  /**
   * Collects MVs from a specific workspace schema.
   */
  private void collectMaterializedViewsFromSchema(AbstractSchema schema,
                                                   List<MaterializedViewCandidate> candidates) {
    Set<String> tableNames = schema.getTableNames();
    for (String tableName : tableNames) {
      try {
        MaterializedView mv = schema.getMaterializedView(tableName);
        if (mv != null) {
          boolean isRefreshed = mv.getRefreshStatus() == MaterializedView.RefreshStatus.COMPLETE;
          candidates.add(new MaterializedViewCandidate(
              mv.getName(),
              schema.getFullSchemaName(),
              mv,
              isRefreshed));
        }
      } catch (IOException e) {
        logger.debug("Error reading MV {}: {}", tableName, e.getMessage());
      }
    }
  }

  /**
   * Represents a candidate materialized view for query rewriting.
   */
  public static class MaterializedViewCandidate {
    private final String name;
    private final String schemaPath;
    private final MaterializedView materializedView;
    private final boolean refreshed;

    public MaterializedViewCandidate(String name, String schemaPath,
                                     MaterializedView materializedView, boolean refreshed) {
      this.name = name;
      this.schemaPath = schemaPath;
      this.materializedView = materializedView;
      this.refreshed = refreshed;
    }

    public String getName() {
      return name;
    }

    public String getSchemaPath() {
      return schemaPath;
    }

    public MaterializedView getMaterializedView() {
      return materializedView;
    }

    public boolean isRefreshed() {
      return refreshed;
    }

    public String getSql() {
      return materializedView.getSql();
    }
  }
}
