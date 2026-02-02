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
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.dotdrill.MaterializedView;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.store.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for materialized view query rewriting.
 * <p>
 * When enabled via planner.enable_materialized_view_rewrite, this class attempts
 * to rewrite queries to use materialized views when beneficial.
 * <p>
 * Current implementation provides the infrastructure for MV rewriting.
 * Future enhancements can add:
 * <ul>
 *   <li>Structural matching using Calcite's SubstitutionVisitor</li>
 *   <li>Partial query matching (query is subset of MV)</li>
 *   <li>Aggregate rollup rewriting</li>
 *   <li>Cost-based selection among multiple candidate MVs</li>
 * </ul>
 */
public class MaterializedViewRewriter {
  private static final Logger logger = LoggerFactory.getLogger(MaterializedViewRewriter.class);

  private final QueryContext context;
  private final SchemaPlus defaultSchema;

  public MaterializedViewRewriter(QueryContext context, SchemaPlus defaultSchema) {
    this.context = context;
    this.defaultSchema = defaultSchema;
  }

  /**
   * Attempts to rewrite the given RelNode to use a materialized view.
   *
   * @param relNode the query plan to potentially rewrite
   * @return the rewritten plan using an MV, or the original plan if no rewrite is possible
   */
  public RelNode rewrite(RelNode relNode) {
    if (!context.getPlannerSettings().isMaterializedViewRewriteEnabled()) {
      return relNode;
    }

    // Find all available materialized views
    List<MaterializedViewCandidate> candidates = findCandidateMaterializedViews();

    if (candidates.isEmpty()) {
      logger.debug("No materialized views available for rewriting");
      return relNode;
    }

    logger.debug("Found {} materialized view candidates for potential rewriting", candidates.size());

    // Future: Implement structural matching here
    // For now, log that rewriting is enabled but not yet implemented
    for (MaterializedViewCandidate candidate : candidates) {
      logger.debug("MV candidate: {} in schema {} (refreshed: {})",
          candidate.getName(),
          candidate.getSchemaPath(),
          candidate.isRefreshed());
    }

    // Return original plan - actual matching not yet implemented
    return relNode;
  }

  /**
   * Finds all materialized views in accessible schemas that could potentially
   * be used for query rewriting.
   */
  private List<MaterializedViewCandidate> findCandidateMaterializedViews() {
    List<MaterializedViewCandidate> candidates = new ArrayList<>();

    // Traverse accessible schemas to find MVs
    collectMaterializedViews(defaultSchema, candidates);

    return candidates;
  }

  /**
   * Recursively collects materialized views from a schema and its subschemas.
   */
  private void collectMaterializedViews(SchemaPlus schema, List<MaterializedViewCandidate> candidates) {
    if (schema == null) {
      return;
    }

    // Check if this schema supports MVs (is an AbstractSchema)
    if (schema.unwrap(AbstractSchema.class) != null) {
      AbstractSchema abstractSchema = schema.unwrap(AbstractSchema.class);
      try {
        collectMaterializedViewsFromSchema(abstractSchema, candidates);
      } catch (Exception e) {
        logger.debug("Error collecting MVs from schema {}: {}", schema.getName(), e.getMessage());
      }
    }

    // Recurse into subschemas
    Set<String> subSchemaNames = schema.getSubSchemaNames();
    for (String subSchemaName : subSchemaNames) {
      SchemaPlus subSchema = schema.getSubSchema(subSchemaName);
      collectMaterializedViews(subSchema, candidates);
    }
  }

  /**
   * Collects MVs from a specific schema.
   */
  private void collectMaterializedViewsFromSchema(AbstractSchema schema,
                                                   List<MaterializedViewCandidate> candidates) {
    // Get table names and check for MVs
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
