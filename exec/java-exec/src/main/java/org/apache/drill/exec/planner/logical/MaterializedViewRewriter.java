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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.dotdrill.MaterializedView;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.conversion.SqlConverter;
import org.apache.drill.exec.store.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for materialized view query rewriting.
 * <p>
 * When enabled via planner.enable_materialized_view_rewrite, this class attempts
 * to rewrite queries to use materialized views when beneficial.
 * <p>
 * The rewriting uses Calcite's SubstitutionVisitor to perform structural matching
 * between the user's query and available materialized view definitions. If the
 * query matches (or is a subset of) an MV's definition, the query is rewritten
 * to scan from the pre-computed MV data instead.
 */
public class MaterializedViewRewriter {
  private static final Logger logger = LoggerFactory.getLogger(MaterializedViewRewriter.class);

  private final QueryContext context;
  private final SchemaPlus defaultSchema;
  private final SqlConverter sqlConverter;

  public MaterializedViewRewriter(QueryContext context, SchemaPlus defaultSchema, SqlConverter sqlConverter) {
    this.context = context;
    this.defaultSchema = defaultSchema;
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

    // Try each candidate MV for structural matching
    for (MaterializedViewCandidate candidate : candidates) {
      if (!candidate.isRefreshed()) {
        logger.debug("Skipping MV {} - not refreshed", candidate.getName());
        continue;
      }

      try {
        RelNode rewritten = tryRewriteWithMV(queryRel, candidate);
        if (rewritten != null) {
          logger.info("Query rewritten to use materialized view: {}", candidate.getName());
          return rewritten;
        }
      } catch (Exception e) {
        logger.debug("Failed to rewrite with MV {}: {}", candidate.getName(), e.getMessage());
      }
    }

    logger.debug("No materialized view matched the query");
    return queryRel;
  }

  /**
   * Attempts to rewrite the query using a specific materialized view.
   *
   * @param queryRel the user's query plan
   * @param candidate the MV candidate to try
   * @return the rewritten plan if successful, null otherwise
   */
  private RelNode tryRewriteWithMV(RelNode queryRel, MaterializedViewCandidate candidate) {
    // Parse the MV's SQL definition into a RelNode
    RelNode mvQueryRel = parseMvSql(candidate);
    if (mvQueryRel == null) {
      return null;
    }

    // Build a RelNode that represents scanning the MV's pre-computed data
    RelNode mvScanRel = buildMvScanRel(candidate);
    if (mvScanRel == null) {
      return null;
    }

    logger.debug("Attempting structural match for MV: {}", candidate.getName());
    if (logger.isDebugEnabled()) {
      logger.debug("Query plan:\n{}", RelOptUtil.toString(queryRel));
      logger.debug("MV definition plan:\n{}", RelOptUtil.toString(mvQueryRel));
    }

    // Use Calcite's SubstitutionVisitor to check if the query matches the MV
    // Constructor takes (target, query) where:
    //   - target: the MV definition (what we want to match against)
    //   - query: the replacement (the MV scan)
    // Then go(queryRel) checks if queryRel can be rewritten using the MV
    SubstitutionVisitor visitor = new SubstitutionVisitor(mvQueryRel, mvScanRel);
    List<RelNode> substitutions = visitor.go(queryRel);

    if (substitutions != null && !substitutions.isEmpty()) {
      RelNode substituted = substitutions.get(0);
      if (logger.isDebugEnabled()) {
        logger.debug("Substitution found! Rewritten plan:\n{}", RelOptUtil.toString(substituted));
      }
      return substituted;
    }

    return null;
  }

  /**
   * Parses the MV's SQL definition into a RelNode.
   */
  private RelNode parseMvSql(MaterializedViewCandidate candidate) {
    try {
      String mvSql = candidate.getSql();
      List<String> schemaPath = candidate.getMaterializedView().getWorkspaceSchemaPath();

      // Parse and convert the MV's SQL to RelNode
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
   * This creates a table scan of the MV's data directory.
   */
  private RelNode buildMvScanRel(MaterializedViewCandidate candidate) {
    try {
      // Build SQL to scan the MV data table
      String mvDataTable = candidate.getSchemaPath() + ".`" + candidate.getName() + "_mv_data`";
      String scanSql = "SELECT * FROM " + mvDataTable;

      // Parse and convert to RelNode
      org.apache.calcite.sql.SqlNode parsedNode = sqlConverter.parse(scanSql);
      org.apache.calcite.sql.SqlNode validatedNode = sqlConverter.validate(parsedNode);
      RelRoot relRoot = sqlConverter.toRel(validatedNode);

      return relRoot.rel;
    } catch (Exception e) {
      logger.debug("Failed to build MV scan for {}: {}", candidate.getName(), e.getMessage());
      return null;
    }
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
