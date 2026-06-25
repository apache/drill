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
package org.apache.drill.metastore.components.materializedviews;

import org.apache.drill.metastore.MetastoreColumn;
import org.apache.drill.metastore.expressions.FilterExpression;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.operate.Delete;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.drill.metastore.expressions.FilterExpression.and;
import static org.apache.drill.metastore.expressions.FilterExpression.equal;

/**
 * Provides handy methods to retrieve Metastore MaterializedViews data.
 * Contains list of common requests without need to write filters manually.
 */
public class BasicMaterializedViewsRequests {

  private final MaterializedViews materializedViews;

  public BasicMaterializedViewsRequests(MaterializedViews materializedViews) {
    this.materializedViews = materializedViews;
  }

  /**
   * Retrieves a materialized view by its location identifiers.
   *
   * @param storagePlugin storage plugin name
   * @param workspace workspace name
   * @param mvName materialized view name
   * @return MaterializedViewMetadataUnit if found, null otherwise
   */
  public MaterializedViewMetadataUnit getMaterializedView(String storagePlugin,
                                                           String workspace,
                                                           String mvName) {
    FilterExpression filter = and(
        equal(MetastoreColumn.STORAGE_PLUGIN, storagePlugin),
        equal(MetastoreColumn.WORKSPACE, workspace),
        equal(MetastoreColumn.MV_NAME, mvName));

    List<MaterializedViewMetadataUnit> results = materializedViews.read()
        .filter(filter)
        .execute();

    return results.isEmpty() ? null : results.get(0);
  }

  /**
   * Retrieves all materialized views in a workspace.
   *
   * @param storagePlugin storage plugin name
   * @param workspace workspace name
   * @return list of MaterializedViewMetadataUnit
   */
  public List<MaterializedViewMetadataUnit> getMaterializedViews(String storagePlugin,
                                                                   String workspace) {
    FilterExpression filter = and(
        equal(MetastoreColumn.STORAGE_PLUGIN, storagePlugin),
        equal(MetastoreColumn.WORKSPACE, workspace));

    return materializedViews.read()
        .filter(filter)
        .execute();
  }

  /**
   * Retrieves all refreshed materialized views in a workspace.
   *
   * @param storagePlugin storage plugin name
   * @param workspace workspace name
   * @return list of refreshed MaterializedViewMetadataUnit
   */
  public List<MaterializedViewMetadataUnit> getRefreshedMaterializedViews(String storagePlugin,
                                                                            String workspace) {
    FilterExpression filter = and(
        equal(MetastoreColumn.STORAGE_PLUGIN, storagePlugin),
        equal(MetastoreColumn.WORKSPACE, workspace),
        equal(MetastoreColumn.MV_REFRESH_STATUS, "COMPLETE"));

    return materializedViews.read()
        .filter(filter)
        .execute();
  }

  /**
   * Retrieves all materialized views across all workspaces.
   *
   * @return list of all MaterializedViewMetadataUnit
   */
  public List<MaterializedViewMetadataUnit> getAllMaterializedViews() {
    return materializedViews.read().execute();
  }

  /**
   * Retrieves materialized view names in a workspace.
   *
   * @param storagePlugin storage plugin name
   * @param workspace workspace name
   * @return list of MV names
   */
  public List<String> getMaterializedViewNames(String storagePlugin, String workspace) {
    FilterExpression filter = and(
        equal(MetastoreColumn.STORAGE_PLUGIN, storagePlugin),
        equal(MetastoreColumn.WORKSPACE, workspace));

    return materializedViews.read()
        .filter(filter)
        .columns(Arrays.asList(MetastoreColumn.MV_NAME))
        .execute()
        .stream()
        .map(MaterializedViewMetadataUnit::name)
        .collect(Collectors.toList());
  }

  /**
   * Checks if a materialized view exists.
   *
   * @param storagePlugin storage plugin name
   * @param workspace workspace name
   * @param mvName materialized view name
   * @return true if exists, false otherwise
   */
  public boolean exists(String storagePlugin, String workspace, String mvName) {
    return getMaterializedView(storagePlugin, workspace, mvName) != null;
  }

  /**
   * Deletes a materialized view from metastore.
   *
   * @param storagePlugin storage plugin name
   * @param workspace workspace name
   * @param mvName materialized view name
   */
  public void delete(String storagePlugin, String workspace, String mvName) {
    FilterExpression filter = and(
        equal(MetastoreColumn.STORAGE_PLUGIN, storagePlugin),
        equal(MetastoreColumn.WORKSPACE, workspace),
        equal(MetastoreColumn.MV_NAME, mvName));

    Delete deleteOp = Delete.builder()
        .metadataType(MetadataType.MATERIALIZED_VIEW)
        .filter(filter)
        .build();

    materializedViews.modify()
        .delete(deleteOp)
        .execute();
  }
}
