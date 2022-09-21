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
package org.apache.drill.exec.planner.sql.handlers;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.tools.RuleSet;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserExceptionUtils;
import org.apache.drill.common.util.function.CheckedSupplier;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.conversion.SqlConverter;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlHandlerConfig {
  private static final Logger logger = LoggerFactory.getLogger(SqlHandlerConfig.class);

  private final QueryContext context;
  private final SqlConverter converter;
  private Map<RelNode, Collection<StoragePlugin>> scannedPluginCache = new HashMap<>();

  public SqlHandlerConfig(QueryContext context, SqlConverter converter) {
    this.context = context;
    this.converter = converter;
  }

  public QueryContext getContext() {
    return context;
  }

  private RuleSet attemptToGetRules(PlannerPhase phase, Collection<StoragePlugin> plugins) throws PluginException{
    long maxAttempts = context.getOption(ExecConstants.STORAGE_PLUGIN_ACCESS_ATTEMPTS).num_val;
    long attemptDelayMs = context.getOption(ExecConstants.STORAGE_PLUGIN_ATTEMPT_DELAY).num_val;
    int attempt = 0;
    PluginException lastAttemptEx = null;

    while (attempt++ < maxAttempts) {
      try {
        return phase.getRules(context, plugins);
      } catch (PluginException ex) {
        // Hold onto the thrown PluginException because it tells us which plugin
        // was the first to run into trouble and we may go on to disable it.
        lastAttemptEx = ex;
        logger.warn(
          "Attempt {} of {} to get planning rules for plugins {} failed.",
          attempt, maxAttempts, plugins,
          ex
        );

        if (attempt < maxAttempts) {
          logger.info(
            "Next attempt to get planning rules for plugins {} will be made in {}ms.",
            plugins,
            attemptDelayMs
          );
          try {
            Thread.sleep(attemptDelayMs);
          } catch (InterruptedException intEx) {
            logger.warn(
              "Interrupted while waiting to make another attempt to get rules for plugins {}.",
              plugins,
              intEx
            );
          }
        }
      }
    }

    throw lastAttemptEx;
  }

  public RuleSet getRules(PlannerPhase phase, RelNode input) {
    return phase.getRules(context, getScannedPlugins(input));
  }

  public Collection<StoragePlugin> getScannedPlugins(RelNode input) {
    Collection<StoragePlugin> scannedPlugins = scannedPluginCache.get(input);
    if (scannedPlugins == null) {
      // Compute and cache for subsequent calls.
      PluginsCollector pluginsCollector = new PluginsCollector(context.getStorage());
      input.accept(pluginsCollector);
      scannedPlugins = pluginsCollector.getPlugins();
      scannedPluginCache.put(input, scannedPlugins);
    }

    return scannedPlugins;
  }

  public SqlConverter getConverter() {
    return converter;
  }

  public static class PluginsCollector extends RelShuttleImpl {
    // A TreeSet that compares plugins by name to remove duplicates and sort
    // alphabetically.
    private final TreeSet<StoragePlugin> plugins = new TreeSet<>(
      Comparator.comparing(StoragePlugin::getName)
    );
    private final StoragePluginRegistry storagePlugins;

    public PluginsCollector(StoragePluginRegistry storagePlugins) {
      this.storagePlugins = storagePlugins;
    }

    @Override
    public RelNode visit(TableScan scan) {
      collectPlugins(scan);
      return scan;
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
      collectPlugins(modify);
      return visitChildren(modify);
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof TableModify) {
        collectPlugins(other);
      }
      return super.visit(other);
    }

    private void collectPlugins(RelNode relNode) {
      String pluginName = SchemaUtilites.getSchemaPathAsList(
        relNode.getTable().getQualifiedName().iterator().next()).iterator().next();
      CheckedSupplier<StoragePlugin, StoragePluginRegistry.PluginException> pluginsProvider =
        () -> storagePlugins.getPlugin(pluginName);

      StoragePlugin storagePlugin = Optional.ofNullable(DrillRelOptUtil.getDrillTable(relNode))
        .map(DrillTable::getPlugin)
        .orElseGet(pluginsProvider);
      plugins.add(storagePlugin);
    }

    /**
     * @return A deduplicated collection of storage plugins scanned by the query.
     */
    public Collection<StoragePlugin> getPlugins() {
      return plugins;
    }
  }
}
