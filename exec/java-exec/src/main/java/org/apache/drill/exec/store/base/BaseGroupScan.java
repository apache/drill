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
package org.apache.drill.exec.store.base;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.base.BaseStoragePlugin.StoragePluginOptions;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Base group scan for storage plugins. A group scan is a "logical" scan: it is
 * the representation of the scan used during the logical and physical planning
 * phases. The group scan is converted to a "sub scan" (an executable scan
 * specification) for inclusion in the physical plan sent to Drillbits for
 * execution. The group scan represents the entire scan of a table or data
 * source. The sub scan divides that scan into files, storage blocks or other
 * forms of parallelism.
 * <p>
 * The group scan participates in both logical and physical planning. As
 * noted below, logical plan information is JSON serialized, but physical
 * plan information is not. The transition from logical
 * to physical planning is not clear. The first call to
 * {@link #getMinParallelizationWidth()} or
 * {@link #getMaxParallelizationWidth()} is a good signal. The group scan
 * is copied multiple times (each with more information) during logical
 * planning, but is not copied during physical planning. This means that
 * physical planning state can be thought of as transient: it should not
 * be serialized or copied.
 * <p>
 * Because the group scan is part of the Calcite planning process, it is
 * very helpful to understand the basics of query planning and how
 * Calcite implements that process.
 *
 * <h4>Serialization</h4>
 *
 * Drill provides the ability to serialize the logical plan. This is most
 * easily seen by issuing the {@code EXPLAIN PLAN FOR} command for a
 * query. The Jackson-serialized representation of the group scan appears
 * in the JSON partition of the {@code EXPLAIN} output, while the
 * {@code toString()} output appears in the text output of that
 * command.
 * <p>
 * Care must be taken when serializing: include only the <i>logical</i>
 * plan information, omit the physical plan information.
 * <p>
 * Any fields that are part of the <i>logical</i> plan must be Jackson
 * serializable. The following information should be serialized in the
 * logical plan:
 * <ul>
 * <li>Information from the scan spec (from the table lookup in the
 * schema) if relevant to the plugin.</li>
 * <li>The list of columns projected in the scan.</li>
 * <li>Any filters pushed into the query (in whatever form makes sense
 * for the plugin.</li>
 * <li>Any other plugin-specific logical plan information.</li>
 * </ul>
 * This base class (and its superclasses) serialize the plugin config,
 * user name, column list and scan stats. The derived class should handle
 * other fields.
 * <p>
 * On the other hand, the kinds of information should <i>not</i> be
 * serialized:
 * <ul>
 * <li>The set of drillbits on which queries will run.</li>
 * <li>The actual number of minor fragments that run scans.</li>
 * <li>Other physical plan information.</li>
 * <li>Cached data (such as the cached scan stats, etc.</li>
 * </ul>
 * <p>
 * Jackson will use the constructor marked with {@code @JsonCreator} to
 * deserialize your group scan. If you create a subclass, and add fields, start
 * with the constructor from this class, then add your custom fields after the
 * fields defined here. Make liberal use of {@code @JsonProperty} to
 * identify fields (getters) to be serialized, and {@code @JsonIgnore}
 * for those that should not be serialized.
 */

public abstract class BaseGroupScan extends AbstractGroupScan {

  protected final BaseStoragePlugin<?> storagePlugin;
  protected final List<SchemaPath> columns;
  protected ScanStats scanStats;
  protected int endpointCount;
  protected OptionManager sessionOptions;

  public BaseGroupScan(BaseStoragePlugin<?> storagePlugin,
      String userName, List<SchemaPath> columns) {
    super(userName);
    this.storagePlugin = storagePlugin;
    this.columns = columns;
  }

  public BaseGroupScan(BaseGroupScan from) {
    this(from.storagePlugin, from.getUserName(), from.getColumns());
    this.endpointCount = from.endpointCount;
    this.sessionOptions = from.sessionOptions;
  }

  public BaseGroupScan(
      StoragePluginConfig config,
      String userName,
      List<SchemaPath> columns,
      StoragePluginRegistry engineRegistry) {
    super(userName);
    this.storagePlugin = BaseStoragePlugin.resolvePlugin(engineRegistry, config);
    this.columns = columns;
  }

  @JsonProperty("config")
  public StoragePluginConfig getConfig() { return storagePlugin.getConfig(); }

  @JsonProperty("columns")
  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @SuppressWarnings("unchecked")
  public <T extends BaseStoragePlugin<?>> T storagePlugin() {
    return (T) storagePlugin;
  }

  public StoragePluginOptions pluginOptions() {
    return storagePlugin.options();
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    // EVF (result set loader and so on) handles project push-down for us.
    return true;
  }

  /**
   * During parallelization, Drill calls this method with a list of endpoints
   * which (it seems) correspond to minor fragments. Since non-file storage
   * plugins generally don't care which node runs which slice of a scan, we
   * retain only the endpoint count. Later, in {@link #getSpecificScan(int)},
   * Drill will ask for subscans with minor fragment IDs from 0 to the number of
   * endpoints specified here.
   */
  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    endpointCount = endpoints.size();
  }

  /**
   * Return the maximum parallelization width. This number is usually the number
   * of distinct sub-scans this group scan can produce. The actual number of
   * minor fragments may be less. (For example, if running in a test, the minor
   * fragment count may be just 1.) In that case,
   * {{@link #getSpecificScan(int)} may need to combine logical sub-scans into a
   * single physical sub scan. That is, a sub scan operator should hold a list
   * of actual scans, and the scan operator should be ready to perform multiple
   * actual scans per sub-scan operator.
   * <p>
   * For convenience, returns 1 by default, which is the minimum. Derived
   * classes should return a different number parallelization.
   *
   * @return the number of sub-scans that this group scan will produce
   */
  @JsonIgnore
  @Override
  public int getMaxParallelizationWidth() { return 1; }

  @JsonIgnore
  @Override
  public ScanStats getScanStats() {
    if (scanStats == null) {
      scanStats = computeScanStats();
    }
    return scanStats;
  }

  /**
   * Compute the statistics for the scan used to plan the query. In general, the
   * most critical aspect is to give a rough estimate of row count: a small row
   * count will encourage the planner to broadcast rows to all Drillbits,
   * something that is undesirable if the row count is actually large.
   * Otherwise, more accurate estimates are better.
   */
  protected abstract ScanStats computeScanStats();

  /**
   * Create a sub scan for the given minor fragment. Override this
   * if you define a custom sub scan. Uses the default
   * {@link BaseSubScan} by default.
   * <p>
   * The group can may have a single "slice". If so, then the
   * {@link #getMaxParallelizationWidth()} should have returned 1,
   * and this method will be called only once, with
   * {@code minorFragmentId = 0}.
   * <p>
   * However, if the group can can partition work into sub-units
   * (let's call them "slices"), then this method must pack
   * <i><b>n</b></i> slices into <i><b>m</b></i> minor fragments
   * where <i><b>n</b></i> >= <i><b>m</b></i>.
   */
  @Override
  public abstract SubScan getSpecificScan(int minorFragmentId);

  @JsonIgnore
  @Override
  public String getDigest() { return toString(); }

  /**
   * Create a copy of the group scan with with candidate projection
   * columns. Calls {@link BaseScanFactory#groupWithColumns()}
   * by default.
   */
  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return pluginOptions().scanFactory.groupWithColumnsShim(this, columns);
  }

  /**
   * Create a copy of the group scan with (non-existent) children.
   * Calls {@link BaseScanFactory#copyGroup()} by default.
   */
  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return pluginOptions().scanFactory.copyGroupShim(this);
  }

  /**
   * Create a string representation of the scan formatted for the
   * {@code EXPLAIN PLAN FOR} output. Do not override this method,
   * override {@link #buildPlanString(PlanStringBuilder)} instead.
   */
  @Override
  public String toString() {
    PlanStringBuilder builder = new PlanStringBuilder(this);
    buildPlanString(builder);
    return builder.toString();
  }

  /**
   * Build an {@code EXPLAIN PLAN FOR} string using the builder
   * provided. Call the {@code super} method first, then add
   * fields for the subclass.
   * @param builder simple builder to create the required
   * format
   */
  public void buildPlanString(PlanStringBuilder builder) {
    builder.field("user", getUserName());
    builder.field("columns", columns);
  }
}
