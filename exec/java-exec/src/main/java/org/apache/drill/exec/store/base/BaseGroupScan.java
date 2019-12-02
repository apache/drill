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

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.base.BaseStoragePlugin.StoragePluginOptions;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
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
 * You can use this class directly for the simplest of cases: a single minor
 * fragment that requires only schema, table and column names. This lets you get
 * something working quickly. You can then extend the scan spec if you need to
 * pass along more information, or extend this class to handle extended
 * plan-time behavior such as filter push-down.
 *
 * <h4>Serialization</h4>
 *
 * The group scan is Jackson-serialized to create the logical plan. The logical
 * plan is not normally seen during normal SQL execution, but is used internally
 * for testing. Each subclass must choose which fields to serialize and which to
 * make ephemeral in-memory state. This is tricky. There are multiple cases:
 * <p>
 * <ol>
 * <li>Materialize and serialize the data. This class serializes a "scan
 * specification" as the "scanSpec" attribute in JSON. Subclasses can add fields
 * to this class (if used only by the group scan) or to the scan spec (if shared
 * with the sub scan.)</li>
 * <li>Serialized ephemeral data. The base class marks the scan stats as a JSON
 * field, however the scan stats are usually recomputed each time they are
 * needed since they change as the scan is refined. This class will serialize
 * the storage plugin config, and gets that from the storage plugin itself.</li>
 * <li>Non-serialized materialized data. A base class of this class includes an
 * id which is not directly serialized. This class holds a reference to the
 * storage plugin.</li>
 * <li>Cached data. Advanced storage plugins work with an external system and
 * will want to cache metadata from that system. Such metadata should be cached
 * on the storage plugin, not in this class.</li>
 * </ol>
 * <p>
 * Jackson will use the constructor marked with <tt>@JsonCreator</tt> to
 * deserialize your group scan. If you create a subclass, and add fields, start
 * with the constructor from this class, then add your custom fields after the
 * fields defined here.
 *
 * <h4>Life Cycle</h4>
 *
 * Drill uses Calcite for planning. Calcite is very complex: it applies a series
 * of rules to transform the query, then chooses the lowest cost among the
 * available transforms. As a result, the group scan object is continually
 * created and recreated. The following is a rough outline of these events.
 *
 * <h5>Create the Group Scan</h5>
 *
 * The storage plugin provides a {@link SchemaFactory} which provides a
 * {@link SchemaConfig} which represents the set of (logical) tables available
 * from the plugin.
 * <p>
 * Calcite offers table names to the schema. If the table is valid, the schema
 * creates a {@link BaseScanSpec} to describe the schema, table and other
 * information unique to the plugin. The schema then creates a group scan using
 * the following constructor:
 * {@link #BaseGroupScan(BaseStoragePlugin, String, BaseScanSpec)}.
 *
 * <h5>Column Resolution</h5>
 *
 * Calcite makes multiple attempts to refine the set of columns from the scan.
 * If we have the following query:<br>
 * <code><pre>
 * SELECT a AS x, b FROM myTable</pre></code><br>
 * Then Calcite will offer the following set of columns as planning proceeds:
 *
 * <ol>
 * <li>['**'] (The default starter set.)</li>
 * <li>['**', 'a', 'b', 'x'] (alias not yet resolved.)</li>
 * <li>['a', 'b'] (aliases resolved)</li>
 * </ol>
 *
 * Each time the column set changes, Calcite makes a copy of the group scan by
 * calling {@link AbstractGroupScan#clone(List<SchemaPath>)}. This class
 * automates the process by calling two constructors. First, it calls
 * {@link BaseScanSpec#BaseScanSpec(BaseScanSpec, List<SchemaPath>)} to create a
 * new scan spec with the columns included.
 * <p>
 * The easiest solution is simply to provide the needed constructors. For
 * special cases, you can override the <tt>clone()</tt> method itself.
 *
 * <h5>Intermediate Copies</h5>
 *
 * At multiple points, Calcite will create a simple copy of the node by invoking
 * {@link GroupScan#getNewWithChildren(List<PhysicalOperator>)}. Scans never
 * have children, so this method should just make a copy. This base class
 * automatically invokes the the copy constructor
 * {@link #BaseGroupScan(BaseGroupScan)}. The code will invoke that constructor
 * on your derived class using Java introspection.
 * <p>
 * At some point, Drill serializes the scan spec to JSON, then recreates the
 * group scan via one of the
 * {@link AbstractStoragePlugin#getPhysicalScan(String userName, JSONOptions selection, SessionOptionManager sessionOptions, MetadataProviderManager metadataProviderManager)}
 * methods. Each offer different levels of detail. The base scan will
 * automatically deserialize the scan spec, then call
 * {@link BaseStoragePlugin#newGroupScan(String, BaseScanSpec, SessionOptionManager, MetadataProviderManager)}
 * or the simpler {@link BaseStoragePlugin#newGroupScan(String, BaseScanSpec)}
 * to create a group scan. You must override one of these methods if you create
 * your own subclass.
 *
 * <h5>Node Assignment</h5>
 *
 * Drill calls {@link #getMaxParallelizationWidth()} to determine how much it
 * can parallelize the scan. Drill calls {@link #applyAssignments(List)} to
 * declare the actual number of minor fragments (offered as Drillbit endpoints.)
 * Since most non-file scans don't care about node affinity, they can simply use
 * the {@link #endpointCount} variable to determine the number of minor
 * fragments which Drill will create.
 * <p>
 * Drill then calls {@link #getSpecificScan(int)}. If your scan is a simple
 * single scan, you can set the
 * {@link StoragePluginOptions#maxParallelizationWidth} value to 1 and assume
 * that Drill will create a single sub scan. Otherwise, you must pack your
 * "slices" (however this plugin defines them) into the available number of
 * minor fragments. This is done by having the sub scan hold a list of "slices",
 * then having the scan operator iterate over those slices.
 *
 * <h4>The Storage Plugin</h4>
 *
 * Group scans are ephemeral and serialized. They should hold only data that
 * describes the scan. Group scans <i>should not</i> hold metadata about the
 * underlying system because of the complexity of recreating that data on each
 * of the many copies that occur.
 * <p>
 * Instead, the implementation should cache metadata in the storage plugin. Each
 * storage plugin instance exists for the duration of a single query: either at
 * plan time or (if requested) at runtime (one instance per minor fragment.) A
 * good practice is:
 * <ul>
 * <li>The group scan asks the storage plugin for metadata as needed to process
 * each group scan operation.</li>
 * <li>The storage plugin retrieves the metadata from the external system and
 * caches it; returning the cached copies on subsequent requests.</li>
 * <li>The sub scan (execution description) should avoid asking for metadata to
 * avoid caching metadata in each of the many execution minor fragments.</li>
 * <li>Cached information is lost once planning completes for a query. If
 * additional caching is needed, the storage plugin can implement a shared cache
 * (with proper concurrency controls) which is shared across multiple plugin
 * instances. (Note, however, than planning is also distributed; each query may
 * be planned on a different Drillbit (Foreman), so even a shared cache will
 * hold as many copies as there are Drillbits (one copy per Drillbit.)</li>
 * </ul>
 * <p>
 * The storage plugin is the only part of the data for a query that persists
 * across the entire planning session. Group scans are created and deleted.
 * Although some of these are done via copies (and so could preserve data), the
 * <tt>getPhysicalScan()</tt> step is not a copy and discards all data except
 * that in the scan spec.
 *
 * <h4>The Scan Specification</h4>
 *
 * Drill has the idea of a scan specification, as mentioned above. The "Base"
 * classes run with the idea to define a {@link BaseScanSpec} that holds data
 * shared between the group and sub scans. This approach avoids the need to copy
 * and serialize the data in two places, and allows the automatic copies
 * described above.
 * <p>
 * Your subclass should include other query-specific data needed at both plan
 * and run time such as file locations, partition information, filter push-downs
 * and so on.
 * <p>
 * Some existing plugins copy the data from group to sub scan, but this is not
 * necessary if you use the scan spec class.
 *
 * <h4>Costs</h4>
 *
 * Calcite is a cost-based optimizer. This means it uses the cost associated
 * with a group scan instance to pick which of several options to use.
 * <p>
 * You must provide a scan cost estimate in terms of rows, data and CPU. For
 * some systems (such as Hive), these numbers are available. For others (such as
 * local files), the data size may be available, from which we can estimate a
 * row count by assuming some reasonable average row width. In other cases (such
 * as REST), we may not have any good estimate at all.
 * <p>
 * In these cases, it helps to know how Drill uses the cost estimates. The scan
 * must occur, so there is no decsion about whether to use the scan or not. But,
 * Drill has to decide which scan to put on which side of a join (the so-called
 * "build" and "probe" sides.) Further, Drill needs to know if the table is
 * small enough to "broadcast" the contents to all nodes.
 * <p>
 * So, at the least, the estimate must identify "small" vs. "large" tables. If
 * the scan is known to be small enough to broadcast (because it is for, say, a
 * small lookup list), then provide a small row and data estimate, say 100 rows
 * and 1K.
 * <p>
 * If, however, the data size is potentially large, then provide a large
 * estimate (10K rows, 100M data, say) to force Drill to not consider broadcast,
 * and to avoid putting the table on the build side of a join unless some other
 * table is even larger.
 *
 * <h5>Projection Push-Down</h5>
 *
 * Suppose your plugin accepts projection push-down. You indicate this by
 * setting {@link StoragePluginOptions#supportsProjectPushDown} to <tt>true</tt>
 * in your storage plugin.
 * <p>
 * Calcite must decide whether to actually do the push down. While this seems
 * like an obvious improvement, Calcite wants the numbers. So, the cost of a
 * scan should be lower (if only by a bit) with project push-down than without.
 * (Also, the cost of a project operator will be removed with push-down, biasing
 * the choice in favor of push-down.
 *
 * <h5>Filter Push-Down</h5>
 *
 * If your plugin supports filter push-down, then the cost with filters applied
 * must be lower than the cost without the filter push-down, or Calcite may not
 * choose to do the push-down. Filter push-down should reduce the number of rows
 * scanned, and that reduction (or even a crude estimate, such as 50%) needs to
 * be reflected in the cost.
 * <p>
 * Your filter push-down competes with Drill's own filter operator. If the
 * filter operator, along with a scan without push down, is less expensive than
 * a scan with push down, then Calcite will choose the former. Thus, the
 * selectivity of filter push down must exceed the selectivity which Drill uses
 * internally. (Drill uses the default selectivity defined by Calcite.)
 *
 * <h4>EXPLAIN PLAN</tt>
 *
 * The group scan appears in the output of the <tt>EXPLAIN PLAN FOR</tt>
 * command. Drill calls the {@ink #toString()} method to obtain the string. The
 * format of the string should follow Drill's conventions. The easiest way to to
 * that is to instead override {@link #buildPlanString(PlanStringBuilder)} and
 * add your custom fields to those already included from this base class.
 * <p>
 * If your fields have structure, ensure that the <tt>toString()</tt> method of
 * those classes also uses {@link PlanStringBuilder}, or create the encoding
 * yourself in your own <tt>buildPlanString</tt> method. Test by calling the
 * method or by examining the output of an <tt>EXPLAIN</tt>.
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
    return pluginOptions().supportsProjectPushDown;
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
   * fragment count may be just 1.) In that case, the
   * {{@link #getSpecificScan(int)} may need to combine logical sub-scans into a
   * single physical sub scan. That is, a sub scan operator should hold a list
   * of actual scans, and the scan operator should be ready to perform multiple
   * actual scans per sub-scan operator.
   *
   * @return the number of sub-scans that this group scan will produce
   */
  @JsonIgnore
  @Override
  public abstract int getMaxParallelizationWidth();

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
   * <code>minorFragmentId = 0</code>.
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
   * columns. Calls
   * {@link BaseScanFactory#groupWithColumns()}
   * by default.
   */
  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return pluginOptions().scanFactory.groupWithColumnsShim(this, columns);
  }

  /**
   * Create a copy of the group scan with (non-existent) children.
   * Calls
   * {@link BaseScanFactory#copyGroupShim()}
   * by default.
   */
  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return pluginOptions().scanFactory.copyGroupShim(this);
  }

  /**
   * Create a string representation of the scan formatted for the
   * <tt>EXPLAIN PLAN FOR</tt> output. Do not override this method,
   * override {@link #buildPlanString(PlanStringBuilder)} instead.
   */
  @Override
  public String toString() {
    PlanStringBuilder builder = new PlanStringBuilder(this);
    buildPlanString(builder);
    return builder.toString();
  }

  /**
   * Build an <tt>EXPLAIN PLAN FOR</tt> string using the builder
   * provided. Call the <tt>super</tt> method first, then add
   * fields for the subclass.
   * @param builder simple builder to create the required
   * format
   */
  public void buildPlanString(PlanStringBuilder builder) {
    builder.field("user", getUserName());
    builder.field("columns", columns);
  }
}
