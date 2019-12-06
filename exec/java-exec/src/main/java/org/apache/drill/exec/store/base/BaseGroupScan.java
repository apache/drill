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
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
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
 * easily seen by issuing the <code>EXPLAIN PLAN FOR</code> command for a
 * query. The Jackson-serialized representation of the group scan appears
 * in the JSON partition of the <code>EXPLAIN</code> output, while the
 * <code>toString()</code> output appears in the text output of that
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
 * Jackson will use the constructor marked with <tt>@JsonCreator</tt> to
 * deserialize your group scan. If you create a subclass, and add fields, start
 * with the constructor from this class, then add your custom fields after the
 * fields defined here. Make liberal use of <code>@JsonProperty</code> to
 * identify fields (getters) to be serialized, and <code>@JsonIgnore</code>
 * for those that should not be serialized.
 *
 * <h4>Life Cycle</h4>
 *
 * Drill uses Calcite for planning. Calcite is a bit complex: it applies a series
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
 * <p>
 * Since Calcite is cost-based, it is important that the cost of the scan
 * decrease after columns are pushed down. This can be done by reducing
 * the disk I/O cost or reducing the CPU factor from 1.0 to, say, 0.75.
 * See {@link DummyGroupScan} for an example.
 *
 * <h5>Intermediate Copies</h5>
 *
 * At multiple points during logical planning, Calcite will create
 * a simple copy of the node by invoking
 * {@link GroupScan#getNewWithChildren(List<PhysicalOperator>)}. Scans never
 * have children, so this method should just make a copy. This base class
 * delegates to {@link BaseStoragePlugin.ScanFactory} to make the copy.
 * <p>
 * Calcite uses the schema to look up a table and obtain a scan spec.
 * The scan spec is then serialized to JSON and passed back to the plugin
 * to be deserialized and to create a group scan for the scan spec. The
 * {#link BaseStoragePlugin} and {@link BaseStoragePlugin.ScanFactory}
 * classes hide the details.
 *
 * <h4>Filter Push-Down</h5>
 *
 * If a plugin allows filter push-down, the plugin must add logical planning
 * rules to implement the push down. The rules rewrite the group scan with
 * the push-downs included (and optionally remove the filters from the
 * query.) See {@link BaseFilterPushDownStragy} for details.
 *
 * <h5>Node Assignment</h5>
 *
 * Drill calls {@link #getMaxParallelizationWidth()} to determine how much it
 * can parallelize the scan. If this method returns 1, Drill assumes that this
 * is a single-threaded scan. If the return is 2 or greater, Drill will create
 * a parallelized scan. This base class assumes a single-threaded scan.
 * Override {@link #getMinParallelizationWidth()} and
 * {@link #getMaxParallelizationWidth()} to enable parallelism.
 * <p>
 * Drill then calls {@link #applyAssignments(List)} to
 * declare the actual number of minor fragments (offered as Drillbit endpoints.)
 * Since most non-file scans don't care about node affinity, they can simply use
 * the {@link #endpointCount} variable to determine the number of minor
 * fragments which Drill will create.
 * <p>
 * Drill then calls {@link #getSpecificScan(int)}. The number of minor fragments
 * is the same as the number of endpoints offered above, which is determined by
 * the min/max parallelization width and Drill configuration.
 * <p>
 * Your group scan may create a set of scan "segments". For example, to read
 * a distributed database, there might be one segment per database server node.
 * The physical planning process maps the segments into minor fragments.
 * Ideally there will be one segment per minor fragment, but there may be
 * multiple if Drill can offer fewer endpoints than requested. Thus, each
 * specific scan might host multiple segments. Each plugin determines what
 * that means for the target engine. At runtime, each segment translates to
 * a distinct batch reader.
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
 * that in the scan spec. Note, however, that if a query contains a join or
 * a union, then the query will contain multiple group scans (one per table)
 * but a single storage plugin instance.
 *
 * <h4>The Scan Specification</h4>
 *
 * Drill has the idea of a scan specification, as mentioned above. The scan
 * spec transfers information from the schema table lookup to the group scan.
 * Each plugin defines its own scan spec; there is no base class. At the
 * least, include the table definition (whatever that means for the plugin.)
 * The scan spec must be Jackson serializable.
 * <p>
 * Some plugins use the scan spec directly in the group scan (store it as
 * a field), others do not. Choose the simplest design for your needs.
 * <p>
 * Your subclass should include other query-specific data needed at both plan
 * and run time such as file locations, partition information, filter push-downs
 * and so on.
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
 * must occur, so there is no decision about whether to use the scan or not. But,
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
 * <p>
 * Logical planning will include projection (column) push-down and optionally
 * filter push-down. Each of these <i>must</i> reduce scan cost so that
 * Calcite decides that doing them improves query performance.
 *
 * <h4>Projection Push-Down</h4>
 *
 * This base class assumes that the scan supports projection push-down. You
 * get this "for free" if you use the EVF-based scan framework (that is,
 * the result set loader and associated classes.) You are, however,
 * responsible for updating costs based on projection push-down.
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
 * <p>
 * Include only the logical planning fields. That is, include only the
 * fields that also appear in the JSON serialized form of the group scan.
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
   * fragment count may be just 1.) In that case, the
   * {{@link #getSpecificScan(int)} may need to combine logical sub-scans into a
   * single physical sub scan. That is, a sub scan operator should hold a list
   * of actual scans, and the scan operator should be ready to perform multiple
   * actual scans per sub-scan operator.
   * <p>
   * For convenience, returns 1 by default, which is the minimum. Derived
   * classes should return a different number if they support filter push-down
   * and parallelization.
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
