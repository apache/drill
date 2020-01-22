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
/**
 * Provides a set of base classes for creating a storage plugin.
 * Handles the "boilerplate" which is otherwise implemented via
 * copy-and-paste.
 * <p>
 * The simplest possible plugin will use many of the base
 * classes as-is, and will implement:
 * <ul>
 * <li>The storage plugin configuration (needed to identify the plugin),</li>
 * <li>The storage plugin class,</li>
 * <li>The schema factory for the plugin (which says which schemas
 * or tables are available),<.li>
 * <li>A scan spec which transfers schema information from the schema
 * factory to the group scan.</li>
 * <li>The GroupScan class to describe the scan during planning.</li>
 * <li>The SubScan class to carry information from the planner to the
 * execution engine, serialized as JSON.</li>
 * <li>The batch reader to read the data for the plugin.</li>
 * </ul>
 *
 * Super classes require a number of standard methods to make
 * copies, present configuration and so on. As much as possible,
 * the classes here handle those details. For example, the
 * {@link StoragePluginOptions} class holds many of the options
 * that otherwise require one-line method implementations.
 * The framework automatically makes copies of scan objects
 * to avoid other standard methods.
 *
 * <h4>Life Cycle</h4>
 *
 * Drill uses Calcite for planning. Calcite is a bit complex: it applies a series
 * of rules to transform the query, then chooses the lowest cost among the
 * available transforms. As a result, the group scan object is continually
 * created and recreated. The following is a rough outline of these events.
 *
 * <h4>The Scan Specification</h4>
 *
 * The storage plugin provides a
 * {@link org.apache.drill.exec.store.SchemaFactory SchemaFactory}
 * which provides a
 * {@link org.apache.drill.exec.store.SchemaConfig SchemaConfig}
 * which represents the set of (logical) tables available
 * from the plugin.
 * <p>
 * Calcite offers table names to the schema. If the table is valid, the schema
 * creates a scan spec instance to describe the schema, table and other
 * information unique to the plugin. The schema then creates a group scan using
 * from the scan spec.
 * <p>
 * Calcite uses the schema to look up a table and obtain a scan spec.
 * The scan spec is then serialized to JSON and passed back to the plugin
 * to be deserialized and to create a group scan for the scan spec. The
 * {@link BaseStoragePlugin} and {@link BaseScanFactory}
 * classes (try to) hide the details.
 *
 * Drill has the idea of a scan specification. The scan
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
 * calling {@link AbstractGroupScan#clone(List<SchemaPath>)}.
 * <p>
 * Since Calcite is cost-based, it is important that the cost of the scan
 * decrease after columns are pushed down. This can be done by reducing
 * the disk I/O cost or reducing the CPU factor from 1.0 to, say, 0.75.
 * See {@link DummyGroupScan} for an example.
 *
 * <h5>Intermediate Copies</h5>
 *
 * At multiple points during logical planning, Calcite will create
 * a simple copy of the group scan by invoking
 * {@link GroupScan#getNewWithChildren(List<PhysicalOperator>)}. Scans never
 * have children, so this method should just make a copy.
 *
 * <h4>Filter Push-Down</h5>
 *
 * If a plugin allows filter push-down, the plugin must add logical planning
 * rules to implement the push down. The rules rewrite the group scan with
 * the push-downs included (and optionally remove the filters from the
 * query.) See {@link FilterPushDownStrategy} for details.
 *
 * <h5>Node Assignment</h5>
 *
 * Drill calls {@link GroupScan#getMaxParallelizationWidth()} to
 * determine how much it
 * can parallelize the scan. If this method returns 1, Drill assumes that this
 * is a single-threaded scan. If the return is 2 or greater, Drill will create
 * a parallelized scan. This base class assumes a single-threaded scan.
 * Override {@link GroupScan#getMinParallelizationWidth()} and
 * {@link GroupScan#getMaxParallelizationWidth()} to enable parallelism.
 * <p>
 * Drill then calls {@link GroupScan#applyAssignments(List)} to
 * declare the actual number of minor fragments (offered as Drillbit endpoints.)
 * Since most non-file scans don't care about node affinity, they can simply use
 * the {@link GroupScan#endpointCount} variable to determine the number of minor
 * fragments which Drill will create.
 * <p>
 * Drill then calls {@link GroupScan#getSpecificScan(int)}. The number of minor fragments
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
 * <h4>EXPLAIN PLAN</h4>
 *
 * The group scan appears in the output of the <tt>EXPLAIN PLAN FOR</tt>
 * command. Drill calls the
 * {@link org.apache.drill.exec.physical.base.GroupScan#toString() GroupScan.toString()}
 * method to obtain the string. The
 * format of the string should follow Drill's conventions. The easiest way to to
 * that is to instead override {@link BaseGroupScan#buildPlanString(PlanStringBuilder)} and
 * add your custom fields to those already included from this base class.
 * <p>
 * If your fields have structure, ensure that the <tt>toString()</tt> method of
 * those classes also uses {@link PlanStringBuilder}, or create the encoding
 * yourself in your own <tt>buildPlanString</tt> method. Test by calling the
 * method or by examining the output of an <tt>EXPLAIN</tt>.
 * <p>
 * Include only the logical planning fields. That is, include only the
 * fields that also appear in the JSON serialized form of the group scan.
 *
 * @see {@link org.apache.drill.exec.store.base.DummyStoragePlugin DummyStoragePlugin}
 * for an example how this
 * framework is used. The Dummy plugin is the "test mule"
 * for this framework.
 */
package org.apache.drill.exec.store.base;

import java.util.List;
