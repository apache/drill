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
package org.apache.drill.exec.physical.base;

import java.util.Collection;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.exec.server.options.OptionManager;

/**
 * A GroupScan operator represents all data which will be scanned by a given physical
 * plan.  It is the superset of all SubScans for the plan.
 */
public interface GroupScan extends Scan, HasAffinity{

  /**
   * columns list in GroupScan : 1) empty_column is for skipAll query.
   *                             2) NULL is interpreted as ALL_COLUMNS.
   *  How to handle skipAll query is up to each storage plugin, with different policy in corresponding RecordReader.
   */
  List<SchemaPath> ALL_COLUMNS = ImmutableList.of(SchemaPath.STAR_COLUMN);

  long NO_COLUMN_STATS = -1;

  void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException;

  SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException;

  @JsonIgnore
  int getMaxParallelizationWidth();

  @JsonIgnore
  boolean isDistributed();

  /**
   * At minimum, the GroupScan requires these many fragments to run.
   * Currently, this is used in {@link org.apache.drill.exec.planner.fragment.SimpleParallelizer}
   * @return the minimum number of fragments that should run
   */
  @JsonIgnore
  int getMinParallelizationWidth();

  /**
   * Check if GroupScan enforces width to be maximum parallelization width.
   * Currently, this is used in {@link org.apache.drill.exec.planner.physical.visitor.ExcessiveExchangeIdentifier}
   * @return if maximum width should be enforced
   *
   * @deprecated Use {@link #getMinParallelizationWidth()} to determine whether this GroupScan spans more than one
   * fragment.
   */
  @JsonIgnore
  @Deprecated
  boolean enforceWidth();

  /**
   * Returns a signature of the {@link GroupScan} which should usually be composed of
   * all its attributes which could describe it uniquely.
   */
  @JsonIgnore
  String getDigest();

  @JsonIgnore
  ScanStats getScanStats(PlannerSettings settings);

  /**
   * Returns a clone of GroupScan instance, except that the new GroupScan will use the provided list of columns .
   */
  GroupScan clone(List<SchemaPath> columns);

  /**
   * GroupScan should check the list of columns, and see if it could support all the columns in the list.
   */
  boolean canPushdownProjects(List<SchemaPath> columns);

  /**
   * Return the number of non-null value in the specified column. Raise exception, if groupscan does not
   * have exact column row count.
   */
  long getColumnValueCount(SchemaPath column);

  /**
   * Whether or not this GroupScan supports pushdown of partition filters (directories for filesystems)
   */
  boolean supportsPartitionFilterPushdown();

  /**
   * Returns a list of columns that can be used for partition pruning
   *
   */
  @JsonIgnore
  List<SchemaPath> getPartitionColumns();

  /**
   * Whether or not this GroupScan supports limit pushdown
   */
  boolean supportsLimitPushdown();

  /**
   * Apply rowcount based prune for "LIMIT n" query.
   * @param maxRecords : the number of rows requested from group scan.
   * @return  a new instance of group scan if the prune is successful.
   *          null when either if row-based prune is not supported, or if prune is not successful.
   */
  GroupScan applyLimit(int maxRecords);

  /**
   * Return true if this GroupScan can return its selection as a list of file names (retrieved by getFiles()).
   */
  @JsonIgnore
  boolean hasFiles();

  /**
   * Returns a collection of file names associated with this GroupScan. This should be called after checking
   * hasFiles().  If this GroupScan cannot provide file names, it returns null.
   */
  Collection<String> getFiles();

  @JsonIgnore
  LogicalExpression getFilter();

  GroupScan applyFilter(LogicalExpression filterExpr, UdfUtilities udfUtilities,
                        FunctionImplementationRegistry functionImplementationRegistry, OptionManager optionManager);

}
