/**
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

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A GroupScan operator represents all data which will be scanned by a given physical
 * plan.  It is the superset of all SubScans for the plan.
 */
public interface GroupScan extends Scan, HasAffinity{

  public static final List<SchemaPath> ALL_COLUMNS = Lists.<SchemaPath>newArrayList(SchemaPath.getSimplePath("*"));

  public abstract void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException;

  public abstract SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException;

  @JsonIgnore
  public int getMaxParallelizationWidth();

  /**
   * Returns a signature of the {@link GroupScan} which should usually be composed of
   * all its attributes which could describe it uniquely.
   */
  @JsonIgnore
  public abstract String getDigest();

  @JsonIgnore
  public ScanStats getScanStats();

  /**
   * Returns a clone of GroupScan instance, except that the new GroupScan will use the provided list of columns .
   */
  public GroupScan clone(List<SchemaPath> columns);

  /**
   * GroupScan should check the list of columns, and see if it could support all the columns in the list.
   */
  public boolean canPushdownProjects(List<SchemaPath> columns);

  /**
   * Return the number of non-null value in the specified column. Raise exception, if groupscan does not
   * have exact column row count.
   */
  public long getColumnValueCount(SchemaPath column);

}