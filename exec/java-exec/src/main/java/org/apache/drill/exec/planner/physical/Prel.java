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
package org.apache.drill.exec.planner.physical;

import java.io.IOException;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.common.DrillRelNode;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.relopt.Convention;

public interface Prel extends DrillRelNode, Iterable<Prel>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Prel.class);

  final static Convention DRILL_PHYSICAL = new Convention.Impl("PHYSICAL", Prel.class);

  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException;

  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E;

  public SelectionVectorMode[] getSupportedEncodings();
  public SelectionVectorMode getEncoding();
  boolean needsFinalColumnReordering();

}
