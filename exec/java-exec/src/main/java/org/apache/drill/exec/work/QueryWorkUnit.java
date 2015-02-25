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
package org.apache.drill.exec.work;

import java.util.List;

import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.proto.BitControl.PlanFragment;

import com.google.common.base.Preconditions;

public class QueryWorkUnit {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryWorkUnit.class);
  private final PlanFragment rootFragment; // for local
  private final FragmentRoot rootOperator; // for local
  private final List<PlanFragment> fragments;

  public QueryWorkUnit(final FragmentRoot rootOperator, final PlanFragment rootFragment,
      final List<PlanFragment> fragments) {
    Preconditions.checkNotNull(rootFragment);
    Preconditions.checkNotNull(fragments);
    Preconditions.checkNotNull(rootOperator);

    this.rootFragment = rootFragment;
    this.fragments = fragments;
    this.rootOperator = rootOperator;
  }

  public PlanFragment getRootFragment() {
    return rootFragment;
  }

  public List<PlanFragment> getFragments() {
    return fragments;
  }

  public FragmentRoot getRootOperator() {
    return rootOperator;
  }
}
