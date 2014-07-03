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
package org.apache.drill.exec.store.mongo;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

public class MongoGroupScan extends AbstractGroupScan implements
    DrillMongoConstants {

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints)
      throws PhysicalOperatorSetupException {

  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId)
      throws ExecutionSetupException {
    return null;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 0;
  }

  @Override
  public String getDigest() {
    return null;
  }

  @Override
  public ScanStats getScanStats() {
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return null;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return null;
  }

}
