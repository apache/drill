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

package org.apache.drill.exec.physical.impl.join;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.record.RecordBatch;

import javax.annotation.Nullable;
import java.util.Set;

public class MechanicalHashJoinMemoryCalculator implements HashJoinMemoryCalculator {
  private final int maxNumInMemBatches;

  private boolean doMemoryCalc;

  public MechanicalHashJoinMemoryCalculator(int maxNumInMemBatches) {
    this.maxNumInMemBatches = maxNumInMemBatches;
  }

  @Override
  public void initialize(boolean doMemoryCalc) {
    this.doMemoryCalc = doMemoryCalc;
  }

  @Nullable
  @Override
  public BuildSidePartitioning next() {
    if (doMemoryCalc) {
      // return the mechanical implementation
      return new MechanicalBuildSidePartitioning(maxNumInMemBatches);
    } else {
      // return Noop implementation
      return new HashJoinMemoryCalculatorImpl.NoopBuildSidePartitioningImpl();
    }
  }

  @Override
  public HashJoinState getState() {
    return HashJoinState.INITIALIZING;
  }

  public static class MechanicalBuildSidePartitioning implements BuildSidePartitioning {
    private final int maxNumInMemBatches;

    private int initialPartitions;
    private PartitionStatSet partitionStatSet;

    public MechanicalBuildSidePartitioning(int maxNumInMemBatches) {
      this.maxNumInMemBatches = maxNumInMemBatches;
    }

    @Override
    public void initialize(boolean autoTune,
                           boolean reserveHash,
                           RecordBatch buildSideBatch,
                           RecordBatch probeSideBatch,
                           Set<String> joinColumns,
                           long memoryAvailable,
                           int initialPartitions,
                           int recordsPerPartitionBatchBuild,
                           int recordsPerPartitionBatchProbe,
                           int maxBatchNumRecordsBuild,
                           int maxBatchNumRecordsProbe,
                           int outputBatchNumRecords,
                           double loadFactor) {
      this.initialPartitions = initialPartitions;
    }

    @Override
    public void setPartitionStatSet(PartitionStatSet partitionStatSet) {
      this.partitionStatSet = Preconditions.checkNotNull(partitionStatSet);
    }

    @Override
    public int getNumPartitions() {
      return initialPartitions;
    }

    @Override
    public long getBuildReservedMemory() {
      return 0;
    }

    @Override
    public long getMaxReservedMemory() {
      return 0;
    }

    @Override
    public boolean shouldSpill() {
      return partitionStatSet.getNumInMemoryBatches() > maxNumInMemBatches;
    }

    @Override
    public String makeDebugString() {
      return "Mechanical build side calculations";
    }

    @Nullable
    @Override
    public PostBuildCalculations next() {
      return new MechanicalPostBuildCalculations(maxNumInMemBatches, partitionStatSet);
    }

    @Override
    public HashJoinState getState() {
      return HashJoinState.BUILD_SIDE_PARTITIONING;
    }
  }

  public static class MechanicalPostBuildCalculations implements PostBuildCalculations {
    private final int maxNumInMemBatches;
    private final PartitionStatSet partitionStatSet;

    public MechanicalPostBuildCalculations(int maxNumInMemBatches,
                                           PartitionStatSet partitionStatSet) {
      this.maxNumInMemBatches = maxNumInMemBatches;
      this.partitionStatSet = Preconditions.checkNotNull(partitionStatSet);
    }

    @Override
    public void initialize() {
      // Do nothing
    }

    @Override
    public boolean shouldSpill() {
      return partitionStatSet.getNumInMemoryBatches() > maxNumInMemBatches;
    }

    @Override
    public String makeDebugString() {
      return "Mechanical post build calculations";
    }

    @Nullable
    @Override
    public HashJoinMemoryCalculator next() {
      return null;
    }

    @Override
    public HashJoinState getState() {
      return HashJoinState.POST_BUILD_CALCULATIONS;
    }
  }
}
