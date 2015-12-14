/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.join;

import java.util.List;

/*
 * Keep status of hash join proble phase.
 */
public class HashJoinProbeStatus {
  // Number of records to process on the probe side
  public int recordsToProcess = 0;
  // Number of records processed on the probe side
  public int recordsProcessed = 0;

  // Indicate if we should drain the next record from the probe side
  public boolean getNextRecord = true;

  // Contains both batch idx and record idx of the matching record in the build side
  public int currentCompositeIdx = -1;

  // Current state the hash join algorithm is in
  public HashJoinProbe.ProbeState probeState = HashJoinProbe.ProbeState.PROBE_PROJECT;

  // For outer or right joins, this is a list of unmatched records that needs to be projected
  public List<Integer> unmatchedBuildIndexes = null;

  public String toString() {
    return String.format("recordsToProcess: %d, recordsProcessed: %d, getNextRecord: %s, currentCompositeIdx: %d, probeState: %s",
      recordsToProcess, recordsProcessed, getNextRecord, currentCompositeIdx, probeState);
  }
}
