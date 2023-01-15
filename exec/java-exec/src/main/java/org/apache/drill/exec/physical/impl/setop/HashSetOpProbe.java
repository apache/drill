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
package org.apache.drill.exec.physical.impl.setop;

import org.apache.calcite.sql.SqlKind;
import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.common.HashPartition;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;

public interface HashSetOpProbe {
  TemplateClassDefinition<HashSetOpProbe> TEMPLATE_DEFINITION = new TemplateClassDefinition<>(HashSetOpProbe.class, HashSetOpProbeTemplate.class);

  /* The probe side of the hash set op can be in the following two states
   * 1. PROBE_PROJECT: We probe our hash table to see if we have a
   *    key match and project the record depends on the set op kind
   * 2. DONE: Once we have projected all possible records we are done
   */
  enum ProbeState {
    PROBE_PROJECT, DONE
  }

  void setupHashSetOpProbe(RecordBatch probeBatch, HashSetOpRecordBatch outgoing, SqlKind opType, boolean isAll,
                          RecordBatch.IterOutcome leftStartState, HashPartition[] partitions, int cycleNum,
                          VectorContainer container, HashSetOpRecordBatch.HashSetOpSpilledPartition[] spilledInners,
                          boolean buildSideIsEmpty, int numPartitions, int rightHVColPosition) throws SchemaChangeException;
  int  probeAndProject() throws SchemaChangeException;
  void changeToFinalProbeState();
  void setTargetOutputCount(int targetOutputCount);
  int getOutputCount();
}
