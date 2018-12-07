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
package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.common.exceptions.RetryAfterSpillException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.common.IndexPointer;
import org.apache.drill.exec.record.RecordBatch;

import java.io.IOException;

public interface HashAggPartition {
  void updateBatches();
  void setup(RecordBatch newIncoming) throws SchemaChangeException, IOException;
  boolean isSpilled();
  int getBatchHolderCount();
  int getSpilledBatchesCount();
  int getPartitionIndex();
  boolean doneOutputting();
  int outputCurrentBatch();
  void spill();
  HashAggSpilledPartition finishSpilling(int originalPartition);
  void addStats(HashTableStats hashTableStats);
  void cleanup();
  void reinitPartition();
  boolean hasPendingRows();

  void addBatchHolder(HashAggBatchHolder batchHolder);
  int buildHashcode(int incomingRowIdx) throws SchemaChangeException;
  HashAggBatchHolder getBatchHolder(int index);
  HashTable.PutStatus put(int incomingRowIdx, IndexPointer htIdxHolder, int hashCode, int batchSize) throws SchemaChangeException, RetryAfterSpillException;

  void resetOutBatchIndex();
}
