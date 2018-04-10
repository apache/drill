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
package org.apache.drill.exec.physical.impl.common;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.common.exceptions.RetryAfterSpillException;

public interface HashTable {
  TemplateClassDefinition<HashTable> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<>(HashTable.class, HashTableTemplate.class);

  /**
   * The maximum capacity of the hash table (in terms of number of buckets).
   */
  int MAXIMUM_CAPACITY = 1 << 30;

  /**
   * The default load factor of a hash table.
   */
  float DEFAULT_LOAD_FACTOR = 0.75f;

  enum PutStatus {KEY_PRESENT, KEY_ADDED, NEW_BATCH_ADDED, KEY_ADDED_LAST, PUT_FAILED;}

  /**
   * The batch size used for internal batch holders
   */
  int BATCH_SIZE = Character.MAX_VALUE + 1;
  int BATCH_MASK = 0x0000FFFF;

  void setup(HashTableConfig htConfig, BufferAllocator allocator, RecordBatch incomingBuild, RecordBatch incomingProbe, RecordBatch outgoing,
             VectorContainer htContainerOrig);

  void updateBatches() throws SchemaChangeException;

  int getHashCode(int incomingRowIdx) throws SchemaChangeException;

  PutStatus put(int incomingRowIdx, IndexPointer htIdxHolder, int hashCode) throws SchemaChangeException, RetryAfterSpillException;

  int containsKey(int incomingRowIdx, boolean isProbe) throws SchemaChangeException;

  void getStats(HashTableStats stats);

  int size();

  boolean isEmpty();

  void clear();

  void reinit(RecordBatch newIncoming);

  void reset();

  void setMaxVarcharSize(int size);

  boolean outputKeys(int batchIdx, VectorContainer outContainer, int outStartIndex, int numRecords, int numExpectedRecords);
}


