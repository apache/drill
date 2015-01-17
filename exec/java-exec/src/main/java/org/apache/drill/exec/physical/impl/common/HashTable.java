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
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;

public interface HashTable {

  public static TemplateClassDefinition<HashTable> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<HashTable>(HashTable.class, HashTableTemplate.class);

  /**
   * The initial default capacity of the hash table (in terms of number of buckets).
   */
  static final public int DEFAULT_INITIAL_CAPACITY = 1 << 16;

  /**
   * The maximum capacity of the hash table (in terms of number of buckets).
   */
  static final public int MAXIMUM_CAPACITY = 1 << 30;

  /**
   * The default load factor of a hash table.
   */
  static final public float DEFAULT_LOAD_FACTOR = 0.75f;

  static public enum PutStatus {KEY_PRESENT, KEY_ADDED, PUT_FAILED;}

  /**
   * The batch size used for internal batch holders
   */
  static final public int BATCH_SIZE = Character.MAX_VALUE + 1;
  static final public int BATCH_MASK = 0x0000FFFF;

  /** Variable width vector size in bytes */
  public static final int VARIABLE_WIDTH_VECTOR_SIZE = 50 * BATCH_SIZE;

  public void setup(HashTableConfig htConfig, FragmentContext context, BufferAllocator allocator,
      RecordBatch incomingBuild, RecordBatch incomingProbe,
      RecordBatch outgoing, VectorContainer htContainerOrig);

  public void updateBatches();

  public void put(int incomingRowIdx, IndexPointer htIdxHolder, int retryCount);

  public int containsKey(int incomingRowIdx, boolean isProbe);

  public void getStats(HashTableStats stats);

  public int size();

  public boolean isEmpty();

  public void clear();

  public boolean outputKeys(int batchIdx, VectorContainer outContainer, int outStartIndex, int numRecords);

  public void addNewKeyBatch();
}


