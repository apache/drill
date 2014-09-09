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
package org.apache.drill.exec.store;

import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField.Key;
import org.apache.drill.exec.vector.ValueVector;

public interface RecordReader {

  public static final long ALLOCATOR_INITIAL_RESERVATION = 1*1024*1024;
  public static final long ALLOCATOR_MAX_RESERVATION = 20L*1000*1000*1000;

  /**
   * Configure the RecordReader with the provided schema and the record batch that should be written to.
   *
   * @param output
   *          The place where output for a particular scan should be written. The record reader is responsible for
   *          mutating the set of schema values for that particular record.
   * @throws ExecutionSetupException
   */
  public abstract void setup(OutputMutator output) throws ExecutionSetupException;

  public abstract void allocate(Map<Key, ValueVector> vectorMap) throws OutOfMemoryException;

  /**
   * Set the operator context. The Reader can use this to access the operator context and allocate direct memory
   * if needed
   * @param operatorContext
   */
  public abstract void setOperatorContext(OperatorContext operatorContext);


  /**
   * Increment record reader forward, writing into the provided output batch.
   *
   * @return The number of additional records added to the output.
   */
  public abstract int next();

  public abstract void cleanup();

}