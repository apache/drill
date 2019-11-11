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
package org.apache.drill.exec.physical.resultSet;

import org.apache.drill.exec.physical.impl.protocol.BatchAccessor;
import org.apache.drill.exec.physical.rowSet.RowSetReader;

/**
 * Iterates over the set of batches in a result set, providing
 * a row set reader to iterate over the rows within each batch.
 * Handles schema changes between batches.
 * <h4>Protocol</h4>
 * <ol>
 * <li>Create an instance.</li>
 * <li>For each incoming batch:
 *   <ol>
 *   <li>Call {@link #start()} to attach the batch. The provided
 *       {@link BatchAccessor} reports if the schema has changed.</li>
 *   <li>Call {@link #reader()} to obtain a reader.</li>
 *   <li>Iterate over the batch using the reader.</li>
 *   <li>Call {@link #release()} to free the memory for the
 *       incoming batch. Or, to call {@link #detach()} to keep
 *       the batch memory.</li>
 *   </ol>
 * <li>Call {@link #close()} after all batches are read.</li>
 * </ol>
 */
public interface ResultSetReader {

  void start(BatchAccessor batch);
  RowSetReader reader();
  void detach();
  void release();
  void close();
}
