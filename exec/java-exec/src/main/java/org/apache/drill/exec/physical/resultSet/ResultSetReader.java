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
 * <p>
 * Designed to handle batches arriving from a single upstream
 * operator. Uses Drill's strict form of schema identity: that
 * not only must the column definitions match; the vectors must
 * be identical from one batch to the next. If the vectors differ,
 * then this class assumes a new schema has occurred, and will
 * rebuild all the underlying readers, which can be costly.
 *
 * <h4>Protocol</h4>
 * <ol>
 * <li>Create an instance, passing in a
 *     {@link BatchAccessor} to hold the batch and optional
 *     selection vector.</li>
 * <li>For each incoming batch:
 *   <ol>
 *   <li>Call {@link #start()} to attach the batch. The associated
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

  /**
   * Start tracking a new batch in the associated
   * vector container.
   */
  void start();

  /**
   * Get the row reader for this batch. The row reader is
   * guaranteed to remain the same for the life of the
   * result set reader.
   *
   * @return the row reader to read rows for the current
   * batch
   */
  RowSetReader reader();

  /**
   * Detach the batch of data from this reader. Does not
   * release the memory for that batch.
   */
  void detach();

  /**
   * Detach the batch of data from this reader and release
   * the memory for that batch. Call this method before
   * loading the underlying vector container with more
   * data, then call {@link #start()} after new data is
   * available.
   */
  void release();

  /**
   * Close this reader. Releases any memory still assigned
   * to any attached batch. Call {@link #detach()} first if
   * you want to preserve the batch memory.
   */
  void close();

  /**
   * Convenience method to access the input batch.
   * @return the batch bound to the reader at construction
   * time
   */
  BatchAccessor inputBatch();
}
