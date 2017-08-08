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
package org.apache.drill.exec.physical.rowSet;

import org.apache.drill.exec.record.VectorContainer;

/**
 * Builds a result set (series of zero or more row sets) based on a defined
 * schema which may
 * evolve (expand) over time. Automatically rolls "overflow" rows over
 * when a batch fills.
 * <p>
 * Many of the methods in this interface are verify that the loader is
 * in the proper state. For example, an exception is thrown if the caller
 * attempts to save a row before starting a batch. However, the per-column
 * write methods are checked only through assertions that should enabled
 * during testing, but will be disabled during production.
 *
 * @see {@link VectorContainerWriter}, the class which this class
 * replaces
 */

public interface ResultSetLoader {

  /**
   * Current schema version. The version increments by one each time
   * a column is added.
   * @return the current schema version
   */

  int schemaVersion();
  int targetRowCount();
  int targetVectorSize();

  /**
   * Total number of batches created. Includes the current batch if
   * the row count in this batch is non-zero.
   * @return the number of batches produced including the current
   * one
   */

  int batchCount();

  /**
   * Total number of rows loaded for all previous batches and the
   * current batch.
   * @return total row count
   */

  int totalRowCount();

  /**
   * Start a new row batch. Valid only when first started, or after the
   * previous batch has been harvested.
   */

  void startBatch();

  /**
   * Writer for the top-level tuple (the entire row). Valid only when
   * the mutator is actively writing a batch (after <tt>startBatch()</tt>
   * but before </tt>harvest()</tt>.)
   *
   * @return writer for the top-level columns
   */

  TupleLoader writer();

  /**
   * Called before writing a new row.
   */

  void startRow();

  /**
   * Called after writing each row to move to the next row. Failing to
   * call this method effectively abandons the in-flight row; something
   * that may be useful to recover from partially-written rows that turn
   * out to contain errors.
   */

  void saveRow();

  /**
   * Indicates that no more rows fit into the current row batch
   * and that the row batch should be harvested and sent downstream.
   * Any overflow row is automatically saved for the next cycle.
   * The value is undefined when a batch is not active.
   *
   * @return true if the current row set has reached capacity,
   * false if more rows can be written
   */

  boolean isFull();

  /**
   * The number of rows in the current row set. Does not count any
   * overflow row saved for the next batch.
   * @return number of rows to be sent downstream
   */

  int rowCount();

  /**
   * Return the output container, primarily to obtain the schema
   * and set of vectors. Depending on when this is called, the
   * data may or may not be populated: call
   * {@link #harvest()} to obtain the container for a batch.
   * <p>
   * This method is useful when the schema is known and fixed.
   * After declaring the schema, call this method to get the container
   * that holds the vectors for use in planning projection, etc.
   * <p>
   * If the result set schema changes, then a call to this method will
   * return the latest schema. But, if the schema changes during the
   * overflow row, then this method will not see those changes until
   * after harvesting the current batch. (This avoid the appearance
   * of phantom columns in the output since the new column won't
   * appear until the next batch.)
   * <p>
   * Never count on the data in the container; it may be empty, half
   * written, or inconistent. Always call
   * {@link #harvest()} to obtain the container for a batch.
   *
   * @return the output container including schema and value
   * vectors
   */

  VectorContainer outputContainer();

  /**
   * Harvest the current row batch, and reset the mutator
   * to the start of the next row batch (which may already contain
   * an overflow row.
   * @return the row batch to send downstream
   */

  VectorContainer harvest(); // ?

  /**
   * Clear the current, empty, in-flight batch to prepare for a new
   * batch. Typically called when the last batch from a reader is
   * empty, but another reader will continue the read.
   */

  void reset();

  /**
   * Called after all rows are returned, whether because no more data is
   * available, or the caller wishes to cancel the current row batch
   * and complete.
   */

  void close();
}
