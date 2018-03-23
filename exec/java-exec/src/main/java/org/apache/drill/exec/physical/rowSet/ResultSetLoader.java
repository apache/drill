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
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;

/**
 * Builds a result set (series of zero or more row sets) based on a defined
 * schema which may
 * evolve (expand) over time. Automatically rolls "overflow" rows over
 * when a batch fills.
 * <p>
 * Many of the methods in this interface verify that the loader is
 * in the proper state. For example, an exception is thrown if the caller
 * attempts to save a row before starting a batch. However, the per-column
 * write methods are checked only through assertions that should enabled
 * during testing, but will be disabled during production.
 *
 * @see {@link VectorContainerWriter}, the class which this class
 * replaces
 */

public interface ResultSetLoader {

  public static final int DEFAULT_ROW_COUNT = BaseValueVector.INITIAL_VALUE_ALLOCATION;

  /**
   * Current schema version. The version increments by one each time
   * a column is added.
   * @return the current schema version
   */

  int schemaVersion();

  /**
   * Adjust the number of rows to produce in the next batch. Takes
   * affect after the next call to {@link #startBatch()}.
   *
   * @param count target batch row count
   */

  void setTargetRowCount(int count);

  /**
   * The number of rows produced by this loader (as configured in the loader
   * options.)
   *
   * @return the target row count for batches that this loader produces
   */

  int targetRowCount();

  /**
   * The largest vector size produced by this loader (as specified by
   * the value vector limit.)
   *
   * @return the largest vector size. Attempting to extend a vector beyond
   * this limit causes automatic vector overflow and terminates the
   * in-flight batch, even if the batch has not yet reached the target
   * row count
   */

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

  RowSetLoader writer();
  boolean writeable();

  /**
   * Load a row using column values passed as variable-length arguments. Expects
   * map values to represented as an array.
   * A schema of (a:int, b:map(c:varchar)) would be>
   * set as <br><tt>loadRow(10, new Object[] {"foo"});</tt><br>
   * Values of arrays can be expressed as a Java
   * array. A schema of (a:int, b:int[]) can be set as<br>
   * <tt>loadRow(10, new int[] {100, 200});</tt><br>.
   * Primarily for testing, too slow for production code.
   * <p>
   * If the row consists of a single map or list, then the one value will be an
   * <tt>Object</tt> array, creating an ambiguity. Use <tt>writer().set(0, value);</tt>
   * in this case.
   *
   * @param values column values in column index order
   * @return this loader
   */

  ResultSetLoader setRow(Object...values);

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
   * written, or inconsistent. Always call
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
   * <p>
   * The schema of the returned container is defined as:
   * <ul>
   * <li>The schema as passed in via the loader options, plus</li>
   * <li>Columns added dynamically during write, minus</li>
   * <li>Any columns not included in the project list, minus</li>
   * <li>Any columns added in the overflow row.</li>
   * </ul>
   * That is, column order is as defined by the initial schema and column
   * additions. In particular, the schema order is <b>not</b> defined by
   * the projection list. (Another mechanism is required to reorder columns
   * for the actual projection.)
   *
   * @return the row batch to send downstream
   */

  VectorContainer harvest();

  /**
   * The schema of the harvested batch. Valid until the start of the
   * next batch.
   *
   * @return the extended schema of the harvested batch which includes
   * any allocation hints used when creating the batch
   */

  TupleMetadata harvestSchema();

  /**
   * Called after all rows are returned, whether because no more data is
   * available, or the caller wishes to cancel the current row batch
   * and complete.
   */

  void close();
}
