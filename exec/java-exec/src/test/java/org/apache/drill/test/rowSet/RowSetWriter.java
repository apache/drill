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
package org.apache.drill.test.rowSet;

import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * Interface for writing values to a row set. Only available
 * for newly-created, single, direct row sets. Eventually, if
 * we want to allow updating a row set, we have to create a
 * new row set with the updated columns, then merge the new
 * and old row sets to create a new immutable row set.
 * <p>
 * Typical usage:
 * <pre></code>
 * void writeABatch() {
 *   RowSetWriter writer = ...
 *   for (;;) {
 *     if (! writer.valid()) { break; }
 *     writer.column(0).setInt(10);
 *     writer.column(1).setString("foo");
 *     ...
 *     writer.save();
 *   }
 * }</code></pre>
 * The above writes until the batch is full, based on size. If values
 * are large enough to potentially cause vector overflow, do the
 * following instead:
 * <pre></code>
 * void writeABatch() {
 *   RowSetWriter writer = ...
 *   for (;;) {
 *     if (! writer.valid()) { break; }
 *     writer.column(0).setInt(10);
 *     if (! writer.column(1).setString("foo")) { break; }
 *     ...
 *     writer.save();
 *   }
 *   // Do something with the partially-written last row.
 * }</code></pre>
 */

public interface RowSetWriter extends TupleWriter {

  /**
   * Write a row of values, given by Java objects. Object type must
   * match expected column type. Stops writing, and returns false,
   * if any value causes vector overflow.
   *
   * @param values variable-length argument list of column values
   * @return true if the row was written, false if any column
   * caused vector overflow.
   * @throws VectorOverflowException
   */
  void setRow(Object...values) throws VectorOverflowException;

  /**
   * Indicates if the current row position is valid for
   * writing. Will be true on the first row, and all subsequent
   * rows until either the maximum number of rows are written,
   * or a vector overflows. After that, will return false. The
   * method returns false as soon as any column writer returns
   * false, even in the middle of a row write.
   *
   * @return true if the current row can be written, false
   * if not
   */

  boolean valid();
  int rowIndex();
  void save();
  void done();
}
