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

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;

/**
 * Fluent builder to quickly build up an row set (record batch)
 * programmatically. Starting with an {@link OperatorFixture}:
 * <pre></code>
 * OperatorFixture fixture = ...
 * RowSet rowSet = fixture.rowSetBuilder(batchSchema)
 *   .addRow(10, "string", new int[] {10.3, 10.4})
 *   ...
 *   .build();</code></pre>
 */

public final class RowSetBuilder {

  private DirectRowSet rowSet;
  private RowSetWriter writer;
  private boolean withSv2;

  public RowSetBuilder(BufferAllocator allocator, BatchSchema schema) {
    this(allocator, schema, 10);
  }

  public RowSetBuilder(BufferAllocator allocator, BatchSchema schema, int capacity) {
    rowSet = new DirectRowSet(allocator, schema);
    writer = rowSet.writer(capacity);
  }

  /**
   * Add a new row using column values passed as variable-length arguments. Expects
   * map values to be flattened. a schema of (a:int, b:map(c:varchar)) would be>
   * set as <br><tt>add(10, "foo");</tt><br> Values of arrays can be expressed as a Java
   * array. A schema of (a:int, b:int[]) can be set as<br>
   * <tt>add(10, new int[] {100, 200});</tt><br>
   * @param values column values in column index order
   * @return this builder
   * @see {@link #addSingleCol(Object)} to create a row of a single column when
   * the value to <tt>add()</tt> is ambiguous
   */

  public RowSetBuilder add(Object...values) {
    writer.setRow(values);
    return this;
  }

  /**
   * The {@link #add(Object...)} method uses Java variable-length arguments to
   * pass a row of values. But, when the row consists of a single array, Java
   * gets confused: is that an array for variable-arguments or is it the value
   * of the first argument? This method clearly states that the single value
   * (including an array) is meant to be the value of the first (and only)
   * column.
   * <p>
   * Examples:<code><pre>
   *     RowSetBuilder twoColsBuilder = ...
   *     // Fine, second item is an array of strings for a repeated Varchar
   *     // column.
   *     twoColsBuilder.add("First", new String[] {"a", "b", "c"});
   *     ...
   *     RowSetBuilder oneColBuilder = ...
   *     // Ambiguous: is this a varargs array of three items?
   *     // That is how Java will perceive it.
   *     oneColBuilder.add(new String[] {"a", "b", "c"});
   *     // Unambiguous: this is a single column value for the
   *     // repeated Varchar column.
   *     oneColBuilder.addSingleCol(new String[] {"a", "b", "c"});
   * </pre></code>
   * @param value value of the first column, which may be an array for a
   * repeated column
   * @return this builder
   */

  public RowSetBuilder addSingleCol(Object value) {
    return add(new Object[] { value });
  }

  /**
   * Build the row set with a selection vector 2. The SV2 is
   * initialized to have a 1:1 index to the rows: SV2 0 points
   * to row 1, SV2 position 1 points to row 1 and so on.
   *
   * @return this builder
   */

  public RowSetBuilder withSv2() {
    withSv2 = true;
    return this;
  }

  public SingleRowSet build() {
    writer.done();
    if (withSv2) {
      return rowSet.toIndirect();
    }
    return rowSet;
  }
}
