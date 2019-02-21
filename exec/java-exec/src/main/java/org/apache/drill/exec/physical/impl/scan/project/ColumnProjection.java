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
package org.apache.drill.exec.physical.impl.scan.project;

/**
 * Core interface for a projected column. Models a column throughout the
 * projection lifecycle. Columns evolve from unresolved to resolved at
 * different times. Each class that derives from this interface can act
 * as a column "node", while declaring its type so it may be processed
 * easily at the proper type.
 * <p>
 * For example, an implicit column is processed at the file schema
 * resolution phase, converting from unresolved to resolved. At the same
 * time, table columns remain unresolved, waiting for the table schema
 * to appear.
 * <p>
 * In an advanced, experimental feature, schema persistence sees some
 * columns transition from resolved to unresolved and back again.
 * <p>
 * Having all column nodes derive from this same interface keeps things
 * tidy.
 */

public interface ColumnProjection {

  public static final int FRAMEWORK_BASE_ID = 100;
  public static final int READER_BASE_ID = 200;

  /**
   * The name of the column as it appears in the output
   * row (record batch.)
   *
   * @return the output column name
   */
  String name();

  /**
   * A node type unique to each node. Nodes defined in this package
   * use IDs less than {@link #FRAMEWORK_BASE_ID}. Nodes defined by
   * frameworks (for file metadata columns or for other special
   * columns) start numbering with {@link #FRAMEWORK_BASE_ID}. Readers
   * may need their own specialized nodes, which must use IDs starting
   * with {@link #READER_BASE_ID}.
   * <p>
   * This system solves two problems:
   * <ol>
   * <li>Provides an efficient way for each mechanism to recognize its
   * own nodes without using <code>instance of</code>.</li>
   * <li>Allows for frameworks and readers to be added without changing
   * any base enum. This works because every instance of this mechanism
   * sees only the base nodes, those from a single framework and those
   * from a single reader; there is no need for a universal ID registry.
   * Two frameworks can use identical IDs because they never mix.
   * Similarly, two readers can use the same IDs because Drill does not
   * allow a single scan operator to use two different kinds of readers.
   * </li>
   * </ol>
   * @return the numeric ID for this node, used for each layer to
   * recognize its own nodes
   */
  int nodeType();
}
