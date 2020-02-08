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
package org.apache.drill.exec.physical.resultSet.project;

/**
 * Plan-time properties of a requested column.Represents
 * a consolidated view of the set of references to a column.
 * For example, the project list might contain:<br>
 * <tt>SELECT columns[4], columns[8]</tt><br>
 * <tt>SELECT a.b, a.c</tt><br>
 * <tt>SELECT columns, columns[1]</tt><br>
 * <tt>SELECT a, a.b</tt><br>
 * In each case, the same column is referenced in different
 * forms which are consolidated into this abstraction.
 * <p>
 * The resulting information is a "pattern": a form of reference
 * which which a concrete type can be compatible or not. The project
 * list does not contain sufficient information to definitively pick
 * a type; it only excludes certain types.
 * <p>
 * Depending on the syntax, we can infer if a column must
 * be an array or map. This is definitive: though we know that
 * columns of the form above must be an array or a map,
 * we cannot know if a simple column reference might refer
 * to an array or map.
 *
 * <h4>Compatibility Rules<h4>
 *
 * The pattern given by projection is consistent with certain concrete types
 * as follows. + means any number of additional qualifiers.
 * <p>
 * <table>
 * <tr><th>Type</th><th>Consistent with</th></tr>
 * <tr><td>Non-repeated MAP</td>
 *     <td>{@code a+} {@code a.b+}</td></tr>
 * <tr><td>Repeated MAP</td>
 *     <td>{@code a+} {@code a.b+} {@code a[n].b+}</td>></tr>
 * <tr><td>Non-repeated Scalar</td>
 *     <td>{@code a}</td></tr>
 * <tr><td>Repeated Scalar</td>
 *     <td>{@code a} {@code a[n]}</td></tr>
 * <tr><td>Non-repeated DICT</td>
 *     <td>{@code a} {@code a['key']}</td></tr>
 * <tr><td>Repeated DICT</td>
 *     <td>{@code a} {@code a[n]} {@code a['key']} {@code a[n]['key']}</td></tr>
 * <tr><td>Non-repeated LIST</td>
 *     <td>{@code a} {@code a[n]}</td></tr>
 * <tr><td>Repeated LIST</td>
 *     <td>{@code a} {@code a[n]} {@code a[n][n]}</td></tr>
 * </table>
 *
 * MAP, DICT, UNION and LIST are structured types: projection can reach
 * into the structure to any number of levels. In such a case, when sufficient
 * schema information is available, the above rules can be applied recursively
 * to each level of structure. The recursion can be done in the class for a
 * DICT (since there is only one child type), but must be external for other
 * complex types. For MAP, the column can report which specific members
 * are projected.
 * <p>
 * The Text reader allows the {@code columns} column, which allows the
 * user to specify indexes. This class reports which indexes were actually
 * selected. Index information is available only at the top level, but
 * not for 2+ dimensions.
 */
public interface RequestedColumn {
  String name();
  String fullName();
  boolean nameEquals(String target);

  boolean isWildcard();

  /**
   * @return true if this column has no qualifiers. Example:
   * {@code a}.
   */
  boolean isSimple();

  /**
   * Report whether the projection implies a tuple. Example:
   * {@code a.b}. Not that this method, and others can only tell
   * if the projection implies a tuple; the actual column may
   * be a tuple (MAP), but be projected simply. The map
   * format also describes a DICT with a VARCHAR key.
   *
   * @return true if the column has a map-like projection.
   */
  boolean isTuple();
  RequestedTuple tuple();

  /**
   * Report whether the first qualifier is an array.
   * Example: {@code a[1]}. The array format also describes
   * a DICT with an integer key.
   * @return true if the column must be an array.
   */
  boolean isArray();

  int arrayDims();

  /**
   * @return true if the column has enumerated indexes.
   * Example: {@code a[2], a[5]}
   */
  boolean hasIndexes();

  /**
   * Return the maximum index value, if indexes where given.
   * Valid if {@code hasIndexes()} returns true.
   * @return the maximum array index value known to the projection
   */
  int maxIndex();

  /**
    * Return a bitmap of the selected indexes. Only valid if
    * {@code hasIndexes()} returns true.
    * @return a bitmap of the selected array indexes
    */
  boolean[] indexes();
  boolean hasIndex(int index);

  Qualifier qualifier();
}
