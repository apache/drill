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

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;

/**
 * Specifies the type of projection obtained by parsing the
 * projection list. The type is returned from a query of the
 * form "how is this column projected, if at all?"
 * <p>
 * The projection type allows the scan framework to catch
 * inconsistencies, such as projecting an array as a map,
 * and so on.
 */

public enum ProjectionType {

  /**
   * The column is not projected in the query.
   */

  UNPROJECTED,

  /**
   * Projection is a wildcard.
   */
  WILDCARD,     // *

  /**
   * Projection is by simple name. "General" means that
   * we have no hints about the type of the column from
   * the projection.
   */

  GENERAL,      // x

  /**
   * The column is projected as a scalar. This state
   * requires metadata beyond the projection list and
   * is returned only when that metadata is available.
   */

  SCALAR,       // x (from schema)

  /**
   * Applies to the parent of an x.y pair in projection: the
   * existence of a dotted-member tells us that the parent
   * must be a tuple (e.g. a Map.)
   */

  TUPLE,        // x.y

  /**
   * The projection includes an array suffix, so the column
   * must be an array.
   */

  ARRAY,        // x[0]

  /**
   * Combination of array and map hints.
   */

  TUPLE_ARRAY,  // x[0].y

  DICT, // x[0] or x['key'] (depends on key type)

  DICT_ARRAY; // x[0][42] or x[0]['key'] (depends on key type)

  public boolean isTuple() {
    return this == ProjectionType.TUPLE || this == ProjectionType.TUPLE_ARRAY;
  }

  public boolean isArray() {
    return this == ProjectionType.ARRAY || this == ProjectionType.TUPLE_ARRAY || this == DICT_ARRAY;
  }

  public boolean isDict() {
    return this == DICT || this == DICT_ARRAY;
  }

  /**
   * We can't tell, just from the project list, if a column must
   * be scalar. A column of the form "a" could be a scalar, but
   * that form is also consistent with maps and arrays.
   */
  public boolean isMaybeScalar() {
    return this == GENERAL || this == SCALAR;
  }

  public static ProjectionType typeFor(MajorType majorType) {
    boolean repeated = Types.isRepeated(majorType);
    if (majorType.getMinorType() == MinorType.MAP) {
      return repeated ? TUPLE_ARRAY : TUPLE;
    } else if (majorType.getMinorType() == MinorType.DICT) {
      return repeated ? DICT_ARRAY : DICT;
    } else if (repeated || majorType.getMinorType() == MinorType.LIST) {
      return ARRAY;
    }
    return SCALAR;
  }

  /**
   * Reports if this type (representing an item in a projection list)
   * is compatible with the projection type representing an actual
   * column produced by an operator. The check is not symmetric.
   * <p>
   * For example, a column of type map array is compatible with a
   * projection of type map "m.a" (project all a members of the map array),
   * but a projection type of map array "m[1].a" is not compatible with
   * a (non-array) map column.
   *
   * @param readType projection type, from {@link #typeFor(MajorType)},
   * for an actual column
   * @return true if this projection type is compatible with the
   * column's projection type
   */

  public boolean isCompatible(ProjectionType readType) {
    switch (readType) {
    case UNPROJECTED:
    case GENERAL:
    case WILDCARD:
      return true;
    default:
      break;
    }

    switch (this) {
    case ARRAY:
      return readType == ARRAY || readType == TUPLE_ARRAY
          || readType == DICT // the actual key type should be validated later
          || readType == DICT_ARRAY;
    case TUPLE_ARRAY:
      return readType == TUPLE_ARRAY || readType == DICT_ARRAY;
    case SCALAR:
      return readType == SCALAR;
    case TUPLE:
      return readType == TUPLE || readType == TUPLE_ARRAY || readType == DICT || readType == DICT_ARRAY;
    case DICT:
      return readType == DICT || readType == DICT_ARRAY;
    case UNPROJECTED:
    case GENERAL:
    case WILDCARD:
      return true;
    default:
      throw new IllegalStateException(toString());
    }
  }

  public String label() {
    switch (this) {
    case SCALAR:
      return "scalar (a)";
    case ARRAY:
      return "array (a[n])";
    case TUPLE:
      return "tuple (a.x)";
    case TUPLE_ARRAY:
      return "tuple array (a[n].x)";
    case DICT:
      return "dict (a['key'])";
    case WILDCARD:
      return "wildcard (*)";
    default:
      return name();
    }
  }
}
