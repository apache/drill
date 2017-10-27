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
package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.record.MaterializedField;

/**
 * Provides access to a "tuple". In Drill, both rows and maps are
 * tuples: both are an ordered collection of values, defined by a
 * schema. Each tuple has a schema that defines the column ordering
 * for indexed access. Each tuple also provides methods to get column
 * accessors by name or index.
 */

public interface TupleAccessor {

  /**
   * Flattened view of the schema as needed for row-based access of scalar
   * members. The scalar view presents scalar fields: those that can be set
   * or retrieved. A separate map view presents map vectors. The scalar
   * view is the one used by row set readers and writers. Column indexes
   * are into the flattened view, with maps removed and map members flattened
   * into the top-level name space with compound names.
   */

  public interface TupleSchema {
    /**
     * Return a column schema given an indexed into the flattened row structure.
     *
     * @param index index of the row in the flattened structure
     * @return schema of the column
     */

    MaterializedField column(int index);

    /**
     * Returns {@code MaterializedField} instance from schema using the name specified in param.
     *
     * @param name name of the column in the schema
     * @return {@code MaterializedField} instance
     */
    MaterializedField column(String name);

    /**
     * Returns index of the column in the schema with name specified in param.
     *
     * @param name name of the column in the schema
     * @return index of the column in the schema
     */
    int columnIndex(String name);

    int count();
  }

  TupleSchema schema();
}
