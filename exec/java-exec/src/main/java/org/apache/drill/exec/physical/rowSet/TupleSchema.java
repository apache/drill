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

import org.apache.drill.exec.physical.rowSet.impl.MaterializedSchema;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Defines the schema of a tuple: either the top-level row or a nested
 * "map" (really structure). A schema is a collection of columns (backed
 * by vectors in the loader itself.) Columns are accessible by name or
 * index. New columns may be added at any time; the new column takes the
 * next available index.
 */

public interface TupleSchema {

  public interface TupleColumnSchema {
    MaterializedField schema();

    /**
     * Report if a column is selected.
     * @param colIndex index of the column to check
     * @return true if the column is selected (data is collected),
     * false if the column is unselected (data is discarded)
     */

    boolean isSelected();
    int vectorIndex();
  }

  int columnCount();
  int columnIndex(String colName);
  MaterializedField column(int colIndex);
  TupleColumnSchema metadata(int colIndex);

  /**
   * Return the schema for the given column name.
   *
   * @param colName column name within the tuple as a
   * single-part name
   * @return the schema if the column is defined, null
   * otherwise
   */

  MaterializedField column(String colName);
  TupleColumnSchema metadata(String colName);

  /**
   * Add a new column to the schema.
   *
   * @param columnSchema
   * @return the index of the new column
   */

  int addColumn(MaterializedField columnSchema);

  /**
   * Create the tuple schema from a batch schema. The tuple schema
   * must be empty.
   * @param schema the schema for the tuple
   */

  void setSchema(BatchSchema schema);

  /**
   * Return the column list as a batch schema. Primarily for testing.
   * @return the current schema as a batch schema, with columns
   * in the same order as they were added (that is, in row index
   * order)
   */

  BatchSchema schema();
  MaterializedSchema materializedSchema();
}
