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
package org.apache.drill.metastore.operate;

import org.apache.drill.metastore.expressions.FilterExpression;

import java.util.Arrays;
import java.util.List;

/**
 * Drill Metastore Modify interface contains methods to be implemented in order
 * to provide modify functionality in the Metastore component.
 *
 * @param <T> component unit type
 */
public interface Modify<T> {

  /**
   * Adds overwrite operation for the Metastore component. For Metastore Tables compoenent,
   * can be used to add new table data or replace partially / fully existing.
   * For example, if one of the table segments has changed,
   * all this segment data and table general information must be replaced with updated data.
   * Thus provided units must include updated data, filter by which existing data will be overwritten
   * will be determined based on given data.
   *
   * @param units component units to be overwritten
   * @return current instance of Modify interface implementation
   */
  Modify<T> overwrite(List<T> units);

  default Modify<T> overwrite(T... units) {
    return overwrite(Arrays.asList(units));
  }

  /**
   * Adds delete operation for the Metastore component based on the given filter expression.
   * For example for Metastore Tables component, if table has two segments
   * and data for one of the segments needs to be deleted.
   * Thus filter must be based on unique identifier of the table's top-level segment:
   * storagePlugin = 'dfs' and workspace = 'tmp' and tableName = 'nation' and metadataKey = 'part_int=3'
   *
   * @param filter filter expression
   * @return current instance of Modify interface implementation
   */
  Modify<T> delete(FilterExpression filter);

  /**
   * Deletes all data from the Metastore component.
   *
   * @return current instance of Modify interface implementation
   */
  Modify<T> purge();

  /**
   * Executes list of provided metastore operations in one transaction if Metastore implementation
   * supports transactions, otherwise executes operations consecutively.
   */
  void execute();
}
