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

/**
 * Interface for reading from tuples (rows or maps). Provides
 * a column reader for each column that can be obtained either
 * by name or column index (as defined in the tuple schema.)
 * Also provides two generic methods to get the value as a
 * Java object or as a string.
 */

public interface TupleReader extends TupleAccessor {
  ColumnReader column(int colIndex);

  /**
   * Returns column reader for the column with name specified in param.
   *
   * @param colName name of the column in the schema
   * @return column reader
   */
  ColumnReader column(String colName);
  Object get(int colIndex);
  String getAsString(int colIndex);
}
