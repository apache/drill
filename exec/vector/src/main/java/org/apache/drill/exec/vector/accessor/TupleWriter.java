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
 * Interface for writing to rows via a column writer.
 * Column writers can be obtained by name or index. Column
 * indexes are defined by the tuple schema. Also provides
 * a convenience method to set the column value from a Java
 * object. The caller is responsible for providing the
 * correct object type for each column. (The object type
 * must match the column accessor type.)
 */

public interface TupleWriter extends TupleAccessor {
  ColumnWriter column(int colIndex);

  /**
   * Returns column writer for the column with name specified in param.
   *
   * @param colName name of the column in the schema
   * @return column writer
   */
  ColumnWriter column(String colName);
  void set(int colIndex, Object value);
}
