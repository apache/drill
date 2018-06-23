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

import org.apache.drill.exec.vector.accessor.TupleReader;

/**
 * Reader for all types of row sets.
 */

public interface RowSetReader extends TupleReader {

  /**
   * Total number of rows in the row set.
   * @return total number of rows
   */
  int rowCount();

  boolean next();
  int logicalIndex();
  void setPosn(int index);

  /**
   * Batch index: 0 for a single batch, batch for the current
   * row is a hyper-batch.
   * @return index of the batch for the current row
   */
  int hyperVectorIndex();

  /**
   * The index of the underlying row which may be indexed by an
   * SV2 or SV4.
   *
   * @return
   */

  int offset();
}