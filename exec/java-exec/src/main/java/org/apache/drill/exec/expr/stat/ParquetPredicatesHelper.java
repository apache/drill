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
package org.apache.drill.exec.expr.stat;

import org.apache.parquet.Preconditions;
import org.apache.parquet.column.statistics.Statistics;

/**
 * Parquet predicates class helper for filter pushdown.
 */
class ParquetPredicatesHelper {
  private ParquetPredicatesHelper() {
  }

  /**
   * @param stat statistics object
   * @return <tt>true</tt> if the input stat object has valid statistics; false otherwise
   */
  static boolean isNullOrEmpty(Statistics stat) {
    return stat == null || stat.isEmpty();
  }

  /**
   * Checks that column chunk's statistics has only nulls
   *
   * @param stat parquet column statistics
   * @param rowCount number of rows in the parquet file
   * @return <tt>true</tt> if all rows are null in the parquet file and <tt>false</tt> otherwise
   */
  static boolean isAllNulls(Statistics stat, long rowCount) {
    Preconditions.checkArgument(rowCount >= 0, String.format("negative rowCount %d is not valid", rowCount));
    return stat.getNumNulls() == rowCount;
  }

  /**
   * Checks that column chunk's statistics does not have nulls
   *
   * @param stat parquet column statistics
   * @return <tt>true</tt> if the parquet file does not have nulls and <tt>false</tt> otherwise
   */
  static boolean hasNoNulls(Statistics stat) {
    return stat.getNumNulls() == 0;
  }

}
