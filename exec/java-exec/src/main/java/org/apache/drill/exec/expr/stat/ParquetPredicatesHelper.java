/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expr.stat;

import org.apache.parquet.column.statistics.Statistics;

/**
 * Parquet predicates class helper for filter pushdown.
 */
@SuppressWarnings("rawtypes")
public class ParquetPredicatesHelper {

  /**
   * @param stat statistics object
   * @return true if the input stat object has valid statistics; false otherwise
   */
  public static boolean hasStats(Statistics stat) {
    return stat != null && !stat.isEmpty();
  }

  /**
   * Checks that column chunk's statistics has only nulls
   *
   * @param stat parquet column statistics
   * @param rowCount number of rows in the parquet file
   * @return True if all rows are null in the parquet file
   *          False if at least one row is not null.
   */
  public static boolean isAllNulls(Statistics stat, long rowCount) {
    return stat.getNumNulls() == rowCount;
  }

  /**
   * Checks that column chunk's statistics has at least one null
   *
   * @param stat parquet column statistics
   * @return True if the parquet file has nulls
   *          False if the parquet file hasn't nulls.
   */
  public static boolean hasNulls(Statistics stat) {
    return stat.getNumNulls() > 0;
  }

}
