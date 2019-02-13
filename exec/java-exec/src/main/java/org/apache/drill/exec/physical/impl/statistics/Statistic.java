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
package org.apache.drill.exec.physical.impl.statistics;

/*
 * Base Statistics class - all statistics classes should extend this class
 */
public abstract class Statistic {
  /*
   * The lifecycle states for statistics
   */
  public enum State {INIT, CONFIG, MERGE, COMPLETE};
  /*
   * List of statistics used in Drill.
   */
  public static final String COLNAME = "column";
  public static final String COLTYPE = "majortype";
  public static final String SCHEMA = "schema";
  public static final String COMPUTED = "computed";
  public static final String ROWCOUNT = "rowcount";
  public static final String NNROWCOUNT = "nonnullrowcount";
  public static final String NDV = "approx_count_distinct";
  public static final String HLL_MERGE = "hll_merge";
  public static final String HLL = "hll";
  public static final String AVG_WIDTH = "avg_width";
  public static final String SUM_WIDTH = "sum_width";
  public static final String CNT_DUPS = "approx_count_dups";
  public static final String SUM_DUPS = "sum";
  public static final String TDIGEST = "tdigest";
  public static final String TDIGEST_MERGE = "tdigest_merge";
}
