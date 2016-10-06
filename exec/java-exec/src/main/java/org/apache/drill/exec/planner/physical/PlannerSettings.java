/**
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
package org.apache.drill.exec.planner.physical;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValidator;
import org.apache.drill.exec.server.options.TypeValidators;
import org.apache.drill.exec.server.options.TypeValidators.BooleanValidator;
import org.apache.drill.exec.server.options.TypeValidators.LongValidator;
import org.apache.drill.exec.server.options.TypeValidators.DoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.PositiveLongValidator;
import org.apache.drill.exec.server.options.TypeValidators.RangeDoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.RangeLongValidator;
import org.apache.drill.exec.server.options.TypeValidators.MinRangeDoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.MaxRangeDoubleValidator;
import org.apache.calcite.plan.Context;

public class PlannerSettings implements Context{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlannerSettings.class);

  private int numEndPoints = 0;
  private boolean useDefaultCosting = false; // True: use default Optiq costing, False: use Drill costing
  private boolean forceSingleMode;

  public static final int MAX_BROADCAST_THRESHOLD = Integer.MAX_VALUE;
  public static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 1024;

  // initial off heap memory allocation (1M)
  private static final long INITIAL_OFF_HEAP_ALLOCATION_IN_BYTES = 1024 * 1024;
  // default off heap memory for planning (256M)
  private static final long DEFAULT_MAX_OFF_HEAP_ALLOCATION_IN_BYTES = 256 * 1024 * 1024;
  // max off heap memory for planning (16G)
  private static final long MAX_OFF_HEAP_ALLOCATION_IN_BYTES = 16l * 1024 * 1024 * 1024;

  public static final OptionValidator CONSTANT_FOLDING = new BooleanValidator("planner.enable_constant_folding", true);
  public static final OptionValidator EXCHANGE = new BooleanValidator("planner.disable_exchanges", false);
  public static final OptionValidator HASHAGG = new BooleanValidator("planner.enable_hashagg", true);
  public static final OptionValidator STREAMAGG = new BooleanValidator("planner.enable_streamagg", true);
  public static final OptionValidator HASHJOIN = new BooleanValidator("planner.enable_hashjoin", true);
  public static final OptionValidator MERGEJOIN = new BooleanValidator("planner.enable_mergejoin", true);
  public static final OptionValidator NESTEDLOOPJOIN = new BooleanValidator("planner.enable_nestedloopjoin", true);
  public static final OptionValidator MULTIPHASE = new BooleanValidator("planner.enable_multiphase_agg", true);
  public static final OptionValidator BROADCAST = new BooleanValidator("planner.enable_broadcast_join", true);
  public static final OptionValidator BROADCAST_THRESHOLD = new PositiveLongValidator("planner.broadcast_threshold", MAX_BROADCAST_THRESHOLD, 10000000);
  public static final OptionValidator BROADCAST_FACTOR = new RangeDoubleValidator("planner.broadcast_factor", 0, Double.MAX_VALUE, 1.0d);
  public static final OptionValidator NESTEDLOOPJOIN_FACTOR = new RangeDoubleValidator("planner.nestedloopjoin_factor", 0, Double.MAX_VALUE, 100.0d);
  public static final OptionValidator NLJOIN_FOR_SCALAR = new BooleanValidator("planner.enable_nljoin_for_scalar_only", true);
  public static final OptionValidator JOIN_ROW_COUNT_ESTIMATE_FACTOR = new RangeDoubleValidator("planner.join.row_count_estimate_factor", 0, Double.MAX_VALUE, 1.0d);
  public static final OptionValidator MUX_EXCHANGE = new BooleanValidator("planner.enable_mux_exchange", true);
  public static final OptionValidator DEMUX_EXCHANGE = new BooleanValidator("planner.enable_demux_exchange", false);
  public static final OptionValidator PARTITION_SENDER_THREADS_FACTOR = new LongValidator("planner.partitioner_sender_threads_factor", 2);
  public static final OptionValidator PARTITION_SENDER_MAX_THREADS = new LongValidator("planner.partitioner_sender_max_threads", 8);
  public static final OptionValidator PARTITION_SENDER_SET_THREADS = new LongValidator("planner.partitioner_sender_set_threads", -1);
  public static final OptionValidator PRODUCER_CONSUMER = new BooleanValidator("planner.add_producer_consumer", false);
  public static final OptionValidator PRODUCER_CONSUMER_QUEUE_SIZE = new LongValidator("planner.producer_consumer_queue_size", 10);
  public static final OptionValidator HASH_SINGLE_KEY = new BooleanValidator("planner.enable_hash_single_key", true);
  public static final OptionValidator HASH_JOIN_SWAP = new BooleanValidator("planner.enable_hashjoin_swap", true);
  public static final OptionValidator HASH_JOIN_SWAP_MARGIN_FACTOR = new RangeDoubleValidator("planner.join.hash_join_swap_margin_factor", 0, 100, 10d);
  public static final String ENABLE_DECIMAL_DATA_TYPE_KEY = "planner.enable_decimal_data_type";
  public static final OptionValidator ENABLE_DECIMAL_DATA_TYPE = new BooleanValidator(ENABLE_DECIMAL_DATA_TYPE_KEY, false);
  public static final OptionValidator HEP_OPT = new BooleanValidator("planner.enable_hep_opt", true);
  public static final OptionValidator HEP_PARTITION_PRUNING = new BooleanValidator("planner.enable_hep_partition_pruning", true);
  public static final OptionValidator PLANNER_MEMORY_LIMIT = new RangeLongValidator("planner.memory_limit",
      INITIAL_OFF_HEAP_ALLOCATION_IN_BYTES, MAX_OFF_HEAP_ALLOCATION_IN_BYTES, DEFAULT_MAX_OFF_HEAP_ALLOCATION_IN_BYTES);
  public static final String UNIONALL_DISTRIBUTE_KEY = "planner.enable_unionall_distribute";
  public static final BooleanValidator UNIONALL_DISTRIBUTE = new BooleanValidator(UNIONALL_DISTRIBUTE_KEY, false);

  public static final OptionValidator IDENTIFIER_MAX_LENGTH =
      new RangeLongValidator("planner.identifier_max_length", 128 /* A minimum length is needed because option names are identifiers themselves */,
                              Integer.MAX_VALUE, DEFAULT_IDENTIFIER_MAX_LENGTH);

  public static final DoubleValidator FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR =
          new MinRangeDoubleValidator("planner.filter.min_selectivity_estimate_factor",
          0.0, 1.0, 0.0d, "planner.filter.max_selectivity_estimate_factor");
  public static final DoubleValidator FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR =
          new MaxRangeDoubleValidator("planner.filter.max_selectivity_estimate_factor",
          0.0, 1.0, 1.0d, "planner.filter.min_selectivity_estimate_factor");

  public static final String TYPE_INFERENCE_KEY = "planner.enable_type_inference";
  public static final BooleanValidator TYPE_INFERENCE = new BooleanValidator(TYPE_INFERENCE_KEY, true);
  public static final LongValidator IN_SUBQUERY_THRESHOLD =
      new PositiveLongValidator("planner.in_subquery_threshold", Integer.MAX_VALUE, 20); /* Same as Calcite's default IN List subquery size */

  public OptionManager options = null;
  public FunctionImplementationRegistry functionImplementationRegistry = null;

  public PlannerSettings(OptionManager options, FunctionImplementationRegistry functionImplementationRegistry){
    this.options = options;
    this.functionImplementationRegistry = functionImplementationRegistry;
  }

  public OptionManager getOptions() {
    return options;
  }

  public boolean isSingleMode() {
    return forceSingleMode || options.getOption(EXCHANGE.getOptionName()).bool_val;
  }

  public void forceSingleMode() {
    forceSingleMode = true;
  }

  public int numEndPoints() {
    return numEndPoints;
  }

  public double getRowCountEstimateFactor(){
    return options.getOption(JOIN_ROW_COUNT_ESTIMATE_FACTOR.getOptionName()).float_val;
  }

  public double getBroadcastFactor(){
    return options.getOption(BROADCAST_FACTOR.getOptionName()).float_val;
  }

  public double getNestedLoopJoinFactor(){
    return options.getOption(NESTEDLOOPJOIN_FACTOR.getOptionName()).float_val;
  }

  public boolean isNlJoinForScalarOnly() {
    return options.getOption(NLJOIN_FOR_SCALAR.getOptionName()).bool_val;
  }

  public boolean useDefaultCosting() {
    return useDefaultCosting;
  }

  public void setNumEndPoints(int numEndPoints) {
    this.numEndPoints = numEndPoints;
  }

  public void setUseDefaultCosting(boolean defcost) {
    this.useDefaultCosting = defcost;
  }

  public boolean isHashAggEnabled() {
    return options.getOption(HASHAGG.getOptionName()).bool_val;
  }

  public boolean isConstantFoldingEnabled() {
    return options.getOption(CONSTANT_FOLDING.getOptionName()).bool_val;
  }

  public boolean isStreamAggEnabled() {
    return options.getOption(STREAMAGG.getOptionName()).bool_val;
  }

  public boolean isHashJoinEnabled() {
    return options.getOption(HASHJOIN.getOptionName()).bool_val;
  }

  public boolean isMergeJoinEnabled() {
    return options.getOption(MERGEJOIN.getOptionName()).bool_val;
  }

  public boolean isNestedLoopJoinEnabled() {
    return options.getOption(NESTEDLOOPJOIN.getOptionName()).bool_val;
  }

  public boolean isMultiPhaseAggEnabled() {
    return options.getOption(MULTIPHASE.getOptionName()).bool_val;
  }

  public boolean isBroadcastJoinEnabled() {
    return options.getOption(BROADCAST.getOptionName()).bool_val;
  }

  public boolean isHashSingleKey() {
    return options.getOption(HASH_SINGLE_KEY.getOptionName()).bool_val;
  }

  public boolean isHashJoinSwapEnabled() {
    return options.getOption(HASH_JOIN_SWAP.getOptionName()).bool_val;
  }

  public boolean isHepPartitionPruningEnabled() { return options.getOption(HEP_PARTITION_PRUNING.getOptionName()).bool_val;}

  public boolean isHepOptEnabled() { return options.getOption(HEP_OPT.getOptionName()).bool_val;}

  public double getHashJoinSwapMarginFactor() {
    return options.getOption(HASH_JOIN_SWAP_MARGIN_FACTOR.getOptionName()).float_val / 100d;
  }

  public long getBroadcastThreshold() {
    return options.getOption(BROADCAST_THRESHOLD.getOptionName()).num_val;
  }

  public long getSliceTarget(){
    return options.getOption(ExecConstants.SLICE_TARGET).num_val;
  }

  public boolean isMemoryEstimationEnabled() {
    return options.getOption(ExecConstants.ENABLE_MEMORY_ESTIMATION_KEY).bool_val;
  }

  public String getFsPartitionColumnLabel() {
    return options.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;
  }

  public long getIdentifierMaxLength(){
    return options.getOption(IDENTIFIER_MAX_LENGTH.getOptionName()).num_val;
  }

  public long getPlanningMemoryLimit() {
    return options.getOption(PLANNER_MEMORY_LIMIT.getOptionName()).num_val;
  }

  public static long getInitialPlanningMemorySize() {
    return INITIAL_OFF_HEAP_ALLOCATION_IN_BYTES;
  }

  public double getFilterMinSelectivityEstimateFactor() {
    return options.getOption(FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR);
  }

  public double getFilterMaxSelectivityEstimateFactor(){
    return options.getOption(FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR);
  }

  public boolean isTypeInferenceEnabled() {
    return options.getOption(TYPE_INFERENCE);
  }

  public long getInSubqueryThreshold() {
    return options.getOption(IN_SUBQUERY_THRESHOLD);
  }

  public boolean isUnionAllDistributeEnabled() {
    return options.getOption(UNIONALL_DISTRIBUTE);
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    if(clazz == PlannerSettings.class){
      return (T) this;
    }else{
      return null;
    }
  }


}
