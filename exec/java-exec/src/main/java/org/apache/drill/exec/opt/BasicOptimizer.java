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
package org.apache.drill.exec.opt;

import com.google.common.collect.Lists;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.logical.data.GroupingAggregate;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.Window;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Limit;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.config.StreamingAggregate;
import org.apache.drill.exec.physical.config.WindowPOP;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.StoragePlugin;
import org.eigenbase.rel.RelFieldCollation.Direction;
import org.eigenbase.rel.RelFieldCollation.NullDirection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class BasicOptimizer extends Optimizer{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicOptimizer.class);

  private DrillConfig config;
  private QueryContext context;
  private UserServer.UserClientConnection userSession;

  public BasicOptimizer(DrillConfig config, QueryContext context, UserServer.UserClientConnection userSession){
    this.config = config;
    this.context = context;
    this.userSession = userSession;
    logCurrentOptionValues();
  }

  private void logCurrentOptionValues(){
//    Iterator<DrillOptionValue> optionVals = userSession.getSessionOptionIterator();
//    DrillOptionValue val = null;
//    String output = "";
//    output += "SessionOptions: {\n";
//    for ( ;optionVals.hasNext(); val = optionVals.next()){
//      if (val != null) {
//        output += val.getOptionName() + ":" + val.getValue() + ",\n";
//      }
//    }
//    output += "}";
//    logger.debug(output);
  }

  /**
   * Get the current value of an option. Session options override global options.
   *
   * @param name - the name of the option
   * @return - value of the option
   */
  private Object getOptionValue(String name) {
//    Object val = userSession.getSessionLevelOption(name);
//    if (val == null) {
//      context.getOptionValue(name);
//    }
//    return val;
    return null;
  }

  @Override
  public void init(DrillConfig config) {

  }

  @Override
  public PhysicalPlan optimize(OptimizationContext context, LogicalPlan plan) throws OptimizerException{
    Object obj = new Object();
    Collection<SinkOperator> roots = plan.getGraph().getRoots();
    List<PhysicalOperator> physOps = new ArrayList<PhysicalOperator>(roots.size());
    LogicalConverter converter = new LogicalConverter(plan);
    for ( SinkOperator op : roots){
      PhysicalOperator pop  = op.accept(converter, obj);
      physOps.add(pop);
    }

    PlanProperties props = PlanProperties.builder()
        .type(PlanProperties.PlanType.APACHE_DRILL_PHYSICAL)
        .version(plan.getProperties().version)
        .generator(plan.getProperties().generator)
        .options(new JSONOptions(context.getOptions().getOptionList())).build();
    PhysicalPlan p = new PhysicalPlan(props, physOps);
    return p;
    //return new PhysicalPlan(props, physOps);
  }

  @Override
  public void close() {

  }

  public static class BasicOptimizationContext implements OptimizationContext {

    private OptionManager ops;
    public BasicOptimizationContext(QueryContext c){
      this.ops = c.getOptions();
    }

    @Override
    public int getPriority() {
      return 1;
    }

    @Override
    public OptionManager getOptions() {
      return ops;
    }
  }

  private class LogicalConverter extends AbstractLogicalVisitor<PhysicalOperator, Object, OptimizerException> {

    // storing a reference to the plan for access to other elements outside of the query graph
    // such as the storage engine configs
    LogicalPlan logicalPlan;

    public LogicalConverter(LogicalPlan logicalPlan){
      this.logicalPlan = logicalPlan;
    }

    @Override
    public PhysicalOperator visitGroupingAggregate(GroupingAggregate groupBy, Object value) throws OptimizerException {

      List<Ordering> orderDefs = Lists.newArrayList();


      PhysicalOperator input = groupBy.getInput().accept(this, value);

      if(groupBy.getKeys().length > 0){
        for(NamedExpression e : groupBy.getKeys()){
          orderDefs.add(new Ordering(Direction.ASCENDING, e.getExpr(), NullDirection.FIRST));
        }
        input = new Sort(input, orderDefs, false);
      }

      StreamingAggregate sa = new StreamingAggregate(input, groupBy.getKeys(), groupBy.getExprs(), 1.0f);
      return sa;
    }

    @Override
    public PhysicalOperator visitWindow(Window window, Object value) throws OptimizerException {
      PhysicalOperator input = window.getInput().accept(this, value);

      List<Ordering> ods = Lists.newArrayList();

      input = new Sort(input, ods, false);

      return new WindowPOP(input, window.getWithins(), window.getAggregations(), window.getStart(), window.getEnd());
    }

    @Override
    public PhysicalOperator visitOrder(Order order, Object value) throws OptimizerException {
      PhysicalOperator input = order.getInput().accept(this, value);
      List<Ordering> ods = Lists.newArrayList();
      for (Ordering o : order.getOrderings()){
        ods.add(o);
      }
      return new SelectionVectorRemover(new Sort(input, ods, false));
    }

    @Override
    public PhysicalOperator visitLimit(org.apache.drill.common.logical.data.Limit limit, Object value) throws OptimizerException {
      PhysicalOperator input = limit.getInput().accept(this, value);
      return new SelectionVectorRemover(new Limit(input, limit.getFirst(), limit.getLast()));
    }

    @Override
    public PhysicalOperator visitJoin(Join join, Object value) throws OptimizerException {
      PhysicalOperator leftOp = join.getLeft().accept(this, value);
      List<Ordering> leftOrderDefs = Lists.newArrayList();
      for(JoinCondition jc : join.getConditions()){
        leftOrderDefs.add(new Ordering(Direction.ASCENDING, jc.getLeft()));
      }
      leftOp = new Sort(leftOp, leftOrderDefs, false);
      leftOp = new SelectionVectorRemover(leftOp);

      PhysicalOperator rightOp = join.getRight().accept(this, value);
      List<Ordering> rightOrderDefs = Lists.newArrayList();
      for(JoinCondition jc : join.getConditions()){
        rightOrderDefs.add(new Ordering(Direction.ASCENDING, jc.getRight()));
      }
      rightOp = new Sort(rightOp, rightOrderDefs, false);
      rightOp = new SelectionVectorRemover(rightOp);

      MergeJoinPOP mjp = new MergeJoinPOP(leftOp, rightOp, Arrays.asList(join.getConditions()), join.getJoinType());
      return new SelectionVectorRemover(mjp);
    }



    @Override
    public PhysicalOperator visitScan(Scan scan, Object obj) throws OptimizerException {
      StoragePluginConfig config = logicalPlan.getStorageEngineConfig(scan.getStorageEngine());
      if(config == null) {
        throw new OptimizerException(String.format("Logical plan referenced the storage engine config %s but the logical plan didn't have that available as a config.", scan.getStorageEngine()));
      }
      StoragePlugin storagePlugin;
      try {
        storagePlugin = context.getStorage().getPlugin(config);
        return storagePlugin.getPhysicalScan(scan.getSelection());
      } catch (IOException | ExecutionSetupException e) {
        throw new OptimizerException("Failure while attempting to retrieve storage engine.", e);
      }
    }

    @Override
    public PhysicalOperator visitStore(Store store, Object obj) throws OptimizerException {
      if (!store.iterator().hasNext()) {
        throw new OptimizerException("Store node in logical plan does not have a child.");
      }
      return new Screen(store.iterator().next().accept(this, obj), context.getCurrentEndpoint());
    }

    @Override
    public PhysicalOperator visitProject(Project project, Object obj) throws OptimizerException {
      return new org.apache.drill.exec.physical.config.Project(Arrays.asList(project.getSelections()), project.iterator().next().accept(this, obj));
    }

    @Override
    public PhysicalOperator visitFilter(Filter filter, Object obj) throws OptimizerException {
      TypeProtos.MajorType.Builder b = TypeProtos.MajorType.getDefaultInstance().newBuilderForType();
      b.setMode(DataMode.REQUIRED);
      b.setMinorType(MinorType.BIGINT);
      PhysicalOperator child = filter.iterator().next().accept(this, obj);
      return new SelectionVectorRemover(new org.apache.drill.exec.physical.config.Filter(child, filter.getExpr(), 1.0f));
    }

  }

}
