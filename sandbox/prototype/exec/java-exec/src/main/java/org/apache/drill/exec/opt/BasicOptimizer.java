package org.apache.drill.exec.opt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.defs.OrderDef;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.CollapsingAggregate;
import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.logical.data.Order.Direction;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Segment;
import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.config.StreamingAggregate;
import org.apache.drill.exec.store.StorageEngine;

import com.beust.jcommander.internal.Lists;

public class BasicOptimizer extends Optimizer{

  private DrillConfig config;
  private QueryContext context;

  public BasicOptimizer(DrillConfig config, QueryContext context){
    this.config = config;
    this.context = context;
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

    PlanProperties props = new PlanProperties();
    props.type = PlanProperties.PlanType.APACHE_DRILL_PHYSICAL;
    props.version = plan.getProperties().version;
    props.generator = plan.getProperties().generator;
    PhysicalPlan p = new PhysicalPlan(props, physOps);
    return p;
    //return new PhysicalPlan(props, physOps);
  }

  @Override
  public void close() {

  }

  public static class BasicOptimizationContext implements OptimizationContext {

    @Override
    public int getPriority() {
      return 1;
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
    public PhysicalOperator visitSegment(Segment segment, Object value) throws OptimizerException {
      throw new OptimizerException("Segment operators aren't currently supported besides next to a collapsing aggregate operator.");
    }



    @Override
    public PhysicalOperator visitOrder(Order order, Object value) throws OptimizerException {
      PhysicalOperator input = order.getInput().accept(this, value);
      List<OrderDef> ods = Lists.newArrayList();
      for(Ordering o : order.getOrderings()){
        ods.add(OrderDef.create(o));
      }
      return new SelectionVectorRemover(new Sort(input, ods, false));
    }



    @Override
    public PhysicalOperator visitCollapsingAggregate(CollapsingAggregate agg, Object value)
        throws OptimizerException {

      if( !(agg.getInput() instanceof Segment) ){
        throw new OptimizerException(String.format("Currently, Drill only supports CollapsingAggregate immediately preceded by a Segment.  The input of this operator is %s.", agg.getInput()));
      }
      Segment segment = (Segment) agg.getInput();

      if(!agg.getWithin().equals(segment.getName())){
        throw new OptimizerException(String.format("Currently, Drill only supports CollapsingAggregate immediately preceded by a Segment where the CollapsingAggregate works on the defined segments.  In this case, the segment has been defined based on the name %s but the collapsing aggregate is working within the field %s.", segment.getName(), agg.getWithin()));
      }
      
      // a collapsing aggregate is a currently implemented as a sort followed by a streaming aggregate.
      List<OrderDef> orderDefs = Lists.newArrayList();
      
      List<NamedExpression> keys = Lists.newArrayList();
      for(LogicalExpression e : segment.getExprs()){
        if( !(e instanceof SchemaPath)) throw new OptimizerException("The basic optimizer doesn't currently support collapsing aggregate where the segment value is something other than a SchemaPath.");
        keys.add(new NamedExpression(e, new FieldReference((SchemaPath) e)));
        orderDefs.add(new OrderDef(Direction.ASC, e));
      }
      Sort sort = new Sort(segment.getInput().accept(this, value), orderDefs, false);
      
      StreamingAggregate sa = new StreamingAggregate(sort, keys.toArray(new NamedExpression[keys.size()]), agg.getAggregations(), 1.0f);
      SelectionVectorRemover svm = new SelectionVectorRemover(sa);
      return svm;
    }



    @Override
    public PhysicalOperator visitJoin(Join join, Object value) throws OptimizerException {
      PhysicalOperator leftOp = join.getLeft().accept(this, value);
      List<OrderDef> leftOrderDefs = Lists.newArrayList();
      for(JoinCondition jc : join.getConditions()){
        leftOrderDefs.add(new OrderDef(Direction.ASC, jc.getLeft()));
      }
      leftOp = new Sort(leftOp, leftOrderDefs, false);
      leftOp = new SelectionVectorRemover(leftOp);
      
      PhysicalOperator rightOp = join.getRight().accept(this, value);
      List<OrderDef> rightOrderDefs = Lists.newArrayList();
      for(JoinCondition jc : join.getConditions()){
        rightOrderDefs.add(new OrderDef(Direction.ASC, jc.getRight()));
      }
      rightOp = new Sort(rightOp, rightOrderDefs, false);
      rightOp = new SelectionVectorRemover(rightOp);
      
      MergeJoinPOP mjp = new MergeJoinPOP(leftOp, rightOp, Arrays.asList(join.getConditions()), join.getJointType());
      return new SelectionVectorRemover(mjp);
    }



    @Override
    public PhysicalOperator visitScan(Scan scan, Object obj) throws OptimizerException {
      StorageEngineConfig config = logicalPlan.getStorageEngineConfig(scan.getStorageEngine());
      if(config == null) throw new OptimizerException(String.format("Logical plan referenced the storage engine config %s but the logical plan didn't have that available as a config.", scan.getStorageEngine()));
      StorageEngine engine;
      try {
        engine = context.getStorageEngine(config);
        return engine.getPhysicalScan(scan);
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
//      return project.getInput().accept(this, obj);
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
