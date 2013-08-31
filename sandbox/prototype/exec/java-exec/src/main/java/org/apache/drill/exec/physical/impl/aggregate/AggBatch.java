package org.apache.drill.exec.physical.impl.aggregate;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.CodeGenerator.BlockType;
import org.apache.drill.exec.expr.CodeGenerator.HoldingContainer;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.HoldingContainerExpression;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.impl.ComparatorFunctions;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.StreamingAggregate;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JVar;

public class AggBatch extends AbstractRecordBatch<StreamingAggregate> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AggBatch.class);

  private Aggregator aggregator;
  private final RecordBatch incoming;
  private boolean done = false;
  
  public static enum AggOutcome {
    RETURN_OUTCOME, CLEANUP_AND_RETURN, UPDATE_AGGREGATOR;
  }

  public AggBatch(StreamingAggregate popConfig, RecordBatch incoming, FragmentContext context) {
    super(popConfig, context);
    this.incoming = incoming;
  }

  @Override
  public int getRecordCount() {
    if(done) return 0;
    return aggregator.getOutputCount();
  }

  @Override
  public IterOutcome next() {

    // this is only called on the first batch. Beyond this, the aggregator manages batches.
    if (aggregator == null) {
      IterOutcome outcome = incoming.next();
      logger.debug("Next outcome of {}", outcome);
      switch (outcome) {
      case NONE:
      case NOT_YET:
      case STOP:
        return outcome;
      case OK_NEW_SCHEMA:
        if (!createAggregator()){
          done = true;
          return IterOutcome.STOP;
        }
        break;
      case OK:
        throw new IllegalStateException("You should never get a first batch without a new schema");
      default:
        throw new IllegalStateException(String.format("unknown outcome %s", outcome));
      }
    }

    while(true){
      AggOutcome out = aggregator.doWork();
      logger.debug("Aggregator response {}, records {}", out, aggregator.getOutputCount());
      switch(out){
      case CLEANUP_AND_RETURN:
        container.clear();
        done = true;
        return aggregator.getOutcome();
      case RETURN_OUTCOME:
        return aggregator.getOutcome();
      case UPDATE_AGGREGATOR:
        aggregator = null;
        if(!createAggregator()){
          return IterOutcome.STOP;
        }
        continue;
      default:
        throw new IllegalStateException(String.format("Unknown state %s.", out));
      }
    }
    
  }

  
  
  /**
   * Creates a new Aggregator based on the current schema. If setup fails, this method is responsible for cleaning up
   * and informing the context of the failure state, as well is informing the upstream operators.
   * 
   * @return true if the aggregator was setup successfully. false if there was a failure.
   */
  private boolean createAggregator() {
    logger.debug("Creating new aggregator.");
    try{
      this.aggregator = createAggregatorInternal();
      return true;
    }catch(SchemaChangeException | ClassTransformationException | IOException ex){
      context.fail(ex);
      container.clear();
      incoming.kill();
      return false;
    }
  }




  private Aggregator createAggregatorInternal() throws SchemaChangeException, ClassTransformationException, IOException{
    CodeGenerator<Aggregator> cg = new CodeGenerator<Aggregator>(AggTemplate.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    container.clear();
    List<VectorAllocator> allocators = Lists.newArrayList();
    
    LogicalExpression[] keyExprs = new LogicalExpression[popConfig.getKeys().length];
    LogicalExpression[] valueExprs = new LogicalExpression[popConfig.getExprs().length];
    TypedFieldId[] keyOutputIds = new TypedFieldId[popConfig.getKeys().length];
    
    ErrorCollector collector = new ErrorCollectorImpl();
    
    for(int i =0; i < keyExprs.length; i++){
      NamedExpression ne = popConfig.getKeys()[i];
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector);
      if(expr == null) continue;
      keyExprs[i] = expr;
      final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());
      ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
      allocators.add(VectorAllocator.getAllocator(vector, 50));
      keyOutputIds[i] = container.add(vector);
    }
    
    for(int i =0; i < valueExprs.length; i++){
      NamedExpression ne = popConfig.getExprs()[i];
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector);
      if(expr == null) continue;
      
      final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());
      ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
      allocators.add(VectorAllocator.getAllocator(vector, 50));
      TypedFieldId id = container.add(vector);
      valueExprs[i] = new ValueVectorWriteExpression(id, expr, true);
    }
    
    if(collector.hasErrors()) throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
    
    setupIsSame(cg, keyExprs);
    setupIsSameApart(cg, keyExprs);
    addRecordValues(cg, valueExprs);
    outputRecordKeys(cg, keyOutputIds, keyExprs);
    outputRecordKeysPrev(cg, keyOutputIds, keyExprs);
    
    cg.getBlock("resetValues")._return(JExpr.TRUE);
    getIndex(cg);
    
    container.buildSchema(SelectionVectorMode.NONE);
    Aggregator agg = context.getImplementationClass(cg);
    agg.setup(context, incoming, this, allocators.toArray(new VectorAllocator[allocators.size()]));
    return agg;
  }
  
  
  
  private static final GeneratorMapping IS_SAME = GeneratorMapping.create("setupInterior", "isSame", null, null);
  private static final MappingSet IS_SAME_I1 = new MappingSet("index1", null, IS_SAME, IS_SAME);
  private static final MappingSet IS_SAME_I2 = new MappingSet("index2", null, IS_SAME, IS_SAME);

  private void setupIsSame(CodeGenerator<Aggregator> cg, LogicalExpression[] keyExprs){
    cg.setMappingSet(IS_SAME_I1);
    for(LogicalExpression expr : keyExprs){
      // first, we rewrite the evaluation stack for each side of the comparison.
      cg.setMappingSet(IS_SAME_I1);
      HoldingContainer first = cg.addExpr(expr, false);
      cg.setMappingSet(IS_SAME_I2);
      HoldingContainer second = cg.addExpr(expr, false);
      
      FunctionCall f = new FunctionCall(ComparatorFunctions.COMPARE_TO, ImmutableList.of((LogicalExpression) new HoldingContainerExpression(first), new HoldingContainerExpression(second)), ExpressionPosition.UNKNOWN);
      HoldingContainer out = cg.addExpr(f, false);
      cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getEvalBlock()._return(JExpr.TRUE);
  }
  
  private static final GeneratorMapping IS_SAME_PREV_INTERNAL_BATCH_READ = GeneratorMapping.create("isSamePrev", "isSamePrev", null, null); // the internal batch changes each time so we need to redo setup.
  private static final GeneratorMapping IS_SAME_PREV = GeneratorMapping.create("setupInterior", "isSamePrev", null, null);
  private static final MappingSet ISA_B1 = new MappingSet("b1Index", null, "b1", null, IS_SAME_PREV_INTERNAL_BATCH_READ, IS_SAME_PREV_INTERNAL_BATCH_READ);
  private static final MappingSet ISA_B2 = new MappingSet("b2Index", null, "incoming", null, IS_SAME_PREV, IS_SAME_PREV);
  
  private void setupIsSameApart(CodeGenerator<Aggregator> cg, LogicalExpression[] keyExprs){
    cg.setMappingSet(ISA_B1);
    for(LogicalExpression expr : keyExprs){
      // first, we rewrite the evaluation stack for each side of the comparison.
      cg.setMappingSet(ISA_B1);
      HoldingContainer first = cg.addExpr(expr, false);
      cg.setMappingSet(ISA_B2);
      HoldingContainer second = cg.addExpr(expr, false);

      FunctionCall f = new FunctionCall(ComparatorFunctions.COMPARE_TO, ImmutableList.of((LogicalExpression) new HoldingContainerExpression(first), new HoldingContainerExpression(second)), ExpressionPosition.UNKNOWN);
      HoldingContainer out = cg.addExpr(f, false);
      cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getEvalBlock()._return(JExpr.TRUE);
  }
  
  private static final GeneratorMapping EVAL_INSIDE = GeneratorMapping.create("setupInterior", "addRecord", null, null);
  private static final GeneratorMapping EVAL_OUTSIDE = GeneratorMapping.create("setupInterior", "outputRecordValues", "resetValues", "cleanup");
  private static final MappingSet EVAL = new MappingSet("index", "outIndex", EVAL_INSIDE, EVAL_OUTSIDE, EVAL_INSIDE);
  
  private void addRecordValues(CodeGenerator<Aggregator> cg, LogicalExpression[] valueExprs){
    cg.setMappingSet(EVAL);
    for(LogicalExpression ex : valueExprs){
      HoldingContainer hc = cg.addExpr(ex);
      cg.getBlock(BlockType.EVAL)._if(hc.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getBlock(BlockType.EVAL)._return(JExpr.TRUE);
  }
  
  private static final MappingSet RECORD_KEYS = new MappingSet(GeneratorMapping.create("setupInterior", "outputRecordKeys", null, null));
  
  private void outputRecordKeys(CodeGenerator<Aggregator> cg, TypedFieldId[] keyOutputIds, LogicalExpression[] keyExprs){
    cg.setMappingSet(RECORD_KEYS);
    for(int i =0; i < keyExprs.length; i++){
      HoldingContainer hc = cg.addExpr(new ValueVectorWriteExpression(keyOutputIds[i], keyExprs[i], true));
      cg.getBlock(BlockType.EVAL)._if(hc.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getBlock(BlockType.EVAL)._return(JExpr.TRUE);
  }
  
  private static final GeneratorMapping PREVIOUS_KEYS_OUT = GeneratorMapping.create("setupInterior", "outputRecordKeysPrev", null, null);
  private static final MappingSet RECORD_KEYS_PREV_OUT = new MappingSet("previousIndex", "outIndex", "previous", "outgoing", PREVIOUS_KEYS_OUT, PREVIOUS_KEYS_OUT);

  private static final GeneratorMapping PREVIOUS_KEYS = GeneratorMapping.create("outputRecordKeysPrev", "outputRecordKeysPrev", null, null);
  private static final MappingSet RECORD_KEYS_PREV = new MappingSet("previousIndex", "outIndex", "previous", null, PREVIOUS_KEYS, PREVIOUS_KEYS);
  
  private void outputRecordKeysPrev(CodeGenerator<Aggregator> cg, TypedFieldId[] keyOutputIds, LogicalExpression[] keyExprs){
    cg.setMappingSet(RECORD_KEYS_PREV);

    for(int i =0; i < keyExprs.length; i++){
      // IMPORTANT: there is an implicit assertion here that the TypedFieldIds for the previous batch and the current batch are the same.  This is possible because InternalBatch guarantees this.
      logger.debug("Writing out expr {}", keyExprs[i]);
      cg.rotateBlock();
      cg.setMappingSet(RECORD_KEYS_PREV);
      HoldingContainer innerExpression = cg.addExpr(keyExprs[i], false);
      cg.setMappingSet(RECORD_KEYS_PREV_OUT);
      HoldingContainer outerExpression = cg.addExpr(new ValueVectorWriteExpression(keyOutputIds[i], new HoldingContainerExpression(innerExpression), true), false);
      cg.getBlock(BlockType.EVAL)._if(outerExpression.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);
      
    }
    cg.getBlock(BlockType.EVAL)._return(JExpr.TRUE);
  }
  
  private void getIndex(CodeGenerator<Aggregator> g){
    switch(incoming.getSchema().getSelectionVectorMode()){
    case FOUR_BYTE: {
      JVar var = g.declareClassField("sv4_", g.getModel()._ref(SelectionVector4.class));
      g.getBlock("setupInterior").assign(var, JExpr.direct("incoming").invoke("getSelectionVector4"));
      g.getBlock("getVectorIndex")._return(var.invoke("get").arg(JExpr.direct("recordIndex")));;
      return;
    }
    case NONE: {
      g.getBlock("getVectorIndex")._return(JExpr.direct("recordIndex"));;
      return;
    }
    case TWO_BYTE: {
      JVar var = g.declareClassField("sv2_", g.getModel()._ref(SelectionVector2.class));
      g.getBlock("setupInterior").assign(var, JExpr.direct("incoming").invoke("getSelectionVector2"));
      g.getBlock("getVectorIndex")._return(var.invoke("get").arg(JExpr.direct("recordIndex")));;
      return;
    }
     
    default:
      throw new IllegalStateException();
      
    }
   
  }

  
  
  @Override
  protected void killIncoming() {
    incoming.kill();
  }

}
