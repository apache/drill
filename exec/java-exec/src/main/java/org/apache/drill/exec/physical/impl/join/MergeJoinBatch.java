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
package org.apache.drill.exec.physical.impl.join;

import static org.apache.drill.exec.compile.sig.GeneratorMapping.GM;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.TypedNullConstant;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.impl.join.JoinWorker.JoinOutcome;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.eigenbase.rel.JoinRelType;

import com.sun.codemodel.JClass;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

/**
 * A merge join combining to incoming in-order batches.
 */
public class MergeJoinBatch extends AbstractRecordBatch<MergeJoinPOP> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergeJoinBatch.class);

  public static final long ALLOCATOR_INITIAL_RESERVATION = 1*1024*1024;
  public static final long ALLOCATOR_MAX_RESERVATION = 20L*1000*1000*1000;
    
  public final MappingSet setupMapping =
      new MappingSet("null", "null", 
                     GM("doSetup", "doSetup", null, null),
                     GM("doSetup", "doSetup", null, null));
  public final MappingSet copyLeftMapping =
      new MappingSet("leftIndex", "outIndex",
                     GM("doSetup", "doCopyLeft", null, null),
                     GM("doSetup", "doCopyLeft", null, null));
  public final MappingSet copyRightMappping =
      new MappingSet("rightIndex", "outIndex",
                     GM("doSetup", "doCopyRight", null, null),
                     GM("doSetup", "doCopyRight", null, null));
  public final MappingSet compareMapping =
      new MappingSet("leftIndex", "rightIndex",
                     GM("doSetup", "doCompare", null, null),
                     GM("doSetup", "doCompare", null, null));
  public final MappingSet compareRightMapping =
      new MappingSet("rightIndex", "null",
                     GM("doSetup", "doCompare", null, null),
                     GM("doSetup", "doCompare", null, null));
  public final MappingSet compareLeftMapping =
      new MappingSet("leftIndex", "null",
                     GM("doSetup", "doCompareNextLeftKey", null, null),
                     GM("doSetup", "doCompareNextLeftKey", null, null));
  public final MappingSet compareNextLeftMapping =
      new MappingSet("nextLeftIndex", "null",
                     GM("doSetup", "doCompareNextLeftKey", null, null),
                     GM("doSetup", "doCompareNextLeftKey", null, null));

  
  private final RecordBatch left;
  private final RecordBatch right;
  private final JoinStatus status;
  private final List<JoinCondition> conditions;
  private final JoinRelType joinType;
  private JoinWorker worker;
  public MergeJoinBatchBuilder batchBuilder;
  
  protected MergeJoinBatch(MergeJoinPOP popConfig, FragmentContext context, RecordBatch left, RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context);

    if (popConfig.getConditions().size() == 0) {
      throw new UnsupportedOperationException("Merge Join currently does not support cartesian join.  This join operator was configured with 0 conditions");
    }
    this.left = left;
    this.right = right;
    this.joinType = popConfig.getJoinType();
    this.status = new JoinStatus(left, right, this);
    this.batchBuilder = new MergeJoinBatchBuilder(oContext.getAllocator(), status);
    this.conditions = popConfig.getConditions();
  }

  public JoinRelType getJoinType() {
    return joinType;
  }

  @Override
  public int getRecordCount() {
    return status.getOutPosition();
  }

  @Override
  public IterOutcome next() {
    
    // we do this in the here instead of the constructor because don't necessary want to start consuming on construction.
    status.ensureInitial();
    
    // loop so we can start over again if we find a new batch was created.
    while(true){

      JoinOutcome outcome = status.getOutcome();
      // if the previous outcome was a change in schema or we sent a batch, we have to set up a new batch.
      if (outcome == JoinOutcome.BATCH_RETURNED ||
          outcome == JoinOutcome.SCHEMA_CHANGED)
        allocateBatch();

      // reset the output position to zero after our parent iterates this RecordBatch
      if (outcome == JoinOutcome.BATCH_RETURNED ||
          outcome == JoinOutcome.SCHEMA_CHANGED ||
          outcome == JoinOutcome.NO_MORE_DATA)
        status.resetOutputPos();

      if (outcome == JoinOutcome.NO_MORE_DATA) {
        logger.debug("NO MORE DATA; returning {}  NONE");
        return IterOutcome.NONE;
      }
      
      boolean first = false;
      if(worker == null){
        try {
          logger.debug("Creating New Worker");
          this.worker = generateNewWorker();
          first = true;
        } catch (ClassTransformationException | IOException | SchemaChangeException e) {
          context.fail(new SchemaChangeException(e));
          kill();
          return IterOutcome.STOP;
        }
      }

      // join until we have a complete outgoing batch
      if (!worker.doJoin(status))
        worker = null;

      // get the outcome of the join.
      switch(status.getOutcome()){
      case BATCH_RETURNED:
        // only return new schema if new worker has been setup.
        logger.debug("BATCH RETURNED; returning {}", (first ? "OK_NEW_SCHEMA" : "OK"));
        return first ? IterOutcome.OK_NEW_SCHEMA : IterOutcome.OK;
      case FAILURE:
        kill();
        return IterOutcome.STOP;
      case NO_MORE_DATA:
        logger.debug("NO MORE DATA; returning {}", (status.getOutPosition() > 0 ? (first ? "OK_NEW_SCHEMA" : "OK") : "NONE"));
        return status.getOutPosition() > 0 ? (first ? IterOutcome.OK_NEW_SCHEMA : IterOutcome.OK): IterOutcome.NONE;
      case SCHEMA_CHANGED:
        worker = null;
        if(status.getOutPosition() > 0){
          // if we have current data, let's return that.
          logger.debug("SCHEMA CHANGED; returning {} ", (first ? "OK_NEW_SCHEMA" : "OK"));
          return first ? IterOutcome.OK_NEW_SCHEMA : IterOutcome.OK;
        }else{
          // loop again to rebuild worker.
          continue;
        }
      case WAITING:
        return IterOutcome.NOT_YET;
      default:
        throw new IllegalStateException();
      }
    }
  }

  public void resetBatchBuilder() {
    batchBuilder = new MergeJoinBatchBuilder(oContext.getAllocator(), status);
  }

  public void addRightToBatchBuilder() {
    batchBuilder.add(right);
  }

  @Override
  protected void killIncoming() {
    left.kill();
    right.kill();
  }

  @Override
  public void cleanup() {
      super.cleanup();

      left.cleanup();
      right.cleanup();
  }
  
  private void generateDoCompareNextLeft(ClassGenerator<JoinWorker> cg, JVar incomingRecordBatch, 
      JVar incomingLeftRecordBatch, JVar joinStatus, ErrorCollector collector) throws ClassTransformationException {
    boolean nextLeftIndexDeclared = false;

    cg.setMappingSet(compareLeftMapping);
    
    for (JoinCondition condition : conditions) {
      final LogicalExpression leftFieldExpr = condition.getLeft();

      // materialize value vector readers from join expression
      final LogicalExpression materializedLeftExpr = ExpressionTreeMaterializer.materialize(leftFieldExpr, left, collector, context.getFunctionRegistry());
      if (collector.hasErrors())
        throw new ClassTransformationException(String.format(
            "Failure while trying to materialize incoming left field.  Errors:\n %s.", collector.toErrorString()));

      // generate compareNextLeftKey()
      ////////////////////////////////
      cg.setMappingSet(compareLeftMapping);
      cg.getSetupBlock().assign(JExpr._this().ref(incomingRecordBatch), JExpr._this().ref(incomingLeftRecordBatch));
  
      if (!nextLeftIndexDeclared) {
        // int nextLeftIndex = leftIndex + 1;
        cg.getEvalBlock().decl(JType.parse(cg.getModel(), "int"), "nextLeftIndex", JExpr.direct("leftIndex").plus(JExpr.lit(1)));
        nextLeftIndexDeclared = true;
      } 
      // check if the next key is in this batch
      cg.getEvalBlock()._if(joinStatus.invoke("isNextLeftPositionInCurrentBatch").eq(JExpr.lit(false)))
                       ._then()
                         ._return(JExpr.lit(-1));
  
      // generate VV read expressions
      ClassGenerator.HoldingContainer compareThisLeftExprHolder = cg.addExpr(materializedLeftExpr, false);
      cg.setMappingSet(compareNextLeftMapping); // change mapping from 'leftIndex' to 'nextLeftIndex'
      ClassGenerator.HoldingContainer compareNextLeftExprHolder = cg.addExpr(materializedLeftExpr, false);
  
      if (compareThisLeftExprHolder.isOptional()) {
        // handle null == null
        cg.getEvalBlock()._if(compareThisLeftExprHolder.getIsSet().eq(JExpr.lit(0))
                              .cand(compareNextLeftExprHolder.getIsSet().eq(JExpr.lit(0))))
                         ._then()
                           ._return(JExpr.lit(0));
    
        // handle null == !null
        cg.getEvalBlock()._if(compareThisLeftExprHolder.getIsSet().eq(JExpr.lit(0))
                              .cor(compareNextLeftExprHolder.getIsSet().eq(JExpr.lit(0))))
                         ._then()
                           ._return(JExpr.lit(1));
      }
  
      // check value equality
  
      LogicalExpression gh = FunctionGenerationHelper.getComparator(compareThisLeftExprHolder,
        compareNextLeftExprHolder,
        context.getFunctionRegistry());
      HoldingContainer out = cg.addExpr(gh, false);
      
      //If not 0, it means not equal. We return this out value. 
      JConditional jc = cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));
      jc._then()._return(out.getValue());
    }
    
    //Pass the equality check for all the join conditions. Finally, return 0.
    cg.getEvalBlock()._return(JExpr.lit(0));
  }  
  
  private JoinWorker generateNewWorker() throws ClassTransformationException, IOException, SchemaChangeException{

    final ClassGenerator<JoinWorker> cg = CodeGenerator.getRoot(JoinWorker.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    final ErrorCollector collector = new ErrorCollectorImpl();

    // Generate members and initialization code
    /////////////////////////////////////////

    // declare and assign JoinStatus member
    cg.setMappingSet(setupMapping);
    JClass joinStatusClass = cg.getModel().ref(JoinStatus.class);
    JVar joinStatus = cg.clazz.field(JMod.NONE, joinStatusClass, "status");
    cg.getSetupBlock().assign(JExpr._this().ref(joinStatus), JExpr.direct("status"));

    // declare and assign outgoing VectorContainer member
    JClass vectorContainerClass = cg.getModel().ref(VectorContainer.class);
    JVar outgoingVectorContainer = cg.clazz.field(JMod.NONE, vectorContainerClass, "outgoing");
    cg.getSetupBlock().assign(JExpr._this().ref(outgoingVectorContainer), JExpr.direct("outgoing"));

    // declare and assign incoming left RecordBatch member
    JClass recordBatchClass = cg.getModel().ref(RecordBatch.class);
    JVar incomingLeftRecordBatch = cg.clazz.field(JMod.NONE, recordBatchClass, "incomingLeft");
    cg.getSetupBlock().assign(JExpr._this().ref(incomingLeftRecordBatch), joinStatus.ref("left"));

    // declare and assign incoming right RecordBatch member
    JVar incomingRightRecordBatch = cg.clazz.field(JMod.NONE, recordBatchClass, "incomingRight");
    cg.getSetupBlock().assign(JExpr._this().ref(incomingRightRecordBatch), joinStatus.ref("right"));

    // declare 'incoming' member so VVReadExpr generated code can point to the left or right batch
    JVar incomingRecordBatch = cg.clazz.field(JMod.NONE, recordBatchClass, "incoming");

    //generate doCompare() method
    /////////////////////////////////////////
    generateDoCompare(cg, incomingRecordBatch, incomingLeftRecordBatch, incomingRightRecordBatch, collector);
    
    //generate doCompareNextLeftKey() method
    /////////////////////////////////////////
    generateDoCompareNextLeft(cg, incomingRecordBatch, incomingLeftRecordBatch, joinStatus, collector);
    
    // generate copyLeft()
    //////////////////////
    cg.setMappingSet(copyLeftMapping);
    int vectorId = 0;
    if (status.isLeftPositionAllowed()) {
      for (VectorWrapper<?> vw : left) {
        JVar vvIn = cg.declareVectorValueSetupAndMember("incomingLeft",
                                                        new TypedFieldId(vw.getField().getType(), vectorId));
        JVar vvOut = cg.declareVectorValueSetupAndMember("outgoing",
                                                         new TypedFieldId(vw.getField().getType(),vectorId));
        // todo: check result of copyFromSafe and grow allocation
        cg.getEvalBlock()._if(vvOut.invoke("copyFromSafe")
                                     .arg(copyLeftMapping.getValueReadIndex())
                                     .arg(copyLeftMapping.getValueWriteIndex())
                                     .arg(vvIn).eq(JExpr.FALSE))
            ._then()
            ._return(JExpr.FALSE);
        ++vectorId;
      }
    }
    cg.getEvalBlock()._return(JExpr.lit(true));

    // generate copyRight()
    ///////////////////////
    cg.setMappingSet(copyRightMappping);

    int rightVectorBase = vectorId;
    if (status.isRightPositionAllowed()) {
      for (VectorWrapper<?> vw : right) {
        JVar vvIn = cg.declareVectorValueSetupAndMember("incomingRight",
                                                        new TypedFieldId(vw.getField().getType(), vectorId - rightVectorBase));
        JVar vvOut = cg.declareVectorValueSetupAndMember("outgoing",
                                                         new TypedFieldId(vw.getField().getType(),vectorId));
        // todo: check result of copyFromSafe and grow allocation
        cg.getEvalBlock()._if(vvOut.invoke("copyFromSafe")
                                   .arg(copyRightMappping.getValueReadIndex())
                                   .arg(copyRightMappping.getValueWriteIndex())
                                   .arg(vvIn).eq(JExpr.FALSE))
            ._then()
            ._return(JExpr.FALSE);
        ++vectorId;
      }
    }
    cg.getEvalBlock()._return(JExpr.lit(true));

    JoinWorker w = context.getImplementationClass(cg);
    w.setupJoin(context, status, this.container);
    return w;
  }

  private void allocateBatch() {
    // allocate new batch space.
    container.clear();
    
    //estimation of joinBatchSize : max of left/right size, expanded by a factor of 16, which is then bounded by MAX_BATCH_SIZE.
    int leftCount = status.isLeftPositionAllowed() ? left.getRecordCount() : 0;
    int rightCount = status.isRightPositionAllowed() ? right.getRecordCount() : 0;
    int joinBatchSize = Math.min(Math.max(leftCount, rightCount) * 16, MAX_BATCH_SIZE);
    
    // add fields from both batches
    if (leftCount > 0) {
      for (VectorWrapper<?> w : left) {
        ValueVector outgoingVector = TypeHelper.getNewVector(w.getField(), oContext.getAllocator());
        VectorAllocator.getAllocator(outgoingVector, (int) Math.ceil(w.getValueVector().getBufferSize() / left.getRecordCount())).alloc(joinBatchSize);
        container.add(outgoingVector);
      }
    }

    if (rightCount > 0) {
      for (VectorWrapper<?> w : right) {
        ValueVector outgoingVector = TypeHelper.getNewVector(w.getField(), oContext.getAllocator());
        VectorAllocator.getAllocator(outgoingVector, (int) Math.ceil(w.getValueVector().getBufferSize() / right.getRecordCount())).alloc(joinBatchSize);
        container.add(outgoingVector);
      }
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    logger.debug("Built joined schema: {}", container.getSchema());
  }

  private void generateDoCompare(ClassGenerator<JoinWorker> cg, JVar incomingRecordBatch, 
      JVar incomingLeftRecordBatch, JVar incomingRightRecordBatch, ErrorCollector collector) throws ClassTransformationException {
    
    cg.setMappingSet(compareMapping);
    
    for (JoinCondition condition : conditions) {
      final LogicalExpression leftFieldExpr = condition.getLeft();
      final LogicalExpression rightFieldExpr = condition.getRight();

      // materialize value vector readers from join expression
      LogicalExpression materializedLeftExpr;
      if (status.isLeftPositionAllowed()) {
        materializedLeftExpr = ExpressionTreeMaterializer.materialize(leftFieldExpr, left, collector, context.getFunctionRegistry());
      } else {
        materializedLeftExpr = new TypedNullConstant(Types.optional(MinorType.INT));
      }
      if (collector.hasErrors())
        throw new ClassTransformationException(String.format(
            "Failure while trying to materialize incoming left field.  Errors:\n %s.", collector.toErrorString()));

      LogicalExpression materializedRightExpr;
      if (status.isRightPositionAllowed()) {
        materializedRightExpr = ExpressionTreeMaterializer.materialize(rightFieldExpr, right, collector, context.getFunctionRegistry());
      } else {
        materializedRightExpr = new TypedNullConstant(Types.optional(MinorType.INT));
      }
      if (collector.hasErrors())
        throw new ClassTransformationException(String.format(
            "Failure while trying to materialize incoming right field.  Errors:\n %s.", collector.toErrorString()));

      // generate compare()
      ////////////////////////
      cg.setMappingSet(compareMapping);
      cg.getSetupBlock().assign(JExpr._this().ref(incomingRecordBatch), JExpr._this().ref(incomingLeftRecordBatch));
      ClassGenerator.HoldingContainer compareLeftExprHolder = cg.addExpr(materializedLeftExpr, false);

      cg.setMappingSet(compareRightMapping);
      cg.getSetupBlock().assign(JExpr._this().ref(incomingRecordBatch), JExpr._this().ref(incomingRightRecordBatch));
      ClassGenerator.HoldingContainer compareRightExprHolder = cg.addExpr(materializedRightExpr, false);

      if (compareLeftExprHolder.isOptional() && compareRightExprHolder.isOptional()) {
        // handle null == null
        cg.getEvalBlock()._if(compareLeftExprHolder.getIsSet().eq(JExpr.lit(0))
            .cand(compareRightExprHolder.getIsSet().eq(JExpr.lit(0))))
            ._then()
            ._return(JExpr.lit(0));
    
        // handle null == !null
        cg.getEvalBlock()._if(compareLeftExprHolder.getIsSet().eq(JExpr.lit(0))
            .cor(compareRightExprHolder.getIsSet().eq(JExpr.lit(0))))
            ._then()
            ._return(JExpr.lit(1));
  
      } else if (compareLeftExprHolder.isOptional()) {
        // handle null == required (null is less than any value)
        cg.getEvalBlock()._if(compareLeftExprHolder.getIsSet().eq(JExpr.lit(0)))
            ._then()
            ._return(JExpr.lit(-1));
  
      } else if (compareRightExprHolder.isOptional()) {
        // handle required == null (null is less than any value)
        cg.getEvalBlock()._if(compareRightExprHolder.getIsSet().eq(JExpr.lit(0)))
            ._then()
            ._return(JExpr.lit(1));
      }
  
      LogicalExpression fh = FunctionGenerationHelper.getComparator(compareLeftExprHolder,
        compareRightExprHolder,
        context.getFunctionRegistry()); 
      HoldingContainer out = cg.addExpr(fh, false);
      
      //If not 0, it means not equal. We return this out value.       
      JConditional jc = cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));
      jc._then()._return(out.getValue());
    }
    
    //Pass the equality check for all the join conditions. Finally, return 0.    
    cg.getEvalBlock()._return(JExpr.lit(0));  
  }
}
