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

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.HoldingContainerExpression;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.impl.ComparatorFunctions;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.impl.filter.ReturnValueExpression;
import org.apache.drill.exec.physical.impl.join.JoinWorker.JoinOutcome;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

import com.google.common.collect.ImmutableList;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

/**
 * A merge join combining to incoming in-order batches.
 */
public class MergeJoinBatch extends AbstractRecordBatch<MergeJoinPOP> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergeJoinBatch.class);
  
//  private static GeneratorMapping setup = GM("doSetup", "doSetup");
//  private static GeneratorMapping copyLeft = GM("doSetup", "doCopyLeft");
//  private static GeneratorMapping copyRight = GM("doSetup", "doCopyRight");
//  private static GeneratorMapping compare = GM("doSetup", "doCompare");
//  private static GeneratorMapping compareLeft= GM("doSetup", "doCompareNextLeftKey");
//  
//  private static final MappingSet SETUP_MAPPING = new MappingSet((String) null, null, setup, setup);
//  private static final MappingSet COPY_LEFT_MAPPING = new MappingSet("leftIndex", "outIndex", copyLeft, copyLeft);
//  private static final MappingSet COPY_RIGHT_MAPPING = new MappingSet("rightIndex", "outIndex", copyRight, copyRight);
//  private static final MappingSet COMPARE_MAPPING = new MappingSet("leftIndex", "rightIndex", compare, compare);
//  private static final MappingSet COMPARE_RIGHT_MAPPING = new MappingSet("rightIndex", null, compare, compare);
//  private static final MappingSet COMPARE_LEFT_MAPPING = new MappingSet("leftIndex", "null", compareLeft, compareLeft);
//  private static final MappingSet COMPARE_NEXT_LEFT_MAPPING = new MappingSet("nextLeftIndex", "null", compareLeft, compareLeft);
//  
  public static final MappingSet SETUP_MAPPING =
      new MappingSet("null", "null", 
                     GM("doSetup", "doSetup", null, null),
                     GM("doSetup", "doSetup", null, null));
  public static final MappingSet COPY_LEFT_MAPPING =
      new MappingSet("leftIndex", "outIndex",
                     GM("doSetup", "doCopyLeft", null, null),
                     GM("doSetup", "doCopyLeft", null, null));
  public static final MappingSet COPY_RIGHT_MAPPING =
      new MappingSet("rightIndex", "outIndex",
                     GM("doSetup", "doCopyRight", null, null),
                     GM("doSetup", "doCopyRight", null, null));
  public static final MappingSet COMPARE_MAPPING =
      new MappingSet("leftIndex", "rightIndex",
                     GM("doSetup", "doCompare", null, null),
                     GM("doSetup", "doCompare", null, null));
  public static final MappingSet COMPARE_RIGHT_MAPPING =
      new MappingSet("rightIndex", "null",
                     GM("doSetup", "doCompare", null, null),
                     GM("doSetup", "doCompare", null, null));
  public static final MappingSet COMPARE_LEFT_MAPPING =
      new MappingSet("leftIndex", "null",
                     GM("doSetup", "doCompareNextLeftKey", null, null),
                     GM("doSetup", "doCompareNextLeftKey", null, null));
  public static final MappingSet COMPARE_NEXT_LEFT_MAPPING =
      new MappingSet("nextLeftIndex", "null",
                     GM("doSetup", "doCompareNextLeftKey", null, null),
                     GM("doSetup", "doCompareNextLeftKey", null, null));

  
  private final RecordBatch left;
  private final RecordBatch right;
  private final JoinStatus status;
  private final JoinCondition condition;
  private final Join.JoinType joinType;
  private JoinWorker worker;
  public MergeJoinBatchBuilder batchBuilder;
  
  protected MergeJoinBatch(MergeJoinPOP popConfig, FragmentContext context, RecordBatch left, RecordBatch right) {
    super(popConfig, context);
    this.left = left;
    this.right = right;
    this.status = new JoinStatus(left, right, this);
    this.batchBuilder = new MergeJoinBatchBuilder(context, status);
    this.joinType = popConfig.getJoinType();
    this.condition = popConfig.getConditions().get(0);
    // currently only one join condition is supported
    assert popConfig.getConditions().size() == 1;
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

      // if the previous outcome was a change in schema or we sent a batch, we have to set up a new batch.
      if (status.getOutcome() == JoinOutcome.BATCH_RETURNED ||
          status.getOutcome() == JoinOutcome.SCHEMA_CHANGED)
        allocateBatch();

      // reset the output position to zero after our parent iterates this RecordBatch
      if (status.getOutcome() == JoinOutcome.BATCH_RETURNED ||
          status.getOutcome() == JoinOutcome.SCHEMA_CHANGED ||
          status.getOutcome() == JoinOutcome.NO_MORE_DATA)
        status.resetOutputPos();

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
    batchBuilder = new MergeJoinBatchBuilder(context, status);
  }

  public void addRightToBatchBuilder() {
    batchBuilder.add(right);
  }

  @Override
  protected void killIncoming() {
    left.kill();
    right.kill();
  }

  private JoinWorker generateNewWorker() throws ClassTransformationException, IOException, SchemaChangeException{

    final CodeGenerator<JoinWorker> cg = new CodeGenerator<>(JoinWorker.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    final ErrorCollector collector = new ErrorCollectorImpl();
    final LogicalExpression leftFieldExpr = condition.getLeft();
    final LogicalExpression rightFieldExpr = condition.getRight();

    // Generate members and initialization code
    /////////////////////////////////////////

    // declare and assign JoinStatus member
    cg.setMappingSet(SETUP_MAPPING);
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

    // materialize value vector readers from join expression
    final LogicalExpression materializedLeftExpr = ExpressionTreeMaterializer.materialize(leftFieldExpr, left, collector);
    if (collector.hasErrors())
      throw new ClassTransformationException(String.format(
          "Failure while trying to materialize incoming left field.  Errors:\n %s.", collector.toErrorString()));

    final LogicalExpression materializedRightExpr = ExpressionTreeMaterializer.materialize(rightFieldExpr, right, collector);
    if (collector.hasErrors())
      throw new ClassTransformationException(String.format(
          "Failure while trying to materialize incoming right field.  Errors:\n %s.", collector.toErrorString()));


    // generate compare()
    ////////////////////////
    cg.setMappingSet(COMPARE_MAPPING);
    cg.getSetupBlock().assign(JExpr._this().ref(incomingRecordBatch), JExpr._this().ref(incomingLeftRecordBatch));
    CodeGenerator.HoldingContainer compareLeftExprHolder = cg.addExpr(materializedLeftExpr, false);
    cg.setMappingSet(COMPARE_RIGHT_MAPPING);
    cg.getSetupBlock().assign(JExpr._this().ref(incomingRecordBatch), JExpr._this().ref(incomingRightRecordBatch));
    CodeGenerator.HoldingContainer compareRightExprHolder = cg.addExpr(materializedRightExpr, false);

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

    FunctionCall f = new FunctionCall(ComparatorFunctions.COMPARE_TO, ImmutableList.of((LogicalExpression) new HoldingContainerExpression(compareLeftExprHolder), (LogicalExpression)  new HoldingContainerExpression(compareRightExprHolder)), ExpressionPosition.UNKNOWN);
    cg.addExpr(new ReturnValueExpression(f, false), false);
//    
//    // equality
//    cg.getEvalBlock()._if(compareLeftExprHolder.getValue().eq(compareRightExprHolder.getValue()))
//                     ._then()
//                       ._return(JExpr.lit(0));
//    // less than
//    cg.getEvalBlock()._if(compareLeftExprHolder.getValue().lt(compareRightExprHolder.getValue()))
//                     ._then()
//                       ._return(JExpr.lit(-1));
//    // greater than
//    cg.getEvalBlock()._return(JExpr.lit(1));


    // generate compareNextLeftKey()
    ////////////////////////////////
    cg.setMappingSet(COMPARE_LEFT_MAPPING);
    cg.getSetupBlock().assign(JExpr._this().ref(incomingRecordBatch), JExpr._this().ref(incomingLeftRecordBatch));

    // int nextLeftIndex = leftIndex + 1;
    cg.getEvalBlock().decl(JType.parse(cg.getModel(), "int"), "nextLeftIndex", JExpr.direct("leftIndex").plus(JExpr.lit(1)));

    // check if the next key is in this batch
    cg.getEvalBlock()._if(joinStatus.invoke("isNextLeftPositionInCurrentBatch").eq(JExpr.lit(false)))
                     ._then()
                       ._return(JExpr.lit(-1));

    // generate VV read expressions
    CodeGenerator.HoldingContainer compareThisLeftExprHolder = cg.addExpr(materializedLeftExpr, false);
    cg.setMappingSet(COMPARE_NEXT_LEFT_MAPPING); // change mapping from 'leftIndex' to 'nextLeftIndex'
    CodeGenerator.HoldingContainer compareNextLeftExprHolder = cg.addExpr(materializedLeftExpr, false);

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
    FunctionCall g = new FunctionCall(ComparatorFunctions.COMPARE_TO, ImmutableList.of((LogicalExpression) new HoldingContainerExpression(compareThisLeftExprHolder), (LogicalExpression)  new HoldingContainerExpression(compareNextLeftExprHolder)), ExpressionPosition.UNKNOWN);
    cg.addExpr(new ReturnValueExpression(g, false), false);

    // generate copyLeft()
    //////////////////////
    cg.setMappingSet(COPY_LEFT_MAPPING);
    int vectorId = 0;
    for (VectorWrapper<?> vw : left) {
      JVar vvIn = cg.declareVectorValueSetupAndMember("incomingLeft",
                                                      new TypedFieldId(vw.getField().getType(), vectorId));
      JVar vvOut = cg.declareVectorValueSetupAndMember("outgoing",
                                                       new TypedFieldId(vw.getField().getType(),vectorId));
      // todo: check result of copyFromSafe and grow allocation
      cg.getEvalBlock()._if(vvOut.invoke("copyFromSafe")
                                   .arg(COPY_LEFT_MAPPING.getValueReadIndex())
                                   .arg(COPY_LEFT_MAPPING.getValueWriteIndex())
                                   .arg(vvIn).eq(JExpr.FALSE))
          ._then()
          ._return(JExpr.FALSE);
      ++vectorId;
    }
    cg.getEvalBlock()._return(JExpr.lit(true));

    // generate copyRight()
    ///////////////////////
    cg.setMappingSet(COPY_RIGHT_MAPPING);

    int rightVectorBase = vectorId;
    for (VectorWrapper<?> vw : right) {
      JVar vvIn = cg.declareVectorValueSetupAndMember("incomingRight",
                                                      new TypedFieldId(vw.getField().getType(), vectorId - rightVectorBase));
      JVar vvOut = cg.declareVectorValueSetupAndMember("outgoing",
                                                       new TypedFieldId(vw.getField().getType(),vectorId));
      // todo: check result of copyFromSafe and grow allocation
      cg.getEvalBlock()._if(vvOut.invoke("copyFromSafe")
                                 .arg(COPY_RIGHT_MAPPING.getValueReadIndex())
                                 .arg(COPY_RIGHT_MAPPING.getValueWriteIndex())
                                 .arg(vvIn).eq(JExpr.FALSE))
          ._then()
          ._return(JExpr.FALSE);
      ++vectorId;
    }
    cg.getEvalBlock()._return(JExpr.lit(true));

    JoinWorker w = context.getImplementationClass(cg);
    w.setupJoin(context, status, this.container);
    return w;
  }

  private void allocateBatch() {
    // allocate new batch space.
    container.clear();
    // add fields from both batches
    for (VectorWrapper<?> w : left) {
      ValueVector outgoingVector = TypeHelper.getNewVector(w.getField(), context.getAllocator());
      VectorAllocator.getAllocator(outgoingVector, (int) Math.ceil(w.getValueVector().getBufferSize() / left.getRecordCount())).alloc(left.getRecordCount() * 16);
      container.add(outgoingVector);
    }

    for (VectorWrapper<?> w : right) {
      ValueVector outgoingVector = TypeHelper.getNewVector(w.getField(), context.getAllocator());
      VectorAllocator.getAllocator(outgoingVector, (int) Math.ceil(w.getValueVector().getBufferSize() / right.getRecordCount())).alloc(right.getRecordCount() * 16);
      container.add(outgoingVector);
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    logger.debug("Built joined schema: {}", container.getSchema());
  }

}
