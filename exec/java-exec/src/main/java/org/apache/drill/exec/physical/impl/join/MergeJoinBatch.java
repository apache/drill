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
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.impl.join.JoinUtils.JoinComparator;
import org.apache.drill.exec.physical.impl.join.JoinWorker.JoinOutcome;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;
import org.apache.calcite.rel.core.JoinRelType;

import com.google.common.base.Preconditions;
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

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergeJoinBatch.class);

  public static final long ALLOCATOR_INITIAL_RESERVATION = 1*1024*1024;
  public static final long ALLOCATOR_MAX_RESERVATION = 20L*1000*1000*1000;

  public final MappingSet setupMapping =
      new MappingSet("null", "null",
                     GM("doSetup", "doSetup", null, null),
                     GM("doSetup", "doSetup", null, null));
  public final MappingSet copyLeftMapping =
      new MappingSet("leftIndex", "outIndex",
                     GM("doSetup", "doSetup", null, null),
                     GM("doSetup", "doCopyLeft", null, null));
  public final MappingSet copyRightMappping =
      new MappingSet("rightIndex", "outIndex",
                     GM("doSetup", "doSetup", null, null),
                     GM("doSetup", "doCopyRight", null, null));
  public final MappingSet compareMapping =
      new MappingSet("leftIndex", "rightIndex",
                     GM("doSetup", "doSetup", null, null),
                     GM("doSetup", "doCompare", null, null));
  public final MappingSet compareRightMapping =
      new MappingSet("rightIndex", "null",
                     GM("doSetup", "doSetup", null, null),
                     GM("doSetup", "doCompare", null, null));
  public final MappingSet compareLeftMapping =
      new MappingSet("leftIndex", "null",
                     GM("doSetup", "doSetup", null, null),
                     GM("doSetup", "doCompareNextLeftKey", null, null));
  public final MappingSet compareNextLeftMapping =
      new MappingSet("nextLeftIndex", "null",
                     GM("doSetup", "doSetup", null, null),
                     GM("doSetup", "doCompareNextLeftKey", null, null));


  private final RecordBatch left;
  private final RecordBatch right;
  private final JoinStatus status;
  private final List<JoinCondition> conditions;
  private final JoinRelType joinType;
  private JoinWorker worker;
  public MergeJoinBatchBuilder batchBuilder;
  private boolean areNullsEqual = false; // whether nulls compare equal

  private static final String LEFT_INPUT = "LEFT INPUT";
  private static final String RIGHT_INPUT = "RIGHT INPUT";

  protected MergeJoinBatch(MergeJoinPOP popConfig, FragmentContext context, RecordBatch left, RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context, true);

    if (popConfig.getConditions().size() == 0) {
      throw new UnsupportedOperationException("Merge Join currently does not support cartesian join.  This join operator was configured with 0 conditions");
    }
    this.left = left;
    this.right = right;
    this.joinType = popConfig.getJoinType();
    this.status = new JoinStatus(left, right, this);
    this.batchBuilder = new MergeJoinBatchBuilder(oContext.getAllocator(), status);
    this.conditions = popConfig.getConditions();

    JoinComparator comparator = JoinComparator.NONE;
    for (JoinCondition condition : conditions) {
      comparator = JoinUtils.checkAndSetComparison(condition, comparator);
    }
    assert comparator != JoinComparator.NONE;
    areNullsEqual = (comparator == JoinComparator.IS_NOT_DISTINCT_FROM);
  }

  public JoinRelType getJoinType() {
    return joinType;
  }

  @Override
  public int getRecordCount() {
    return status.getOutPosition();
  }

  @Override
  public void buildSchema() throws SchemaChangeException {
    status.ensureInitial();

    final IterOutcome leftOutcome = status.getLastLeft();
    final IterOutcome rightOutcome = status.getLastRight();
    if (leftOutcome == IterOutcome.STOP || rightOutcome == IterOutcome.STOP) {
      state = BatchState.STOP;
      return;
    }

    if (leftOutcome == IterOutcome.OUT_OF_MEMORY || rightOutcome == IterOutcome.OUT_OF_MEMORY) {
      state = BatchState.OUT_OF_MEMORY;
      return;
    }

    allocateBatch(true);
  }

  @Override
  public IterOutcome innerNext() {
    // we do this in the here instead of the constructor because don't necessary want to start consuming on construction.
    status.ensureInitial();

    // loop so we can start over again if we find a new batch was created.
    while (true) {

      JoinOutcome outcome = status.getOutcome();
      // if the previous outcome was a change in schema or we sent a batch, we have to set up a new batch.
      if (outcome == JoinOutcome.SCHEMA_CHANGED) {
        allocateBatch(true);
      } else if (outcome == JoinOutcome.BATCH_RETURNED) {
        allocateBatch(false);
      }

      // reset the output position to zero after our parent iterates this RecordBatch
      if (outcome == JoinOutcome.BATCH_RETURNED ||
          outcome == JoinOutcome.SCHEMA_CHANGED ||
          outcome == JoinOutcome.NO_MORE_DATA) {
        status.resetOutputPos();
      }

      if (outcome == JoinOutcome.NO_MORE_DATA) {
        logger.debug("NO MORE DATA; returning {}  NONE");
        return IterOutcome.NONE;
      }

      boolean first = false;
      if (worker == null) {
        try {
          logger.debug("Creating New Worker");
          stats.startSetup();
          this.worker = generateNewWorker();
          first = true;
        } catch (ClassTransformationException | IOException | SchemaChangeException e) {
          context.fail(new SchemaChangeException(e));
          kill(false);
          return IterOutcome.STOP;
        } finally {
          stats.stopSetup();
        }
      }

      // join until we have a complete outgoing batch
      if (!worker.doJoin(status)) {
        worker = null;
      }

      // get the outcome of the join.
      switch (status.getOutcome()) {
      case BATCH_RETURNED:
        // only return new schema if new worker has been setup.
        logger.debug("BATCH RETURNED; returning {}", (first ? "OK_NEW_SCHEMA" : "OK"));
        setRecordCountInContainer();
        return first ? IterOutcome.OK_NEW_SCHEMA : IterOutcome.OK;
      case FAILURE:
        kill(false);
        return IterOutcome.STOP;
      case NO_MORE_DATA:
        logger.debug("NO MORE DATA; returning {}", (status.getOutPosition() > 0 ? (first ? "OK_NEW_SCHEMA" : "OK") : (first ? "OK_NEW_SCHEMA" :"NONE")));
        setRecordCountInContainer();
        state = BatchState.DONE;
        return status.getOutPosition() > 0 ? (first ? IterOutcome.OK_NEW_SCHEMA : IterOutcome.OK): (first ? IterOutcome.OK_NEW_SCHEMA : IterOutcome.NONE);
      case SCHEMA_CHANGED:
        worker = null;
        if (status.getOutPosition() > 0) {
          // if we have current data, let's return that.
          logger.debug("SCHEMA CHANGED; returning {} ", (first ? "OK_NEW_SCHEMA" : "OK"));
          setRecordCountInContainer();
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

  private void setRecordCountInContainer() {
    for (VectorWrapper vw : container) {
      Preconditions.checkArgument(!vw.isHyper());
      vw.getValueVector().getMutator().setValueCount(getRecordCount());
    }
  }

  public void resetBatchBuilder() {
    batchBuilder.close();
    batchBuilder = new MergeJoinBatchBuilder(oContext.getAllocator(), status);
  }

  public void addRightToBatchBuilder() {
    batchBuilder.add(right);
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    left.kill(sendUpstream);
    right.kill(sendUpstream);
  }

  private void generateDoCompareNextLeft(ClassGenerator<JoinWorker> cg, JVar incomingRecordBatch,
      LogicalExpression[] leftExpression, JVar incomingLeftRecordBatch, JVar joinStatus,
      ErrorCollector collector) throws ClassTransformationException {
    boolean nextLeftIndexDeclared = false;

    cg.setMappingSet(compareLeftMapping);

    for (int i = 0; i < leftExpression.length; i++) {

      // materialize value vector readers from join expression
      final LogicalExpression materializedLeftExpr = leftExpression[i];

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

      LogicalExpression gh =
          FunctionGenerationHelper.getOrderingComparatorNullsHigh(compareThisLeftExprHolder,
                                                                  compareNextLeftExprHolder,
                                                                  context.getFunctionRegistry());
      HoldingContainer out = cg.addExpr(gh, false);

      // If not 0, it means not equal. We return this out value.
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

    /*
     * Materialize expressions on both sides of the join condition. Check if both the sides
     * have the same return type, if not then inject casts so that comparison function will work as
     * expected
     */
    LogicalExpression leftExpr[] = new LogicalExpression[conditions.size()];
    LogicalExpression rightExpr[] = new LogicalExpression[conditions.size()];
    IterOutcome lastLeftStatus = status.getLastLeft();
    IterOutcome lastRightStatus = status.getLastRight();
    for (int i = 0; i < conditions.size(); i++) {
      JoinCondition condition = conditions.get(i);
      leftExpr[i] =  materializeExpression(condition.getLeft(), lastLeftStatus, left, collector);
      rightExpr[i] = materializeExpression(condition.getRight(), lastRightStatus, right, collector);
    }

    // if right side is empty, rightExpr will most likely default to NULLABLE INT which may cause the following
    // call to throw an exception. In this case we can safely skip adding the casts
    if (lastRightStatus != IterOutcome.NONE) {
      JoinUtils.addLeastRestrictiveCasts(leftExpr, left, rightExpr, right, context);
    }
    //generate doCompare() method
    /////////////////////////////////////////
    generateDoCompare(cg, incomingRecordBatch, leftExpr, incomingLeftRecordBatch, rightExpr,
        incomingRightRecordBatch, collector);

    //generate doCompareNextLeftKey() method
    /////////////////////////////////////////
    generateDoCompareNextLeft(cg, incomingRecordBatch, leftExpr, incomingLeftRecordBatch, joinStatus, collector);

    // generate copyLeft()
    //////////////////////
    cg.setMappingSet(copyLeftMapping);
    int vectorId = 0;
    if (worker == null || status.isLeftPositionAllowed()) {
      for (VectorWrapper<?> vw : left) {
        MajorType inputType = vw.getField().getType();
        MajorType outputType;
        if (joinType == JoinRelType.RIGHT && inputType.getMode() == DataMode.REQUIRED) {
          outputType = Types.overrideMode(inputType, DataMode.OPTIONAL);
        } else {
          outputType = inputType;
        }
        JVar vvIn = cg.declareVectorValueSetupAndMember("incomingLeft",
                                                        new TypedFieldId(inputType, vectorId));
        JVar vvOut = cg.declareVectorValueSetupAndMember("outgoing",
                                                         new TypedFieldId(outputType,vectorId));
        // todo: check result of copyFromSafe and grow allocation
        cg.getEvalBlock().add(vvOut.invoke("copyFromSafe")
                                     .arg(copyLeftMapping.getValueReadIndex())
                                     .arg(copyLeftMapping.getValueWriteIndex())
                                     .arg(vvIn));
        ++vectorId;
      }
    }

    // generate copyRight()
    ///////////////////////
    cg.setMappingSet(copyRightMappping);

    int rightVectorBase = vectorId;
    if (status.getLastRight() != IterOutcome.NONE && (worker == null || status.isRightPositionAllowed())) {
      for (VectorWrapper<?> vw : right) {
        MajorType inputType = vw.getField().getType();
        MajorType outputType;
        if (joinType == JoinRelType.LEFT && inputType.getMode() == DataMode.REQUIRED) {
          outputType = Types.overrideMode(inputType, DataMode.OPTIONAL);
        } else {
          outputType = inputType;
        }
        JVar vvIn = cg.declareVectorValueSetupAndMember("incomingRight",
                                                        new TypedFieldId(inputType, vectorId - rightVectorBase));
        JVar vvOut = cg.declareVectorValueSetupAndMember("outgoing",
                                                         new TypedFieldId(outputType,vectorId));
        // todo: check result of copyFromSafe and grow allocation
        cg.getEvalBlock().add(vvOut.invoke("copyFromSafe")
                                   .arg(copyRightMappping.getValueReadIndex())
                                   .arg(copyRightMappping.getValueWriteIndex())
                                   .arg(vvIn));
        ++vectorId;
      }
    }

    JoinWorker w = context.getImplementationClass(cg);
    w.setupJoin(context, status, this.container);
    return w;
  }

  private void allocateBatch(boolean newSchema) {
    // allocate new batch space.
    container.zeroVectors();

    boolean leftAllowed = status.getLastLeft() != IterOutcome.NONE;
    boolean rightAllowed = status.getLastRight() != IterOutcome.NONE;

    if (newSchema) {
    // add fields from both batches
      if (leftAllowed) {
        for (VectorWrapper<?> w : left) {
          MajorType inputType = w.getField().getType();
          MajorType outputType;
          if (joinType == JoinRelType.RIGHT && inputType.getMode() == DataMode.REQUIRED) {
            outputType = Types.overrideMode(inputType, DataMode.OPTIONAL);
          } else {
            outputType = inputType;
          }
          MaterializedField newField = MaterializedField.create(w.getField().getPath(), outputType);
          ValueVector v = container.addOrGet(newField);
          if (v instanceof AbstractContainerVector) {
            w.getValueVector().makeTransferPair(v);
            v.clear();
          }
        }
      }

      if (rightAllowed) {
        for (VectorWrapper<?> w : right) {
          MajorType inputType = w.getField().getType();
          MajorType outputType;
          if (joinType == JoinRelType.LEFT && inputType.getMode() == DataMode.REQUIRED) {
            outputType = Types.overrideMode(inputType, DataMode.OPTIONAL);
          } else {
            outputType = inputType;
          }
          MaterializedField newField = MaterializedField.create(w.getField().getPath(), outputType);
          ValueVector v = container.addOrGet(newField);
          if (v instanceof AbstractContainerVector) {
            w.getValueVector().makeTransferPair(v);
            v.clear();
          }
        }
      }
    }

    for (VectorWrapper w : container) {
      AllocationHelper.allocateNew(w.getValueVector(), Character.MAX_VALUE);
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    logger.debug("Built joined schema: {}", container.getSchema());
  }

  private void generateDoCompare(ClassGenerator<JoinWorker> cg, JVar incomingRecordBatch,
      LogicalExpression[] leftExpression, JVar incomingLeftRecordBatch, LogicalExpression[] rightExpression,
      JVar incomingRightRecordBatch, ErrorCollector collector) throws ClassTransformationException {

    cg.setMappingSet(compareMapping);
    if (status.getLastRight() != IterOutcome.NONE) {
      assert leftExpression.length == rightExpression.length;

      for (int i = 0; i < leftExpression.length; i++) {


        // generate compare()
        ////////////////////////
        cg.setMappingSet(compareMapping);
        cg.getSetupBlock().assign(JExpr._this().ref(incomingRecordBatch), JExpr._this().ref(incomingLeftRecordBatch));
        ClassGenerator.HoldingContainer compareLeftExprHolder = cg.addExpr(leftExpression[i], false);

        cg.setMappingSet(compareRightMapping);
        cg.getSetupBlock().assign(JExpr._this().ref(incomingRecordBatch), JExpr._this().ref(incomingRightRecordBatch));
        ClassGenerator.HoldingContainer compareRightExprHolder = cg.addExpr(rightExpression[i], false);

        LogicalExpression fh =
            FunctionGenerationHelper.getOrderingComparatorNullsHigh(compareLeftExprHolder,
                                                                    compareRightExprHolder,
                                                                    context.getFunctionRegistry());
        HoldingContainer out = cg.addExpr(fh, false);

        // If not 0, it means not equal.
        // Null compares to Null should returns null (unknown). In such case, we return 1 to indicate they are not equal.
        if (compareLeftExprHolder.isOptional() && compareRightExprHolder.isOptional()
            && ! areNullsEqual) {
          JConditional jc = cg.getEvalBlock()._if(compareLeftExprHolder.getIsSet().eq(JExpr.lit(0)).
                                      cand(compareRightExprHolder.getIsSet().eq(JExpr.lit(0))));
          jc._then()._return(JExpr.lit(1));
          jc._elseif(out.getValue().ne(JExpr.lit(0)))._then()._return(out.getValue());
        } else {
          cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)))._then()._return(out.getValue());
        }
      }
    }

    //Pass the equality check for all the join conditions. Finally, return 0.
    cg.getEvalBlock()._return(JExpr.lit(0));
  }

  private LogicalExpression materializeExpression(LogicalExpression expression, IterOutcome lastStatus,
                                                  RecordBatch input, ErrorCollector collector) throws ClassTransformationException {
    LogicalExpression materializedExpr = null;
    if (lastStatus != IterOutcome.NONE) {
      materializedExpr = ExpressionTreeMaterializer.materialize(expression, input, collector, context.getFunctionRegistry());
    } else {
      materializedExpr = new TypedNullConstant(Types.optional(MinorType.INT));
    }
    if (collector.hasErrors()) {
      throw new ClassTransformationException(String.format(
          "Failure while trying to materialize incoming field from %s batch.  Errors:\n %s.",
          (input == left ? LEFT_INPUT : RIGHT_INPUT), collector.toErrorString()));
    }
    return materializedExpr;
  }

}
