/*******************************************************************************
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
 ******************************************************************************/

package org.apache.drill.exec.physical.impl.partitionsender;

import com.google.common.collect.Lists;
import com.sun.codemodel.*;
import org.apache.drill.common.expression.*;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.physical.impl.filter.Filterer;
import org.apache.drill.exec.physical.impl.filter.ReturnValueExpression;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;

import java.io.IOException;
import java.util.List;

class PartitionSenderRootExec implements RootExec {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionSenderRootExec.class);
  private RecordBatch incoming;
  private HashPartitionSender operator;
  private OutgoingRecordBatch[] outgoing;
  private Partitioner partitioner;
  private FragmentContext context;
  private boolean ok = true;

  public PartitionSenderRootExec(FragmentContext context,
                                 RecordBatch incoming,
                                 HashPartitionSender operator) {

    this.incoming = incoming;
    this.operator = operator;
    this.context = context;
    this.outgoing = new OutgoingRecordBatch[operator.getDestinations().size()];
    int fieldId = 0;
    for (CoordinationProtos.DrillbitEndpoint endpoint : operator.getDestinations())
      outgoing[fieldId++] = new OutgoingRecordBatch(operator,
                                                    context.getCommunicator().getTunnel(endpoint),
                                                    incoming,
                                                    context);
    try {
      createPartitioner();
    } catch (SchemaChangeException e) {
      ok = false;
      logger.error("Failed to create partitioning sender during query ", e);
      context.fail(e);
    }
  }

  @Override
  public boolean next() {

    if (!ok) {
      stop();
      return false;
    }

    RecordBatch.IterOutcome out = incoming.next();
    logger.debug("Partitioner.next(): got next record batch with status {}", out);
    switch(out){
      case NONE:
      case STOP:
        try {
          // send any pending batches
          for (OutgoingRecordBatch batch : outgoing) {
            batch.setIsLast();
            batch.flush();
          }
        } catch (SchemaChangeException e) {
          incoming.kill();
          logger.error("Error while creating partitioning sender or flushing outgoing batches", e);
          context.fail(e);
        }
        return false;

      case OK_NEW_SCHEMA:
        try {
          // send all existing batches
          flushOutgoingBatches(false, true);
          // update OutgoingRecordBatch's schema and generate partitioning code
          createPartitioner();
        } catch (SchemaChangeException e) {
          incoming.kill();
          logger.error("Error while creating partitioning sender or flushing outgoing batches", e);
          context.fail(e);
          return false;
        }
      case OK:
        partitioner.partitionBatch(incoming);
        context.batchesCompleted.inc(1);
        context.recordsCompleted.inc(incoming.getRecordCount());
        return true;
      case NOT_YET:
      default:
        throw new IllegalStateException();
    }
  }

  public void stop() {
    ok = false;
    incoming.kill();
  }
  
  private void generatePartitionFunction() throws SchemaChangeException {

    LogicalExpression filterExpression = operator.getExpr();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final CodeGenerator<Partitioner> cg = new CodeGenerator<Partitioner>(Partitioner.TEMPLATE_DEFINITION, context.getFunctionRegistry());

    final LogicalExpression expr = ExpressionTreeMaterializer.materialize(filterExpression, incoming, collector);
    if(collector.hasErrors()){
      throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
    }

    cg.addExpr(new ReturnValueExpression(expr));
    
    try {
      Partitioner p = context.getImplementationClass(cg);
      p.setup(context, incoming, outgoing);
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }


  }

  private void createPartitioner() throws SchemaChangeException {

    // set up partitioning function
    final LogicalExpression expr = operator.getExpr();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final CodeGenerator<Partitioner> cg = new CodeGenerator<Partitioner>(Partitioner.TEMPLATE_DEFINITION,
                                                                         context.getFunctionRegistry());

    final LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(expr, incoming, collector);
    if (collector.hasErrors()) {
      throw new SchemaChangeException(String.format(
          "Failure while trying to materialize incoming schema.  Errors:\n %s.",
          collector.toErrorString()));
    }

    // generate code to copy from an incoming value vector to the destination partition's outgoing value vector
    JExpression inIndex = JExpr.direct("inIndex");
    JExpression bucket = JExpr.direct("bucket");
    JType outgoingVectorArrayType = cg.getModel().ref(ValueVector.class).array().array();
    JType outgoingBatchArrayType = cg.getModel().ref(OutgoingRecordBatch.class).array();

    // generate evaluate expression to determine the hash
    CodeGenerator.HoldingContainer exprHolder = cg.addExpr(materializedExpr);
    cg.getBlock().decl(JType.parse(cg.getModel(), "int"), "bucket", exprHolder.getValue().mod(JExpr.lit(outgoing.length)));

    // declare and assign the array of outgoing record batches
    JVar outgoingBatches = cg.clazz.field(JMod.NONE,
        outgoingBatchArrayType,
        "outgoingBatches");
    cg.getSetupBlock().assign(outgoingBatches, JExpr.direct("outgoing"));

    // declare a two-dimensional array of value vectors; batch is first dimension, ValueVector is the second
    JVar outgoingVectors = cg.clazz.field(JMod.NONE,
                                          outgoingVectorArrayType,
                                          "outgoingVectors");

    // create 2d array and build initialization list.  For example:
    //     outgoingVectors = new ValueVector[][] { 
    //                              new ValueVector[] {vv1, vv2},
    //                              new ValueVector[] {vv3, vv4}
    //                       });
    JArray outgoingVectorInit = JExpr.newArray(cg.getModel().ref(ValueVector.class).array());

    int fieldId = 0;
    int batchId = 0;
    for (OutgoingRecordBatch batch : outgoing) {

      JArray outgoingVectorInitBatch = JExpr.newArray(cg.getModel().ref(ValueVector.class));
      for (VectorWrapper<?> vv : batch) {
        // declare outgoing value vector and assign it to the array
        JVar outVV = cg.declareVectorValueSetupAndMember("outgoing[" + batchId + "]",
                                                         new TypedFieldId(vv.getField().getType(),
                                                                          fieldId,
                                                                          false));
        // add vv to initialization list (e.g. { vv1, vv2, vv3 } )
        outgoingVectorInitBatch.add(outVV);
        ++fieldId;
      }

      // add VV array to initialization list (e.g. new ValueVector[] { ... })
      outgoingVectorInit.add(outgoingVectorInitBatch);
      ++batchId;
      fieldId = 0;
    }

    // generate outgoing value vector 2d array initialization list.
    cg.getSetupBlock().assign(outgoingVectors, outgoingVectorInit);

    for (VectorWrapper<?> vvIn : incoming) {
      // declare incoming value vectors
      JVar incomingVV = cg.declareVectorValueSetupAndMember("incoming", new TypedFieldId(vvIn.getField().getType(),
                                                                                         fieldId,
                                                                                         vvIn.isHyper()));

      // generate the copyFrom() invocation with explicit cast to the appropriate type
      Class<?> vvType = TypeHelper.getValueVectorClass(vvIn.getField().getType().getMinorType(),
                                                       vvIn.getField().getType().getMode());
      JClass vvClass = cg.getModel().ref(vvType);
      // the following block generates calls to copyFrom(); e.g.:
      // ((IntVector) outgoingVectors[bucket][0]).copyFrom(inIndex,
      //                                                     outgoingBatches[bucket].getRecordCount(),
      //                                                     vv1);
      cg.getBlock().add(
        ((JExpression) JExpr.cast(vvClass,
              ((JExpression)
                     outgoingVectors
                       .component(bucket))
                       .component(JExpr.lit(fieldId))))
                       .invoke("copyFrom")
                       .arg(inIndex)
                       .arg(((JExpression) outgoingBatches.component(bucket)).invoke("getRecordCount"))
                       .arg(incomingVV));

      ++fieldId;
    }
    // generate the OutgoingRecordBatch helper invocations
    cg.getBlock().add(((JExpression) outgoingBatches.component(bucket)).invoke("incRecordCount"));
    cg.getBlock().add(((JExpression) outgoingBatches.component(bucket)).invoke("flushIfNecessary"));
    try {
      // compile and setup generated code
      partitioner = context.getImplementationClassMultipleOutput(cg);
      partitioner.setup(context, incoming, outgoing);

    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }

  /**
   * Flush each outgoing record batch, and optionally reset the state of each outgoing record
   * batch (on schema change).  Note that the schema is updated based on incoming at the time
   * this function is invoked.
   *
   * @param isLastBatch    true if this is the last incoming batch
   * @param schemaChanged  true if the schema has changed
   */
  public void flushOutgoingBatches(boolean isLastBatch, boolean schemaChanged) throws SchemaChangeException {
    for (OutgoingRecordBatch batch : outgoing) {
      logger.debug("Attempting to flush all outgoing batches");
      if (isLastBatch)
        batch.setIsLast();
      batch.flush();
      if (schemaChanged) {
        batch.resetBatch();
        batch.initializeBatch();
      }
    }
  }
}
