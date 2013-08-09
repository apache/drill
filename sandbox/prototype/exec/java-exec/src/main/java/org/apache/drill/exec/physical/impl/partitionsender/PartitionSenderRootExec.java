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

import com.beust.jcommander.internal.Lists;
import com.sun.codemodel.*;
import org.apache.drill.common.expression.*;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;

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
      outgoing[fieldId] = new OutgoingRecordBatch(operator,
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
      case STOP:
      case NONE:
        // populate outgoing batches
        if (incoming.getRecordCount() > 0)
          partitioner.partitionBatch(incoming);

        // send all pending batches
        flushOutgoingBatches(true, false);
        return false;

      case OK_NEW_SCHEMA:
        // send all existing batches
        flushOutgoingBatches(false, true);
        // update OutgoingRecordBatch's schema and value vectors
        try {
          createPartitioner();
          partitioner.setup(context, incoming, outgoing);
        } catch (SchemaChangeException e) {
          incoming.kill();
          logger.error("Failed to create partitioning sender during query ", e);
          context.fail(e);
          return false;
        }
      case OK:
        partitioner.partitionBatch(incoming);
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

  private void createPartitioner() throws SchemaChangeException {

    // set up partitioning function
    final LogicalExpression expr = operator.getExpr();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final CodeGenerator<Partitioner> cg = new CodeGenerator<Partitioner>(Partitioner.TEMPLATE_DEFINITION,
                                                                         context.getFunctionRegistry());

    final LogicalExpression logicalExp = ExpressionTreeMaterializer.materialize(expr, incoming, collector);
    if (collector.hasErrors()) {
      throw new SchemaChangeException(String.format(
          "Failure while trying to materialize incoming schema.  Errors:\n %s.",
          collector.toErrorString()));
    }

    // generate code to copy from an incoming value vector to the destination partition's outgoing value vector
    int fieldId = 0;
    JExpression inIndex = JExpr.direct("inIndex");
    JExpression outIndex = JExpr.direct("outIndex");
    cg.rotateBlock();

    // declare array of record batches for each partition
    JVar outgoingBatches = cg.clazz.field(JMod.NONE,
                                          cg.getModel().ref(OutgoingRecordBatch.class).array(),
                                          "outgoingBatches");

    cg.getSetupBlock().assign(outgoingBatches, JExpr.direct("outgoing"));

    // declare incoming value vectors
    List<JVar> incomingVVs = Lists.newArrayList();
    for (VectorWrapper<?> vvIn : incoming)
      incomingVVs.add(cg.declareVectorValueSetupAndMember("incoming", new TypedFieldId(vvIn.getField().getType(),
                                                                                       fieldId++,
                                                                                       vvIn.isHyper())));

    int batchId = 0;
    fieldId = 0;
    // generate switch statement for each destination batch
    JSwitch switchStatement = cg.getBlock()._switch(outIndex);
    for (OutgoingRecordBatch batch : outgoing) {

      // generate case statement for this batch
      JBlock caseBlock = switchStatement._case(JExpr.lit(batchId)).body();

      for (VectorWrapper<?> vv : batch) {
        // declare outgoing value vector and a corresponding counter
        JVar outVV = cg.declareVectorValueSetupAndMember("outgoing[" + batchId + "]",
                                                         new TypedFieldId(vv.getField().getType(),
                                                                          fieldId,
                                                                          false));

        caseBlock.add(outVV.invoke("copyFrom")
                              .arg(inIndex)
                              .arg(JExpr.direct("outgoingBatches[" + batchId + "]").invoke("getRecordCount"))
                              .arg(incomingVVs.get(fieldId)));
        ++fieldId;
      }
      caseBlock.add(JExpr.direct("outgoingBatches[" + batchId + "]").invoke("incRecordCount"));
      caseBlock.add(JExpr.direct("outgoingBatches[" + batchId + "]").invoke("flushIfNecessary"));
      fieldId = 0;
      caseBlock._break();
      ++batchId;
    }

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
  public void flushOutgoingBatches(boolean isLastBatch, boolean schemaChanged) {
    for (OutgoingRecordBatch batch : outgoing) {
      logger.debug("Attempting to flush all outgoing batches");
      if (isLastBatch)
        batch.setIsLast();
      batch.flush();
      if (schemaChanged)
        batch.resetBatch();
    }
  }
}
