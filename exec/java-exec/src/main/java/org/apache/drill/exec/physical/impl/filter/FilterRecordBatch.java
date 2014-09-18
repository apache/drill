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
package org.apache.drill.exec.physical.impl.filter;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;

public class FilterRecordBatch extends AbstractSingleRecordBatch<Filter>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterRecordBatch.class);

  private SelectionVector2 sv2;
  private SelectionVector4 sv4;
  private BufferAllocator.PreAllocator svAllocator;
  private Filterer filter;

  public FilterRecordBatch(Filter pop, RecordBatch incoming, FragmentContext context) throws OutOfMemoryException {
    super(pop, context, incoming);
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public int getRecordCount() {
    return sv2 != null ? sv2.getCount() : sv4.getCount();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return sv2;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return sv4;
  }

  @Override
  protected void doWork() {
    int recordCount = incoming.getRecordCount();
    filter.filterBatch(recordCount);
//    for (VectorWrapper<?> v : container) {
//      ValueVector.Mutator m = v.getValueVector().getMutator();
//      m.setValueCount(recordCount);
//    }
  }


  @Override
  public void cleanup() {
    if (sv2 != null) {
      sv2.clear();
    }
    if (sv4 != null) {
      sv4.clear();
    }
    super.cleanup();
  }

  @Override
  protected void setupNewSchema() throws SchemaChangeException {
    container.clear();
    if (sv2 != null) {
      sv2.clear();
    }

    switch (incoming.getSchema().getSelectionVectorMode()) {
      case NONE:
        sv2 = new SelectionVector2(oContext.getAllocator());
        this.filter = generateSV2Filterer();
        break;
      case TWO_BYTE:
        sv2 = new SelectionVector2(oContext.getAllocator());
        this.filter = generateSV2Filterer();
        break;
      case FOUR_BYTE:
        /*
         * Filter does not support SV4 handling. There are couple of minor issues in the
         * logic that handles SV4 + filter should always be pushed beyond sort so disabling
         * it in FilterPrel.
         *

        // set up the multi-batch selection vector
        this.svAllocator = oContext.getAllocator().getNewPreAllocator();
        if (!svAllocator.preAllocate(incoming.getRecordCount()*4))
          throw new SchemaChangeException("Attempted to filter an SV4 which exceeds allowed memory (" +
                                          incoming.getRecordCount() * 4 + " bytes)");
        sv4 = new SelectionVector4(svAllocator.getAllocation(), incoming.getRecordCount(), Character.MAX_VALUE);
        this.filter = generateSV4Filterer();
        break;
        */
      default:
        throw new UnsupportedOperationException();
    }

  }

  protected Filterer generateSV4Filterer() throws SchemaChangeException {
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();
    final ClassGenerator<Filterer> cg = CodeGenerator.getRoot(Filterer.TEMPLATE_DEFINITION4, context.getFunctionRegistry());

    final LogicalExpression expr = ExpressionTreeMaterializer.materialize(popConfig.getExpr(), incoming, collector, context.getFunctionRegistry());
    if (collector.hasErrors()) {
      throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
    }

    cg.addExpr(new ReturnValueExpression(expr));

//    for (VectorWrapper<?> i : incoming) {
//      ValueVector v = TypeHelper.getNewVector(i.getField(), context.getAllocator());
//      container.add(v);
//      allocators.add(getAllocator4(v));
//    }

    for (VectorWrapper<?> vw : incoming) {
      for (ValueVector vv : vw.getValueVectors()) {
        TransferPair pair = vv.getTransferPair();
        container.add(pair.getTo());
        transfers.add(pair);
      }
    }

    // allocate outgoing sv4
    container.buildSchema(SelectionVectorMode.FOUR_BYTE);

    try {
      TransferPair[] tx = transfers.toArray(new TransferPair[transfers.size()]);
      Filterer filter = context.getImplementationClass(cg);
      filter.setup(context, incoming, this, tx);
      return filter;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }

  }

  protected Filterer generateSV2Filterer() throws SchemaChangeException {
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();
    final ClassGenerator<Filterer> cg = CodeGenerator.getRoot(Filterer.TEMPLATE_DEFINITION2, context.getFunctionRegistry());

    final LogicalExpression expr = ExpressionTreeMaterializer.materialize(popConfig.getExpr(), incoming, collector, context.getFunctionRegistry());
    if (collector.hasErrors()) {
      throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
    }

    cg.addExpr(new ReturnValueExpression(expr));

    for (VectorWrapper<?> v : incoming) {
      TransferPair pair = v.getValueVector().getTransferPair();
      container.add(pair.getTo());
      transfers.add(pair);
    }

    container.buildSchema(SelectionVectorMode.TWO_BYTE);

    try {
      TransferPair[] tx = transfers.toArray(new TransferPair[transfers.size()]);
      Filterer filter = context.getImplementationClass(cg);
      filter.setup(context, incoming, this, tx);
      return filter;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }

  }

}
