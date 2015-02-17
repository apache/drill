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
package org.apache.drill.exec.physical.impl.TopN;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.TopN;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.physical.impl.svremover.Copier;
import org.apache.drill.exec.physical.impl.svremover.RemovingRecordBatch;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;
import org.eigenbase.rel.RelFieldCollation.Direction;
import org.eigenbase.rel.RelFieldCollation.NullDirection;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

public class TopNBatch extends AbstractRecordBatch<TopN> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TopNBatch.class);

  private static final long MAX_SORT_BYTES = 1L * 1024 * 1024 * 1024;
  public static final long ALLOCATOR_INITIAL_RESERVATION = 1*1024*1024;
  public static final long ALLOCATOR_MAX_RESERVATION = 20L*1000*1000*1000;
  private  final int batchPurgeThreshold;

  public final MappingSet MAIN_MAPPING = new MappingSet( (String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  public final MappingSet LEFT_MAPPING = new MappingSet("leftIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  public final MappingSet RIGHT_MAPPING = new MappingSet("rightIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);


  private final RecordBatch incoming;
  private BatchSchema schema;
  private PriorityQueue priorityQueue;
  private TopN config;
  SelectionVector4 sv4;
  private long countSincePurge;
  private int batchCount;
  private Copier copier;
  private boolean schemaBuilt = false;
  private boolean first = true;
  private int recordCount = 0;
  private boolean stop;

  public TopNBatch(TopN popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context);
    this.incoming = incoming;
    this.config = popConfig;
    batchPurgeThreshold = context.getConfig().getInt(ExecConstants.BATCH_PURGE_THRESHOLD);
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return sv4;
  }

  @Override
  public void cleanup() {
    if (sv4 != null) {
      sv4.clear();
    }
    if (priorityQueue != null) {
      priorityQueue.cleanup();
    }
    super.cleanup();
    incoming.cleanup();
  }

  public void buildSchema() throws SchemaChangeException {
    VectorContainer c = new VectorContainer(oContext);
    IterOutcome outcome = next(incoming);
    switch (outcome) {
      case OK:
      case OK_NEW_SCHEMA:
        for (VectorWrapper w : incoming) {
          ValueVector v = c.addOrGet(w.getField());
          if (v instanceof AbstractContainerVector) {
            w.getValueVector().makeTransferPair(v);
            v.clear();
          }
        }
        c = VectorContainer.canonicalize(c);
        for (VectorWrapper w : c) {
          ValueVector v = container.addOrGet(w.getField());
          if (v instanceof AbstractContainerVector) {
            w.getValueVector().makeTransferPair(v);
            v.clear();
          }
          v.allocateNew();
        }
        container.buildSchema(SelectionVectorMode.NONE);
        container.setRecordCount(0);
        return;
      case STOP:
        stop = true;
      case NONE:
        state = BatchState.DONE;
      default:
        return;
    }
  }

  @Override
  public IterOutcome innerNext() {
    if (state == BatchState.DONE) {
      return IterOutcome.NONE;
    }
    if (schema != null) {
      if (getSelectionVector4().next()) {
        recordCount = sv4.getCount();
        return IterOutcome.OK;
      } else {
        recordCount = 0;
        return IterOutcome.NONE;
      }
    }


    try{
      outer: while (true) {
        Stopwatch watch = new Stopwatch();
        watch.start();
        IterOutcome upstream;
        if (first) {
          upstream = IterOutcome.OK_NEW_SCHEMA;
          first = false;
        } else {
          upstream = next(incoming);
        }
        if (upstream == IterOutcome.OK && schema == null) {
          upstream = IterOutcome.OK_NEW_SCHEMA;
          container.clear();
        }
        logger.debug("Took {} us to get next", watch.elapsed(TimeUnit.MICROSECONDS));
        switch (upstream) {
        case NONE:
          break outer;
        case NOT_YET:
          throw new UnsupportedOperationException();
        case STOP:
          return upstream;
        case OK_NEW_SCHEMA:
          // only change in the case that the schema truly changes.  Artificial schema changes are ignored.
          if (!incoming.getSchema().equals(schema)) {
            if (schema != null) {
              throw new UnsupportedOperationException("Sort doesn't currently support sorts with changing schemas.");
            }
            this.schema = incoming.getSchema();
          }
          // fall through.
        case OK:
          if (incoming.getRecordCount() == 0) {
            for (VectorWrapper w : incoming) {
              w.clear();
            }
            break;
          }
          countSincePurge += incoming.getRecordCount();
          batchCount++;
          RecordBatchData batch = new RecordBatchData(incoming);
          batch.canonicalize();
          if (priorityQueue == null) {
            priorityQueue = createNewPriorityQueue(context, config.getOrderings(), new ExpandableHyperContainer(batch.getContainer()), MAIN_MAPPING, LEFT_MAPPING, RIGHT_MAPPING);
          }
          priorityQueue.add(context, batch);
          if (countSincePurge > config.getLimit() && batchCount > batchPurgeThreshold) {
            purge();
            countSincePurge = 0;
            batchCount = 0;
          }
          break;
        default:
          throw new UnsupportedOperationException();
        }
      }

      if (schema == null || priorityQueue == null) {
        // builder may be null at this point if the first incoming batch is empty
        state = BatchState.DONE;
        return IterOutcome.NONE;
      }

      priorityQueue.generate();

      this.sv4 = priorityQueue.getFinalSv4();
      container.clear();
      for (VectorWrapper w : priorityQueue.getHyperBatch()) {
        container.add(w.getValueVectors());
      }
      container.buildSchema(BatchSchema.SelectionVectorMode.FOUR_BYTE);

      recordCount = sv4.getCount();
      return IterOutcome.OK_NEW_SCHEMA;

    } catch(SchemaChangeException | ClassTransformationException | IOException ex) {
      kill(false);
      logger.error("Failure during query", ex);
      context.fail(ex);
      return IterOutcome.STOP;
    }
  }

  private void purge() throws SchemaChangeException {
    Stopwatch watch = new Stopwatch();
    watch.start();
    VectorContainer c = priorityQueue.getHyperBatch();
    VectorContainer newContainer = new VectorContainer(oContext);
    SelectionVector4 selectionVector4 = priorityQueue.getHeapSv4();
    SimpleRecordBatch batch = new SimpleRecordBatch(c, selectionVector4, context);
    SimpleRecordBatch newBatch = new SimpleRecordBatch(newContainer, null, context);
    if (copier == null) {
      copier = RemovingRecordBatch.getGenerated4Copier(batch, context, oContext.getAllocator(),  newContainer, newBatch);
    } else {
      for (VectorWrapper<?> i : batch) {

        ValueVector v = TypeHelper.getNewVector(i.getField(), oContext.getAllocator());
        newContainer.add(v);
      }
      copier.setupRemover(context, batch, newBatch);
    }
    SortRecordBatchBuilder builder = new SortRecordBatchBuilder(oContext.getAllocator(), MAX_SORT_BYTES);
    do {
      int count = selectionVector4.getCount();
      int copiedRecords = copier.copyRecords(0, count);
      assert copiedRecords == count;
      for (VectorWrapper<?> v : newContainer) {
        ValueVector.Mutator m = v.getValueVector().getMutator();
        m.setValueCount(count);
      }
      newContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      newContainer.setRecordCount(count);
      builder.add(newBatch);
    } while (selectionVector4.next());
    selectionVector4.clear();
    c.clear();
    VectorContainer newQueue = new VectorContainer();
    builder.canonicalize();
    builder.build(context, newQueue);
    priorityQueue.resetQueue(newQueue, builder.getSv4().createNewWrapperCurrent());
    builder.getSv4().clear();
    selectionVector4.clear();
    logger.debug("Took {} us to purge", watch.elapsed(TimeUnit.MICROSECONDS));
  }

  public PriorityQueue createNewPriorityQueue(FragmentContext context, List<Ordering> orderings,
                                                     VectorAccessible batch, MappingSet mainMapping, MappingSet leftMapping, MappingSet rightMapping)
          throws ClassTransformationException, IOException, SchemaChangeException{
    CodeGenerator<PriorityQueue> cg = CodeGenerator.get(PriorityQueue.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<PriorityQueue> g = cg.getRoot();
    g.setMappingSet(mainMapping);

    for (Ordering od : orderings) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector, context.getFunctionRegistry());
      if (collector.hasErrors()) {
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      }
      g.setMappingSet(leftMapping);
      HoldingContainer left = g.addExpr(expr, false);
      g.setMappingSet(rightMapping);
      HoldingContainer right = g.addExpr(expr, false);
      g.setMappingSet(mainMapping);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh =
        FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right,
                                                       context.getFunctionRegistry());
      HoldingContainer out = g.addExpr(fh, false);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      } else {
        jc._then()._return(out.getValue().minus());
      }
      g.rotateBlock();
    }

    g.rotateBlock();
    g.getEvalBlock()._return(JExpr.lit(0));

    PriorityQueue q = context.getImplementationClass(cg);
    q.init(config.getLimit(), context, oContext.getAllocator(), schema.getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE);
    return q;
  }

  @Override
  public WritableBatch getWritableBatch() {
    throw new UnsupportedOperationException("A sort batch is not writable.");
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }


  public static class SimpleRecordBatch implements RecordBatch {

    private VectorContainer container;
    private SelectionVector4 sv4;
    private FragmentContext context;

    public SimpleRecordBatch(VectorContainer container, SelectionVector4 sv4, FragmentContext context) {
      this.container = container;
      this.sv4 = sv4;
      this.context = context;
    }

    @Override
    public FragmentContext getContext() {
      return context;
    }

    @Override
    public BatchSchema getSchema() {
      return container.getSchema();
    }

    @Override
    public int getRecordCount() {
      if (sv4 != null) {
        return sv4.getCount();
      } else {
        return container.getRecordCount();
      }
    }

    @Override
    public void kill(boolean sendUpstream) {
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
      return sv4;
    }

    @Override
    public TypedFieldId getValueVectorId(SchemaPath path) {
      return container.getValueVectorId(path);
    }

    @Override
    public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
      return container.getValueAccessorById(clazz, ids);
    }

    @Override
    public IterOutcome next() {
      throw new UnsupportedOperationException();
    }

    @Override
    public WritableBatch getWritableBatch() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void cleanup() {
    }

    @Override
    public Iterator<VectorWrapper<?>> iterator() {
      return container.iterator();
    }

    @Override
    public VectorContainer getOutgoingContainer() {
      throw new UnsupportedOperationException(String.format(" You should not call getOutgoingContainer() for class %s", this.getClass().getCanonicalName()));
    }

  }

}
