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
package org.apache.drill.exec.physical.impl.orderedpartitioner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.defs.OrderDef;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.cache.HazelCache;
import org.apache.drill.exec.cache.VectorWrap;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.*;
import org.apache.drill.exec.expr.fn.impl.ComparatorFunctions;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.OrderedPartitionSender;
import org.apache.drill.exec.physical.impl.sort.SortBatch;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.physical.impl.sort.Sorter;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.util.BatchPrinter;
import org.apache.drill.exec.vector.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class OrderedPartitionRecordBatch extends AbstractRecordBatch<OrderedPartitionSender>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderedPartitionRecordBatch.class);

  public static final MappingSet MAIN_MAPPING = new MappingSet( (String) null, null, CodeGenerator.DEFAULT_SCALAR_MAP, CodeGenerator.DEFAULT_SCALAR_MAP);
  public static final MappingSet INCOMING_MAPPING = new MappingSet("inIndex", null, "incoming", null, CodeGenerator.DEFAULT_SCALAR_MAP, CodeGenerator.DEFAULT_SCALAR_MAP);
  public static final MappingSet PARTITION_MAPPING = new MappingSet("partitionIndex", null, "partitionVectors", null,
          CodeGenerator.DEFAULT_SCALAR_MAP, CodeGenerator.DEFAULT_SCALAR_MAP);

  private static long MAX_SORT_BYTES = 8l * 1024 * 1024 * 1024;
  private static int RECORDS_TO_SAMPLE = 5000;
  private static int SAMPLING_FACTOR = 10;
  private static float COMPLETION_FACTOR = .75f;
  private static String SAMPLE_MAP_NAME = "sampleMap";
  private static String TABLE_MAP_NAME = "TableMap";
  protected final RecordBatch incoming;
  private boolean first = true;
  private OrderedPartitionProjector projector;
  private List<ValueVector> allocationVectors;
  private VectorContainer partitionVectors = new VectorContainer();
  private int partitions;
  private Queue<VectorContainer> batchQueue;
  private SortRecordBatchBuilder builder;
  private int recordsSampled;
  private int sendingMajorFragmentWidth;
  private boolean startedUnsampledBatches = false;
  private boolean upstreamNone = false;
  private int recordCount;

  public OrderedPartitionRecordBatch(OrderedPartitionSender pop, RecordBatch incoming, FragmentContext context){
    super(pop, context);
    this.incoming = incoming;
    this.partitions = pop.getDestinations().size();
    this.sendingMajorFragmentWidth = pop.getSendingWidth();
  }

  private boolean getPartitionVectors() {
    VectorContainer sampleContainer = new VectorContainer();
    recordsSampled = 0;
    IterOutcome upstream;
    builder = new SortRecordBatchBuilder(context.getAllocator(), MAX_SORT_BYTES, sampleContainer);
    builder.add(incoming);
    recordsSampled += incoming.getRecordCount();
    try {
      outer: while (recordsSampled < RECORDS_TO_SAMPLE) {
        upstream = incoming.next();
        switch(upstream) {
          case NONE:
          case NOT_YET:
          case STOP:
            upstreamNone = true;
            break outer;
        }
        builder.add(incoming);
        recordsSampled += incoming.getRecordCount();
        if (upstream == IterOutcome.NONE) break;
        //TODO handle upstream cases
      }
      builder.build(context);
      Sorter sorter = SortBatch.createNewSorter(context, popConfig.getOrderings(), sampleContainer);
      SelectionVector4 sv4 = builder.getSv4();
      sorter.setup(context, sv4, sampleContainer);
      sorter.sort(sv4, sampleContainer);

      VectorContainer containerToCache = new VectorContainer();
      SampleCopier copier = getCopier(sv4, sampleContainer, containerToCache, popConfig.getOrderings());
      copier.copyRecords(recordsSampled/(SAMPLING_FACTOR * partitions), 0, SAMPLING_FACTOR * partitions);

      for (VectorWrapper vw : containerToCache) {
        vw.getValueVector().getMutator().setValueCount(copier.getOutputRecords());
      }

//      BatchPrinter.printBatch(containerToCache);

      HazelCache cache = new HazelCache(DrillConfig.create());
      cache.run();
      String mapKey = String.format("%s_%d", context.getHandle().getQueryId(), context.getHandle().getMajorFragmentId());
      MultiMap<String, VectorWrap> mmap = cache.getMultiMap(SAMPLE_MAP_NAME);

      List<ValueVector> vectorList = Lists.newArrayList();
      for (VectorWrapper vw : containerToCache) {
        vectorList.add(vw.getValueVector());
      }

      VectorWrap wrap = new VectorWrap(vectorList);

      mmap.put(mapKey, wrap);

      AtomicNumber minorFragmentSampleCount = cache.getAtomicNumber(mapKey);

      long val = minorFragmentSampleCount.incrementAndGet();
      logger.debug("Incremented mfsc, got {}", val);

      for (int i = 0; i < 10 && minorFragmentSampleCount.get() < sendingMajorFragmentWidth * COMPLETION_FACTOR; i++) {
        Thread.sleep(10);
      }

      Collection<VectorWrap> allSamplesWrap = mmap.get(mapKey);
      VectorContainer allSamplesContainer = new VectorContainer();
      int orderSize = popConfig.getOrderings().size();
      SortContainerBuilder containerBuilder = new SortContainerBuilder(context.getAllocator(), MAX_SORT_BYTES, allSamplesContainer, orderSize);
      for (VectorWrap w : allSamplesWrap) {
        containerBuilder.add(w.get());
      }
      containerBuilder.build(context);

//      BatchPrinter.printHyperBatch(allSamplesContainer);

      List<OrderDef> orderDefs = Lists.newArrayList();
      int i = 0;
      for (OrderDef od : popConfig.getOrderings()) {
        SchemaPath sp = new SchemaPath("f" + i++, ExpressionPosition.UNKNOWN);
        orderDefs.add(new OrderDef(od.getDirection(), new FieldReference(sp)));
      }

      SelectionVector4 newSv4 = containerBuilder.getSv4();
      Sorter sorter2 = SortBatch.createNewSorter(context, orderDefs, allSamplesContainer);
      sorter2.setup(context, newSv4, allSamplesContainer);
      sorter2.sort(newSv4, allSamplesContainer);

      VectorContainer candidatePartitionTable = new VectorContainer();
      SampleCopier copier2 = getCopier(newSv4, allSamplesContainer, candidatePartitionTable, orderDefs);
      int skipRecords = containerBuilder.getSv4().getTotalCount() / partitions;
      copier2.copyRecords(skipRecords, skipRecords, partitions - 1);
      assert copier2.getOutputRecords() == partitions - 1 : String.format("output records: %d partitions: %d", copier2.getOutputRecords(), partitions);
      for (VectorWrapper vw : candidatePartitionTable) {
        vw.getValueVector().getMutator().setValueCount(copier2.getOutputRecords());
      }

//      BatchPrinter.printBatch(candidatePartitionTable);

      vectorList = Lists.newArrayList();
      for (VectorWrapper vw: candidatePartitionTable) {
        vectorList.add(vw.getValueVector());
      }
      wrap = new VectorWrap(vectorList);

      IMap<String, VectorWrap> tableMap = cache.getMap(TABLE_MAP_NAME);
      tableMap.putIfAbsent(mapKey + "final", wrap, 1, TimeUnit.MINUTES);
      wrap = tableMap.get(mapKey + "final");
      Preconditions.checkState(wrap != null);
      for (ValueVector vv : wrap.get()) {
        partitionVectors.add(vv);
      }
    } catch (ClassTransformationException | IOException | SchemaChangeException | InterruptedException ex) {
      kill();
      logger.error("Failure during query", ex);
      context.fail(ex);
      return false;
    }
    return true;
  }

  private SampleCopier getCopier(SelectionVector4 sv4, VectorContainer incoming, VectorContainer outgoing, List<OrderDef> orderings) throws SchemaChangeException{
    List<ValueVector> localAllocationVectors = Lists.newArrayList();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final CodeGenerator<SampleCopier> cg = new CodeGenerator<SampleCopier>(SampleCopier.TEMPLATE_DEFINITION, context.getFunctionRegistry());

    int i = 0;
    for(OrderDef od : orderings) {
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), incoming, collector);
      SchemaPath schemaPath = new SchemaPath("f" + i++, ExpressionPosition.UNKNOWN);
      TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder().mergeFrom(expr.getMajorType()).clearMode().setMode(TypeProtos.DataMode.REQUIRED);
      TypeProtos.MajorType newType = builder.build();
      MaterializedField outputField = MaterializedField.create(schemaPath, newType);
      if(collector.hasErrors()){
        throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
      }

      ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
      localAllocationVectors.add(vector);
      TypedFieldId fid = outgoing.add(vector);
      ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr);
      cg.addExpr(write);
      logger.debug("Added eval.");
    }
    for (ValueVector vv : localAllocationVectors) {
      AllocationHelper.allocate(vv, SAMPLING_FACTOR * partitions, 50);
    }
//    outgoing.addCollection(allocationVectors);
    outgoing.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    try {
      SampleCopier sampleCopier = context.getImplementationClass(cg);
      sampleCopier.setupCopier(context, sv4, incoming, outgoing);
      return sampleCopier;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException(e);
    }
  }

  @Override
  protected void killIncoming() {
    incoming.kill();
  }

  @Override
  public IterOutcome next() {
    if (upstreamNone && (batchQueue == null || batchQueue.size() == 0)) return IterOutcome.NONE;
    if (batchQueue != null && batchQueue.size() > 0) {
      VectorContainer vc = batchQueue.poll();
      recordCount = vc.getRecordCount();
      try{
        setupNewSchema(vc);
      }catch(SchemaChangeException ex){
        kill();
        logger.error("Failure during query", ex);
        context.fail(ex);
        return IterOutcome.STOP;
      }
      doWork(vc);
      return IterOutcome.OK_NEW_SCHEMA;
    }
    IterOutcome upstream = incoming.next();
    recordCount = incoming.getRecordCount();
    if(this.first && upstream == IterOutcome.OK) {
      upstream = IterOutcome.OK_NEW_SCHEMA;
    }
    if(this.first && upstream == IterOutcome.OK_NEW_SCHEMA) {
      if (!getPartitionVectors()) return IterOutcome.STOP;
      batchQueue = new LinkedBlockingQueue<>(builder.getContainers());
      first = false;
      VectorContainer vc = batchQueue.poll();
      try{
        setupNewSchema(vc);
      }catch(SchemaChangeException ex){
        kill();
        logger.error("Failure during query", ex);
        context.fail(ex);
        return IterOutcome.STOP;
      }
      doWork(vc);
      return IterOutcome.OK_NEW_SCHEMA;
    }
    if (this.startedUnsampledBatches == false) {
      this.startedUnsampledBatches = true;
      if (upstream == IterOutcome.OK) upstream = IterOutcome.OK_NEW_SCHEMA;
    }
    switch(upstream){
      case NONE:
      case NOT_YET:
      case STOP:
        container.zeroVectors();
        return upstream;
      case OK_NEW_SCHEMA:
        try{
          setupNewSchema(incoming);
        }catch(SchemaChangeException ex){
          kill();
          logger.error("Failure during query", ex);
          context.fail(ex);
          return IterOutcome.STOP;
        }
        // fall through.
      case OK:
        doWork(incoming);
        return upstream; // change if upstream changed, otherwise normal.
      default:
        throw new UnsupportedOperationException();
    }
  }
  
  @Override
  public int getRecordCount() {
    return recordCount;
  }

  protected void doWork(VectorAccessible batch) {
    int recordCount = batch.getRecordCount();
    for(ValueVector v : this.allocationVectors){
      AllocationHelper.allocate(v, recordCount, 50);
    }
    projector.projectRecords(recordCount, 0);
    for(VectorWrapper<?> v : container){
      ValueVector.Mutator m = v.getValueVector().getMutator();
      m.setValueCount(recordCount);
    }
  }

  /** hack to make ref and full work together... need to figure out if this is still necessary. **/
  private FieldReference getRef(NamedExpression e){
    FieldReference ref = e.getRef();
    PathSegment seg = ref.getRootSegment();
    if(seg.isNamed() && "output".contentEquals(seg.getNameSegment().getPath())){
      return new FieldReference(ref.getPath().toString().subSequence(7, ref.getPath().length()), ref.getPosition());
    }
    return ref;
  }
  
  protected void setupNewSchema(VectorAccessible batch) throws SchemaChangeException{
    this.allocationVectors = Lists.newArrayList();
    container.clear();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();
    
    final CodeGenerator<OrderedPartitionProjector> cg = new CodeGenerator<OrderedPartitionProjector>(OrderedPartitionProjector.TEMPLATE_DEFINITION, context.getFunctionRegistry());

    for (VectorWrapper vw : batch) {
      TransferPair tp = vw.getValueVector().getTransferPair();
      transfers.add(tp);
      container.add(tp.getTo());
    }

    cg.setMappingSet(MAIN_MAPPING);

    int count = 0;
    for(OrderDef od : popConfig.getOrderings()){
      // first, we rewrite the evaluation stack for each side of the comparison.
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector);
      if(collector.hasErrors()) throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      cg.setMappingSet(INCOMING_MAPPING);
      CodeGenerator.HoldingContainer left = cg.addExpr(expr, false);
      cg.setMappingSet(PARTITION_MAPPING);
      CodeGenerator.HoldingContainer right = cg.addExpr(new ValueVectorReadExpression(new TypedFieldId(expr.getMajorType(), count++)), false);
      cg.setMappingSet(MAIN_MAPPING);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      FunctionCall f = new FunctionCall(ComparatorFunctions.COMPARE_TO, ImmutableList.of((LogicalExpression) new HoldingContainerExpression(left), new HoldingContainerExpression(right)), ExpressionPosition.UNKNOWN);
      CodeGenerator.HoldingContainer out = cg.addExpr(f, false);
      JConditional jc = cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if(od.getDirection() == Order.Direction.ASC){
        jc._then()._return(out.getValue());
      }else{
        jc._then()._return(out.getValue().minus());
      }
    }

    cg.getEvalBlock()._return(JExpr.lit(0));

    SchemaPath outputPath = new SchemaPath(popConfig.getRef().getPath(), ExpressionPosition.UNKNOWN);
    MaterializedField outputField = MaterializedField.create(outputPath, Types.required(TypeProtos.MinorType.INT));
    ValueVector v = TypeHelper.getNewVector(outputField, context.getAllocator());
    container.add(v);
    this.allocationVectors.add(v);
    container.buildSchema(batch.getSchema().getSelectionVectorMode());
    
    try {
      this.projector = context.getImplementationClass(cg);
      projector.setup(context, batch, this, transfers, partitionVectors, partitions, outputPath);
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }
  
  
}
