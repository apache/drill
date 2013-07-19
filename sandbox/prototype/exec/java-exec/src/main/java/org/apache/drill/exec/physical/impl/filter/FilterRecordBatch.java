package org.apache.drill.exec.physical.impl.filter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.physical.impl.project.Projector;
import org.apache.drill.exec.proto.SchemaDefProtos.FieldDef;
import org.apache.drill.exec.proto.SchemaDefProtos.NamePart;
import org.apache.drill.exec.proto.SchemaDefProtos.NamePart.Type;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SchemaBuilder;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.NonRepeatedMutator;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class FilterRecordBatch implements RecordBatch{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterRecordBatch.class);

  private final Filter filterConfig;
  private final RecordBatch incoming;
  private final FragmentContext context;
  private final SelectionVector2 sv;
  private BatchSchema outSchema;
  private Filterer filter;
  private List<ValueVector> outputVectors;
  private VectorHolder vh;
  
  public FilterRecordBatch(Filter pop, RecordBatch incoming, FragmentContext context){
    this.filterConfig = pop;
    this.incoming = incoming;
    this.context = context;
    sv = new SelectionVector2(context.getAllocator());
  }
  
  
  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    Preconditions.checkNotNull(outSchema);
    return outSchema;
  }

  @Override
  public int getRecordCount() {
    return sv.getCount();
  }

  @Override
  public void kill() {
    incoming.kill();
  }

  
  @Override
  public Iterator<ValueVector> iterator() {
    return outputVectors.iterator();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return sv;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return vh.getValueVector(path);
  }

  @Override
  public <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> clazz) {
    return vh.getValueVector(fieldId, clazz);
  }

  @Override
  public IterOutcome next() {
    
    IterOutcome upstream = incoming.next();
    logger.debug("Upstream... {}", upstream);
    switch(upstream){
    case NONE:
    case NOT_YET:
    case STOP:
      return upstream;
    case OK_NEW_SCHEMA:
      try{
        filter = createNewFilterer();
      }catch(SchemaChangeException ex){
        incoming.kill();
        logger.error("Failure during query", ex);
        context.fail(ex);
        return IterOutcome.STOP;
      }
      // fall through.
    case OK:
      int recordCount = incoming.getRecordCount();
      sv.allocateNew(recordCount);
      filter.filterBatch(recordCount);
      for(ValueVector v : this.outputVectors){
        ValueVector.Mutator m = v.getMutator();
        if(m instanceof NonRepeatedMutator){
          ((NonRepeatedMutator) m).setValueCount(recordCount);
        }else{
          throw new UnsupportedOperationException();
        }
      }
      return upstream; // change if upstream changed, otherwise normal.
    default:
      throw new UnsupportedOperationException();
    }
  }
  

  private Filterer createNewFilterer() throws SchemaChangeException{
    if(outputVectors != null){
      for(ValueVector v : outputVectors){
        v.close();
      }
    }
    this.outputVectors = Lists.newArrayList();
    this.vh = new VectorHolder(outputVectors);
    LogicalExpression filterExpression = filterConfig.getExpr();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();
    final CodeGenerator<Filterer> cg = new CodeGenerator<Filterer>(Filterer.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    
    final LogicalExpression expr = ExpressionTreeMaterializer.materialize(filterExpression, incoming, collector);
    if(collector.hasErrors()){
      throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
    }
    
    cg.addExpr(new ReturnValueExpression(expr));
    
    for(ValueVector v : incoming){
      TransferPair pair = v.getTransferPair();
      outputVectors.add(pair.getTo());
      transfers.add(pair);
    }
    
    SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(SelectionVectorMode.TWO_BYTE);
    for(ValueVector v : outputVectors){
      bldr.addField(v.getField());
    }
    this.outSchema = bldr.build();
    
    try {
      TransferPair[] tx = transfers.toArray(new TransferPair[transfers.size()]);
      Filterer filterer = context.getImplementationClass(cg);
      filterer.setup(context, incoming, this, tx);
      return filterer;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }
  
  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }
  
  
}
