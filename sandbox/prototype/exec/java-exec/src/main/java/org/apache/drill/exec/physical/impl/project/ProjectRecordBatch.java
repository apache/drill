package org.apache.drill.exec.physical.impl.project;

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
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.physical.impl.VectorHolder;
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

public class ProjectRecordBatch implements RecordBatch{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectRecordBatch.class);

  private final Project pop;
  private final RecordBatch incoming;
  private final FragmentContext context;
  private BatchSchema outSchema;
  private Projector projector;
  private List<ValueVector> allocationVectors;
  private List<ValueVector> outputVectors;
  private VectorHolder vh;
  
  
  public ProjectRecordBatch(Project pop, RecordBatch incoming, FragmentContext context){
    this.pop = pop;
    this.incoming = incoming;
    this.context = context;
  }
  
  @Override
  public Iterator<ValueVector> iterator() {
    return outputVectors.iterator();
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
    return incoming.getRecordCount();
  }

  @Override
  public void kill() {
    incoming.kill();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
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
        projector = createNewProjector();
      }catch(SchemaChangeException ex){
        incoming.kill();
        logger.error("Failure during query", ex);
        context.fail(ex);
        return IterOutcome.STOP;
      }
      // fall through.
    case OK:
      int recordCount = incoming.getRecordCount();
      for(ValueVector v : this.allocationVectors){
        AllocationHelper.allocate(v, recordCount, 50);
      }
      projector.projectRecords(recordCount, 0);
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
  

  private Projector createNewProjector() throws SchemaChangeException{
    this.allocationVectors = Lists.newArrayList();
    if(outputVectors != null){
      for(ValueVector v : outputVectors){
        v.close();
      }
    }
    this.outputVectors = Lists.newArrayList();
    this.vh = new VectorHolder(outputVectors);
    final List<NamedExpression> exprs = pop.getExprs();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();
    
    final CodeGenerator<Projector> cg = new CodeGenerator<Projector>(Projector.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    
    for(int i =0; i < exprs.size(); i++){
      final NamedExpression namedExpression = exprs.get(i);
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(namedExpression.getExpr(), incoming, collector);
      final MaterializedField outputField = getMaterializedField(namedExpression.getRef(), expr);
      if(collector.hasErrors()){
        throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
      }
      
      // add value vector to transfer if direct reference and this is allowed, otherwise, add to evaluation stack.
      if(expr instanceof ValueVectorReadExpression && incoming.getSchema().getSelectionVector() == SelectionVectorMode.NONE){
        ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
        ValueVector vvIn = incoming.getValueVectorById(vectorRead.getFieldId(), TypeHelper.getValueVectorClass(vectorRead.getMajorType().getMinorType(), vectorRead.getMajorType().getMode()));
        Preconditions.checkNotNull(incoming);

        TransferPair tp = vvIn.getTransferPair();
        transfers.add(tp);
        outputVectors.add(tp.getTo());
      }else{
        // need to do evaluation.
        ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
        allocationVectors.add(vector);
        outputVectors.add(vector);
        ValueVectorWriteExpression write = new ValueVectorWriteExpression(outputVectors.size() - 1, expr);
        cg.addExpr(write);
      }
      
    }
    
    SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(incoming.getSchema().getSelectionVector());
    for(ValueVector v : outputVectors){
      bldr.addField(v.getField());
    }
    this.outSchema = bldr.build();
    
    try {
      Projector projector = context.getImplementationClass(cg);
      projector.setup(context, incoming, this, transfers);
      return projector;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }
  
  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }
  
  private MaterializedField getMaterializedField(FieldReference reference, LogicalExpression expr){
    return new MaterializedField(getFieldDef(reference, expr.getMajorType()));
  }

  private FieldDef getFieldDef(SchemaPath path, MajorType type){
    return FieldDef //
        .newBuilder() //
        .addAllName(getNameParts(path.getRootSegment())) //
        .setMajorType(type) //
        .build();
  }
  
  private List<NamePart> getNameParts(PathSegment seg){
    List<NamePart> parts = Lists.newArrayList();
    while(seg != null){
      if(seg.isArray()){
        parts.add(NamePart.newBuilder().setType(Type.ARRAY).build());
      }else{
        parts.add(NamePart.newBuilder().setType(Type.NAME).setName(seg.getNameSegment().getPath().toString()).build());
      }
      seg = seg.getChild();
    }
    return parts;
  }
  
}
