package org.apache.drill.exec.physical.impl.project;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
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
import org.apache.drill.exec.proto.SchemaDefProtos.FieldDef;
import org.apache.drill.exec.proto.SchemaDefProtos.NamePart;
import org.apache.drill.exec.proto.SchemaDefProtos.NamePart.Type;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SchemaBuilder;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.vector.SelectionVector2;
import org.apache.drill.exec.record.vector.SelectionVector4;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ProjectRecordBatch implements RecordBatch{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectRecordBatch.class);

  private final Project pop;
  private final RecordBatch incoming;
  private final FragmentContext context;
  private BatchSchema outSchema;
  private Projector projector;
  private List<ValueVector<?>> allocationVectors;
  private List<ValueVector<?>> outputVectors;
  
  
  public ProjectRecordBatch(Project pop, RecordBatch incoming, FragmentContext context){
    this.pop = pop;
    this.incoming = incoming;
    this.context = context;
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
  public TypedFieldId getValueVector(SchemaPath path) {
    return null;
  }

  @Override
  public <T extends ValueVector<T>> T getValueVectorById(int fieldId, Class<?> vvClass) {
    return null;
  }

  @Override
  public IterOutcome next() {
    
    IterOutcome upstream = incoming.next();
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
        context.fail(ex);
        return IterOutcome.STOP;
      }
      // fall through.
    case OK:
      int recordCount = incoming.getRecordCount();
      for(ValueVector<?> v : this.allocationVectors){
        v.allocateNew(recordCount);
      }
      projector.projectRecords(recordCount, 0);
      return upstream; // change if upstream changed, otherwise normal.
    default:
      throw new UnsupportedOperationException();
    }
  }
  

  private Projector createNewProjector() throws SchemaChangeException{
    this.allocationVectors = Lists.newArrayList();
    if(outputVectors != null){
      for(ValueVector<?> v : outputVectors){
        v.close();
      }
    }
    this.outputVectors = Lists.newArrayList();
    
    final List<NamedExpression> exprs = pop.getExprs();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPairing<?>> transfers = Lists.newArrayList();
    
    final CodeGenerator cg = new CodeGenerator("setupEvaluators", "doPerRecordWork", context.getFunctionRegistry());
    
    for(int i =0; i < exprs.size(); i++){
      final NamedExpression namedExpression = exprs.get(i);
      final MaterializedField outputField = getMaterializedField(namedExpression);
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(namedExpression.getExpr(), incoming, collector);
      if(collector.hasErrors()){
        throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
      }
      
      
      
      
      // add value vector to transfer if direct reference and this is allowed, otherwise, add to evaluation stack.
      if(expr instanceof ValueVectorReadExpression && incoming.getSchema().getSelectionVector() == SelectionVectorMode.NONE){
        ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
        ValueVector<?> vvIn = incoming.getValueVectorById(vectorRead.getFieldId(), TypeHelper.getValueVectorClass(vectorRead.getMajorType()));
        Preconditions.checkNotNull(incoming);

        TransferPairing<?> tp = vvIn.getTransferPair(outputField);
        transfers.add(tp);
        outputVectors.add(tp.getTo());
      }else{
        // need to do evaluation.
        ValueVector<?> vector = TypeHelper.getNewVector(outputField, context.getAllocator());
        allocationVectors.add(vector);
        outputVectors.add(vector);
        ValueVectorWriteExpression write = new ValueVectorWriteExpression(outputVectors.size() - 1, expr);
        cg.addNextWrite(write);
      }
      
    }
    
    SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(SelectionVectorMode.NONE);
    for(ValueVector<?> v : outputVectors){
      bldr.addField(v.getField());
    }
    this.outSchema = bldr.build();
    
    try {
      return context.getImplementationClass(Projector.TEMPLATE_DEFINITION, cg);
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }
  
  
  @Override
  public WritableBatch getWritableBatch() {
    return null;
  }
  
  
  private MaterializedField getMaterializedField(NamedExpression ex){
    return new MaterializedField(getFieldDef(ex.getRef(), ex.getExpr().getMajorType()));
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
    }
    return parts;
  }
}
