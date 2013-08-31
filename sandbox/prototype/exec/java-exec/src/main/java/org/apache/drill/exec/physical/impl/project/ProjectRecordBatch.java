package org.apache.drill.exec.physical.impl.project;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ProjectRecordBatch extends AbstractSingleRecordBatch<Project>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectRecordBatch.class);

  private Projector projector;
  private List<ValueVector> allocationVectors;
  
  public ProjectRecordBatch(Project pop, RecordBatch incoming, FragmentContext context){
    super(pop, context, incoming);
  }
  
  @Override
  public int getRecordCount() {
    return incoming.getRecordCount();
  }

  @Override
  protected void doWork() {
    int recordCount = incoming.getRecordCount();
    for(ValueVector v : this.allocationVectors){
      AllocationHelper.allocate(v, recordCount, 50);
    }
    projector.projectRecords(recordCount, 0);
    for(VectorWrapper<?> v : container){
      ValueVector.Mutator m = v.getValueVector().getMutator();
      m.setValueCount(recordCount);
    }
  }

  @Override
  protected void setupNewSchema() throws SchemaChangeException{
    this.allocationVectors = Lists.newArrayList();
    container.clear();
    final List<NamedExpression> exprs = popConfig.getExprs();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();
    
    final CodeGenerator<Projector> cg = new CodeGenerator<Projector>(Projector.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    
    for(int i =0; i < exprs.size(); i++){
      final NamedExpression namedExpression = exprs.get(i);
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(namedExpression.getExpr(), incoming, collector);
      final MaterializedField outputField = MaterializedField.create(namedExpression.getRef(), expr.getMajorType());
      if(collector.hasErrors()){
        throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
      }
      
      // add value vector to transfer if direct reference and this is allowed, otherwise, add to evaluation stack.
      if(expr instanceof ValueVectorReadExpression && incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE){
        ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
        ValueVector vvIn = incoming.getValueAccessorById(vectorRead.getFieldId().getFieldId(), TypeHelper.getValueVectorClass(vectorRead.getMajorType().getMinorType(), vectorRead.getMajorType().getMode())).getValueVector();
        Preconditions.checkNotNull(incoming);

        TransferPair tp = vvIn.getTransferPair(namedExpression.getRef());
        transfers.add(tp);
        container.add(tp.getTo());
        logger.debug("Added transfer.");
      }else{
        // need to do evaluation.
        ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
        allocationVectors.add(vector);
        TypedFieldId fid = container.add(vector);
        ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr);
        cg.addExpr(write);
        logger.debug("Added eval.");
      }
      
    }
    
    container.buildSchema(incoming.getSchema().getSelectionVectorMode());
    
    try {
      this.projector = context.getImplementationClass(cg);
      projector.setup(context, incoming, this, transfers);
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }
  
  
}
