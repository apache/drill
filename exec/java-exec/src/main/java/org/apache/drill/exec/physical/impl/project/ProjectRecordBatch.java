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
package org.apache.drill.exec.physical.impl.project;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
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
      AllocationHelper.allocate(v, recordCount, 250);
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

//    if(seg.isNamed() && "output".contentEquals(seg.getNameSegment().getPath())){
//      return new FieldReference(ref.getPath().toString().subSequence(7, ref.getPath().length()), ref.getPosition());
//    }
    return ref;
  }

  private boolean isAnyWildcard(List<NamedExpression> exprs){
    for(NamedExpression e : exprs){
      if(isWildcard(e)) return true;
    }
    return false;
  }

  private boolean isWildcard(NamedExpression ex){
    if( !(ex.getExpr() instanceof SchemaPath)) return false;
    NameSegment expr = ((SchemaPath)ex.getExpr()).getRootSegment();
    NameSegment ref = ex.getRef().getRootSegment();
    return ref.getPath().equals("*") && expr.getPath().equals("*");
  }

  @Override
  protected void setupNewSchema() throws SchemaChangeException{
    this.allocationVectors = Lists.newArrayList();
    container.clear();
    final List<NamedExpression> exprs = popConfig.getExprs();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();

    final ClassGenerator<Projector> cg = CodeGenerator.getRoot(Projector.TEMPLATE_DEFINITION, context.getFunctionRegistry());

    Set<Integer> transferFieldIds = new HashSet();

    boolean isAnyWildcard = isAnyWildcard(exprs);

    if(isAnyWildcard){
      for(VectorWrapper<?> wrapper : incoming){
        ValueVector vvIn = wrapper.getValueVector();
        String name = vvIn.getField().getDef().getName(vvIn.getField().getDef().getNameCount() - 1).getName();
        FieldReference ref = new FieldReference(name);
        TransferPair tp = wrapper.getValueVector().getTransferPair(ref);
        transfers.add(tp);
        container.add(tp.getTo());
      }
    }else{
      for(int i = 0; i < exprs.size(); i++){
        final NamedExpression namedExpression = exprs.get(i);
        final LogicalExpression expr = ExpressionTreeMaterializer.materialize(namedExpression.getExpr(), incoming, collector, context.getFunctionRegistry());
        final MaterializedField outputField = MaterializedField.create(getRef(namedExpression), expr.getMajorType());
        if(collector.hasErrors()){
          throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
        }


        // add value vector to transfer if direct reference and this is allowed, otherwise, add to evaluation stack.
        if(expr instanceof ValueVectorReadExpression && incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE
                && !isAnyWildcard
                &&!transferFieldIds.contains(((ValueVectorReadExpression) expr).getFieldId().getFieldId())
                && !((ValueVectorReadExpression) expr).isArrayElement()) {
          ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
          ValueVector vvIn = incoming.getValueAccessorById(vectorRead.getFieldId().getFieldId(), TypeHelper.getValueVectorClass(vectorRead.getMajorType().getMinorType(), vectorRead.getMajorType().getMode())).getValueVector();
          Preconditions.checkNotNull(incoming);

          TransferPair tp = vvIn.getTransferPair(getRef(namedExpression));
          transfers.add(tp);
          container.add(tp.getTo());
          transferFieldIds.add(vectorRead.getFieldId().getFieldId());
//          logger.debug("Added transfer.");
        }else{
          // need to do evaluation.
          ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
          allocationVectors.add(vector);
          TypedFieldId fid = container.add(vector);
          ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr);
          cg.addExpr(write);
//          logger.debug("Added eval.");
        }
    }


    }

    container.buildSchema(incoming.getSchema().getSelectionVectorMode());

    try {
      this.projector = context.getImplementationClass(cg.getCodeGenerator());
      projector.setup(context, incoming, this, transfers);
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }


}
