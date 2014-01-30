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
package org.apache.drill.exec.physical.impl.svremover;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.allocator.FixedVectorAllocator;
import org.apache.drill.exec.vector.allocator.VariableEstimatedVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;

public class RemovingRecordBatch extends AbstractSingleRecordBatch<SelectionVectorRemover>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemovingRecordBatch.class);

  private Copier copier;
  private int recordCount;
  
  public RemovingRecordBatch(SelectionVectorRemover popConfig, FragmentContext context, RecordBatch incoming) {
    super(popConfig, context, incoming);
    logger.debug("Created.");
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  protected void setupNewSchema() throws SchemaChangeException {
    container.clear();

    switch(incoming.getSchema().getSelectionVectorMode()){
    case NONE:
      this.copier = getStraightCopier();
      break;
    case TWO_BYTE:
      this.copier = getGenerated2Copier();
      break;
    case FOUR_BYTE:
      this.copier = getGenerated4Copier();
      break;
    default:
      throw new UnsupportedOperationException();
    }
    
    container.buildSchema(SelectionVectorMode.NONE);

  }

  @Override
  protected void doWork() {
    recordCount = incoming.getRecordCount();
    copier.copyRecords();
    for(VectorWrapper<?> v : container){
      ValueVector.Mutator m = v.getValueVector().getMutator();
      m.setValueCount(recordCount);
    }
  }

  public void cleanup(){
    super.cleanup();
  }
  
  private class StraightCopier implements Copier{

    private List<TransferPair> pairs = Lists.newArrayList();
    private List<ValueVector> out = Lists.newArrayList();
    
    @Override
    public void setupRemover(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, VectorAllocator[] allocators){
      for(VectorWrapper<?> vv : incoming){
        TransferPair tp = vv.getValueVector().getTransferPair();
        pairs.add(tp);
        out.add(tp.getTo());
      }
    }

    @Override
    public void copyRecords() {
      for(TransferPair tp : pairs){
        tp.transfer();
      }
    }

    public List<ValueVector> getOut() {
      return out;
    }
    
  }

  private Copier getStraightCopier(){
    StraightCopier copier = new StraightCopier();
    copier.setupRemover(context, incoming, this, null);
    container.addCollection(copier.getOut());
    return copier;
  }
  
  private Copier getGenerated2Copier() throws SchemaChangeException{
    Preconditions.checkArgument(incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE);
    
    List<VectorAllocator> allocators = Lists.newArrayList();
    for(VectorWrapper<?> i : incoming){
      ValueVector v = TypeHelper.getNewVector(i.getField(), context.getAllocator());
      container.add(v);
      allocators.add(VectorAllocator.getAllocator(i.getValueVector(), v));
    }

    try {
      final CodeGenerator<Copier> cg = CodeGenerator.get(Copier.TEMPLATE_DEFINITION2, context.getFunctionRegistry());
      generateCopies(cg.getRoot(), false);
      Copier copier = context.getImplementationClass(cg);
      copier.setupRemover(context, incoming, this, allocators.toArray(new VectorAllocator[allocators.size()]));
      return copier;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }
  
  private Copier getGenerated4Copier() throws SchemaChangeException{
    Preconditions.checkArgument(incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.FOUR_BYTE);
    
    List<VectorAllocator> allocators = Lists.newArrayList();
    for(VectorWrapper<?> i : incoming){
      
      ValueVector v = TypeHelper.getNewVector(i.getField(), context.getAllocator());
      container.add(v);
      allocators.add(getAllocator4(v));
    }

    try {
      final CodeGenerator<Copier> cg = CodeGenerator.get(Copier.TEMPLATE_DEFINITION4, context.getFunctionRegistry());
      generateCopies(cg.getRoot(), true);
      Copier copier = context.getImplementationClass(cg);
      copier.setupRemover(context, incoming, this, allocators.toArray(new VectorAllocator[allocators.size()]));
      return copier;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }
  
  private void generateCopies(ClassGenerator<Copier> g, boolean hyper){
    // we have parallel ids for each value vector so we don't actually have to deal with managing the ids at all.
    int fieldId = 0;
    
    JExpression inIndex = JExpr.direct("inIndex");
    JExpression outIndex = JExpr.direct("outIndex");
    g.rotateBlock();
    for(VectorWrapper<?> vv : incoming){
      JVar inVV = g.declareVectorValueSetupAndMember("incoming", new TypedFieldId(vv.getField().getType(), fieldId, vv.isHyper()));
      JVar outVV = g.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(vv.getField().getType(), fieldId, false));

      if(hyper){
        
        g.getEvalBlock().add( 
            outVV
            .invoke("copyFrom")
            .arg(
                inIndex.band(JExpr.lit((int) Character.MAX_VALUE)))
            .arg(outIndex)
            .arg(
                inVV.component(inIndex.shrz(JExpr.lit(16)))
                )
            );  
      }else{
        g.getEvalBlock().add(outVV.invoke("copyFrom").arg(inIndex).arg(outIndex).arg(inVV));
      }
      
      
      fieldId++;
    }
  }
  

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }
  
  private VectorAllocator getAllocator4(ValueVector outgoing){
    if(outgoing instanceof FixedWidthVector){
      return new FixedVectorAllocator((FixedWidthVector) outgoing);
    }else if(outgoing instanceof VariableWidthVector ){
      return new VariableEstimatedVector( (VariableWidthVector) outgoing, 250);
    }else{
      throw new UnsupportedOperationException();
    }
  }
  
  
}
