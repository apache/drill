package org.apache.drill.exec.physical.impl.svremover;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SchemaBuilder;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.NonRepeatedMutator;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

public class RemovingRecordBatch implements RecordBatch{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemovingRecordBatch.class);

  private final RecordBatch incoming;
  private final FragmentContext context;
  private BatchSchema outSchema;
  private Copier copier;
  private List<ValueVector> outputVectors;
  private VectorHolder vh;
  
  
  public RemovingRecordBatch(RecordBatch incoming, FragmentContext context){
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
        copier = createCopier();
      }catch(SchemaChangeException ex){
        incoming.kill();
        logger.error("Failure during query", ex);
        context.fail(ex);
        return IterOutcome.STOP;
      }
      // fall through.
    case OK:
      int recordCount = incoming.getRecordCount();
      copier.copyRecords();
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
  
  

  
  private class StraightCopier implements Copier{

    private List<TransferPair> pairs = Lists.newArrayList();
    private List<ValueVector> out = Lists.newArrayList();
    
    @Override
    public void setupRemover(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, VectorAllocator[] allocators){
      for(ValueVector vv : incoming){
        TransferPair tp = vv.getTransferPair();
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
    outputVectors.addAll(copier.getOut());
    return copier;
  }
  
  private Copier getGeneratedCopier() throws SchemaChangeException{
    Preconditions.checkArgument(incoming.getSchema().getSelectionVector() == SelectionVectorMode.TWO_BYTE);
    
    List<VectorAllocator> allocators = Lists.newArrayList();
    for(ValueVector i : incoming){
      TransferPair t = i.getTransferPair();
      outputVectors.add(t.getTo());
      allocators.add(getAllocator(i, t.getTo()));
    }

    try {
      final CodeGenerator<Copier> cg = new CodeGenerator<Copier>(Copier.TEMPLATE_DEFINITION, context.getFunctionRegistry());
      generateCopies(cg);
      Copier copier = context.getImplementationClass(cg);
      copier.setupRemover(context, incoming, this, allocators.toArray(new VectorAllocator[allocators.size()]));
      return copier;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }
  
  
  private Copier createCopier() throws SchemaChangeException{
    if(outputVectors != null){
      for(ValueVector v : outputVectors){
        v.close();
      }
    }
    this.outputVectors = Lists.newArrayList();
    this.vh = new VectorHolder(outputVectors);

    SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(SelectionVectorMode.NONE);
    for(ValueVector v : outputVectors){
      bldr.addField(v.getField());
    }
    this.outSchema = bldr.build();
    
    switch(incoming.getSchema().getSelectionVector()){
    case NONE:
      return getStraightCopier();
    case TWO_BYTE:
      return getGeneratedCopier();
    default:
      throw new UnsupportedOperationException();
    }

  }
  
  private void generateCopies(CodeGenerator<Copier> g){
    // we have parallel ids for each value vector so we don't actually have to deal with managing the ids at all.
    int fieldId = 0;
    


    JExpression inIndex = JExpr.direct("inIndex");
    JExpression outIndex = JExpr.direct("outIndex");
    g.rotateBlock();
    for(ValueVector vv : incoming){
      JClass vvClass = (JClass) g.getModel()._ref(vv.getClass());
      JVar inVV = declareVVSetup("incoming", g, fieldId, vvClass);
      JVar outVV = declareVVSetup("outgoing", g, fieldId, vvClass);
      
      g.getBlock().add(inVV.invoke("copyValue").arg(inIndex).arg(outIndex).arg(outVV));
      
      fieldId++;
    }
  }
  
  private JVar declareVVSetup(String varName, CodeGenerator<?> g, int fieldId, JClass vvClass){
    JVar vv = g.declareClassField("vv", vvClass);
    JClass t = (JClass) g.getModel()._ref(SchemaChangeException.class);
    JType objClass = g.getModel()._ref(Object.class);
    JBlock b = g.getSetupBlock();
    JVar obj = b.decl( //
        objClass, //
        g.getNextVar("tmp"), // 
        JExpr.direct(varName).invoke("getValueVectorById").arg(JExpr.lit(fieldId)).arg( vvClass.dotclass()));
        b._if(obj.eq(JExpr._null()))._then()._throw(JExpr._new(t).arg(JExpr.lit(String.format("Failure while loading vector %s with id %d", vv.name(), fieldId))));
        b.assign(vv, JExpr.cast(vvClass, obj));
        
    return vv;
  }
  
  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }
  
  private VectorAllocator getAllocator(ValueVector in, ValueVector outgoing){
    if(outgoing instanceof FixedWidthVector){
      return new FixedVectorAllocator((FixedWidthVector) outgoing);
    }else if(outgoing instanceof VariableWidthVector && in instanceof VariableWidthVector){
      return new VariableVectorAllocator( (VariableWidthVector) in, (VariableWidthVector) outgoing);
    }else{
      throw new UnsupportedOperationException();
    }
  }
  
  private class FixedVectorAllocator implements VectorAllocator{
    FixedWidthVector out;
    
    public FixedVectorAllocator(FixedWidthVector out) {
      super();
      this.out = out;
    }

    public void alloc(int recordCount){
      out.allocateNew(recordCount);
      out.getMutator().setValueCount(recordCount);
    }

    
    
  }
  
  private class VariableVectorAllocator implements VectorAllocator{
    VariableWidthVector in;
    VariableWidthVector out;
    
    public VariableVectorAllocator(VariableWidthVector in, VariableWidthVector out) {
      super();
      this.in = in;
      this.out = out;
    }

    public void alloc(int recordCount){
      out.allocateNew(in.getByteCapacity(), recordCount);
      out.getMutator().setValueCount(recordCount);
    }
  }
  
  public interface VectorAllocator{
    public void alloc(int recordCount);
  }
}
