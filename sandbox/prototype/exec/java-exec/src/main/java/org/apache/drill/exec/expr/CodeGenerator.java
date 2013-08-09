package org.apache.drill.exec.expr;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.partitionsender.OutgoingRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.TypeHelper;

import com.google.common.base.Preconditions;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldRef;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

public class CodeGenerator<T> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CodeGenerator.class);
  
  public JDefinedClass clazz;
  private JBlock parentEvalBlock;
  private JBlock parentSetupBlock;
  private JBlock currentEvalBlock;
  private JBlock currentSetupBlock;
  private final EvaluationVisitor evaluationVisitor;
  private final TemplateClassDefinition<T> definition;
  private JCodeModel model;
  private int index = 0;
  private static AtomicLong classCreator = new AtomicLong(0);
  private String className;
  private String fqcn;
  private String packageName = "org.apache.drill.exec.test.generated";
  
  public CodeGenerator(TemplateClassDefinition<T> definition, FunctionImplementationRegistry funcRegistry) {
    super();
    className = "Gen" + classCreator.incrementAndGet();
    fqcn = packageName + "." + className;
    try{
      this.definition = definition;
      this.model = new JCodeModel();
      this.clazz = model._package(packageName)._class(className);
      clazz._implements(definition.getInternalInterface());
      this.parentEvalBlock = new JBlock();
      this.parentSetupBlock = new JBlock();
      this.evaluationVisitor = new EvaluationVisitor(funcRegistry);
    } catch (JClassAlreadyExistsException e) {
      throw new IllegalStateException(e);
    }
  }
  

  public JVar declareVectorValueSetupAndMember(String batchName, TypedFieldId fieldId){
    Class<?> valueVectorClass = TypeHelper.getValueVectorClass(fieldId.getType().getMinorType(), fieldId.getType().getMode());
    JClass vvClass = model.ref(valueVectorClass);
    JClass retClass = vvClass;
    String vectorAccess = "getValueVector";
    if(fieldId.isHyperReader()){
      retClass = retClass.array();
      vectorAccess = "getValueVectors";
    }
    
    JVar vv = declareClassField("vv", retClass);
    JClass t = model.ref(SchemaChangeException.class);
    JType wrapperClass = model.ref(VectorWrapper.class);
    JType objClass = model.ref(Object.class);
    JBlock b = getSetupBlock();
    JVar obj = b.decl( //
        objClass, //
        getNextVar("tmp"), // 
        JExpr.direct(batchName)
          .invoke("getValueAccessorById") //
          .arg(JExpr.lit(fieldId.getFieldId())) //
          .arg( vvClass.dotclass())
          .invoke(vectorAccess)//
          );
        
        b._if(obj.eq(JExpr._null()))._then()._throw(JExpr._new(t).arg(JExpr.lit(String.format("Failure while loading vector %s with id: %s.", vv.name(), fieldId.toString()))));
        //b.assign(vv, JExpr.cast(retClass, ((JExpression) JExpr.cast(wrapperClass, obj) ).invoke(vectorAccess)));
        b.assign(vv, JExpr.cast(retClass, obj ));
        
    return vv;
  }

  public HoldingContainer addExpr(LogicalExpression ex){
    logger.debug("Adding next write {}", ex);
    rotateBlock();
    return ex.accept(evaluationVisitor, this);
  }
  
  public void rotateBlock(){
    currentEvalBlock = new JBlock();
    parentEvalBlock.add(currentEvalBlock);
    currentSetupBlock = new JBlock();
    parentSetupBlock.add(currentSetupBlock);

  }
  
  public JBlock getBlock() {
    return currentEvalBlock;
  }

  public String getMaterializedClassName(){
    return fqcn;
  }
  
  public JBlock getSetupBlock(){
    return currentSetupBlock;
  }
  
  
  public TemplateClassDefinition<T> getDefinition() {
    return definition;
  }

  public String generate() throws IOException{

    {
      //setup method
      JMethod m = clazz.method(JMod.PUBLIC, model.VOID, "doSetup");
      m.param(model._ref(FragmentContext.class), "context");
      m.param(model._ref(RecordBatch.class), "incoming");
      m.param(model._ref(RecordBatch.class), "outgoing");
      m._throws(SchemaChangeException.class);
      m.body().add(parentSetupBlock);
    }
    
    {
      // eval method.
      JType ret = definition.getEvalReturnType() == null ? model.VOID : model._ref(definition.getEvalReturnType());
      JMethod m = clazz.method(JMod.PUBLIC, ret, "doEval");  
      m.param(model.INT, "inIndex");
      m.param(model.INT, "outIndex");
      m.body().add(parentEvalBlock);
    }
    
    SingleClassStringWriter w = new SingleClassStringWriter();
    model.build(w);
    return w.getCode().toString();
  }

  public String generateMultipleOutputs() throws IOException{

    {
      //setup method
      JMethod m = clazz.method(JMod.PUBLIC, model.VOID, "doSetup");
      m.param(model._ref(FragmentContext.class), "context");
      m.param(model._ref(RecordBatch.class), "incoming");
      m.param(model._ref(OutgoingRecordBatch.class).array(), "outgoing");
      m._throws(SchemaChangeException.class);
      m.body().add(parentSetupBlock);
    }

    {
      // eval method.
      JType ret = definition.getEvalReturnType() == null ? model.VOID : model._ref(definition.getEvalReturnType());
      JMethod m = clazz.method(JMod.PUBLIC, ret, "doEval");
      m.param(model.INT, "inIndex");
      m.param(model.INT, "outIndex");
      m.body().add(parentEvalBlock);
    }

    SingleClassStringWriter w = new SingleClassStringWriter();
    model.build(w);
    return w.getCode().toString();
  }
  
  public JCodeModel getModel() {
    return model;
  }

  public String getNextVar() {
    return "v" + index++;
  }
  
  public String getNextVar(String prefix){
    return prefix + index++;
  }
  
  public JVar declareClassField(String prefix, JType t){
    return clazz.field(JMod.NONE, t, prefix + index++);
  }
  
  public HoldingContainer declare(MajorType t){
    return declare(t, true);
  }
  
  public HoldingContainer declare(MajorType t, boolean includeNewInstance){
    JType holderType = getHolderType(t);
    JVar var;
    if(includeNewInstance){
      var = currentEvalBlock.decl(holderType, "out" + index, JExpr._new(holderType));
    }else{
      var = currentEvalBlock.decl(holderType, "out" + index);
    }
    JFieldRef outputSet = null;
    if(t.getMode() == DataMode.OPTIONAL){
      outputSet = var.ref("isSet");  
    }
    index++;
    return new HoldingContainer(t, var, var.ref("value"), outputSet);
  }
  
  
  public static class HoldingContainer{
    private final JVar holder;
    private final JFieldRef value;
    private final JFieldRef isSet;
    private final MajorType type;
    
    public HoldingContainer(MajorType t, JVar holder, JFieldRef value, JFieldRef isSet) {
      super();
      this.holder = holder;
      this.value = value;
      this.isSet = isSet;
      this.type = t;
    }

    public JVar getHolder() {
      return holder;
    }

    public JFieldRef getValue() {
      return value;
    }

    public JFieldRef getIsSet() {
      Preconditions.checkNotNull(isSet, "You cannot access the isSet variable when operating on a non-nullable output value.");
      return isSet;
    }
    
    public boolean isOptional(){
      return type.getMode() == DataMode.OPTIONAL;
    }
    
    public boolean isRepeated(){
      return type.getMode() == DataMode.REPEATED;
    }

    public TypeProtos.MinorType getMinorType() {
      return type.getMinorType();
    }
  }
  
  public JType getHolderType(MajorType t){
    return TypeHelper.getHolderType(model, t.getMinorType(), t.getMode());
  }
}
