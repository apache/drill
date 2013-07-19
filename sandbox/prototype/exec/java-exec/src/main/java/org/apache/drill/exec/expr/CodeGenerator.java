package org.apache.drill.exec.expr;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.holders.BooleanHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.LongHolder;
import org.apache.drill.exec.expr.holders.NullableBooleanHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableLongHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

import com.google.common.base.Preconditions;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
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
  


  public void addExpr(LogicalExpression ex){
    logger.debug("Adding next write {}", ex);
    rotateBlock();
    ex.accept(evaluationVisitor, this);
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
  }
  
  public JType getHolderType(MajorType t){
    switch(t.getMode()){
    case REQUIRED:
      switch(t.getMinorType()){
      case BOOLEAN:
        return model._ref(BooleanHolder.class);
      case INT:
        return model._ref(IntHolder.class);
      case BIGINT:  
        return model._ref(LongHolder.class);
      
      }
      
    case OPTIONAL:
      switch(t.getMinorType()){
      case BOOLEAN:
        return model._ref(NullableBooleanHolder.class);
      case INT:
        return model._ref(NullableIntHolder.class);
      case BIGINT:  
        return model._ref(NullableLongHolder.class);
      }

    }
    
    
    throw new UnsupportedOperationException();
  }
}
