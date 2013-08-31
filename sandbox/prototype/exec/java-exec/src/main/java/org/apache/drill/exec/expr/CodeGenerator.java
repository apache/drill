package org.apache.drill.exec.expr;

import static org.apache.drill.exec.compile.sig.GeneratorMapping.GM;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.compile.sig.CodeGeneratorArgument;
import org.apache.drill.exec.compile.sig.CodeGeneratorMethod;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.compile.sig.SignatureHolder;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.record.TypedFieldId;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.google.common.base.Preconditions;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JFieldRef;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

public class CodeGenerator<T>{
  
  public static final GeneratorMapping DEFAULT_SCALAR_MAP = GM("doSetup", "doEval", null, null);
  public static final GeneratorMapping DEFAULT_CONSTANT_MAP = GM("doSetup", "doSetup", null, null);
  public static final MappingSet DEFAULT_MAPPING = new MappingSet("inIndex", "outIndex", DEFAULT_SCALAR_MAP, DEFAULT_SCALAR_MAP);

  
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CodeGenerator.class);
  public static enum BlockType {SETUP, EVAL, RESET, CLEANUP};
  
  public JDefinedClass clazz;
  private LinkedList<JBlock>[] blocks;
  private final TemplateClassDefinition<T> definition;
  private JCodeModel model;
  private int index = 0;
  private static AtomicLong classCreator = new AtomicLong(0);
  private String className;
  private String fqcn;
  private String packageName = "org.apache.drill.exec.test.generated";
  private final SignatureHolder sig;
  private MappingSet mappings;
  private final EvaluationVisitor evaluationVisitor;
  private final Map<ValueVectorSetup, JVar> vvDeclaration = Maps.newHashMap();
  
  
  public CodeGenerator(TemplateClassDefinition<T> definition, FunctionImplementationRegistry funcRegistry) {
    this(DEFAULT_MAPPING, definition, funcRegistry);
  }
  
  @SuppressWarnings("unchecked")
  public CodeGenerator(MappingSet mappingSet, TemplateClassDefinition<T> definition, FunctionImplementationRegistry funcRegistry) {
    Preconditions.checkNotNull(definition.getSignature(), "The signature for defintion %s was incorrectly initialized.", definition);
    this.sig = definition.getSignature();
    this.mappings = mappingSet;
    this.className = "Gen" + classCreator.incrementAndGet();
    this.fqcn = packageName + "." + className;
    try{
      this.definition = definition;
      this.model = new JCodeModel();
      this.clazz = model._package(packageName)._class(className);
      blocks = (LinkedList<JBlock>[]) new LinkedList[sig.size()];
      for(int i =0; i < sig.size(); i++){
        blocks[i] = Lists.newLinkedList();
      }
      this.evaluationVisitor = new EvaluationVisitor(funcRegistry);
      rotateBlock();
    } catch (JClassAlreadyExistsException e) {
      throw new IllegalStateException(e);
    }
  }
  
  public MappingSet getMappingSet(){
    return mappings;
  }
  
  public void setMappingSet(MappingSet mappings){
    this.mappings = mappings;
  }
  
  private GeneratorMapping getCurrentMapping(){
    return mappings.getCurrentMapping();
  }
  
  public JBlock getBlock(String methodName){
    JBlock blk = this.blocks[sig.get(methodName)].getLast();
    Preconditions.checkNotNull(blk, "Requested method name of %s was not available for signature %s.",  methodName, this.sig);
    return blk;
  }
  
  public JBlock getBlock(BlockType type){
    return getBlock(getCurrentMapping().getMethodName(type)); 
  }
  
  public JBlock getSetupBlock(){
    return getBlock(getCurrentMapping().getMethodName(BlockType.SETUP));
  }
  public JBlock getEvalBlock(){
    return getBlock(getCurrentMapping().getMethodName(BlockType.EVAL));
  }
  public JBlock getResetBlock(){
    return getBlock(getCurrentMapping().getMethodName(BlockType.RESET));
  }
  public JBlock getCleanupBlock(){
    return getBlock(getCurrentMapping().getMethodName(BlockType.CLEANUP));
  }
    
  public JVar declareVectorValueSetupAndMember(String batchName, TypedFieldId fieldId){
    return declareVectorValueSetupAndMember( DirectExpression.direct(batchName), fieldId);
  }

  public JVar declareVectorValueSetupAndMember(DirectExpression batchName, TypedFieldId fieldId){
    final ValueVectorSetup setup = new ValueVectorSetup(batchName, fieldId);
    JVar var = this.vvDeclaration.get(setup);
    if(var != null) return var;
    
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
    JType objClass = model.ref(Object.class);
    JBlock b = getSetupBlock();
    JVar obj = b.decl( //
        objClass, //
        getNextVar("tmp"), // 
        batchName
          .invoke("getValueAccessorById") //
          .arg(JExpr.lit(fieldId.getFieldId())) //
          .arg( vvClass.dotclass())
          .invoke(vectorAccess)//
          );
        
        
    b._if(obj.eq(JExpr._null()))._then()._throw(JExpr._new(t).arg(JExpr.lit(String.format("Failure while loading vector %s with id: %s.", vv.name(), fieldId.toString()))));
    //b.assign(vv, JExpr.cast(retClass, ((JExpression) JExpr.cast(wrapperClass, obj) ).invoke(vectorAccess)));
    b.assign(vv, JExpr.cast(retClass, obj ));
    vvDeclaration.put(setup, vv);
        
    return vv;
  }

  public HoldingContainer addExpr(LogicalExpression ex){
    return addExpr(ex, true);
  }
  
  public HoldingContainer addExpr(LogicalExpression ex, boolean rotate){
//    logger.debug("Adding next write {}", ex);
    if(rotate) rotateBlock();
    return evaluationVisitor.addExpr(ex, this);
  }
  
  public void rotateBlock(){
    for(LinkedList<JBlock> b : blocks){
      b.add(new JBlock(true, true));
    }
  }
  
  public String getMaterializedClassName(){
    return fqcn;
  }
    
  public TemplateClassDefinition<T> getDefinition() {
    return definition;
  }

  public String generate() throws IOException{
    int i =0;
    for(CodeGeneratorMethod method : sig){
      JMethod m = clazz.method(JMod.PUBLIC, model._ref(method.getReturnType()), method.getMethodName());
      for(CodeGeneratorArgument arg : method){
        m.param(arg.getType(), arg.getName());
      }
      for(Class<?> c : method.getThrowsIterable()){
        m._throws(model.ref(c));
      }
      m._throws(SchemaChangeException.class);
      
      for(JBlock b : blocks[i++]){
        if(!b.isEmpty()) m.body().add(b);
      }
      
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
      var = getEvalBlock().decl(holderType, "out" + index, JExpr._new(holderType));
    }else{
      var = getEvalBlock().decl(holderType, "out" + index);
    }
    JFieldRef outputSet = null;
    if(t.getMode() == DataMode.OPTIONAL){
      outputSet = var.ref("isSet");  
    }
    index++;
    return new HoldingContainer(t, var, var.ref("value"), outputSet);
  }
  
  private static class ValueVectorSetup{
    final DirectExpression batch;
    final TypedFieldId fieldId;
    
    public ValueVectorSetup(DirectExpression batch, TypedFieldId fieldId) {
      super();
      this.batch = batch;
      this.fieldId = fieldId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((batch == null) ? 0 : batch.hashCode());
      result = prime * result + ((fieldId == null) ? 0 : fieldId.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      ValueVectorSetup other = (ValueVectorSetup) obj;
      if (batch == null) {
        if (other.batch != null)
          return false;
      } else if (!batch.equals(other.batch))
        return false;
      if (fieldId == null) {
        if (other.fieldId != null)
          return false;
      } else if (!fieldId.equals(other.fieldId))
        return false;
      return true;
    }

    
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
    
    public MajorType getMajorType(){
      return type;
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