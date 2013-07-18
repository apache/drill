package org.apache.drill.exec.expr.fn;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.fn.FunctionHolder.ValueReference;
import org.apache.drill.exec.expr.fn.FunctionHolder.WorkspaceReference;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java;
import org.codehaus.janino.Java.CompilationUnit;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.mortbay.util.IO;

import com.google.common.collect.Lists;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;

/**
 * Converts FunctionCalls to Java Expressions.
 */
public class FunctionConverter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionConverter.class);
  
  
  public <T extends DrillFunc> FunctionHolder getHolder(Class<T> clazz){
    FunctionTemplate template = clazz.getAnnotation(FunctionTemplate.class);
    if(template == null){
      return failure("Class does not declare FunctionTemplate annotation.", clazz);
    }
    
    // start by getting field information.
    List<ValueReference> params = Lists.newArrayList();
    List<WorkspaceReference> workspaceFields = Lists.newArrayList();
    
    ValueReference outputField = null;
    
    
    for(Field field : clazz.getDeclaredFields()){

      Param param = field.getAnnotation(Param.class);
      Output output = field.getAnnotation(Output.class);
      Workspace workspace = field.getAnnotation(Workspace.class);
      
      int i =0;
      if(param != null) i++;
      if(output != null) i++;
      if(workspace != null) i++;
      if(i == 0){
        return failure("The field must be either a @Param, @Output or @Workspace field.", clazz, field);
      }else if(i > 1){
        return failure("The field must be only one of @Param, @Output or @Workspace.  It currently has more than one of these annotations.", clazz, field);
      }

      
      
      if(param != null || output != null){
        
        // check that param and output are value holders.
        if(!ValueHolder.class.isAssignableFrom(field.getType())){
          return failure(String.format("The field doesn't holds value of type %s which does not implement the ValueHolder interface.  All fields of type @Param or @Output must extend this interface..", field.getType()), clazz, field);
        }
        
        // get the type field from the value holder.
        MajorType type = null;
        try{
          type = getStaticFieldValue("TYPE", field.getType(), MajorType.class);
        }catch(Exception e){
          return failure("Failure while trying to access the ValueHolder's TYPE static variable.  All ValueHolders must contain a static TYPE variable that defines their MajorType.", e, clazz, field.getName());
        }
        
        
        ValueReference p = new ValueReference(type, field.getName());
        if(param != null){
          params.add(p);
        }else{ 
          if(outputField != null){
            return failure("You've declared more than one @Output field.  You must declare one and only @Output field per Function class.", clazz, field);
          }else{
            outputField = p; 
            
          }
           
        }
        
      }else{
        // workspace work.
        logger.debug("Found workspace field {}:{}", field.getType(), field.getName());
        workspaceFields.add(new WorkspaceReference(field.getType(), field.getName()));
      }
      
    }
    
    
   // if(!workspaceFields.isEmpty()) return failure("This function declares one or more workspace fields.  However, those have not yet been implemented.", clazz);
    if(outputField == null)  return failure("This function declares zero output fields.  A function must declare one output field.", clazz);
    
    // get function body.     
   
    CompilationUnit cu;
    try {
      cu = getClassBody(clazz);
    } catch (CompileException | IOException e) {
      return failure("Failure while getting class body.", e, clazz);
    }
    
    Map<String, String> methods = MethodGrabbingVisitor.getMethods(cu, clazz);
    List<String> imports = ImportGrabber.getMethods(cu);
    // return holder
    ValueReference[] ps = params.toArray(new ValueReference[params.size()]);
    WorkspaceReference[] works = workspaceFields.toArray(new WorkspaceReference[workspaceFields.size()]);
    if(!methods.containsKey("eval")){
      return failure("Failure finding eval method for function.", clazz);
    }
    
    try{
      FunctionHolder fh = new FunctionHolder(template.scope(), template.nulls(), template.isBinaryCommutative(), template.name(), ps, outputField, works, methods, imports);
      return fh;
    }catch(Exception ex){
      return failure("Failure while creating function holder.", ex, clazz);
    }
    
  }
  
  
  
  private Java.CompilationUnit getClassBody(Class<?> c) throws CompileException, IOException{
    String path = c.getName();
    path = path.replaceFirst("\\$.*", "");
    path = path.replace(".", File.separator);
    path = "/" + path + ".java";
    URL u = Resources.getResource(FunctionConverter.class, path);
    InputSupplier<InputStream> supplier = Resources.newInputStreamSupplier(u);
    try(InputStream is = supplier.getInput()){
      if(is == null){
        throw new IOException(String.format("Failure trying to located source code for Class %s, tried to read on classpath location %s", c.getName(), path));
      }
      String body = IO.toString(is);
      
      //TODO: Hack to remove annotations so Janino doesn't choke.  Need to reconsider this problem...
      body = body.replaceAll("@(?:Output|Param|Workspace|Override|FunctionTemplate\\([^\\\\]*?\\))", "");
      return new Parser(new Scanner(null, new StringReader(body))).parseCompilationUnit();
    }
    
  }
  
  @SuppressWarnings("unchecked")
  private <T> T getStaticFieldValue(String fieldName, Class<?> valueType, Class<T> c) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException{
      Field f = valueType.getDeclaredField(fieldName);
      Object val = f.get(null);
      return (T) val;
  }
  
  private static FunctionHolder failure(String message, Throwable t, Class<?> clazz, String fieldName){
    logger.warn("Failure loading function class {}, field {}. " + message, clazz.getName(), fieldName, t);
    return null;
  }  
  
  private FunctionHolder failure(String message, Class<?> clazz, String fieldName){
    logger.warn("Failure loading function class {}, field {}. " + message, clazz.getName(), fieldName);
    return null;
  }

  private FunctionHolder failure(String message, Class<?> clazz){
    logger.warn("Failure loading function class {}. " + message, clazz.getName());
    return null;
  }

  private FunctionHolder failure(String message, Throwable t, Class<?> clazz){
    logger.warn("Failure loading function class {}. " + message, t, clazz.getName());
    return null;
  }
  
  private FunctionHolder failure(String message, Class<?> clazz, Field field){
    return failure(message, clazz, field.getName());
  }
  
  public static void main(String[] args) throws Exception{
    
    URL u = Resources.getResource(FunctionConverter.class, "/org/apache/drill/exec/expr/fn/impl/MathFunctions.java");
    InputStream is = Resources.newInputStreamSupplier(u).getInput();
    String s = IO.toString(is);
    System.out.println(s);
  }
  
}
