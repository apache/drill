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
import org.apache.drill.exec.expr.fn.DrillFuncHolder.ValueReference;
import org.apache.drill.exec.expr.fn.DrillFuncHolder.WorkspaceReference;
import org.apache.drill.exec.expr.fn.impl.GCompareBigIntNullableBigInt;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java.CompilationUnit;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.mortbay.util.IO;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;

/**
 * Converts FunctionCalls to Java Expressions.
 */
public class FunctionConverter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionConverter.class);
  
  private Map<String, CompilationUnit> functionUnits = Maps.newHashMap();
  
  private CompilationUnit get(Class<?> c) throws IOException{
    String path = c.getName();
    path = path.replaceFirst("\\$.*", "");
    path = path.replace(".", File.separator);
    path = "/" + path + ".java";
    CompilationUnit cu = functionUnits.get(path);
    if(cu != null) return cu;

    URL u = Resources.getResource(c, path);
    InputSupplier<InputStream> supplier = Resources.newInputStreamSupplier(u);
    try(InputStream is = supplier.getInput()){
      if(is == null){
        throw new IOException(String.format("Failure trying to located source code for Class %s, tried to read on classpath location %s", c.getName(), path));
      }
      String body = IO.toString(is);
      
      //TODO: Hack to remove annotations so Janino doesn't choke.  Need to reconsider this problem...
      body = body.replaceAll("@\\w+(?:\\([^\\\\]*?\\))?", "");
      try{
        cu = new Parser(new Scanner(null, new StringReader(body))).parseCompilationUnit();
        functionUnits.put(path, cu);
        return cu;
      } catch (CompileException e) {
        logger.warn("Failure while parsing function class:\n{}", body, e);
        return null;
      }
      
    }
    
  }
  
  public <T extends DrillFunc> DrillFuncHolder getHolder(Class<T> clazz){
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
//        logger.debug("Found workspace field {}:{}", field.getType(), field.getName());
        workspaceFields.add(new WorkspaceReference(field.getType(), field.getName()));
      }
      
    }
    
    
   // if(!workspaceFields.isEmpty()) return failure("This function declares one or more workspace fields.  However, those have not yet been implemented.", clazz);
    if(outputField == null)  return failure("This function declares zero output fields.  A function must declare one output field.", clazz);
    
    // get function body.     
   
    CompilationUnit cu;
    try {
      cu = get(clazz);
      if(cu == null) return null;
    } catch (IOException e) {
      return failure("Failure while getting class body.", e, clazz);
    }
    
    Map<String, String> methods = MethodGrabbingVisitor.getMethods(cu, clazz);
    List<String> imports = ImportGrabber.getMethods(cu);
    // return holder
    ValueReference[] ps = params.toArray(new ValueReference[params.size()]);
    WorkspaceReference[] works = workspaceFields.toArray(new WorkspaceReference[workspaceFields.size()]);
    
    try{
      switch(template.scope()){
      case POINT_AGGREGATE:
        return new DrillAggFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(), template.name(), ps, outputField, works, methods, imports);
      case SIMPLE:
        DrillFuncHolder fh = new DrillSimpleFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(), template.name(), ps, outputField, works, methods, imports);
        return fh;

      case HOLISTIC_AGGREGATE:
      case RANGE_AGGREGATE:
      default:
        return failure("Unsupported Function Type.", clazz);
      }
    }catch(Exception ex){
      return failure("Failure while creating function holder.", ex, clazz);
    }
    
  }
  
  
  
  private String getClassBody(Class<?> c) throws CompileException, IOException{
    String path = c.getName();
    path = path.replaceFirst("\\$.*", "");
    path = path.replace(".", File.separator);
    path = "/" + path + ".java";
    URL u = Resources.getResource(c, path);
    InputSupplier<InputStream> supplier = Resources.newInputStreamSupplier(u);
    try(InputStream is = supplier.getInput()){
      if(is == null){
        throw new IOException(String.format("Failure trying to located source code for Class %s, tried to read on classpath location %s", c.getName(), path));
      }
      String body = IO.toString(is);
      
      //TODO: Hack to remove annotations so Janino doesn't choke.  Need to reconsider this problem...
      //return body.replaceAll("@(?:Output|Param|Workspace|Override|SuppressWarnings\\([^\\\\]*?\\)|FunctionTemplate\\([^\\\\]*?\\))", "");
      return body.replaceAll("@(?:\\([^\\\\]*?\\))?", "");
    }
    
  }
  
  
  
  @SuppressWarnings("unchecked")
  private <T> T getStaticFieldValue(String fieldName, Class<?> valueType, Class<T> c) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException{
      Field f = valueType.getDeclaredField(fieldName);
      Object val = f.get(null);
      return (T) val;
  }
  
  private static DrillFuncHolder failure(String message, Throwable t, Class<?> clazz, String fieldName){
    logger.warn("Failure loading function class {}, field {}. " + message, clazz.getName(), fieldName, t);
    return null;
  }  
  
  private DrillFuncHolder failure(String message, Class<?> clazz, String fieldName){
    logger.warn("Failure loading function class {}, field {}. " + message, clazz.getName(), fieldName);
    return null;
  }

  private DrillFuncHolder failure(String message, Class<?> clazz){
    logger.warn("Failure loading function class [{}]. Message: {}", clazz.getName(), message);
    return null;
  }

  private DrillFuncHolder failure(String message, Throwable t, Class<?> clazz){
    logger.warn("Failure loading function class [{}]. Message: {}", clazz.getName(), message, t);
    return null;
  }
  
  private DrillFuncHolder failure(String message, Class<?> clazz, Field field){
    return failure(message, clazz, field.getName());
  }
  

  
}
