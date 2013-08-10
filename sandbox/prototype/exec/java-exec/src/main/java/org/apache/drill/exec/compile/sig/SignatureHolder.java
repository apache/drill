package org.apache.drill.exec.compile.sig;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

public class SignatureHolder implements Iterable<CodeGeneratorMethod>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SignatureHolder.class);
  
  private final CodeGeneratorMethod[] methods;
  private final Map<String, Integer> methodMap;
  
  public <T extends CodeGeneratorSignature> SignatureHolder(Class<T> signature){
    Method[] reflectMethods = signature.getDeclaredMethods();
    methods = new CodeGeneratorMethod[reflectMethods.length];
    Map<String, Integer> newMap = Maps.newHashMap(); 
    
    for(int i =0; i < methods.length; i++){
      methods[i] = new CodeGeneratorMethod(reflectMethods[i]);
      newMap.put(methods[i].getMethodName(), i);
    }
    
    methodMap = ImmutableMap.copyOf(newMap);
    
  }

  @Override
  public Iterator<CodeGeneratorMethod> iterator() {
    return Iterators.forArray(methods);
  }
  
  public int size(){
    return methods.length;
  }
  
  public int get(String method){
    Integer meth =  methodMap.get(method);
    if(meth == null){
      throw new IllegalStateException(String.format("Unknown method requested of name %s.", method));
    }
    return meth;
  }

  @Override
  public String toString() {
    final int maxLen = 10;
    return "SignatureHolder [methods="
        + (methods != null ? Arrays.asList(methods).subList(0, Math.min(methods.length, maxLen)) : null) + "]";
  }
  
  
}
