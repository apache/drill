package org.apache.drill.exec.expr.fn;

import java.io.StringWriter;
import java.util.Map;

import org.codehaus.janino.Java;
import org.codehaus.janino.Java.ClassDeclaration;
import org.codehaus.janino.Java.MethodDeclarator;
import org.codehaus.janino.util.Traverser;

import com.google.common.collect.Maps;


public class MethodGrabbingVisitor{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MethodGrabbingVisitor.class);
  
  private Class<?> c;
  private Map<String, String> methods = Maps.newHashMap();
  private ClassFinder classFinder = new ClassFinder();
  private boolean captureMethods = false;
  
  private MethodGrabbingVisitor(Class<?> c) {
    super();
    this.c = c;
  }
  
  public class ClassFinder extends Traverser{

    @Override
    public void traverseClassDeclaration(ClassDeclaration cd) {
      logger.debug("Traversing: {}", cd.getClassName());
      boolean prevCapture = captureMethods;
      captureMethods = c.getName().equals(cd.getClassName());
      super.traverseClassDeclaration(cd);
      captureMethods = prevCapture;
    }

    @Override
    public void traverseMethodDeclarator(MethodDeclarator md) {
      logger.debug(c.getName() + ": Found {}, include {}", md.name, captureMethods);
      if(captureMethods){
        StringWriter writer = new StringWriter();
        ModifiedUnparseVisitor v = new ModifiedUnparseVisitor(writer);
        md.accept(v);
        methods.put(md.name, writer.getBuffer().toString());  
      }
    } 
  }

  
  public static Map<String, String> getMethods(Java.CompilationUnit cu, Class<?> c){
    MethodGrabbingVisitor visitor = new MethodGrabbingVisitor(c);
    cu.getPackageMemberTypeDeclarations()[0].accept(visitor.classFinder.comprehensiveVisitor());
    return visitor.methods;
  }

}
