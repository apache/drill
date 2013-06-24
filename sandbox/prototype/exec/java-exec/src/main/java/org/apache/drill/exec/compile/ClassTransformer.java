/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.compile;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.exec.exception.ClassTransformationException;
import org.codehaus.commons.compiler.CompileException;
import org.objectweb.asm.ClassAdapter;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.commons.EmptyVisitor;
import org.objectweb.asm.commons.RemappingClassAdapter;
import org.objectweb.asm.commons.RemappingMethodAdapter;
import org.objectweb.asm.commons.SimpleRemapper;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.util.TraceClassVisitor;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Resources;

public class ClassTransformer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClassTransformer.class);

  private AtomicLong index = new AtomicLong(0);
  private AtomicLong interfaceIndex = new AtomicLong(0);
  private LoadingCache<String, byte[]> byteCode = CacheBuilder.newBuilder()
      .maximumSize(10000)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build(new ClassBytesCacheLoader());
  
  
  private class ClassBytesCacheLoader extends CacheLoader<String, byte[]>{
    public byte[] load(String path) throws ClassTransformationException, IOException {
      URL u = this.getClass().getResource(path);
      if (u == null) throw new ClassTransformationException(String.format("Unable to find TemplateClass at path %s",path));
      return Resources.toByteArray(u);              
    }
  };
  
  private byte[] getClassByteCodeFromPath(String path) throws ClassTransformationException, IOException {
    try{
      return byteCode.get(path);
    } catch (ExecutionException e) {
      Throwable c = e.getCause();
      if(c instanceof ClassTransformationException) throw (ClassTransformationException) c;
      if(c instanceof IOException) throw (IOException) c;
      throw new ClassTransformationException(c);
    }
  }

  
  
  @SuppressWarnings("unchecked")
  public <T, I> T getImplementationClass(QueryClassLoader classLoader,
      TemplateClassDefinition<T, I> templateDefinition, String internalClassBody, I initObject)
      throws ClassTransformationException {
    final String implClassName = templateDefinition.getTemplateClassName() + interfaceIndex.getAndIncrement();
    final String materializedClassName = "org.apache.drill.generated."
        + templateDefinition.getExternalInterface().getSimpleName() + index.getAndIncrement();
    // final String materializedClassName = templateDefinition.getTemplateClassName();
    try {

      // Get Implementation Class
      String classBody = ClassBodyBuilder.newBuilder() //
          .setClassName(implClassName) //
          .setImplementedInterfaces(templateDefinition.getInternalInterface()) //
          .setBody(internalClassBody) //
          .build();
      final byte[] implementationClass = classLoader.getClassByteCode(implClassName, classBody);

      // Get Template Class
      final String templateClassName = templateDefinition.getTemplateClassName().replaceAll("\\.", File.separator);
      final String templateClassPath = File.separator + templateClassName + ".class";
      final byte[] templateClass = getClassByteCodeFromPath(templateClassPath);

      // Generate Merge Class
      ClassNode impl = getClassNodeFromByteCode(implementationClass);
      // traceClassToSystemOut(implementationClass);
      // traceClassToSystemOut(templateClass);
      ClassWriter cw = new ClassWriter(0);
      MergeAdapter adapter = new MergeAdapter(cw, impl);
      ClassReader tReader = new ClassReader(templateClass);
      tReader.accept(adapter, 0);

      byte[] outputClass = cw.toByteArray();

      cw = new ClassWriter(0);
      RemappingClassAdapter r = new RemappingClassAdapter(cw, new SimpleRemapper(templateClassName,
          materializedClassName.replace('.', '/')));
      new ClassReader(outputClass).accept(r, 0);
      outputClass = cw.toByteArray();
      // traceClassToSystemOut(outputClass);

      // Load the class
      classLoader.injectByteCode(materializedClassName, outputClass);
      Class<?> c = classLoader.findClass(materializedClassName);
      if (templateDefinition.getExternalInterface().isAssignableFrom(c)) {
        return (T) c.newInstance();
      } else {
        throw new ClassTransformationException("The requested class did not implement the expected interface.");
      }

    } catch (CompileException | IOException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new ClassTransformationException("Failure generating transformation classes.", e);
    }

  }

  private ClassNode getClassNodeFromByteCode(byte[] bytes) {
    ClassReader iReader = new ClassReader(bytes);
    ClassNode impl = new ClassNode();
    iReader.accept(impl, 0);
    return impl;
  }

  private void traceClassToSystemOut(byte[] bytecode) {
    TraceClassVisitor tcv = new TraceClassVisitor(new EmptyVisitor(), new PrintWriter(System.out));
    ClassReader cr = new ClassReader(bytecode);
    cr.accept(tcv, 0);
  }

  public class MergeAdapter extends ClassAdapter {
    private ClassNode classToMerge;
    private String cname;

    public MergeAdapter(ClassVisitor cv, ClassNode cn) {
      super(cv);
      this.classToMerge = cn;
    }

    // visit the class
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
      // use the access and names of the impl class.
      super.visit(version, access ^ Modifier.ABSTRACT | Modifier.FINAL, name, signature, superName, interfaces);
      this.cname = name;
    }

    @Override
    public MethodVisitor visitMethod(int access, String arg1, String arg2, String arg3, String[] arg4) {
      // finalize all methods.

      // skip all abstract methods as they should have implementations.
      if ((access & Modifier.ABSTRACT) != 0) {
        // logger.debug("Skipping copy of '{}()' since it is abstract.", arg1);
        return null;
      }

      // if( (access & Modifier.PUBLIC) == 0){
      // access = access ^ Modifier.PUBLIC ^ Modifier.PROTECTED | Modifier.PRIVATE;
      // }
      if (!arg1.equals("<init>")) {
        access = access | Modifier.FINAL;
      }
      return super.visitMethod(access, arg1, arg2, arg3, arg4);
    }

    @SuppressWarnings("unchecked")
    public void visitEnd() {
      for (Iterator<?> it = classToMerge.fields.iterator(); it.hasNext();) {
        ((FieldNode) it.next()).accept(this);
      }
      for (Iterator<?> it = classToMerge.methods.iterator(); it.hasNext();) {
        MethodNode mn = (MethodNode) it.next();

        // skip the init.
        if (mn.name.equals("<init>")) continue;

        String[] exceptions = new String[mn.exceptions.size()];
        mn.exceptions.toArray(exceptions);
        MethodVisitor mv = cv.visitMethod(mn.access | Modifier.FINAL, mn.name, mn.desc, mn.signature, exceptions);
        mn.instructions.resetLabels();
        // mn.accept(new RemappingMethodAdapter(mn.access, mn.desc, mv, new
        // SimpleRemapper("org.apache.drill.exec.compile.ExampleTemplate", "Bunky")));
        mn.accept(new RemappingMethodAdapter(mn.access, mn.desc, mv, new SimpleRemapper(cname.replace('.', '/'),
            classToMerge.name.replace('.', '/'))));
      }
      super.visitEnd();
    }
  }

}
