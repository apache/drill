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
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.exec.exception.ClassTransformationException;
import org.codehaus.commons.compiler.CompileException;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.Remapper;
import org.objectweb.asm.commons.RemappingClassAdapter;
import org.objectweb.asm.commons.RemappingMethodAdapter;
import org.objectweb.asm.commons.SimpleRemapper;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.MethodNode;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class ClassTransformer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClassTransformer.class);

  private AtomicLong index = new AtomicLong(0);
  private AtomicLong interfaceIndex = new AtomicLong(0);
  private LoadingCache<String, byte[]> byteCode = CacheBuilder.newBuilder().maximumSize(10000)
      .expireAfterWrite(10, TimeUnit.MINUTES).build(new ClassBytesCacheLoader());

  private class ClassBytesCacheLoader extends CacheLoader<String, byte[]> {
    public byte[] load(String path) throws ClassTransformationException, IOException {
      URL u = this.getClass().getResource(path);
      if (u == null)
        throw new ClassTransformationException(String.format("Unable to find TemplateClass at path %s", path));
      return Resources.toByteArray(u);
    }
  };

  private byte[] getClassByteCodeFromPath(String path) throws ClassTransformationException, IOException {
    try {
      return byteCode.get(path);
    } catch (ExecutionException e) {
      Throwable c = e.getCause();
      if (c instanceof ClassTransformationException)
        throw (ClassTransformationException) c;
      if (c instanceof IOException)
        throw (IOException) c;
      throw new ClassTransformationException(c);
    }
  }

  public <T, I> T getImplementationClassByBody( //
      QueryClassLoader classLoader, //
      TemplateClassDefinition<T> templateDefinition, //
      String internalClassBody //
  ) throws ClassTransformationException {
    final String materializedClassName = "org.apache.drill.generated."
        + "Gen" + templateDefinition.getExternalInterface().getSimpleName() //
        + index.getAndIncrement();
    // Get Implementation Class
    try {
      String classBody = ClassBodyBuilder.newBuilder() //
          .setClassName(materializedClassName) //
          .setImplementedInterfaces(templateDefinition.getInternalInterface()) //
          .setBody(internalClassBody) //
          .build();
      return getImplementationClass(classLoader, templateDefinition, classBody, materializedClassName);
    } catch (IOException | CompileException e) {
      throw new ClassTransformationException("Failure generating class body for runtime generated class.", e);
    }

  }

  @SuppressWarnings("unchecked")
  public <T, I> T getImplementationClass( //
      QueryClassLoader classLoader, //
      TemplateClassDefinition<T> templateDefinition, //
      String entireClass, //
      String materializedClassName) throws ClassTransformationException {

    try {
      long t0 = System.nanoTime();
      final byte[] implementationClass = classLoader.getClassByteCode(materializedClassName, entireClass);

      // Get Template Class
      final String templateClassName = templateDefinition.getTemplateClassName().replaceAll("\\.", File.separator);
      final String templateClassPath = File.separator + templateClassName + ".class";
      long t1 = System.nanoTime();
      final byte[] templateClass = getClassByteCodeFromPath(templateClassPath);
//      int fileNum = new Random().nextInt(100);
      //Files.write(templateClass, new File(String.format("/tmp/%d-template.class", fileNum)));
      // Generate Merge Class

      // Setup adapters for merging, remapping class names and class writing. This is done in reverse order of how they
      // will be evaluated.
      String oldTemplateSlashName = templateDefinition.getTemplateClassName().replace('.', '/');
      String materializedSlashName = materializedClassName.replace('.', '/');
      RemapClasses remapper = new RemapClasses(oldTemplateSlashName, materializedSlashName);
      
      {
        
        ClassNode impl = getClassNodeFromByteCode(implementationClass);
        long t2 = System.nanoTime();
        logger.debug("Compile {}, decode template {}", (t1 - t0)/1000/1000, (t2- t1)/1000/1000);
        
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

        ClassVisitor remappingAdapter = new RemappingClassAdapter(cw, remapper);
        MergeAdapter mergingAdapter = new MergeAdapter(oldTemplateSlashName, materializedSlashName, remappingAdapter,
            impl);
        ClassReader tReader = new ClassReader(templateClass);
        tReader.accept(mergingAdapter, ClassReader.EXPAND_FRAMES);
        byte[] outputClass = cw.toByteArray();
//        Files.write(outputClass, new File(String.format("/tmp/%d-output.class", fileNum)));
        outputClass = cw.toByteArray();

        // Load the class
        classLoader.injectByteCode(materializedClassName, outputClass);
      }

      int i = 0;
      for (String s : remapper.getSubclasses()) {
        logger.debug("Setting up sub class {}", s);
        // for each sub class, remap them into the new class.
        String subclassPath = File.separator + s + ".class";
        final byte[] bytecode = getClassByteCodeFromPath(subclassPath);
        RemapClasses localRemapper = new RemapClasses(oldTemplateSlashName, materializedSlashName);
        Preconditions.checkArgument(localRemapper.getSubclasses().isEmpty(), "Class transformations are only supported for classes that have a single level of inner classes.");
        ClassWriter subcw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        ClassVisitor remap = new RemappingClassAdapter(subcw, localRemapper);
        ClassReader reader = new ClassReader(bytecode);
        reader.accept(remap, ClassReader.EXPAND_FRAMES);
        byte[] newByteCode = subcw.toByteArray();
        classLoader.injectByteCode(s.replace(oldTemplateSlashName, materializedSlashName).replace('/', '.'), newByteCode);
//        Files.write(subcw.toByteArray(), new File(String.format("/tmp/%d-sub-%d.class", fileNum, i)));
        i++;
      }

      Class<?> c = classLoader.findClass(materializedClassName);
      if (templateDefinition.getExternalInterface().isAssignableFrom(c)) {
        return (T) c.newInstance();
      } else {
        throw new ClassTransformationException("The requested class did not implement the expected interface.");
      }

    } catch (CompileException | IOException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new ClassTransformationException(String.format(
          "Failure generating transformation classes for value: \n %s", entireClass), e);
    }

  }

  private ClassNode getClassNodeFromByteCode(byte[] bytes) {
    ClassReader iReader = new ClassReader(bytes);
    ClassNode impl = new ClassNode();
    iReader.accept(impl, 0);
    return impl;
  }

  // private void traceClassToSystemOut(byte[] bytecode) {
  // TraceClassVisitor tcv = new TraceClassVisitor(new EmptyVisitor(), new PrintWriter(System.out));
  // ClassReader cr = new ClassReader(bytecode);
  // cr.accept(tcv, 0);
  // }

  public class MergeAdapter extends ClassVisitor {
    private ClassNode classToMerge;
    private String cname;
    private String templateName;
    private String newName;

    public MergeAdapter(String templateName, String newName, ClassVisitor cv, ClassNode cn) {
      super(Opcodes.ASM4, cv);
      this.classToMerge = cn;
      this.templateName = templateName;
      this.newName = newName.replace('.', '/');
      ;

    }

    @Override
    public void visitInnerClass(String name, String outerName, String innerName, int access) {
      logger.debug(String.format(
          "[Inner Class] Name: %s, outerName: %s, innerName: %s, templateName: %s, newName: %s.", name, outerName,
          innerName, templateName, newName));
      if (name.startsWith(templateName)) {
        super.visitInnerClass(name.replace(templateName, newName), newName, innerName, access);
      } else {
        super.visitInnerClass(name, outerName, innerName, access);
      }
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
        if (mn.name.equals("<init>"))
          continue;

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

    @Override
    public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
      return super.visitField(access, name, desc, signature, value);
    }

  }

  static class RemapClasses extends Remapper {

    final Set<String> subclasses = Sets.newHashSet();

    String oldName;
    String newName;

    public RemapClasses(String oldName, String newName) {
      super();
      Preconditions.checkArgument(!newName.startsWith(oldName), "The new name of a class cannot start with the old name of a class, otherwise class renaming will cause problems.");
      this.oldName = oldName;
      this.newName = newName;
    }

    @Override
    public String map(String typeName) {
      // remap the names of all classes that start with the old class name.
      if (typeName.startsWith(oldName)) {
        
        // write down all the sub classes.
        if (typeName.startsWith(oldName + "$")){
          subclasses.add(typeName);
        }
          
        return typeName.replace(oldName, newName);
      }
      return typeName;
    }

    public Set<String> getSubclasses() {
      return subclasses;
    }

  }

}