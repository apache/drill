/**
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
 */
package org.apache.drill.exec.compile;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.drill.exec.compile.ClassTransformer.ClassNames;
import org.apache.drill.exec.compile.ClassTransformer.ClassSet;
import org.objectweb.asm.AnnotationVisitor;
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
import com.google.common.collect.Sets;
import com.google.common.io.Files;

/**
 * Serves two purposes. Renames all inner classes references to the outer class to the new name. Also adds all the
 * methods and fields of the class to merge to the class that is being visited.
 */
class MergeAdapter extends ClassVisitor {
  
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergeAdapter.class);
  
  private ClassNode classToMerge;
  private ClassSet set;
  
  private Set<String> mergingNames = Sets.newHashSet();
  
  private MergeAdapter(ClassSet set, ClassVisitor cv, ClassNode cn) {
    super(Opcodes.ASM4, cv);
    this.classToMerge = cn;
    this.set = set;
    for(Object o  : classToMerge.methods){
      String name = ((MethodNode)o).name;
      if(name.equals("<init>")) continue;
      mergingNames.add( name);
    }
  }

  @Override
  public void visitInnerClass(String name, String outerName, String innerName, int access) {
    // logger.debug(String.format("[Inner Class] Name: %s, outerName: %s, innerName: %s, templateName: %s, newName: %s.",
    // name, outerName, innerName, templateName, newName));
    if (name.startsWith(set.precompiled.slash)) {
//      outerName = outerName.replace(precompiled.slash, generated.slash);
      name = name.replace(set.precompiled.slash, set.generated.slash);
      int i = name.lastIndexOf('$');
      outerName = name.substring(0, i);
      super.visitInnerClass(name, outerName, innerName, access);
    } else {
      super.visitInnerClass(name, outerName, innerName, access);
    }
  }

  public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
    System.out.println("Annotation");
    return super.visitAnnotation(desc, visible);
  }
  
  // visit the class
  public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
    // use the access and names of the impl class.
    if(name.contains("$")){
      super.visit(version, access, name, signature, superName, interfaces);
    }else{
      super.visit(version, access ^ Modifier.ABSTRACT | Modifier.FINAL, name, signature, superName, interfaces);  
    }
    
//    this.cname = name;
  }

  @Override
  public MethodVisitor visitMethod(int access, String arg1, String arg2, String arg3, String[] arg4) {
    // finalize all methods.

    // skip all abstract methods as they should have implementations.
    if ((access & Modifier.ABSTRACT) != 0 || mergingNames.contains(arg1)) {
      
      logger.debug("Skipping copy of '{}()' since it is abstract or listed elsewhere.", arg1);
      return null;
    }
    if(arg3 != null){
      arg3 = arg3.replace(set.precompiled.slash, set.generated.slash);
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

    // add all the fields of the class we're going to merge.
    for (Iterator<?> it = classToMerge.fields.iterator(); it.hasNext();) {
      ((FieldNode) it.next()).accept(this);
    }

    // add all the methods that we to include.
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
      ClassSet top = set;
      while(top.parent != null) top = top.parent;
      mn.accept(new RemappingMethodAdapter(mn.access, mn.desc, mv, new SimpleRemapper(top.precompiled.slash, top.generated.slash)));
    }
    super.visitEnd();
  }

  @Override
  public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
    return super.visitField(access, name, desc, signature, value);
  }
  
  
  public static class MergedClassResult{
    public byte[] bytes;
    public Collection<String> innerClasses;
    public MergedClassResult(byte[] bytes, Collection<String> innerClasses) {
      super();
      this.bytes = bytes;
      this.innerClasses = innerClasses;
    }
    
    
  }
  
  public static MergedClassResult getMergedClass(ClassSet set, byte[] precompiledClass, ClassNode generatedClass) throws IOException{

    // Setup adapters for merging, remapping class names and class writing. This is done in reverse order of how they
    // will be evaluated.
    
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    RemapClasses re = new RemapClasses(set);
    ClassVisitor remappingAdapter = new RemappingClassAdapter(cw, re);
    ClassVisitor visitor = remappingAdapter;
    if(generatedClass != null){
      visitor = new MergeAdapter(set, remappingAdapter, generatedClass);  
    }
    ClassReader tReader = new ClassReader(precompiledClass);
    tReader.accept(visitor, ClassReader.EXPAND_FRAMES);
    byte[] outputClass = cw.toByteArray();
    
    // enable when you want all the generated merged class files to also be written to disk.
    //Files.write(outputClass, new File(String.format("/tmp/drill-generated-classes/%s-output.class", set.generated.dot)));

    return new MergedClassResult(outputClass, re.getInnerClasses());
  }
  

  static class RemapClasses extends Remapper {

    final Set<String> innerClasses = Sets.newHashSet();
    ClassSet top;
    ClassSet current;
    public RemapClasses(ClassSet set) {
      super();
      this.current = set;
      ClassSet top = set;
      while(top.parent != null) top = top.parent;
      this.top = top;
    }

    @Override
    public String map(String typeName) {
      
      // remap the names of all classes that start with the old class name.
      if (typeName.startsWith(top.precompiled.slash)) {
        
        // write down all the sub classes.
        if (typeName.startsWith(current.precompiled.slash + "$")){
          innerClasses.add(typeName);
        }
          
        return typeName.replace(top.precompiled.slash, top.generated.slash);
      }
      return typeName;
    }

    public Set<String> getInnerClasses() {
      return innerClasses;
    }

  }
}