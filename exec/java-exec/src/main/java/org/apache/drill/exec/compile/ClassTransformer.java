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

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.util.DrillStringUtils;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.compile.MergeAdapter.MergedClassResult;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.codehaus.commons.compiler.CompileException;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.tree.ClassNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ClassTransformer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClassTransformer.class);

  private final ByteCodeLoader byteCodeLoader = new ByteCodeLoader();

  public ClassTransformer() {
  }

  public static class ClassSet{
    public final ClassSet parent;
    public final ClassNames precompiled;
    public final ClassNames generated;

    public ClassSet(ClassSet parent, String precompiled, String generated) {
      super();
      this.parent = parent;

      this.precompiled = new ClassNames(precompiled);
      this.generated = new ClassNames(generated);
      Preconditions.checkArgument(!generated.startsWith(precompiled),
          String.format("The new name of a class cannot start with the old name of a class, otherwise class renaming will cause problems.  Precompiled class name %s.  Generated class name %s", precompiled, generated));
    }

    public ClassSet getChild(String precompiled, String generated) {
      return new ClassSet(this, precompiled, generated);
    }

    public ClassSet getChild(String precompiled) {
      return new ClassSet(this, precompiled, precompiled.replace(this.precompiled.dot, this.generated.dot));
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((generated == null) ? 0 : generated.hashCode());
      result = prime * result + ((parent == null) ? 0 : parent.hashCode());
      result = prime * result + ((precompiled == null) ? 0 : precompiled.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ClassSet other = (ClassSet) obj;
      if (generated == null) {
        if (other.generated != null) {
          return false;
        }
      } else if (!generated.equals(other.generated)) {
        return false;
      }
      if (parent == null) {
        if (other.parent != null) {
          return false;
        }
      } else if (!parent.equals(other.parent)) {
        return false;
      }
      if (precompiled == null) {
        if (other.precompiled != null) {
          return false;
        }
      } else if (!precompiled.equals(other.precompiled)) {
        return false;
      }
      return true;
    }

  }

  public static class ClassNames {

    public final String dot;
    public final String slash;
    public final String clazz;

    public ClassNames(String className) {
      dot = className;
      slash = className.replace('.', FileUtils.separatorChar);
      clazz = FileUtils.separatorChar + slash + ".class";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((clazz == null) ? 0 : clazz.hashCode());
      result = prime * result + ((dot == null) ? 0 : dot.hashCode());
      result = prime * result + ((slash == null) ? 0 : slash.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ClassNames other = (ClassNames) obj;
      if (clazz == null) {
        if (other.clazz != null) {
          return false;
        }
      } else if (!clazz.equals(other.clazz)) {
        return false;
      }
      if (dot == null) {
        if (other.dot != null) {
          return false;
        }
      } else if (!dot.equals(other.dot)) {
        return false;
      }
      if (slash == null) {
        if (other.slash != null) {
          return false;
        }
      } else if (!slash.equals(other.slash)) {
        return false;
      }
      return true;
    }
  }

  private static ClassNode getClassNodeFromByteCode(byte[] bytes) {
    ClassReader iReader = new ClassReader(bytes);
    ClassNode impl = new ClassNode();
    iReader.accept(impl, ClassReader.EXPAND_FRAMES);
    return impl;
  }

  public Class<?> getImplementationClass( //
      QueryClassLoader classLoader, //
      TemplateClassDefinition<?> templateDefinition, //
      String entireClass, //
      String materializedClassName) throws ClassTransformationException {

    try {
      long t1 = System.nanoTime();
      final ClassSet set = new ClassSet(null, templateDefinition.getTemplateClassName(), materializedClassName);
      final byte[][] implementationClasses = classLoader.getClassByteCode(set.generated, entireClass);

      long totalBytecodeSize = 0;
      Map<String, ClassNode> classesToMerge = Maps.newHashMap();
      for (byte[] clazz : implementationClasses) {
        totalBytecodeSize += clazz.length;
        ClassNode node = getClassNodeFromByteCode(clazz);
        classesToMerge.put(node.name, node);
      }

      LinkedList<ClassSet> names = Lists.newLinkedList();
      Set<ClassSet> namesCompleted = Sets.newHashSet();
      names.add(set);

      while ( !names.isEmpty() ) {
        final ClassSet nextSet = names.removeFirst();
        if (namesCompleted.contains(nextSet)) {
          continue;
        }
        final ClassNames nextPrecompiled = nextSet.precompiled;
        final byte[] precompiledBytes = byteCodeLoader.getClassByteCodeFromPath(nextPrecompiled.clazz);
        ClassNames nextGenerated = nextSet.generated;
        ClassNode generatedNode = classesToMerge.get(nextGenerated.slash);
        MergedClassResult result = MergeAdapter.getMergedClass(nextSet, precompiledBytes, generatedNode);

        for (String s : result.innerClasses) {
          s = s.replace(FileUtils.separatorChar, '.');
          names.add(nextSet.getChild(s));
        }
        classLoader.injectByteCode(nextGenerated.dot, result.bytes);
        namesCompleted.add(nextSet);
      }

      Class<?> c = classLoader.findClass(set.generated.dot);
      if (templateDefinition.getExternalInterface().isAssignableFrom(c)) {
        logger.debug("Done compiling (bytecode size={}, time:{} millis).", DrillStringUtils.readable(totalBytecodeSize), (System.nanoTime() - t1) / 1000000);
        return c;
      } else {
        throw new ClassTransformationException("The requested class did not implement the expected interface.");
      }
    } catch (CompileException | IOException | ClassNotFoundException e) {
      throw new ClassTransformationException(String.format("Failure generating transformation classes for value: \n %s", entireClass), e);
    }

  }

}

