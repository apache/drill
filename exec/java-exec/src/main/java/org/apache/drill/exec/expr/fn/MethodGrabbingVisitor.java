/*
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
package org.apache.drill.exec.expr.fn;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.janino.Java;
import org.codehaus.janino.Java.AbstractClassDeclaration;
import org.codehaus.janino.Java.MethodDeclarator;
import org.codehaus.janino.util.AbstractTraverser;

public class MethodGrabbingVisitor {

  private final Class<?> clazz;
  private final Map<String, String> methods = new HashMap<>();
  private final ClassFinder classFinder = new ClassFinder();
  private boolean captureMethods = false;

  private MethodGrabbingVisitor(Class<?> clazz) {
    this.clazz = clazz;
  }

  /**
   * Creates a map with all method names and their modified bodies
   * from specified {@link Java.CompilationUnit}.
   *
   * @param compilationUnit the source of the methods to collect
   * @param clazz           type of the class to handle
   * @return a map with all method names and their modified bodies.
   */
  public static Map<String, String> getMethods(Java.CompilationUnit compilationUnit, Class<?> clazz) {
    MethodGrabbingVisitor visitor = new MethodGrabbingVisitor(clazz);
    visitor.classFinder.visitTypeDeclaration(compilationUnit.getPackageMemberTypeDeclarations()[0]);
    return visitor.methods;
  }

  public class ClassFinder extends AbstractTraverser<RuntimeException> {

    @Override
    public void traverseClassDeclaration(AbstractClassDeclaration classDeclaration) {
      boolean prevCapture = captureMethods;
      captureMethods = clazz.getName().equals(classDeclaration.getClassName());
      super.traverseClassDeclaration(classDeclaration);
      captureMethods = prevCapture;
    }

    @Override
    public void traverseMethodDeclarator(MethodDeclarator methodDeclarator) {
      if (captureMethods) {
        StringWriter writer = new StringWriter();
        ModifiedUnparser unparser = new ModifiedUnparser(writer);
        unparser.visitMethodDeclarator(methodDeclarator);
        unparser.close();
        writer.flush();
        methods.put(methodDeclarator.name, writer.getBuffer().toString());
      }
    }
  }

}
