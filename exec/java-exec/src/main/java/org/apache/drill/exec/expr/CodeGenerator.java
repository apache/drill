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
package org.apache.drill.exec.expr;

import java.io.IOException;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;

import com.google.common.base.Preconditions;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import org.apache.drill.exec.server.options.OptionManager;

/**
 * A code generator is responsible for generating the Java source code required to complete the implementation of an
 * abstract template. It is used with a class transformer to merge precompiled template code with runtime generated and
 * compiled query specific code to create a runtime instance.
 *
 * A code generator can contain one or more ClassGenerators that implement outer and inner classes associated with a
 * particular runtime generated instance.
 *
 * @param <T>
 *          The interface that results from compiling and merging the runtime code that is generated.
 */
public class CodeGenerator<T> {

  private static final String PACKAGE_NAME = "org.apache.drill.exec.test.generated";

  private final TemplateClassDefinition<T> definition;
  private final String className;
  private final String fqcn;

  private final JCodeModel model;
  private final ClassGenerator<T> rootGenerator;
  private String generatedCode;
  private String generifiedCode;

  CodeGenerator(TemplateClassDefinition<T> definition, FunctionImplementationRegistry funcRegistry, OptionManager optionManager) {
    this(ClassGenerator.getDefaultMapping(), definition, funcRegistry, optionManager);
  }

  CodeGenerator(MappingSet mappingSet, TemplateClassDefinition<T> definition,
      FunctionImplementationRegistry funcRegistry, OptionManager optionManager) {
    Preconditions.checkNotNull(definition.getSignature(),
        "The signature for defintion %s was incorrectly initialized.", definition);
    this.definition = definition;
    this.className = definition.getExternalInterface().getSimpleName() + "Gen" + definition.getNextClassNumber();
    this.fqcn = PACKAGE_NAME + "." + className;
    try {
      this.model = new JCodeModel();
      JDefinedClass clazz = model._package(PACKAGE_NAME)._class(className);
      rootGenerator = new ClassGenerator<>(this, mappingSet, definition.getSignature(), new EvaluationVisitor(
          funcRegistry), clazz, model, optionManager);
    } catch (JClassAlreadyExistsException e) {
      throw new IllegalStateException(e);
    }
  }

  public ClassGenerator<T> getRoot() {
    return rootGenerator;
  }

  public void generate() throws IOException {
    rootGenerator.flushCode();

    SingleClassStringWriter w = new SingleClassStringWriter();
    model.build(w);

    this.generatedCode = w.getCode().toString();
    this.generifiedCode = generatedCode.replaceAll(this.className, "GenericGenerated");

  }

  public String generateAndGet() throws IOException {
    generate();
    return generatedCode;
  }

  public String getGeneratedCode() {
    return generatedCode;
  }

  public TemplateClassDefinition<T> getDefinition() {
    return definition;
  }

  public String getMaterializedClassName() {
    return fqcn;
  }

  public static <T> CodeGenerator<T> get(TemplateClassDefinition<T> definition,
      FunctionImplementationRegistry funcRegistry) {
    return get(definition, funcRegistry, null);
  }

  public static <T> CodeGenerator<T> get(TemplateClassDefinition<T> definition,
      FunctionImplementationRegistry funcRegistry, OptionManager optionManager) {
    return new CodeGenerator<T>(definition, funcRegistry, optionManager);
  }

  public static <T> ClassGenerator<T> getRoot(TemplateClassDefinition<T> definition,
      FunctionImplementationRegistry funcRegistry, OptionManager optionManager) {
    return get(definition, funcRegistry, optionManager).getRoot();
  }

  public static <T> ClassGenerator<T> getRoot(MappingSet mappingSet, TemplateClassDefinition<T> definition,
      FunctionImplementationRegistry funcRegistry, OptionManager optionManager) {
    return get(mappingSet, definition, funcRegistry, optionManager).getRoot();
  }

  public static <T> CodeGenerator<T> get(MappingSet mappingSet, TemplateClassDefinition<T> definition,
      FunctionImplementationRegistry funcRegistry, OptionManager optionManager) {
    return new CodeGenerator<T>(mappingSet, definition, funcRegistry, optionManager);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((definition == null) ? 0 : definition.hashCode());
    result = prime * result + ((generifiedCode == null) ? 0 : generifiedCode.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj){
      return true;
    }
    if (obj == null){
      return false;
    }
    if (getClass() != obj.getClass()){
      return false;
    }
    CodeGenerator<?> other = (CodeGenerator<?>) obj;
    if (definition == null) {
      if (other.definition != null){
        return false;
      }
    } else if (!definition.equals(other.definition)){
      return false;
    }
    if (generifiedCode == null) {
      if (other.generifiedCode != null){
        return false;
      }

    } else if (!generifiedCode.equals(other.generifiedCode)){
      return false;
    }
    return true;
  }

}
