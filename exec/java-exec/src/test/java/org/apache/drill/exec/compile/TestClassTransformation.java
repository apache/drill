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

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestClassTransformation extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestClassTransformation.class);

  private static final int ITERATION_COUNT = 1;

  private static SessionOptionManager sessionOptions;

  @BeforeClass
  public static void beforeTestClassTransformation() throws Exception {
    sessionOptions = new SessionOptionManager(bit.getContext().getOptionManager());
  }

  @Test
  public void testJaninoClassCompiler() throws Exception {
    logger.debug("Testing JaninoClassCompiler");
    sessionOptions.setOption(OptionValue.createString(OptionType.SESSION, QueryClassLoader.JAVA_COMPILER_OPTION, QueryClassLoader.CompilerPolicy.JANINO.name()));
    QueryClassLoader loader = new QueryClassLoader(sessionOptions);
    for (int i = 0; i < ITERATION_COUNT; i++) {
      compilationInnerClass(loader);
    }
  }

  @Test
  public void testJDKClassCompiler() throws Exception {
    logger.debug("Testing JDKClassCompiler");
    sessionOptions.setOption(OptionValue.createString(OptionType.SESSION, QueryClassLoader.JAVA_COMPILER_OPTION, QueryClassLoader.CompilerPolicy.JDK.name()));
    QueryClassLoader loader = new QueryClassLoader(sessionOptions);
    for (int i = 0; i < ITERATION_COUNT; i++) {
      compilationInnerClass(loader);
    }
  }

  /**
   * Do a test of a three level class to ensure that nested code generators works correctly.
   * @throws Exception
   */
  public void compilationInnerClass(QueryClassLoader loader) throws Exception{
    final TemplateClassDefinition<ExampleInner> template = new TemplateClassDefinition<>(ExampleInner.class, ExampleTemplateWithInner.class);
    CodeGenerator<ExampleInner> cg = CodeGenerator.get(template, bit.getContext().getFunctionImplementationRegistry());

    ClassGenerator<ExampleInner> root = cg.getRoot();
    root.setMappingSet(new MappingSet(new GeneratorMapping("doOutside", null, null, null)));
    root.getSetupBlock().directStatement("System.out.println(\"outside\");");


    ClassGenerator<ExampleInner> inner = root.getInnerGenerator("TheInnerClass");
    inner.setMappingSet(new MappingSet(new GeneratorMapping("doInside", null, null, null)));
    inner.getSetupBlock().directStatement("System.out.println(\"inside\");");

    ClassGenerator<ExampleInner> doubleInner = inner.getInnerGenerator("DoubleInner");
    doubleInner.setMappingSet(new MappingSet(new GeneratorMapping("doDouble", null, null, null)));
    doubleInner.getSetupBlock().directStatement("System.out.println(\"double\");");

    ClassTransformer ct = new ClassTransformer();
    ExampleInner t = ct.getImplementationClass(loader, cg.getDefinition(), cg.generate(), cg.getMaterializedClassName());

    t.doOutside();
    t.doInsideOutside();
  }

}
