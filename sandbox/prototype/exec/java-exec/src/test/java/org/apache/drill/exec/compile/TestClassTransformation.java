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

import static org.junit.Assert.*;

import org.apache.drill.exec.exception.ClassTransformationException;
import org.junit.Test;

public class TestClassTransformation {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestClassTransformation.class);

  @Test
  public void testJaninoCompilation() throws ClassTransformationException {
    testBasicClassCompilation(true);
  }
  
  @Test 
  public void testJDKCompilation() throws ClassTransformationException{
    testBasicClassCompilation(false);
  }
  
  private void testBasicClassCompilation(boolean useJanino) throws ClassTransformationException{
    final String output = "hello world, the time is now " + System.currentTimeMillis();

    TemplateClassDefinition<ExampleExternalInterface> def = new TemplateClassDefinition<ExampleExternalInterface>(
        ExampleExternalInterface.class, "org.apache.drill.exec.compile.ExampleTemplate",
        ExampleInternalInterface.class, null);
    
    
    ClassTransformer ct = new ClassTransformer();
    QueryClassLoader loader = new QueryClassLoader(useJanino);
    ExampleExternalInterface instance = ct.getImplementationClassByBody(loader, def,
        "public String getInternalData(){return \"" + output + "\";}");
    System.out.println(String.format("Generated a new class %s that provides the following getData response '%s'.",
        instance.getClass().getCanonicalName(), instance.getData()));
    assertEquals(instance.getData(), output);
  }
}
