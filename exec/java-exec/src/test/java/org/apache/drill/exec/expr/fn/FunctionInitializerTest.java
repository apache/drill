/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Lists;
import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.test.TestTools;
import org.apache.drill.exec.util.JarUtil;
import org.codehaus.janino.Java.CompilationUnit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.class)
@Category(SqlFunctionTest.class)
public class FunctionInitializerTest {

  private static final String CLASS_NAME = "com.drill.udf.CustomLowerFunction";
  private static URLClassLoader classLoader;

  @BeforeClass
  public static void init() throws Exception {
    Path jars = TestTools.WORKING_PATH
      .resolve(TestTools.TEST_RESOURCES)
      .resolve("jars");
    String binaryName = "DrillUDF-1.0.jar";
    String sourceName = JarUtil.getSourceName(binaryName);
    URL[] urls = {jars.resolve(binaryName).toUri().toURL(), jars.resolve(sourceName).toUri().toURL()};
    classLoader = new URLClassLoader(urls);
  }

  @Test
  public void testGetImports() {
    FunctionInitializer functionInitializer = new FunctionInitializer(CLASS_NAME, classLoader);
    List<String> actualImports = functionInitializer.getImports();

    List<String> expectedImports = Lists.newArrayList(
        "import io.netty.buffer.DrillBuf;",
        "import org.apache.drill.exec.expr.DrillSimpleFunc;",
        "import org.apache.drill.exec.expr.annotations.FunctionTemplate;",
        "import org.apache.drill.exec.expr.annotations.Output;",
        "import org.apache.drill.exec.expr.annotations.Param;",
        "import org.apache.drill.exec.expr.holders.VarCharHolder;",
        "import javax.inject.Inject;"
    );

    assertEquals("List of imports should match", expectedImports, actualImports);
  }

  @Test
  public void testGetMethod() {
    FunctionInitializer functionInitializer = new FunctionInitializer(CLASS_NAME, classLoader);
    String actualMethod = functionInitializer.getMethod("eval");
    assertTrue("Method body should match", actualMethod.contains("CustomLowerFunction_eval:"));
  }

  @Test
  public void testConcurrentFunctionBodyLoad() throws Exception {
    final FunctionInitializer spyFunctionInitializer = spy(new FunctionInitializer(CLASS_NAME, classLoader));
    final AtomicInteger counter = new AtomicInteger();

    doAnswer(new Answer<CompilationUnit>() {
      @Override
      public CompilationUnit answer(InvocationOnMock invocation) throws Throwable {
        counter.incrementAndGet();
        return (CompilationUnit) invocation.callRealMethod();
      }
    }).when(spyFunctionInitializer).convertToCompilationUnit(any(Class.class));

    int threadsNumber = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threadsNumber);

    try {
      List<Future<String>> results = executor.invokeAll(Collections.nCopies(threadsNumber, new Callable<String>() {
        @Override
        public String call() {
          return spyFunctionInitializer.getMethod("eval");
        }
      }));

      final Set<String> uniqueResults = new HashSet<>();
      for (Future<String> result : results) {
        uniqueResults.add(result.get());
      }

      assertEquals("All threads should have received the same result", 1, uniqueResults.size());
      assertEquals("Number of function body loads should match", 1, counter.intValue());

    } finally {
      executor.shutdownNow();
    }
  }
}
