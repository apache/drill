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
package org.apache.drill.exec.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.testing.SimulatedExceptions.InjectionOption;
import org.apache.drill.exec.testing.SimulatedExceptions.InjectionOptions;
import org.junit.Test;

public class TestExceptionInjection extends BaseTestQuery {
  private final static String NO_THROW_FAIL = "Didn't throw expected exception";

  /**
   * Class whose methods we want to simulate exceptions at run-time for testing
   * purposes.
   */
  public static class DummyClass {
    private final static ExceptionInjector injector = ExceptionInjector.getInjector(DummyClass.class);
    private final DrillbitContext drillbitContext;

    public DummyClass(final DrillbitContext drillbitContext) {
      this.drillbitContext = drillbitContext;
    }

    /**
     * Method that injects an unchecked exception with the given site description.
     *
     * @param desc the injection site description
     */
    public void descPassthroughMethod(final String desc) {
      // ... code ...

      // simulated unchecked exception
      injector.injectUnchecked(drillbitContext, desc);

      // ... code ...
    }

    public final static String THROWS_IOEXCEPTION = "<<throwsIOException>>";

    /**
     * Method that injects an IOException with a site description of THROWS_IOEXCEPTION.
     *
     * @throws IOException
     */
    public void throwsIOException() throws IOException {
      // ... code ...

      // simulated IOException
      injector.injectChecked(drillbitContext, THROWS_IOEXCEPTION, IOException.class);

      // ... code ...
    }
  }

  @SuppressWarnings("static-method")
  @Test
  public void testNoInjection() throws Exception {
    test("select * from sys.drillbits");
  }

  private static void setInjections(final String jsonInjections) {
    for(Drillbit bit : bits) {
      ExceptionInjectionUtil.setInjections(bit, jsonInjections);
    }
  }

  @SuppressWarnings("static-method")
  @Test
  public void testEmptyInjection() throws Exception {
    setInjections("{\"injections\":[]}");
    test("select * from sys.drillbits");
  }

  /**
   * Assert that DummyClass.descPassThroughMethod does indeed throw the expected exception.
   *
   * @param dummyClass the instance of DummyClass
   * @param exceptionClassName the expected exception
   * @param exceptionDesc the expected exception site description
   */
  private static void assertPassthroughThrows(
      final DummyClass dummyClass, final String exceptionClassName, final String exceptionDesc) {
    try {
      dummyClass.descPassthroughMethod(exceptionDesc);
      fail(NO_THROW_FAIL);
    } catch(Exception e) {
      assertEquals(exceptionClassName, e.getClass().getName());
      assertEquals(exceptionDesc, e.getMessage());
    }
  }

  @SuppressWarnings("static-method")
  @Test
  public void testUncheckedStringInjection() {
    // set injections via a string
    final String exceptionDesc = "<<injected from descPassthroughMethod()>>";
    final String exceptionClassName = "java.lang.RuntimeException";
    final String jsonString = "{\"injections\":[{"
        + "\"siteClass\":\"org.apache.drill.exec.testing.TestExceptionInjection$DummyClass\","
        + "\"desc\":\"" + exceptionDesc + "\","
        + "\"nSkip\":0,"
        + "\"nFire\":1,"
        + "\"exceptionClass\":\"" + exceptionClassName + "\""
        + "}]}";
    setInjections(jsonString);

    // test that the exception gets thrown
    final DummyClass dummyClass = new DummyClass(bits[0].getContext());
    assertPassthroughThrows(dummyClass, exceptionClassName, exceptionDesc);
  }

  private static InjectionOptions buildDefaultJson() {
    final InjectionOption injectionOption = new InjectionOption();
    injectionOption.siteClass = "org.apache.drill.exec.testing.TestExceptionInjection$DummyClass";
    injectionOption.desc = DummyClass.THROWS_IOEXCEPTION;
    injectionOption.nSkip = 0;
    injectionOption.nFire = 1;
    injectionOption.exceptionClass = "java.io.IOException";
    final InjectionOptions injectionOptions = new InjectionOptions();
    injectionOptions.injections = new InjectionOption[1];
    injectionOptions.injections[0] = injectionOption;
    return injectionOptions;
  }

  @SuppressWarnings("static-method")
  @Test
  public void testCheckedJsonInjection() {
    // set the injection via the parsing POJOs
    final InjectionOptions injectionOptions = buildDefaultJson();
    ExceptionInjectionUtil.setInjections(bits[0], injectionOptions);

    // test that the expected exception (checked) gets thrown
    final DummyClass dummyClass = new DummyClass(bits[0].getContext());
    try {
      dummyClass.throwsIOException();
      fail(NO_THROW_FAIL);
    } catch(IOException e) {
      assertEquals(DummyClass.THROWS_IOEXCEPTION, e.getMessage());
    }
  }

  @SuppressWarnings("static-method")
  @Test
  public void testSkipAndLimit() {
    final String passthroughDesc = "<<injected from descPassthrough>>";
    final InjectionOptions injectionOptions = buildDefaultJson();
    final InjectionOption injectionOption = injectionOptions.injections[0];
    injectionOption.desc = passthroughDesc;
    injectionOption.nSkip = 7;
    injectionOption.nFire = 3;
    injectionOption.exceptionClass = RuntimeException.class.getName();
    ExceptionInjectionUtil.setInjections(bits[0], injectionOptions);

    final DummyClass dummyClass = new DummyClass(bits[0].getContext());

    // these shouldn't throw
    for(int i = 0; i < injectionOption.nSkip; ++i) {
      dummyClass.descPassthroughMethod(passthroughDesc);
    }

    // these should throw
    for(int i = 0; i < injectionOption.nFire; ++i) {
      assertPassthroughThrows(dummyClass, injectionOption.exceptionClass, passthroughDesc);
    }

    // this shouldn't throw
    dummyClass.descPassthroughMethod(passthroughDesc);
  }
}
