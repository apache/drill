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

import org.apache.drill.exec.server.DrillbitContext;

/**
 * Injects exceptions at execution time for testing. Any class that wants to simulate exceptions
 * for testing should have it's own private static instance of an injector (similar to the use
 * of loggers).
 *
 * <p>See {@link org.apache.drill.exec.testing.TestExceptionInjection} for examples of
 * use.
 */
public class ExceptionInjector {
  private final Class<?> clazz; // the class that owns this injector

  /**
   * Constructor. Classes should use the static {@link #getInjector()} method to obtain
   * their injector.
   *
   * @param clazz the owning class
   */
  private ExceptionInjector(final Class<?> clazz) {
    this.clazz = clazz;
  }

  /**
   * Create an injector.
   *
   * @param clazz the owning class
   * @return the newly created injector
   */
  public static ExceptionInjector getInjector(final Class<?> clazz) {
    return new ExceptionInjector(clazz);
  }

  /**
   * Get the injector's owning class.
   *
   * @return the injector's owning class
   */
  public Class<?> getSiteClass() {
    return clazz;
  }

  /**
   * Lookup an injection within this class that matches the site description.
   *
   * @param drillbitContext
   * @param desc the site description
   * @return the injection, if there is one; null otherwise
   */
  private ExceptionInjection getInjection(final DrillbitContext drillbitContext, final String desc) {
    final SimulatedExceptions simulatedExceptions = drillbitContext.getSimulatedExceptions();
    final ExceptionInjection exceptionInjection = simulatedExceptions.lookupInjection(drillbitContext, this, desc);
    return exceptionInjection;
  }

  /**
   * Inject (throw) an unchecked exception at this point, if an injection is specified, and it is time
   * for it to be thrown.
   *
   * <p>Implementors use this in their code at a site where they want to simulate an exception
   * during testing.
   *
   * @param drillbitContext
   * @param desc the site description
   * throws the exception specified by the injection, if it is time
   */
  public void injectUnchecked(final DrillbitContext drillbitContext, final String desc) {
    final ExceptionInjection exceptionInjection = getInjection(drillbitContext, desc);
    if (exceptionInjection != null) {
      exceptionInjection.throwUnchecked();
    }
  }

  /**
   * Inject (throw) a checked exception at this point, if an injection is specified, and it is time
   * for it to be thrown.
   *
   * <p>Implementors use this in their code at a site where they want to simulate an exception
   * during testing.
   *
   * @param drillbitContext
   * @param desc the site description
   * @param exceptionClass the expected class of the exception (or a super class of it)
   * @throws T the exception specified by the injection, if it is time
   */
  public <T extends Throwable> void injectChecked(
      final DrillbitContext drillbitContext, final String desc, final Class<T> exceptionClass) throws T {
    final ExceptionInjection exceptionInjection = getInjection(drillbitContext, desc);
    if (exceptionInjection != null) {
      exceptionInjection.throwChecked(exceptionClass);
    }
  }
}
