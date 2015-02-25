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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Injection for a single exception. Specifies how many times to inject it, and how many times to skip
 * injecting it before the first injection. This class is used internally for tracking injected
 * exceptions; injected exceptions are specified via the
 * {@link org.apache.drill.exec.ExecConstants#DRILLBIT_EXCEPTION_INJECTIONS} system option.
 */
public class ExceptionInjection {
  private final String desc; // description of the injection site

  private final AtomicInteger nSkip; // the number of times to skip the injection; starts >= 0
  private final AtomicInteger nThrow; // the number of times to do the injection, after any skips; starts > 0

  private final Class<? extends Throwable> exceptionClass;

  /**
   * Constructor.
   *
   * @param desc description of the injection site; useful for multiple injections in a single class
   * @param nSkip non-negative number of times to skip injecting the exception
   * @param nFire positive number of times to inject the exception
   * @param exceptionClass
   */
  public ExceptionInjection(final String desc, final int nSkip, final int nFire,
      final Class<? extends Throwable> exceptionClass) {
    this.desc = desc;
    this.nSkip = new AtomicInteger(nSkip);
    this.nThrow = new AtomicInteger(nFire);
    this.exceptionClass = exceptionClass;
  }

  /**
   * Constructs the exception to throw, if it is time to throw it.
   *
   * @return the exception to throw, or null if it isn't time to throw it
   */
  private Throwable constructException() {
    final int remainingSkips = nSkip.decrementAndGet();
    if (remainingSkips >= 0) {
      return null;
    }

    final int remainingFirings = nThrow.decrementAndGet();
    if (remainingFirings < 0) {
      return null;
    }

    // if we get here, we should throw the specified exception
    Constructor<?> constructor;
    try {
      constructor = exceptionClass.getConstructor(String.class);
    } catch(NoSuchMethodException e) {
      throw new RuntimeException("No constructor found that takes a single String argument");
    }

    Throwable throwable;
    try {
      throwable = (Throwable) constructor.newInstance(desc);
    } catch(InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new IllegalStateException("Couldn't construct exception instance", e);
    }

    return throwable;
  }

  /**
   * Throw the unchecked exception specified by this injection.
   *
   * @throws IllegalStateException if it's time to throw, and the injection specified a checked exception
   */
  public void throwUnchecked() {
    final Throwable throwable = constructException();
    if (throwable == null) {
      return;
    }

    if (throwable instanceof RuntimeException) {
      final RuntimeException e = (RuntimeException) throwable;
      throw e;
    }
    if (throwable instanceof Error) {
      final Error e = (Error) throwable;
      throw e;
    }

    throw new IllegalStateException("throwable was not an unchecked exception");
  }

  /**
   * Throw the checked exception specified by this injection.
   *
   * @param exceptionClass the class of the exception to throw
   * @throws T if it is time to throw the exception
   * @throws IllegalStateException if it is time to throw the exception, and the exception's class
   *   is incompatible with the class specified by the injection
   */
  public <T extends Throwable> void throwChecked(final Class<T> exceptionClass) throws T {
    final Throwable throwable = constructException();
    if (throwable == null) {
      return;
    }

    if (exceptionClass.isAssignableFrom(throwable.getClass())) {
      final T exception = exceptionClass.cast(throwable);
      throw exception;
    }

    throw new IllegalStateException("Constructed Throwable(" + throwable.getClass().getName()
        + ") is incompatible with exceptionClass("+ exceptionClass.getName() + ")");
  }
}
