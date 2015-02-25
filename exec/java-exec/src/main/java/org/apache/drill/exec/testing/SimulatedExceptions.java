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

import java.io.IOException;
import java.util.HashMap;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tracks the simulated exceptions that will be injected for testing purposes.
 */
public class SimulatedExceptions {
  /**
   * Caches the currently specified ExceptionInjections. Updated when
   * {@link org.apache.drill.exec.ExecConstants.DRILLBIT_EXCEPTION_INJECTIONS} is noticed
   * to have changed.
   */
  private HashMap<InjectionSite, ExceptionInjection> exMap = null;

  /**
   * The string that was parsed to produce exMap; we keep it as a means to quickly detect whether
   * the option string has changed or not between calls to getOption().
   */
  private String exString = null;

  /**
   * POJO used to parse JSON-specified exception injection.
   */
  public static class InjectionOption {
    public String siteClass;
    public String desc;
    public int nSkip;
    public int nFire;
    public String exceptionClass;
  }

  /**
   * POJO used to parse JSON-specified set of exception injections.
   */
  public static class InjectionOptions {
    public InjectionOption injections[];
  }

  /**
   * Look for an exception injection matching the given injector and site description.
   *
   * @param drillbitContext
   * @param injector the injector, which indicates a class
   * @param desc the injection site description
   * @return the exception injection, if there is one for the injector and site; null otherwise
   */
  public synchronized ExceptionInjection lookupInjection(
      final DrillbitContext drillbitContext, final ExceptionInjector injector, final String desc) {
    // get the option string
    final OptionManager optionManager = drillbitContext.getOptionManager();
    final OptionValue optionValue = optionManager.getOption(ExecConstants.DRILLBIT_EXCEPTION_INJECTIONS);
    final String opString = optionValue.string_val;

    // if the option string is empty, there's nothing to inject
    if ((opString == null) || opString.isEmpty()) {
      // clear these in case there used to be something to inject
      exMap = null;
      exString = null;
      return null;
    }

    // if the option string is different from before, recreate the injection map
    if ((exString == null) || (exString != opString) && !exString.equals(opString)) {
      // parse the option string into JSON
      final ObjectMapper objectMapper = new ObjectMapper();
      InjectionOptions injectionOptions;
      try {
        injectionOptions = objectMapper.readValue(opString, InjectionOptions.class);
      } catch(IOException e) {
        throw new RuntimeException("Couldn't parse exception injections", e);
      }

      // create a new map from the option JSON
      exMap = new HashMap<>();
      for(InjectionOption injectionOption : injectionOptions.injections) {
        addToMap(exMap, injectionOption);
      }

      // this is the current set of options in effect
      exString = opString;
    }

    // lookup the request
    final InjectionSite injectionSite = new InjectionSite(injector.getSiteClass(), desc);
    final ExceptionInjection injection = exMap.get(injectionSite);
    return injection;
  }

  /**
   * Adds a single exception injection to the injection map
   *
   * <p>Validates injection options before adding to the map, and throws various exceptions for
   * validation failures.
   *
   * @param exMap the injection map
   * @param injectionOption the option to add
   */
  private static void addToMap(
      final HashMap<InjectionSite, ExceptionInjection> exMap, final InjectionOption injectionOption) {
    Class<?> siteClass;
    try {
      siteClass = Class.forName(injectionOption.siteClass);
    } catch(ClassNotFoundException e) {
      throw new RuntimeException("Injection siteClass not found", e);
    }

    if ((injectionOption.desc == null) || injectionOption.desc.isEmpty()) {
      throw new RuntimeException("Injection desc is null or empty");
    }

    if (injectionOption.nSkip < 0) {
      throw new RuntimeException("Injection nSkip is not non-negative");
    }

    if (injectionOption.nFire <= 0) {
      throw new RuntimeException("Injection nFire is non-positive");
    }

    Class<?> clazz;
    try {
      clazz = Class.forName(injectionOption.exceptionClass);
    } catch(ClassNotFoundException e) {
      throw new RuntimeException("Injected exceptionClass not found", e);
    }

    if (!Throwable.class.isAssignableFrom(clazz)) {
      throw new RuntimeException("Injected exceptionClass is not a Throwable");
    }

    @SuppressWarnings("unchecked")
    final Class<? extends Throwable> exceptionClass = (Class<? extends Throwable>) clazz;

    final InjectionSite injectionSite = new InjectionSite(siteClass, injectionOption.desc);
    final ExceptionInjection exceptionInjection = new ExceptionInjection(
        injectionOption.desc, injectionOption.nSkip, injectionOption.nFire, exceptionClass);
    exMap.put(injectionSite, exceptionInjection);
  }
}
