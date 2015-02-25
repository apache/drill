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
import java.io.StringWriter;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.testing.SimulatedExceptions.InjectionOptions;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Static methods for constructing exception injections for testing purposes.
 */
public class ExceptionInjectionUtil {
  /**
   * Constructor. Prevent instantiation of static utility class.
   */
  private ExceptionInjectionUtil() {
  }

  /**
   * Add a set of injections to a drillbit.
   *
   * @param drillbit the drillbit
   * @param injections the JSON-specified injections
   */
  public static void setInjections(final Drillbit drillbit, final String injections) {
    final DrillbitContext drillbitContext = drillbit.getContext();
    final OptionValue stringValue = OptionValue.createString(
        OptionValue.OptionType.SYSTEM, ExecConstants.DRILLBIT_EXCEPTION_INJECTIONS, injections);
    final OptionManager optionManager = drillbitContext.getOptionManager();
    optionManager.setOption(stringValue);
  }

  /**
   * Add a set of injections to a drillbit.
   *
   * @param drillbit the drillbit
   * @param injectionOptions the injections, specified using the parsing POJOs
   */
  public static void setInjections(final Drillbit drillbit, final InjectionOptions injectionOptions) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final StringWriter stringWriter = new StringWriter();
    try {
      objectMapper.writeValue(stringWriter, injectionOptions);
    } catch(IOException e) {
      throw new RuntimeException("Couldn't serialize injectionOptions to JSON", e);
    }

    setInjections(drillbit, stringWriter.toString());
  }

  /**
   * Clear all injections on a drillbit.
   *
   * @param drillbit the drillbit
   */
  public static void clearInjections(final Drillbit drillbit) {
    setInjections(drillbit, "");
  }
}
