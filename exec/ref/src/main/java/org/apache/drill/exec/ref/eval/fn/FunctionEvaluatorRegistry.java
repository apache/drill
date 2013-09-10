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
package org.apache.drill.exec.ref.eval.fn;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

public class FunctionEvaluatorRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionEvaluatorRegistry.class);

  static Map<String, Constructor<? extends BasicEvaluator>> map;

  static {
    final String scanPackage = "org.apache.drill.exec";

    String s = FilterBuilder.Include.prefix(scanPackage);

    Reflections r = new Reflections(new ConfigurationBuilder().filterInputsBy(new FilterBuilder().include(s))
        .setUrls(ClasspathHelper.forPackage(scanPackage))
        .setScanners(new SubTypesScanner(), new TypeAnnotationsScanner(), new ResourcesScanner()));

    Set<Class<? extends BasicEvaluator>> providerClasses = r.getSubTypesOf(BasicEvaluator.class);
    Map<String, Constructor<? extends BasicEvaluator>> funcs = new HashMap<String, Constructor<? extends BasicEvaluator>>();
    for (Class<? extends BasicEvaluator> c : providerClasses) {
      try {
        FunctionEvaluator annotation = c.getAnnotation(FunctionEvaluator.class);
        if (annotation == null) {
          // only basic evaluator marked with a function evaluator interface will be examinged. 
          continue;
        }

        Constructor<? extends BasicEvaluator> constructor = c.getConstructor(RecordPointer.class, FunctionArguments.class);
//        if (!constructor.isAccessible()) {
//          logger.warn("Unable to register Basic Evaluator {} because is not acccessible.", c);
//          continue;
//        }

        funcs.put(annotation.value(), constructor);
      } catch (NoSuchMethodException e) {
        logger.warn(
                "Unable to register Basic Evaluator {} because it does not have a constructor that accepts arguments of [SimpleRecord, ArgumentEvaluators] as its arguments.",
                c);
      } catch (SecurityException e) {
        logger.warn("Unable to register Basic Evaluator {} because of security exception.", c, e);
      }
    }

    map = Collections.unmodifiableMap(funcs);
  }

  public static BasicEvaluator getEvaluator(String name, FunctionArguments args, RecordPointer record) {
    Constructor<? extends BasicEvaluator> c = map.get(name);
    if (c == null) throw new SetupException(String.format("Unable to find requested basic evaluator %s.", name));
    try {
      try {
        BasicEvaluator e = c.newInstance(record, args);
        return e;
      } catch (InvocationTargetException e) {
        Throwable ex = e.getCause();
        if (ex instanceof SetupException) {
          throw (SetupException) ex;
        } else {
          if(ex instanceof RuntimeException){
            throw (RuntimeException) ex;
          }else{
            throw new SetupException(String.format("Failure while attempting to create a new evaluator of type '%s'.", name));
          }
        }
      }
    } catch (RuntimeException | IllegalAccessException | InstantiationException ex) {
      throw new SetupException(String.format("Failure while attempting to create a new evaluator of type '%s'.", name),
          ex);
    }

  }

  public static void main(String[] args) {
    System.out.println("loaded.");
  }
}
