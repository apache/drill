/*
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
package org.apache.drill.common.util;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.scopedpool.ScopedClassPoolRepository;
import javassist.scopedpool.ScopedClassPoolRepositoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuavaPatcher {
  private static final Logger logger = LoggerFactory.getLogger(GuavaPatcher.class);

  private static boolean patchingAttempted;

  public static synchronized void patch() {
    if (!patchingAttempted) {
      patchingAttempted = true;
      patchStopwatch();
      patchCloseables();
      patchPreconditions();
    }
  }

  /**
   * Makes Guava stopwatch look like the old version for compatibility with hbase-server (for test purposes).
   */
  private static void patchStopwatch() {
    try {
      ClassPool cp = getClassPool();
      CtClass cc = cp.get("com.google.common.base.Stopwatch");

      // Expose the constructor for Stopwatch for old libraries who use the pattern new Stopwatch().start().
      for (CtConstructor c : cc.getConstructors()) {
        if (!Modifier.isStatic(c.getModifiers())) {
          c.setModifiers(Modifier.PUBLIC);
        }
      }

      // Add back the Stopwatch.elapsedMillis() method for old consumers.
      CtMethod newMethod = CtNewMethod.make(
          "public long elapsedMillis() { return elapsed(java.util.concurrent.TimeUnit.MILLISECONDS); }", cc);
      cc.addMethod(newMethod);

      // Load the modified class instead of the original.
      cc.toClass();

      logger.info("Google's Stopwatch patched for old HBase Guava version.");
    } catch (Exception e) {
      logger.warn("Unable to patch Guava classes.", e);
    }
  }

  private static void patchCloseables() {
    try {
      ClassPool cp = getClassPool();
      CtClass cc = cp.get("com.google.common.io.Closeables");

      // Add back the Closeables.closeQuietly() method for old consumers.
      CtMethod newMethod = CtNewMethod.make(
          "public static void closeQuietly(java.io.Closeable closeable) { try{closeable.close();}catch(Exception e){} }",
          cc);
      cc.addMethod(newMethod);

      // Load the modified class instead of the original.
      cc.toClass();

      logger.info("Google's Closeables patched for old HBase Guava version.");
    } catch (Exception e) {
      logger.warn("Unable to patch Guava classes.", e);
    }
  }

  /**
   * Patches Guava Preconditions with missing methods, added for the Apache Iceberg.
   */
  private static void patchPreconditions() {
    try {
      ClassPool cp = getClassPool();
      CtClass cc = cp.get("com.google.common.base.Preconditions");

      // Javassist does not support varargs, generate methods with varying number of arguments
      int startIndex = 1;
      int endIndex = 5;

      List<String> methodsWithVarargsTemplates = Arrays.asList(
          "public static void checkArgument(boolean expression, String errorMessageTemplate, %s) {\n"
              + "    if (!expression) {\n"
              + "      throw new IllegalArgumentException(format(errorMessageTemplate, new Object[] { %s }));\n"
              + "    }\n"
              + "  }",

          "public static Object checkNotNull(Object reference, String errorMessageTemplate, %s) {\n"
              + "    if (reference == null) {\n"
              + "      throw new NullPointerException(format(errorMessageTemplate, new Object[] { %s }));\n"
              + "    } else {\n"
              + "      return reference;\n"
              + "    }\n"
              + "  }",

          "public static void checkState(boolean expression, String errorMessageTemplate, %s) {\n"
              + "    if (!expression) {\n"
              + "      throw new IllegalStateException(format(errorMessageTemplate, new Object[] { %s }));\n"
              + "    }\n"
              + "  }"
      );

      List<String> methodsWithPrimitives = Arrays.asList(
          "public static void checkArgument(boolean expression, String errorMessageTemplate, int arg1) {\n"
              + "    if (!expression) {\n"
              + "      throw new IllegalArgumentException(format(errorMessageTemplate, new Object[] { new Integer(arg1) }));\n"
              + "    }\n"
              + "  }",
          "public static void checkArgument(boolean expression, String errorMessageTemplate, long arg1) {\n"
              + "    if (!expression) {\n"
              + "      throw new IllegalArgumentException(format(errorMessageTemplate, new Object[] { new Long(arg1) }));\n"
              + "    }\n"
              + "  }",
          "public static void checkArgument(boolean expression, String errorMessageTemplate, long arg1, long arg2) {\n"
              + "    if (!expression) {\n"
              + "      throw new IllegalArgumentException(format(errorMessageTemplate, new Object[] { new Long(arg1), new Long(arg2)}));\n"
              + "    }\n"
              + "  }",
          "public static Object checkNotNull(Object reference, String errorMessageTemplate, int arg1) {\n"
              + "    if (reference == null) {\n"
              + "      throw new NullPointerException(format(errorMessageTemplate, new Object[] { new Integer(arg1) }));\n"
              + "    } else {\n"
              + "      return reference;\n"
              + "    }\n"
              + "  }",
          "public static void checkState(boolean expression, String errorMessageTemplate, int arg1) {\n"
              + "    if (!expression) {\n"
              + "      throw new IllegalStateException(format(errorMessageTemplate, new Object[] { new Integer(arg1) }));\n"
              + "    }\n"
              + "  }"
    );

      List<String> newMethods = IntStream.rangeClosed(startIndex, endIndex)
          .mapToObj(
              i -> {
                List<String> args = IntStream.rangeClosed(startIndex, i)
                    .mapToObj(j -> "arg" + j)
                    .collect(Collectors.toList());

                String methodInput = args.stream()
                    .map(arg -> "Object " + arg)
                    .collect(Collectors.joining(", "));

                String arrayInput = String.join(", ", args);

                return methodsWithVarargsTemplates.stream()
                    .map(method -> String.format(method, methodInput, arrayInput))
                    .collect(Collectors.toList());
              })
          .flatMap(Collection::stream)
          .collect(Collectors.toList());

      newMethods.addAll(methodsWithPrimitives);

      for (String method : newMethods) {
        CtMethod newMethod = CtNewMethod.make(method, cc);
        cc.addMethod(newMethod);
      }

      cc.toClass();
      logger.info("Google's Preconditions were patched to hold new methods.");
    } catch (Exception e) {
      logger.warn("Unable to patch Guava classes.", e);
    }
  }

  /**
   * Returns {@link javassist.scopedpool.ScopedClassPool} instance which uses the same class loader
   * which was used for loading current class.
   *
   * @return {@link javassist.scopedpool.ScopedClassPool} instance
   */
  private static ClassPool getClassPool() {
    ScopedClassPoolRepository classPoolRepository = ScopedClassPoolRepositoryImpl.getInstance();
    // sets prune flag to false to avoid freezing and pruning classes right after obtaining CtClass instance
    classPoolRepository.setPrune(false);
    return classPoolRepository.createScopedClassPool(GuavaPatcher.class.getClassLoader(), null);
  }
}
