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
package org.apache.drill.exec.store.openTSDB;

import org.apache.drill.exec.store.openTSDB.schema.OpenTSDBSchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

class SizeEstimator {

  private static final Logger logger = LoggerFactory.getLogger(SizeEstimator.class);

  /**
   * The state of an ongoing size estimation. Contains a stack of objects to visit as well as an
   * IdentityHashMap of visited objects, and provides utility methods for enqueueing new objects
   * to visit.
   */
  private static class SearchState {

    private final IdentityHashMap<Object, Object> visited;
    final LinkedList<Object> stack = new LinkedList<>();
    long size = 0L;

    SearchState(IdentityHashMap<Object, Object> visited) {
      this.visited = visited;
    }

    void enqueue(Object obj) {
      if (obj != null && !visited.containsKey(obj)) {
        visited.put(obj, null);
        stack.add(obj);
      }
    }

    boolean isFinished() {
      return stack.isEmpty();
    }

    Object dequeue() {
      Object elem = stack.removeLast();
      return elem;
    }
  }

  /**
   * Cached information about each class. We remember two things: the "shell size" of the class
   * (size of all non-static fields plus the java.lang.Object size), and any fields that are
   * pointers to objects.
   */
  private static class ClassInfo {
    private final long shellSize;
    private final List<Field> pointerFields;

    ClassInfo(final long shellSize, final List<Field> pointerFields) {
      this.shellSize = shellSize;
      this.pointerFields = pointerFields;
    }

    long getShellSize() {
      return shellSize;
    }

    List<Field> getPointerFields() {
      return pointerFields;
    }
  }

  // Sizes of primitive types
  private static final int BYTE_SIZE    = 1;
  private static final int BOOLEAN_SIZE = 1;
  private static final int CHAR_SIZE    = 2;
  private static final int SHORT_SIZE   = 2;
  private static final int INT_SIZE     = 4;
  private static final int LONG_SIZE    = 8;
  private static final int FLOAT_SIZE   = 4;
  private static final int DOUBLE_SIZE  = 8;

  // Alignment boundary for objects
  // TODO: Is this arch dependent ?
  private static final int ALIGN_SIZE = 8;

  // A cache of ClassInfo objects for each class
  private static final ConcurrentHashMap<Class<?>, ClassInfo> classInfos = new ConcurrentHashMap<>();

  // Object and pointer sizes are arch dependent
  private static boolean is64bit = false;

  // Size of an object reference
  // Based on https://wikis.oracle.com/display/HotSpotInternals/CompressedOops
  private static boolean isCompressedOops = false;
  private static int pointerSize = 4;

  // Minimum size of a java.lang.Object
  private static int objectSize = 8;

  static {
    // Sets object size, pointer size based on architecture and CompressedOops settings
    // from the JVM.
    is64bit = System.getProperty("os.arch").contains("64");
    isCompressedOops = getIsCompressedOops();

    objectSize = !is64bit ? 8 : (!isCompressedOops ? 16 : 12);
    pointerSize = (is64bit && !isCompressedOops) ? 8 : 4;
    classInfos.clear();
    classInfos.put(Object.class, new ClassInfo(objectSize, Collections.emptyList()));
  }

  private static boolean getIsCompressedOops() {
    // This is only used by tests to override the detection of compressed oops. The test
    // actually uses a system property instead of a SparkConf, so we'll stick with that.
    if (System.getProperty("spark.test.useCompressedOops") != null) {
      return Boolean.getBoolean("spark.test.useCompressedOops");
    }

    try {
      final String hotSpotMBeanName = "com.sun.management:type=HotSpotDiagnostic";
      final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

      // NOTE: This should throw an exception in non-Sun JVMs
      final Class<?> hotSpotMBeanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
      final Method getVMMethod = hotSpotMBeanClass.getDeclaredMethod("getVMOption",
        Class.forName("java.lang.String"));

      final Object bean = ManagementFactory.newPlatformMXBeanProxy(server,
        hotSpotMBeanName, hotSpotMBeanClass);
      // TODO: We could use reflection on the VMOption returned ?
      return getVMMethod.invoke(bean, "UseCompressedOops").toString().contains("true");
    } catch(Exception e) {
      // Guess whether they've enabled UseCompressedOops based on whether maxMemory < 32 GB
      final boolean guess = Runtime.getRuntime().maxMemory() < (32L*1024*1024*1024);
      logger.warn("Failed to check whether UseCompressedOops is set; assuming {}", guess);
      return guess;
    }
  }

  private long primitiveSize(Class<?> cls) {
    if (cls == byte.class) {
      return BYTE_SIZE;
    } else if (cls == boolean.class) {
      return BOOLEAN_SIZE;
    } else if (cls == char.class) {
      return CHAR_SIZE;
    } else if (cls == short.class) {
      return SHORT_SIZE;
    } else if (cls == int.class) {
      return INT_SIZE;
    } else if (cls == long.class) {
      return LONG_SIZE;
    } else if (cls == float.class) {
      return FLOAT_SIZE;
    } else if (cls == double.class) {
      return DOUBLE_SIZE;
    } else {
      throw new IllegalArgumentException(
        "Non-primitive class " + cls + " passed to primitiveSize()");
    }
  }

  private long alignSize(long size) {
    final long rem = size % ALIGN_SIZE;
    return (rem == 0) ? size : (size + ALIGN_SIZE - rem);
  }
}
