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
package org.apache.drill.common.scanner;

import static java.util.Arrays.asList;
import static java.util.Collections.sort;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.persistence.AnnotationDescriptor;
import org.apache.drill.common.scanner.persistence.FieldDescriptor;
import org.apache.drill.common.scanner.persistence.AnnotatedClassDescriptor;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.fn.impl.testing.GeneratorFunctions.IncreasingBigInt;
import org.apache.drill.exec.fn.impl.testing.GeneratorFunctions.RandomBigIntGauss;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.junit.Assert;
import org.junit.Test;

public class TestClassPathScanner {

  @SafeVarargs
  final private <T extends Comparable<? super T>> void assertListEqualsUnordered(Collection<T> list, T... expected) {
    List<T> expectedList = asList(expected);
    sort(expectedList);
    List<T> gotList = new ArrayList<>(list);
    sort(gotList);
    assertEquals(expectedList.toString(), gotList.toString());
  }

  @Test
  public void test() throws Exception {
    ScanResult result = ClassPathScanner.fromPrescan(DrillConfig.create());
    // if the build has run properly. BuildTimeScan.REGISTRY_FILE was created with a prescan
//    assertListEqualsUnordered(result.getPrescannedPackages(),
//      "org.apache.drill.common.logical",
//      "org.apache.drill.exec.expr",
//      "org.apache.drill.exec.physical.base",
//      "org.apache.drill.exec.expr.fn.impl",
//      "org.apache.drill.exec.physical.impl",
//      "org.apache.drill.exec.rpc.user.security",
//      "org.apache.drill.exec.store",
//      "org.apache.drill.exec.store.mock",
//      "org.apache.drill.exec.physical.config",
//      "org.apache.drill.storage"
//    );
//    // this is added in the unit test folder that was not scanned so far
//    assertListEqualsUnordered(result.getScannedPackages(),
//      "org.apache.drill.exec.testing",
//      "org.apache.drill.exec.fn.impl.testing",
//      "org.apache.drill.exec.rpc.user.security.testing"
//    );
    List<AnnotatedClassDescriptor> functions = result.getAnnotatedClasses();
    Set<String> scanned = new HashSet<>();
    AnnotatedClassDescriptor functionRandomBigIntGauss = null;
    for (AnnotatedClassDescriptor function : functions) {
      assertTrue(function.getClassName() + " scanned twice", scanned.add(function.getClassName()));
      if (function.getClassName().equals(RandomBigIntGauss.class.getName())) {
        functionRandomBigIntGauss = function;
      }
    }
    if (functionRandomBigIntGauss == null) {
      Assert.fail("functionRandomBigIntGauss not found");
    }
    // TODO: use Andrew's randomized test framework to verify a subset of the functions
    for (AnnotatedClassDescriptor function : functions) {
      Class<?> c = Class.forName(function.getClassName(), false, this.getClass().getClassLoader());

      Field[] fields = c.getDeclaredFields();
      assertEquals("fields count for " + function, fields.length, function.getFields().size());
      for (int i = 0; i < fields.length; i++) {
        FieldDescriptor fieldDescriptor = function.getFields().get(i);
        Field field = fields[i];
        assertEquals(
            "Class fields:\n" + Arrays.toString(fields) + "\n != \nDescriptor fields:\n" + function.getFields(),
            field.getName(), fieldDescriptor.getName());
        verifyAnnotations(field.getDeclaredAnnotations(), fieldDescriptor.getAnnotations());
        assertEquals(field.getType(), fieldDescriptor.getFieldClass());
      }

      Annotation[] annotations = c.getDeclaredAnnotations();
      List<AnnotationDescriptor> scannedAnnotations = function.getAnnotations();
      verifyAnnotations(annotations, scannedAnnotations);
      FunctionTemplate bytecodeAnnotation = function.getAnnotationProxy(FunctionTemplate.class);
      FunctionTemplate reflectionAnnotation = c.getAnnotation(FunctionTemplate.class);
      assertEquals(reflectionAnnotation.name(), bytecodeAnnotation.name());
      assertArrayEquals(reflectionAnnotation.names(), bytecodeAnnotation.names());
      assertEquals(reflectionAnnotation.scope(), bytecodeAnnotation.scope());
      assertEquals(reflectionAnnotation.nulls(), bytecodeAnnotation.nulls());
      assertEquals(reflectionAnnotation.isBinaryCommutative(), bytecodeAnnotation.isBinaryCommutative());
      assertEquals(reflectionAnnotation.desc(), bytecodeAnnotation.desc());
      assertEquals(reflectionAnnotation.costCategory(), bytecodeAnnotation.costCategory());
    }
    for (String baseType : result.getScannedClasses()) {
      validateType(result, baseType);
    }
    assertTrue(result.getImplementations(PhysicalOperator.class).size() > 0);
    assertTrue(result.getImplementations(DrillFunc.class).size() > 0);
  }

  private <T> void validateType(ScanResult result, String baseType) throws ClassNotFoundException {
    if (baseType.startsWith("org.apache.hadoop.hive")) {
      return;
    }
    @SuppressWarnings("unchecked")
    Class<T> baseClass = (Class<T>)Class.forName(baseType);
    Set<Class<? extends T>> impls = result.getImplementations(baseClass);
    if (impls != null) {
      for (Class<? extends T> impl : impls) {
        assertTrue(impl + " extends " + baseType, baseClass.isAssignableFrom(impl));
      }
    }
  }

  private void verifyAnnotations(Annotation[] annotations, List<AnnotationDescriptor> scannedAnnotations) throws Exception {
    assertEquals(Arrays.toString(annotations) + " expected but got " + scannedAnnotations, annotations.length, scannedAnnotations.size());
    for (int i = 0; i < annotations.length; i++) {
      Annotation annotation = annotations[i];
      AnnotationDescriptor scannedAnnotation = scannedAnnotations.get(i);
      Class<? extends Annotation> annotationType = annotation.annotationType();
      assertEquals(annotationType.getName(), scannedAnnotation.getAnnotationType());
      if (annotation instanceof FunctionTemplate) {
        FunctionTemplate ft = (FunctionTemplate)annotation;
        if (ft.name() != null && !ft.name().equals("")) {
          assertEquals(ft.name(), scannedAnnotation.getSingleValue("name"));
        }
      }
      // generally verify all properties
      Annotation proxy = scannedAnnotation.getProxy(annotationType);
      Method[] declaredMethods = annotationType.getDeclaredMethods();
      for (Method method : declaredMethods) {
        if (method.getParameterTypes().length == 0) {
          Object reflectValue = method.invoke(annotation);
          Object byteCodeValue = method.invoke(proxy);
          String message = annotationType.getSimpleName() + "." + method.getName();
          if (method.getReturnType().isArray()) {
            assertArrayEquals(message, (Object[])reflectValue, (Object[])byteCodeValue);
          } else {
            assertEquals(message, reflectValue, byteCodeValue);
          }
        }
      }
    }
  }

}
