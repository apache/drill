/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expr.fn.registry;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(SqlFunctionTest.class)
public class FunctionRegistryHolderTest {

  private static final String built_in = "built-in";
  private static final String udf_jar = "DrillUDF-1.0.jar";

  private static Map<String, List<FunctionHolder>> newJars;
  private FunctionRegistryHolder registryHolder;

  @BeforeClass
  public static void init() {
    newJars = Maps.newHashMap();
    FunctionHolder lower = new FunctionHolder("lower", "lower(VARCHAR-REQUIRED)", mock(DrillFuncHolder.class));
    FunctionHolder upper = new FunctionHolder("upper", "upper(VARCHAR-REQUIRED)", mock(DrillFuncHolder.class));
    newJars.put(built_in, Lists.newArrayList(lower, upper));
    FunctionHolder custom_lower = new FunctionHolder("custom_lower", "lower(VARCHAR-REQUIRED)", mock(DrillFuncHolder.class));
    FunctionHolder custom_upper = new FunctionHolder("custom_upper", "custom_upper(VARCHAR-REQUIRED)", mock(DrillFuncHolder.class));
    newJars.put(udf_jar, Lists.newArrayList(custom_lower, custom_upper));
  }

  @Before
  public void setup() {
    resetRegistry();
    fillInRegistry(1);
  }

  @Test
  public void testVersion() {
    resetRegistry();
    long expectedVersion = 0;
    assertEquals("Initial version should be 0", expectedVersion, registryHolder.getVersion());
    registryHolder.addJars(Maps.<String, List<FunctionHolder>>newHashMap(), ++expectedVersion);
    assertEquals("Version can change if no jars were added.", expectedVersion, registryHolder.getVersion());
    fillInRegistry(++expectedVersion);
    assertEquals("Version should have incremented by 1", expectedVersion, registryHolder.getVersion());
    registryHolder.removeJar(built_in);
    assertEquals("Version should have incremented by 1", expectedVersion, registryHolder.getVersion());
    fillInRegistry(++expectedVersion);
    assertEquals("Version should have incremented by 1", expectedVersion, registryHolder.getVersion());
    fillInRegistry(++expectedVersion);
    assertEquals("Version should have incremented by 1", expectedVersion, registryHolder.getVersion());
  }

  @Test
  public void testAddJars() {
    resetRegistry();
    int functionsSize = 0;
    List<String> jars = Lists.newArrayList();
    ListMultimap<String, DrillFuncHolder> functionsWithHolders = ArrayListMultimap.create();
    ListMultimap<String, String> functionsWithSignatures = ArrayListMultimap.create();
    for (Map.Entry<String, List<FunctionHolder>> jar : newJars.entrySet()) {
      jars.add(jar.getKey());
      for (FunctionHolder functionHolder : jar.getValue()) {
        functionsWithHolders.put(functionHolder.getName(), functionHolder.getHolder());
        functionsWithSignatures.put(functionHolder.getName(), functionHolder.getSignature());
        functionsSize++;
      }
    }

    long expectedVersion = 0;
    registryHolder.addJars(newJars, ++expectedVersion);
    assertEquals("Version number should match", expectedVersion, registryHolder.getVersion());
    compareTwoLists(jars, registryHolder.getAllJarNames());
    assertEquals(functionsSize, registryHolder.functionsSize());
    compareListMultimaps(functionsWithHolders, registryHolder.getAllFunctionsWithHolders());
    compareListMultimaps(functionsWithSignatures, registryHolder.getAllFunctionsWithSignatures());
  }

  @Test
  public void testAddTheSameJars() {
    resetRegistry();
    int functionsSize = 0;
    List<String> jars = Lists.newArrayList();
    ListMultimap<String, DrillFuncHolder> functionsWithHolders = ArrayListMultimap.create();
    ListMultimap<String, String> functionsWithSignatures = ArrayListMultimap.create();
    for (Map.Entry<String, List<FunctionHolder>> jar : newJars.entrySet()) {
      jars.add(jar.getKey());
      for (FunctionHolder functionHolder : jar.getValue()) {
        functionsWithHolders.put(functionHolder.getName(), functionHolder.getHolder());
        functionsWithSignatures.put(functionHolder.getName(), functionHolder.getSignature());
        functionsSize++;
      }
    }
    long expectedVersion = 0;
    registryHolder.addJars(newJars, ++expectedVersion);
    assertEquals("Version number should match", expectedVersion, registryHolder.getVersion());
    compareTwoLists(jars, registryHolder.getAllJarNames());
    assertEquals(functionsSize, registryHolder.functionsSize());
    compareListMultimaps(functionsWithHolders, registryHolder.getAllFunctionsWithHolders());
    compareListMultimaps(functionsWithSignatures, registryHolder.getAllFunctionsWithSignatures());

    // adding the same jars should not cause adding duplicates, should override existing jars only
    registryHolder.addJars(newJars, ++expectedVersion);
    assertEquals("Version number should match", expectedVersion, registryHolder.getVersion());
    compareTwoLists(jars, registryHolder.getAllJarNames());
    assertEquals(functionsSize, registryHolder.functionsSize());
    compareListMultimaps(functionsWithHolders, registryHolder.getAllFunctionsWithHolders());
    compareListMultimaps(functionsWithSignatures, registryHolder.getAllFunctionsWithSignatures());
  }

  @Test
  public void testRemoveJar() {
    registryHolder.removeJar(built_in);
    assertFalse("Jar should be absent", registryHolder.containsJar(built_in));
    assertTrue("Jar should be present", registryHolder.containsJar(udf_jar));
    assertEquals("Functions size should match", newJars.get(udf_jar).size(), registryHolder.functionsSize());
  }

  @Test
  public void testGetAllJarNames() {
    ArrayList<String> expectedResult = Lists.newArrayList(newJars.keySet());
    compareTwoLists(expectedResult, registryHolder.getAllJarNames());
  }

  @Test
  public void testGetFunctionNamesByJar() {
    ArrayList<String> expectedResult = Lists.newArrayList();
    for (FunctionHolder functionHolder : newJars.get(built_in)) {
      expectedResult.add(functionHolder.getName());
    }
    compareTwoLists(expectedResult, registryHolder.getFunctionNamesByJar(built_in));
  }

  @Test
  public void testGetAllFunctionsWithHoldersWithVersion() {
    ListMultimap<String, DrillFuncHolder> expectedResult = ArrayListMultimap.create();
    for (List<FunctionHolder> functionHolders : newJars.values()) {
      for(FunctionHolder functionHolder : functionHolders) {
        expectedResult.put(functionHolder.getName(), functionHolder.getHolder());
      }
    }
    AtomicLong version = new AtomicLong();
    compareListMultimaps(expectedResult, registryHolder.getAllFunctionsWithHolders(version));
    assertEquals("Version number should match", version.get(), registryHolder.getVersion());
  }

  @Test
  public void testGetAllFunctionsWithHolders() {
    ListMultimap<String, DrillFuncHolder> expectedResult = ArrayListMultimap.create();
    for (List<FunctionHolder> functionHolders : newJars.values()) {
      for(FunctionHolder functionHolder : functionHolders) {
        expectedResult.put(functionHolder.getName(), functionHolder.getHolder());
      }
    }
    compareListMultimaps(expectedResult, registryHolder.getAllFunctionsWithHolders());
  }

  @Test
  public void testGetAllFunctionsWithSignatures() {
    ListMultimap<String, String> expectedResult = ArrayListMultimap.create();
    for (List<FunctionHolder> functionHolders : newJars.values()) {
      for(FunctionHolder functionHolder : functionHolders) {
        expectedResult.put(functionHolder.getName(), functionHolder.getSignature());
      }
    }
    compareListMultimaps(expectedResult, registryHolder.getAllFunctionsWithSignatures());
  }

  @Test
  public void testGetHoldersByFunctionNameWithVersion() {
    List<DrillFuncHolder> expectedResult = Lists.newArrayList();
    for (List<FunctionHolder> functionHolders : newJars.values()) {
      for (FunctionHolder functionHolder : functionHolders) {
        if ("lower".equals(functionHolder.getName())) {
          expectedResult.add(functionHolder.getHolder()) ;
        }
      }
    }
    assertFalse(expectedResult.isEmpty());
    AtomicLong version = new AtomicLong();
    compareTwoLists(expectedResult, registryHolder.getHoldersByFunctionName("lower", version));
    assertEquals("Version number should match", version.get(), registryHolder.getVersion());
  }

  @Test
  public void testGetHoldersByFunctionName() {
    List<DrillFuncHolder> expectedResult = Lists.newArrayList();
    for (List<FunctionHolder> functionHolders : newJars.values()) {
      for (FunctionHolder functionHolder : functionHolders) {
        if ("lower".equals(functionHolder.getName())) {
          expectedResult.add(functionHolder.getHolder()) ;
        }
      }
    }
    assertFalse(expectedResult.isEmpty());
    compareTwoLists(expectedResult, registryHolder.getHoldersByFunctionName("lower"));
  }

  @Test
  public void testContainsJar() {
    assertTrue("Jar should be present in registry holder", registryHolder.containsJar(built_in));
    assertFalse("Jar should be absent in registry holder", registryHolder.containsJar("unknown.jar"));
  }

  @Test
  public void testFunctionsSize() {
    int count = 0;
    for (List<FunctionHolder> functionHolders : newJars.values()) {
      count += functionHolders.size();
    }
    assertEquals("Functions size should match", count, registryHolder.functionsSize());
  }

  @Test
  public void testJarNameByFunctionSignature() {
    FunctionHolder functionHolder = newJars.get(built_in).get(0);
    assertEquals("Jar name should match",
        built_in, registryHolder.getJarNameByFunctionSignature(functionHolder.getName(), functionHolder.getSignature()));
    assertNull("Jar name should be null",
        registryHolder.getJarNameByFunctionSignature("unknown_function", "unknown_function(unknown-input)"));
  }

  private void resetRegistry() {
    registryHolder = new FunctionRegistryHolder();
  }

  private void fillInRegistry(long version) {
    registryHolder.addJars(newJars, version);
  }

  private <T> void compareListMultimaps(ListMultimap<String, T> lm1, ListMultimap<String, T> lm2) {
    Map<String, Collection<T>> m1 = lm1.asMap();
    Map<String, Collection<T>> m2 = lm2.asMap();
    assertEquals("Multimaps size should match", m1.size(), m2.size());
    for (Map.Entry<String, Collection<T>> entry : m1.entrySet()) {
      try {
        compareTwoLists(Lists.newArrayList(entry.getValue()), Lists.newArrayList(m2.get(entry.getKey())));
      } catch (AssertionError e) {
        throw new AssertionError("Multimaps values should match", e);
      }
    }
  }

  private <T> void compareTwoLists(List<T> l1, List<T> l2) {
    assertEquals("Lists size should match", l1.size(), l2.size());
    for (T item : l1) {
      assertTrue("Two lists should have the same values", l2.contains(item));
    }
  }

}
