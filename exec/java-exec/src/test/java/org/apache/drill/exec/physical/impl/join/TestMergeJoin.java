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
package org.apache.drill.exec.physical.impl.join;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.compile.CodeCompilerTestFactory;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.SimpleRootExec;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Ignore;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import mockit.Injectable;
import mockit.NonStrictExpectations;


public class TestMergeJoin extends PopUnitTestBase {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestMergeJoin.class);
  private final DrillConfig c = DrillConfig.create();

  @Test
  @Ignore // this doesn't have a sort.  it also causes an infinite loop.  these may or may not be related.
  public void simpleEqualityJoin(@Injectable final DrillbitContext bitContext,
                                 @Injectable UserServer.UserClientConnection connection) throws Throwable {

    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getConfig(); result = c;
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    final PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/join/merge_join.json"), Charsets.UTF_8));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int totalRecordCount = 0;
    while (exec.next()) {
      totalRecordCount += exec.getRecordCount();
      for (final ValueVector v : exec) {
        System.out.print("[" + v.getField().toExpr() + "]        ");
      }
      System.out.println("\n");
      for (int valueIdx = 0; valueIdx < exec.getRecordCount(); valueIdx++) {
        final List<Object> row = new ArrayList();
        for (final ValueVector v : exec) {
           row.add(v.getAccessor().getObject(valueIdx));
        }
        for (final Object cell : row) {
          if (cell == null) {
            System.out.print("<null>          ");
            continue;
          }
          final int len = cell.toString().length();
          System.out.print(cell);
          for (int i = 0; i < (14 - len); ++i) {
            System.out.print(" ");
          }
        }
        System.out.println();
      }
      System.out.println();
    }
    assertEquals(100, totalRecordCount);
    System.out.println("Total Record Count: " + totalRecordCount);
    if (context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }

  @Test
  @Ignore
  public void orderedEqualityLeftJoin(@Injectable final DrillbitContext bitContext,
                                      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getConfig(); result = c;
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c, new StoragePluginRegistry(bitContext));
    final PhysicalPlan plan = reader.readPhysicalPlan(
        Files.toString(
            FileUtils.getResourceAsFile("/join/merge_single_batch.json"), Charsets.UTF_8)
            .replace("#{LEFT_FILE}", FileUtils.getResourceAsFile("/join/merge_single_batch.left.json").toURI().toString())
            .replace("#{RIGHT_FILE}", FileUtils.getResourceAsFile("/join/merge_single_batch.right.json").toURI().toString()));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int totalRecordCount = 0;
    while (exec.next()) {
      totalRecordCount += exec.getRecordCount();
      System.out.println("got next with record count: " + exec.getRecordCount() + " (total: " + totalRecordCount + "):");
      System.out.println("       t1                 t2");

      for (int valueIdx = 0; valueIdx < exec.getRecordCount(); valueIdx++) {
        final List<Object> row = Lists.newArrayList();
        for (final ValueVector v : exec) {
          row.add(v.getField().toExpr() + ":" + v.getAccessor().getObject(valueIdx));
        }
        for (final Object cell : row) {
          if (cell == null) {
            System.out.print("<null>    ");
            continue;
          }
          final int len = cell.toString().length();
          System.out.print(cell + " ");
          for (int i = 0; i < (10 - len); ++i) {
            System.out.print(" ");
          }
        }
        System.out.println();
      }
    }
    System.out.println("Total Record Count: " + totalRecordCount);
    assertEquals(25, totalRecordCount);

    if (context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }

  @Test
  @Ignore
  public void orderedEqualityInnerJoin(@Injectable final DrillbitContext bitContext,
                                       @Injectable UserServer.UserClientConnection connection) throws Throwable {
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getConfig(); result = c;
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c, new StoragePluginRegistry(bitContext));
    final PhysicalPlan plan = reader.readPhysicalPlan(
        Files.toString(
            FileUtils.getResourceAsFile("/join/merge_inner_single_batch.json"), Charsets.UTF_8)
            .replace("#{LEFT_FILE}", FileUtils.getResourceAsFile("/join/merge_single_batch.left.json").toURI().toString())
            .replace("#{RIGHT_FILE}", FileUtils.getResourceAsFile("/join/merge_single_batch.right.json").toURI().toString()));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int totalRecordCount = 0;
    while (exec.next()) {
      totalRecordCount += exec.getRecordCount();
      System.out.println("got next with record count: " + exec.getRecordCount() + " (total: " + totalRecordCount + "):");
      System.out.println("       t1                 t2");

      for (int valueIdx = 0; valueIdx < exec.getRecordCount(); valueIdx++) {
        final List<Object> row = Lists.newArrayList();
        for (final ValueVector v : exec) {
          row.add(v.getField().toExpr() + ":" + v.getAccessor().getObject(valueIdx));
        }
        for (final Object cell : row) {
          if (cell == null) {
            System.out.print("<null>    ");
            continue;
          }
          final int len = cell.toString().length();
          System.out.print(cell + " ");
          for (int i = 0; i < (10 - len); ++i) {
            System.out.print(" ");
          }
        }
        System.out.println();
      }
    }
    System.out.println("Total Record Count: " + totalRecordCount);
    assertEquals(23, totalRecordCount);

    if (context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }

  @Test
  @Ignore
  public void orderedEqualityMultiBatchJoin(@Injectable final DrillbitContext bitContext,
                                            @Injectable UserServer.UserClientConnection connection) throws Throwable {
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getConfig(); result = c;
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c, new StoragePluginRegistry(bitContext));
    final PhysicalPlan plan = reader.readPhysicalPlan(
        Files.toString(
            FileUtils.getResourceAsFile("/join/merge_multi_batch.json"), Charsets.UTF_8)
            .replace("#{LEFT_FILE}", FileUtils.getResourceAsFile("/join/merge_multi_batch.left.json").toURI().toString())
            .replace("#{RIGHT_FILE}", FileUtils.getResourceAsFile("/join/merge_multi_batch.right.json").toURI().toString()));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int totalRecordCount = 0;
    while (exec.next()) {
      totalRecordCount += exec.getRecordCount();
      System.out.println("got next with record count: " + exec.getRecordCount() + " (total: " + totalRecordCount + "):");

      for (int valueIdx = 0; valueIdx < exec.getRecordCount(); valueIdx++) {
        final List<Object> row = Lists.newArrayList();
        for (final ValueVector v : exec) {
          row.add(v.getField().toExpr() + ":" + v.getAccessor().getObject(valueIdx));
        }
        for (final Object cell : row) {
          if (cell == null) {
            System.out.print("<null>    ");
            continue;
          }
          int len = cell.toString().length();
          System.out.print(cell + " ");
          for (int i = 0; i < (10 - len); ++i) {
            System.out.print(" ");
          }
        }
        System.out.println();
      }
    }
    System.out.println("Total Record Count: " + totalRecordCount);
    assertEquals(25, totalRecordCount);

    if (context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }

  @Test
  public void testJoinBatchSize(@Injectable final DrillbitContext bitContext, @Injectable UserClientConnection connection) throws Throwable{
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getConfig(); result = c;
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    final PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/join/join_batchsize.json"), Charsets.UTF_8));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));
    exec.next(); // skip schema batch
    while (exec.next()) {
      assertEquals(100, exec.getRecordCount());
    }

    if (context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }

  @Test
  public void testMergeJoinInnerEmptyBatch() throws Exception {
    final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (final Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
        final DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/join/merge_join_empty_batch.json"),
                      Charsets.UTF_8)
                      .replace("${JOIN_TYPE}", "INNER"));
      int count = 0;
      for (final QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }
      assertEquals(0, count);
    }
  }

  @Test
  public void testMergeJoinLeftEmptyBatch() throws Exception {
    final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (final Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
        final DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
          Files.toString(FileUtils.getResourceAsFile("/join/merge_join_empty_batch.json"),
              Charsets.UTF_8)
              .replace("${JOIN_TYPE}", "LEFT"));
      int count = 0;
      for (final QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }
      assertEquals(50, count);
    }
  }

  @Test
  public void testMergeJoinRightEmptyBatch() throws Exception {
    final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (final Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
        final DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/join/merge_join_empty_batch.json"),
                      Charsets.UTF_8)
                      .replace("${JOIN_TYPE}", "RIGHT"));
      int count = 0;
      for (final QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }
      assertEquals(0, count);
    }
  }

  @Test
  public void testMergeJoinExprInCondition() throws Exception {
    final RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (final Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
        final DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/join/mergeJoinExpr.json"), Charsets.UTF_8));
      int count = 0;
      for (final QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }
      assertEquals(10, count);
    }
  }
}
