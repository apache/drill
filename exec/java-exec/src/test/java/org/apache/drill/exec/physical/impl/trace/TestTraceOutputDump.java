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
package org.apache.drill.exec.physical.impl.trace;

import static org.junit.Assert.assertTrue;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.cache.VectorAccessibleSerializable;
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
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

import mockit.Injectable;
import mockit.NonStrictExpectations;

/*
 * This test uses a simple physical plan with a mock-scan that
 * generates one row. The physical plan also consists of the
 * trace operator which will dump the records as bytes to the
 * log file.
 *
 * Objective of this test is not only to verify if the injected
 * trace operator dumps the output to the log file, but also
 * to read the dumped output and verify if it matches what we expect
 * it to be. Since our scan produces only one record we expect record count to
 * be one, expect there are no selection vectors and we know the value of
 * the record that is dumped (Integer.MIN_VALUE) so we compare it with this
 * known value.
 */
public class TestTraceOutputDump extends ExecTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTraceOutputDump.class);
  private final DrillConfig c = DrillConfig.create();

  @Test
  public void testFilter(@Injectable final DrillbitContext bitContext, @Injectable UserClientConnection connection) throws Throwable {
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getConfig(); result = c;
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};

    final PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    final PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/trace/simple_trace.json"), Charsets.UTF_8));
    final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    final FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    while(exec.next()) {
    }

    exec.close();

    if(context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());

    final FragmentHandle handle = context.getHandle();

    /* Form the file name to which the trace output will dump the record batches */
    final String qid = QueryIdHelper.getQueryId(handle.getQueryId());
    final int majorFragmentId = handle.getMajorFragmentId();
    final int minorFragmentId = handle.getMinorFragmentId();

    final String logLocation = c.getString(ExecConstants.TRACE_DUMP_DIRECTORY);
    System.out.println("Found log location: " + logLocation);

    final String filename = String.format("%s//%s_%d_%d_mock-scan", logLocation, qid, majorFragmentId, minorFragmentId);
    System.out.println("File Name: " + filename);

    final Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, c.getString(ExecConstants.TRACE_DUMP_FILESYSTEM));

    final FileSystem fs = FileSystem.get(conf);
    final Path path = new Path(filename);
    assertTrue("Trace file does not exist", fs.exists(path));
    final FSDataInputStream in = fs.open(path);

    final VectorAccessibleSerializable wrap = new VectorAccessibleSerializable(context.getAllocator());
    wrap.readFromStream(in);
    final VectorAccessible container = wrap.get();

    /* Assert there are no selection vectors */
    assertTrue(wrap.getSv2() == null);

    /* Assert there is only one record */
    assertTrue(container.getRecordCount() == 1);

    /* Read the Integer value and ASSERT its Integer.MIN_VALUE */
    final int value = (int) container.iterator().next().getValueVector().getAccessor().getObject(0);
    assertTrue(value == Integer.MIN_VALUE);
  }
}
