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
package org.apache.drill.exec.client;

import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.SimpleRootExec;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * The unit test case will read a physical plan in json format. The physical plan contains a "trace" operator,
 * which will produce a dump file.  The dump file will be input into DumpCat to test query mode and batch mode.
 */

public class DumpCatTest  extends ExecTest{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DumpCatTest.class);
  DrillConfig c = DrillConfig.create();

  @Test
  public void testDumpCat(@Injectable final DrillbitContext bitContext, @Injectable UserClientConnection connection) throws Throwable
  {

      new NonStrictExpectations(){{
          bitContext.getMetrics(); result = new MetricRegistry();
          bitContext.getAllocator(); result = new TopLevelAllocator();
          bitContext.getConfig(); result = c;
          bitContext.getCompiler(); result = CodeCompiler.getTestCompiler(c);
          bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
      }};

      PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
      PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/trace/simple_trace.json"), Charsets.UTF_8));
      FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
      FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);
      SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

      while(exec.next()){
      }

      if(context.getFailureCause() != null){
          throw context.getFailureCause();
      }
      assertTrue(!context.isFailed());

      exec.stop();

      FragmentHandle handle = context.getHandle();

      /* Form the file name to which the trace output will dump the record batches */
      String qid = QueryIdHelper.getQueryId(handle.getQueryId());

      int majorFragmentId = handle.getMajorFragmentId();
      int minorFragmentId = handle.getMinorFragmentId();

      String logLocation = c.getString(ExecConstants.TRACE_DUMP_DIRECTORY);

      System.out.println("Found log location: " + logLocation);

      String filename = String.format("%s//%s_%d_%d_mock-scan", logLocation, qid, majorFragmentId, minorFragmentId);

      System.out.println("File Name: " + filename);

      Configuration conf = new Configuration();
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, c.getString(ExecConstants.TRACE_DUMP_FILESYSTEM));

      FileSystem fs = FileSystem.get(conf);
      Path path = new Path(filename);
      assertTrue("Trace file does not exist", fs.exists(path));

      DumpCat dumpCat = new DumpCat();

      //Test Query mode
      FileInputStream input = new FileInputStream(filename);

      dumpCat.doQuery(input);
      input.close();

      //Test Batch mode
      input = new FileInputStream(filename);
      dumpCat.doBatch(input,0,true);

      input.close();
  }

}
