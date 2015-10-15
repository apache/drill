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
package org.apache.drill.exec;


import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.SimpleRootExec;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;

public class RunRootExec {
  public static DrillConfig c = DrillConfig.create();

  public static void main(String args[]) throws Exception {
    String path = args[0];
    int iterations = Integer.parseInt(args[1]);
    Drillbit bit = new Drillbit(c, RemoteServiceSet.getLocalServiceSet(), ClassPathScanner.fromPrescan(c));
    bit.run();
    DrillbitContext bitContext = bit.getContext();
    PhysicalPlanReader reader = bitContext.getPlanReader();
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(new File(path), Charsets.UTF_8));
    FunctionImplementationRegistry registry = bitContext.getFunctionImplementationRegistry();
    FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), null, registry);
    SimpleRootExec exec;
    for (int i = 0; i < iterations; i ++) {
      Stopwatch w= new Stopwatch().start();
      System.out.println("STARTITER:" + i);
      exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

      while (exec.next()) {
        for (ValueVector v : exec) {
          v.clear();
        }
      }
      System.out.println("ENDITER: " + i);
      System.out.println("TIME: " + w.elapsed(TimeUnit.MILLISECONDS) + "ms");
      exec.close();
    }
    context.close();
    bit.close();
  }

}
