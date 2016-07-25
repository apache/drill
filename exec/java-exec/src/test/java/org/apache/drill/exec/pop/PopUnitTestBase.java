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
package org.apache.drill.exec.pop;

import java.io.IOException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.Fragment.ExchangeFragmentPair;
import org.apache.drill.exec.planner.fragment.MakeFragmentsVisitor;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.junit.BeforeClass;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public abstract class PopUnitTestBase  extends ExecTest{
//  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PopUnitTestBase.class);

  protected static DrillConfig CONFIG;

  @BeforeClass
  public static void setup() {
    CONFIG = DrillConfig.create();
  }


  public static int getFragmentCount(Fragment b) {
    int i = 1;
    for (ExchangeFragmentPair p : b) {
      i += getFragmentCount(p.getNode());
    }
    return i;
  }

  public static Fragment getRootFragment(PhysicalPlanReader reader, String file) throws FragmentSetupException,
      IOException, ForemanSetupException {
    return getRootFragmentFromPlanString(reader, Files.toString(FileUtils.getResourceAsFile(file), Charsets.UTF_8));
  }


  public static Fragment getRootFragmentFromPlanString(PhysicalPlanReader reader, String planString)
      throws FragmentSetupException, IOException, ForemanSetupException {
    PhysicalPlan plan = reader.readPhysicalPlan(planString);
    PhysicalOperator o = plan.getSortedOperators(false).iterator().next();
    return o.accept(MakeFragmentsVisitor.INSTANCE, null);
  }
}
