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
package org.apache.drill.exec.opt;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.junit.Test;

public class BasicOptimizerTest extends ExecTest {

    @Test
    public void parseSimplePlan() throws Exception{
        DrillConfig c = DrillConfig.create();
        LogicalPlanPersistence lpp = PhysicalPlanReaderTestFactory.defaultLogicalPlanPersistence(c);
        LogicalPlan plan = LogicalPlan.parse(lpp, FileUtils.getResourceAsString("/scan_screen_logical.json"));
        String unparse = plan.unparse(lpp);
//        System.out.println(unparse);
        //System.out.println( new BasicOptimizer(DrillConfig.create()).convert(plan).unparse(c.getMapper().writer()));
    }
}
