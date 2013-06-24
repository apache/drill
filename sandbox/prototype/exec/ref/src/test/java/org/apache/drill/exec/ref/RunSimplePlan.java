/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ref;

import static org.junit.Assert.*;

import java.util.Collection;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ref.RunOutcome.OutcomeType;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class RunSimplePlan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunSimplePlan.class);
  
  
  @Test
  public void parseSimplePlan() throws Throwable{
    DrillConfig config = DrillConfig.create();
    LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile("/simple_plan.json"), Charsets.UTF_8));
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
    i.setup();
    Collection<RunOutcome> outcomes = i.run();
    assertEquals(outcomes.size(), 1);
    assertEquals(outcomes.iterator().next().records, 2);
  }
  
  @Test
  public void joinPlan() throws Throwable{
    DrillConfig config = DrillConfig.create();
    LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile("/simple_join.json"), Charsets.UTF_8));
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
    i.setup();
    Collection<RunOutcome> outcomes = i.run();
    assertEquals(outcomes.size(), 1);
    RunOutcome out = outcomes.iterator().next();
    if(out.outcome != OutcomeType.FAILED && out.exception != null) logger.error("Failure while running {}", out.exception);
  }
  
  @Test
  public void flattenPlan() throws Throwable{
    DrillConfig config = DrillConfig.create();
    LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile("/simple_plan_flattened.json"), Charsets.UTF_8));
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
    i.setup();
    Collection<RunOutcome> outcomes = i.run();
    assertEquals(outcomes.size(), 1);
    RunOutcome out = outcomes.iterator().next();
    if(out.outcome != OutcomeType.FAILED && out.exception != null) logger.error("Failure while running {}", out.exception);
    assertEquals(out.outcome, RunOutcome.OutcomeType.SUCCESS);
  }
}
