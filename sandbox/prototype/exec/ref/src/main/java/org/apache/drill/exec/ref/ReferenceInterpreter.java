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

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.rops.SinkROP;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class ReferenceInterpreter {
  static final Logger logger = LoggerFactory.getLogger(ReferenceInterpreter.class);
  
  private List<SinkROP> sinks = new ArrayList<SinkROP>();
  private LogicalPlan plan;
  private ROPConverter converter;
  private IteratorRegistry registry;
  
  public ReferenceInterpreter(LogicalPlan p, IteratorRegistry r, EvaluatorFactory builder, RSERegistry rses){
    this.plan = p;
    this.registry = r;
    this.converter = new ROPConverter(p, registry, builder, rses);
  }
  
  /** Generate Reference equivalents to each operation and then collect and store all the sinks. 
   * @throws IOException **/
  public void setup() throws IOException{
    for(LogicalOperator op : plan.getSortedOperators()){
      converter.convert(op);
    }
    sinks.addAll(registry.getSinks());
  }
  
  public  Collection<RunOutcome> run(){
    Collection<RunOutcome> outcomes  = new LinkedList<RunOutcome>();
    
    for(SinkROP r : sinks){
      outcomes.add(r.run(new BasicStatusHandle()));
    }
    
    return outcomes;
  }
  
  public void cleanup(){
    
  }
  
  
  public static void main(String[] args) throws Exception{
    DrillConfig config = DrillConfig.create();
    final String jsonFile = args[0];
    LogicalPlan plan = LogicalPlan.parse(config, Files.toString(new File(jsonFile), Charsets.UTF_8));
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
    i.setup();
    Collection<RunOutcome> outcomes = i.run();
    
    for(RunOutcome outcome : outcomes){
      System.out.println("============");
      System.out.println(outcome);
      if(outcome.outcome == RunOutcome.OutcomeType.FAILED && outcome.exception != null){
        outcome.exception.printStackTrace();
      }
      
    }
     
  }
}

