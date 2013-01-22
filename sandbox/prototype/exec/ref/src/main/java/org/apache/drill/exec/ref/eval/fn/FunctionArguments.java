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
package org.apache.drill.exec.ref.eval.fn;

import java.util.List;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.exceptions.SetupException;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

public class FunctionArguments {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionArguments.class);
  
  private final boolean onlyConstants;
  private final boolean includesAggregates;
  private final List<BasicEvaluator> ev;  
  private ObjectIntOpenHashMap<String> nameToPosition = new ObjectIntOpenHashMap<String>();
  
  public FunctionArguments(boolean onlyConstants, boolean includesAggregates, List<BasicEvaluator> evaluators, FunctionCall call){
    this.onlyConstants = onlyConstants;
    this.includesAggregates = includesAggregates;
    String[] names = call.getDefinition().getArgumentNames();
    for(int i =0; i < names.length; i++){
      if(names[i] != null){
        nameToPosition.put(names[i], i);
      }
    }
    ev = evaluators;
  }
  
  
  public boolean includesAggregates() {
    return includesAggregates;
  }


  public boolean isOnlyConstants(){
    return onlyConstants;
  }
  
  public BasicEvaluator getEvaluator(String name){
    if(!nameToPosition.containsKey(name)) throw new RuntimeException("Unknown Item provided.");
    int i = nameToPosition.lget();
    return getEvaluator(i);
  }
  
  public BasicEvaluator getEvaluator(int argIndex){
    BasicEvaluator eval = ev.get(argIndex);
    if(eval == null) throw new RuntimeException("Unknown Item provided.");
    return eval;
    
  }
  
  /**
   * Return this single argument evaluator in this set of evaluators.  If there isn't exactly one, throw a SetupException
   * @return The single evaluator.
   */
  public BasicEvaluator getOnlyEvaluator() throws SetupException{
    if(ev.size() != 1) throw new SetupException(String.format("Looking for a single argument.  Received %d arguments.", ev.size()));
    return ev.get(0);
  }
  
  public int size(){
    return ev.size();
  }
  
  public BasicEvaluator[] getArgsAsArray(){
    return ev.toArray(new BasicEvaluator[size()]);
  }
}
