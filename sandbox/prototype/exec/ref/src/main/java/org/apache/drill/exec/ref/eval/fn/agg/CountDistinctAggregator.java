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
package org.apache.drill.exec.ref.eval.fn.agg;

import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorTypes;
import org.apache.drill.exec.ref.eval.fn.FunctionArguments;
import org.apache.drill.exec.ref.eval.fn.FunctionEvaluator;
import org.apache.drill.exec.ref.eval.fn.agg.CountAggregator;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues;

import java.util.HashSet;
import java.util.Set;

@FunctionEvaluator("countDistinct")
public class CountDistinctAggregator implements EvaluatorTypes.AggregatingEvaluator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CountDistinctAggregator.class);

  long l = 0;
  boolean checkForNull = false;
  EvaluatorTypes.BasicEvaluator child;
  boolean isConstant;
  private Set<Integer> duplicate = new HashSet<Integer>();

  public CountDistinctAggregator(RecordPointer recordPointer, FunctionArguments e){
    isConstant = e.isOnlyConstants();
    if(!e.getOnlyEvaluator().isConstant()){
      checkForNull = true;
      child = e.getOnlyEvaluator();
    }
  }

  @Override
  public void addRecord() {
    if(checkForNull){
      DataValue dataValue = child.eval();
      if((dataValue != DataValue.NULL_VALUE) && (!duplicate.contains(dataValue.hashCode()))){
        l++;
        duplicate.add(dataValue.hashCode());
      }
    }else{
      l++;
    }
  }

  @Override
  public DataValue eval() {
    duplicate.clear();
    DataValue v = new ScalarValues.LongScalar(l);
    l = 0;
    return v;
  }

  @Override
  public boolean isConstant() {
    return isConstant;
  }
}
