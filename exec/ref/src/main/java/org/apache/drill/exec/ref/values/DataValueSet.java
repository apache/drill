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
package org.apache.drill.exec.ref.values;

import java.util.Arrays;

import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;

public class DataValueSet {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataValueSet.class);
  
  private final DataValue[] values;
  private final BasicEvaluator[] evaluators;
  
  public DataValueSet(BasicEvaluator... evaluators){
    this.values = new DataValue[evaluators.length];
    this.evaluators = evaluators;
  }
  
  private DataValueSet(DataValue[] values){
    this.values = new DataValue[values.length];
    System.arraycopy(values, 0, this.values, 0, values.length);
    this.evaluators = null;
  }
  
  public void grabValues(){
    for(int i =0; i < values.length; i++){
      values[i] = evaluators[i].eval();
    }
  }

  public void copyFrom(DataValueSet set){
    System.arraycopy(set.values, 0, values, 0, set.values.length);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(values);
    return result;
  }

  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    DataValueSet other = (DataValueSet) obj;
    if (!Arrays.equals(values, other.values)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "DataValueSet [values=" + Arrays.toString(values) + "]";
  }

  public DataValueSet cloneValuesOnly(){
    return new DataValueSet(this.values);
  }
  
  
}
