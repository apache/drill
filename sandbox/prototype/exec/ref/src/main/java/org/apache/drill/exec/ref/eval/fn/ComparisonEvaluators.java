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

import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.BaseBasicEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.exceptions.RecordException;
import org.apache.drill.exec.ref.values.ComparableValue;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues.BooleanScalar;

public class ComparisonEvaluators {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComparisonEvaluators.class);
  
  @FunctionEvaluator("and")
  public static class And extends BaseBasicEvaluator{
    private final BasicEvaluator left;
    private final BasicEvaluator right;

    public And(RecordPointer record, FunctionArguments args) {
      super(args.isOnlyConstants(), record);
      left = args.getEvaluator(0);
      right = args.getEvaluator(1);
    }

    @Override
    public BooleanScalar eval() {
      return new BooleanScalar(left.eval().getAsBooleanValue().getBoolean() && right.eval().getAsBooleanValue().getBoolean());
    }

  }

  @FunctionEvaluator("equal")
  public static class EqualEvaluator extends BaseBasicEvaluator{
    private final BasicEvaluator left;
    private final BasicEvaluator right;
    
    public EqualEvaluator(RecordPointer record, FunctionArguments args){
      super(args.isOnlyConstants(), record);
      left = args.getEvaluator(0);
      right = args.getEvaluator(1);
    }
    
    @Override
    public BooleanScalar eval() {
      return new BooleanScalar(left.eval().equals(right.eval()));
    }

  }

  public static boolean isComparable(DataValue a, DataValue b) {
      return a instanceof ComparableValue && b instanceof ComparableValue && ((ComparableValue) a).supportsCompare(b);
  }
  
  private abstract static class ComparisonEvaluator extends BaseBasicEvaluator{
    private final BasicEvaluator left;
    private final BasicEvaluator right;
    
    public ComparisonEvaluator(RecordPointer record, FunctionArguments args){
      super(args.isOnlyConstants(), record);
      left = args.getEvaluator(0);
      right = args.getEvaluator(1);
    }
    
    public abstract boolean valid(int i);
    
    @Override
    public BooleanScalar eval() {
      DataValue a = left.eval();
      DataValue b = right.eval();
      
      if(isComparable(a, b)){
        int i = ((ComparableValue)a).compareTo(b);
        return new BooleanScalar(valid( i));
      }else{
        throw new RecordException(String.format("Values cannot be compared.  A %s cannot be compared to a %s.", a, b), null);
      }
    }
  }
  
  @FunctionEvaluator("less than")
  public static class LessThan extends ComparisonEvaluator{

    public LessThan(RecordPointer record, FunctionArguments args) {
      super(record, args);
    }

    @Override
    public boolean valid(int i) {
      return i == -1;
    }
    
  }
  
  @FunctionEvaluator("greater than")
  public static class GreaterThan extends ComparisonEvaluator{

    public GreaterThan(RecordPointer record, FunctionArguments args) {
      super(record, args);
    }

    @Override
    public boolean valid(int i) {
      return i == 1;
    }
    
  }
  
  @FunctionEvaluator("greater than or equal to")
  public static class GreaterOrEqualTo extends ComparisonEvaluator{

    public GreaterOrEqualTo(RecordPointer record, FunctionArguments args) {
      super(record, args);
    }

    @Override
    public boolean valid(int i) {
      return i >= 0;
    }
    
  }
  
  @FunctionEvaluator("less than or equal to")
  public static class LessThanOrEqualTo extends ComparisonEvaluator{

    public LessThanOrEqualTo(RecordPointer record, FunctionArguments args) {
      super(record, args);
    }

    @Override
    public boolean valid(int i) {
      return i <= 0;
    }
    
  }
  

  
}
