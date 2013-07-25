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

import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.BaseBasicEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.exceptions.RecordException;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.NumericValue;
import org.apache.drill.exec.ref.values.NumericValue.NumericType;
import org.apache.drill.exec.ref.values.ScalarValues;

public class MathEvaluators {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MathEvaluators.class);
  
  @FunctionEvaluator("add")
  public static class AddEvaluator extends BaseBasicEvaluator{
    private final BasicEvaluator args[];

    public AddEvaluator(RecordPointer record, FunctionArguments args){
      super(args.isOnlyConstants(), record);
      this.args = args.getArgsAsArray();
    }
    
    @Override
    public NumericValue eval() {
      NumericValue[] values = new NumericValue[args.length];
      for(int i =0; i < values.length; i++){
        DataValue v = args[i].eval();
        if(Types.isNumericType(v.getDataType())){
          values[i] = v.getAsNumeric();
        }
      }
      return NumericValue.add(values);
    }

    
  }
  
  @FunctionEvaluator("multiply")
  public static class MultiplyE extends BaseBasicEvaluator{
    private final BasicEvaluator args[];

    public MultiplyE(RecordPointer record, FunctionArguments args){
      super(args.isOnlyConstants(), record);
      this.args = args.getArgsAsArray();
    }
    
    @Override
    public NumericValue eval() {
      long l = 1;
      double d = 1;
      boolean isFloating = false;
      
      for(int i =0; i < args.length; i++){
        final DataValue v = args[i].eval();
//        logger.debug("DataValue {}", v);
        if(Types.isNumericType(v.getDataType())){
          NumericValue n = v.getAsNumeric();
          NumericType nt = n.getNumericType();
//          logger.debug("Numeric Type: {}", nt);
          if(isFloating || nt == NumericType.FLOAT || nt == NumericType.DOUBLE){
            if(!isFloating){
              d = l;
              isFloating = true;
            }
            d *= n.getAsDouble();
          }else{
            l *= n.getAsLong();
          }
          
        }else{
          throw new RecordException(String.format("Unable to multiply a value of  %s.", v), null);
        }
      }
      
      NumericValue out = null;
      if(isFloating){
        out = new ScalarValues.DoubleScalar(d);
      }else{
        out = new ScalarValues.LongScalar(l);
      }
      
      return out;
    }

    
  }
}
