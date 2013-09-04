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
package org.apache.drill.exec.ref.values;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.values.ScalarValues.DoubleScalar;
import org.apache.drill.exec.ref.values.ScalarValues.FloatScalar;
import org.apache.drill.exec.ref.values.ScalarValues.IntegerScalar;
import org.apache.drill.exec.ref.values.ScalarValues.LongScalar;

public abstract class NumericValue extends BaseDataValue implements ComparableValue, BasicEvaluator{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NumericValue.class);
  
  
  public static enum NumericType {
    // order is important for conversion
    INT, LONG, BIG_INTEGER, FLOAT, DOUBLE, BIG_DECIMAL;
  }
  
  public abstract NumericType getNumericType();

  @Override
  public int compareTo(DataValue dv2) {
    NumericValue other = dv2.getAsNumeric();
    NumericType mutual = getMutualType(this, other);
    switch(mutual){
    case BIG_DECIMAL:
      return this.getAsBigDecimal().compareTo(other.getAsBigDecimal());
    case BIG_INTEGER:
      return this.getAsBigInteger().compareTo(other.getAsBigInteger());
    case DOUBLE:
      return Double.compare(this.getAsDouble(), other.getAsDouble());
    case FLOAT:
      return Float.compare(this.getAsFloat(), other.getAsFloat());
    case INT:
      return Integer.compare(this.getAsInt(), other.getAsInt());
    case LONG:
      return Long.compare(this.getAsLong(), other.getAsLong());
    default:
      throw new UnsupportedOperationException();
    }
  }
  
  public int getHashCode(double d){
    Long l = Double.doubleToLongBits(d);
    return (int)(l ^ (l >>> 32));
  }
  
  @Override
  public DataValue eval() {
    return this;
  }

  private static NumericType getMutualType(NumericValue... values){
    int ord = 0;
    for(int i =0; i < values.length; i++){
      ord = Math.max(ord, values[i].getNumericType().ordinal());
    }
    return NumericType.values()[ord]; 
  }
  

  
  @Override
  public boolean equals(DataValue v) {
    if(v == null) return false;
    if(Types.isNumericType(v.getDataType())){
      return this.compareTo(v) == 0;
    }else{
      return false;
    }
  }

  
  public static NumericValue add(NumericValue... values){
    NumericType mutual = getMutualType(values);
    switch(mutual){
    case BIG_DECIMAL:
      throw new UnsupportedOperationException();
//      BigDecimal bd = new BigDecimal(0);
//      for(int i =0; i < values.length; i++){
//        bd = bd.add(values[i].getAsBigDecimal());
//      }
//      return new BigDecimalScalar(bd);
    case BIG_INTEGER:
      throw new UnsupportedOperationException();
//
//      BigInteger bi = BigInteger.valueOf(0);
//      for(int i =0; i < values.length; i++){
//        bi = bi.add(values[i].getAsBigInteger());
//      }
//      return new BigIntegerScalar(bi);
    case DOUBLE:
      double d = 0d;
      for(int i =0; i < values.length; i++){
        d += values[i].getAsDouble();
      }
      return new DoubleScalar(d);
    case FLOAT:
      float f = 0f;
      for(int i =0; i < values.length; i++){
        f += values[i].getAsFloat();
      }
      return new FloatScalar(f);      
    case INT:
      int x = 0;
      for(int i =0; i < values.length; i++){
        x += values[i].getAsInt();
      }
      return new IntegerScalar(x);      
    case LONG:
      int l = 0;
      for(int i =0; i < values.length; i++){
        l += values[i].getAsLong();
      }
      return new LongScalar(l);      
    default:
      throw new UnsupportedOperationException();
    }
  }
  
  @Override
  public boolean supportsCompare(DataValue dv2) {
    return Types.isNumericType(dv2.getDataType());
  }

  
  
  
  @Override
  public NumericValue getAsNumeric() {
    return this;
  }

  public long getAsLong(){
    throw new DrillRuntimeException(String.format("A %s value can not be implicitly cast to a long.", this.getDataType()));
  }
  public int getAsInt(){
    throw new DrillRuntimeException(String.format("A %s value can not be implicitly cast to an int.", this.getDataType()));
  }
  public float getAsFloat(){
    throw new DrillRuntimeException(String.format("A %s value can not be implicitly cast to an float.", this.getDataType()));
  }
  public double getAsDouble(){
    throw new DrillRuntimeException(String.format("A %s value can not be implicitly cast to a double.", this.getDataType()));
  }
  public BigDecimal getAsBigDecimal(){
    throw new DrillRuntimeException(String.format("A %s value can not be implicitly cast to an big decimal.", this.getDataType()));
  }
  public BigInteger getAsBigInteger(){
    throw new DrillRuntimeException(String.format("A %s value can not be implicitly cast to a big integer.", this.getDataType()));
  }
  

}
