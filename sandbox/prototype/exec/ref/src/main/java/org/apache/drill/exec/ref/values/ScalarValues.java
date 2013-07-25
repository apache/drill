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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tools.ant.types.DataType;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


public final class ScalarValues {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScalarValues.class);
  
  private ScalarValues(){}
  
  public static class StringScalar extends BaseDataValue implements StringValue, ComparableValue, BasicEvaluator {
    private CharSequence seq;
    private MajorType type = MajorType.newBuilder().setMinorType(MinorType.VARCHAR4).setMode(DataMode.OPTIONAL).build();
    
    public StringScalar(CharSequence seq){
      this.seq = seq;
    }
    
    @Override
    public int compareTo(DataValue o) {
      CharSequence seq1 = seq;
      CharSequence seq2 = o.getAsStringValue().getString();
      final int len = Math.min(seq1.length(), seq2.length());
      for(int i =0; i < len; i++){
        char c1 = seq1.charAt(i);
        char c2 = seq2.charAt(i);
        if(c1 != c2){
          return c1 - c2;
        }
      }
      return seq1.length() - seq2.length();
    }

    @Override
    public boolean supportsCompare(DataValue dv2) {
      return Types.isStringScalarType(dv2.getDataType());
    }

    @Override
    public void write(DataWriter writer) throws IOException {
      writer.writeCharSequence(seq);
    }

    @Override
    public MajorType getDataType() {
      return type;
    }

    @Override
    public StringValue getAsStringValue() {
      return this;
    }

    @Override
    public CharSequence getString() {
      return seq;
    }
    
    @Override
    public boolean isConstant() {
      return true;
    }
    
    public DataValue eval(){
      return this;
    }

    @Override
    public String toString() {
      return "StringScalar [seq=" + seq + "]";
    }

    @Override
    public boolean equals(DataValue v) {
      if(!v.getDataType().equals(this.getDataType())) return false;
      return seq.equals(v.getAsStringValue().getString());
    }

    @Override
    public int hashCode() {
      return seq.hashCode();
    }

    @Override
    public DataValue copy() {
        return new StringScalar(seq.toString());
    }
  }
  
 
  
  public static class BooleanScalar extends BaseDataValue implements BooleanValue, BasicEvaluator{
    private boolean b;
    private MajorType type = MajorType.newBuilder().setMinorType(MinorType.BOOLEAN).setMode(DataMode.OPTIONAL).build();
    
    public BooleanScalar(boolean b){
      this.b = b;
    }

    @Override
    public void write(DataWriter writer) throws IOException {
      writer.writeBoolean(b);
    }
    
    @Override
    public boolean getBoolean() {
      return b;
    }

    @Override
    public BooleanValue getAsBooleanValue() {
      return this;
    }

    @Override
    public MajorType getDataType() {
      return type;
    }

    @Override
    public boolean isConstant() {
      return true;
    }

    @Override
    public DataValue eval() {
      return this;
    }

    @Override
    public String toString() {
      return "BooleanScalar [b=" + b + "]";
    }
    
    @Override
    public boolean equals(DataValue v) {
      if(v.getDataType().equals(this.getDataType())) return false;
      return b == v.getAsBooleanValue().getBoolean();
    }


    @Override
    public int hashCode() {
      return b ? 1 : 0;
    }

    @Override
    public DataValue copy() {
        return new BooleanScalar(b);
    }
  }
  
  public static class LongScalar extends NumericValue{
    
    private MajorType type = MajorType.newBuilder().setMinorType(MinorType.BIGINT).setMode(DataMode.OPTIONAL).build();
    long l;
    public LongScalar(long l) {
      this.l = l;
    }
    
    @Override
    public long getAsLong() {
      return l;
    }

    @Override
    public float getAsFloat() {
      return l;
    }

    @Override
    public double getAsDouble() {
      return l;
    }

    @Override
    public BigInteger getAsBigInteger() {
      return BigInteger.valueOf(l);
    }

    @Override
    public BigDecimal getAsBigDecimal() {
      return BigDecimal.valueOf(l);
    }

    @Override
    public void write(DataWriter writer) throws IOException {
      writer.writeSInt64(l);
    }

    @Override
    public MajorType getDataType() {
      return type;
    }

    @Override
    public NumericType getNumericType() {
      return NumericType.LONG;
    }
    
    @Override
    public boolean isConstant() {
      return true;
    }

    @Override
    public String toString() {
      return "LongScalar [l=" + l + "]";
    }
    

    @Override
    public int hashCode() {
      return getHashCode(l);
    }

    @Override
    public DataValue copy() {
        return new LongScalar(l);
    }
  }
  
  public static class IntegerScalar extends NumericValue{
    private MajorType type = MajorType.newBuilder().setMinorType(MinorType.INT).setMode(DataMode.OPTIONAL).build();
    int i;
    
    public IntegerScalar(int i){
      this.i = i;
    }
    
    @Override
    public BigInteger getAsBigInteger() {
      return BigInteger.valueOf(i);
    }

    @Override
    public BigDecimal getAsBigDecimal() {
      return BigDecimal.valueOf(i);
    }
    
    @Override
    public void write(DataWriter writer) throws IOException {
      writer.writeSInt32(i);
    }

    @Override
    public MajorType getDataType() {
      return type;
    }

    @Override
    public NumericType getNumericType() {
      return NumericType.INT;
    }

    @Override
    public long getAsLong() {
      return i;
    }

    @Override
    public int getAsInt() {
      return i;
    }

    @Override
    public float getAsFloat() {
      return i;
    }

    @Override
    public double getAsDouble() {
      return i;
    }
    
    @Override
    public boolean isConstant() {
      return true;
    }

    @Override
    public String toString() {
      return "IntegerScalar [i=" + i + "]";
    }

    
    @Override
    public int hashCode() {
      return getHashCode(i);
    }

    @Override
    public DataValue copy() {
        return new IntegerScalar(i);
    }
  }
  

  
  public static class FloatScalar extends NumericValue{
    
    private MajorType type = MajorType.newBuilder().setMinorType(MinorType.FLOAT4).setMode(DataMode.OPTIONAL).build();
    float f;
    public FloatScalar(float f){
      this.f = f;
    }

    @Override
    public BigDecimal getAsBigDecimal() {
      return BigDecimal.valueOf(f);
    }

    @Override
    public void write(DataWriter writer) throws IOException {
      writer.writeSFloat32(f);
    }

    @Override
    public MajorType getDataType() {
      return type;
    }

    @Override
    public NumericType getNumericType() {
      return NumericType.FLOAT;
    }

    @Override
    public double getAsDouble() {
      return f;
    }
    
    @Override
    public boolean isConstant() {
      return true;
    }

    @Override
    public String toString() {
      return "FloatScalar [f=" + f + "]";
    }

    @Override
    public int hashCode() {
      return getHashCode(f);
    }

    @Override
    public DataValue copy() {
      return new FloatScalar(f);
    }
  }
 
  
  public static class DoubleScalar extends NumericValue{
    private MajorType type = MajorType.newBuilder().setMinorType(MinorType.FLOAT8).setMode(DataMode.OPTIONAL).build();
    private double d;
    
    public DoubleScalar(double d){
      this.d = d;
    }

    @Override
    public MajorType getDataType() {
      return type;
    }

    @Override
    public NumericType getNumericType() {
      return NumericType.DOUBLE;
    }


    @Override
    public double getAsDouble() {
      return d;
    }


    @Override
    public BigDecimal getAsBigDecimal() {
      return BigDecimal.valueOf(d);
    }

    @Override
    public void write(DataWriter writer) throws IOException {
      writer.writeSFloat64(d);
    }
    
    @Override
    public boolean isConstant() {
      return true;
    }

    @Override
    public String toString() {
      return "DoubleScalar [d=" + d + "]";
    }
 
    @Override
    public int hashCode() {
      return getHashCode(d);
    }

    @Override
    public DataValue copy() {
      return new DoubleScalar(d);
    }
  }
  
  public static class BytesScalar extends BaseDataValue implements BytesValue{
    private MajorType type = MajorType.newBuilder().setMinorType(MinorType.VARBINARY4).setMode(DataMode.OPTIONAL).build();
    private BytesWritable.Comparator comparator = new BytesWritable.Comparator();
    private final static HashFunction HASH = Hashing.murmur3_32();

    private byte[] bytes;
    public BytesScalar(byte[] value){
      this.bytes = value;
    }

    @Override
    public void write(DataWriter writer) throws IOException {
      writer.writeBytes(bytes);
    }

    @Override
    public boolean supportsCompare(DataValue dv2) {
      return Types.isBytesScalarType(dv2.getDataType());
    }


    @Override
    public int compareTo(DataValue other) {
      byte[] b1 = bytes;
      byte[] b2 = other.getAsBytesValue().getAsArray();
      return comparator.compare(b1, 0, bytes.length, b2, 0, bytes.length);
    }

    @Override
    public MajorType getDataType() {
      return type;
    }

    @Override
    public byte[] getAsArray() {
      return bytes;
    }

    @Override
    public int getLength() {
      return bytes.length;
    }

    @Override
    public byte get(int pos) {
      return bytes[pos];
    }

    @Override
    public String toString() {
      return "BytesScalar [bytes=" + Arrays.toString(bytes) + "]";
    }

    @Override
    public boolean equals(DataValue v) {
      if(!v.getDataType().equals(this.getDataType())) return false;
      BytesValue other = v.getAsBytesValue();
      if(this.getLength() != other.getLength()) return false;
      for(int i =0; i < this.getLength(); i++){
        if(this.get(i) !=  other.get(i)) return false;
      }
      return true;
    }

    @Override
    public int hashCode() {
      return HASH.hashBytes(bytes).asInt();
    }

    @Override
    public DataValue copy() {
      byte[] out = Arrays.copyOf(bytes, bytes.length);
      return new BytesScalar(out);
    }

  }
 
  
  
  static class NullValue extends BaseDataValue{
    
    // not sure what to do here... 
    MajorType type = MajorType.newBuilder().setMode(DataMode.OPTIONAL).setMinorType(MinorType.LATE).build();
    
    @Override
    public void write(DataWriter writer) throws IOException {
      writer.writeNullValue();
    }

    @Override
    public MajorType getDataType() {
      return type;
    }

    @Override
    public String toString() {
      return "NullValue";
    }

    @Override
    public boolean equals(DataValue v) {
      // identity since there should be only one.
      return v == this;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public DataValue copy() {
      return this;
    }
    
  }

  
}
