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
package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

public class HashFunctions {

  public static final FunctionDefinition HASH = FunctionDefinition.simple("hash", new ArgumentValidators.AnyTypeAllowed(1),
      OutputTypeDeterminer.FIXED_INT, "hash");
  
  public static class Provider implements CallProvider {

    @Override
    public FunctionDefinition[] getFunctionDefintions() {
      return new FunctionDefinition[] { HASH };
    }

  }

  @FunctionTemplate(name = "hash", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableFloatHash implements DrillSimpleFunc {

    @Param NullableFloat4Holder in;
    @Output IntHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      if (in.isSet == 0)
        out.value = 0;
      else
        out.value = com.google.common.hash.Hashing.murmur3_128().hashInt(Float.floatToIntBits(in.value)).asInt();
    }
  }

  @FunctionTemplate(name = "hash", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class FloatHash implements DrillSimpleFunc {

    @Param Float4Holder in;
    @Output IntHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      out.value = com.google.common.hash.Hashing.murmur3_128().hashInt(Float.floatToIntBits(in.value)).asInt();
    }
  }

  @FunctionTemplate(name = "hash", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableDoubleHash implements DrillSimpleFunc {

    @Param NullableFloat8Holder in;
    @Output IntHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      if (in.isSet == 0)
        out.value = 0;
      else
        out.value = com.google.common.hash.Hashing.murmur3_128().hashLong(Double.doubleToLongBits(in.value)).asInt();
    }
  }

  @FunctionTemplate(name = "hash", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class DoubleHash implements DrillSimpleFunc {

    @Param Float8Holder in;
    @Output IntHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      out.value = com.google.common.hash.Hashing.murmur3_128().hashLong(Double.doubleToLongBits(in.value)).asInt();
    }
  }

  @FunctionTemplate(name = "hash", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableVarBinaryHash implements DrillSimpleFunc {

    @Param NullableVarBinaryHolder in;
    @Output IntHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      if (in.isSet == 0)
        out.value = 0;
      else
        out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash(in.buffer.nioBuffer(), 0);
    }
  }

  @FunctionTemplate(name = "hash", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableBigIntHash implements DrillSimpleFunc {

    @Param NullableBigIntHolder in;
    @Output IntHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      // TODO: implement hash function for other types
      if (in.isSet == 0)
        out.value = 0;
      else
        out.value = com.google.common.hash.Hashing.murmur3_128().hashLong(in.value).asInt();
    }
  }

  @FunctionTemplate(name = "hash", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableIntHash implements DrillSimpleFunc {
    @Param NullableIntHolder in;
    @Output IntHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      // TODO: implement hash function for other types
      if (in.isSet == 0)
        out.value = 0;
      else
        out.value = com.google.common.hash.Hashing.murmur3_128().hashInt(in.value).asInt();
    }
  }

  @FunctionTemplate(name = "hash", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class VarBinaryHash implements DrillSimpleFunc {

    @Param VarBinaryHolder in;
    @Output IntHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash(in.buffer.nioBuffer(in.start, in.end - in.start), 0);
    }
  }
  
  @FunctionTemplate(name = "hash", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class HashBigInt implements DrillSimpleFunc {

    @Param BigIntHolder in;
    @Output IntHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      // TODO: implement hash function for other types
      out.value = com.google.common.hash.Hashing.murmur3_128().hashLong(in.value).asInt();
    }
  }

  @FunctionTemplate(name = "hash", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class IntHash implements DrillSimpleFunc {
    @Param IntHolder in;
    @Output IntHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      // TODO: implement hash function for other types
      out.value = com.google.common.hash.Hashing.murmur3_128().hashInt(in.value).asInt();
    }
  }

}
