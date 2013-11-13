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

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.record.RecordBatch;

public class MathFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MathFunctions.class);
  
  private MathFunctions(){}
  
  @FunctionTemplate(name = "add", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Add1 implements DrillSimpleFunc{
    
    @Param IntHolder left;
    @Param IntHolder right;
    @Output IntHolder out;

    public void setup(RecordBatch b){}
    
    public void eval(){
      out.value = left.value + right.value;
    }

  }
  
  @FunctionTemplate(name = "add", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class LongAdd1 implements DrillSimpleFunc{
    
    @Param BigIntHolder left;
    @Param BigIntHolder right;
    @Output BigIntHolder out;

    public void setup(RecordBatch b){}
    
    public void eval(){
      out.value = left.value + right.value;
    }

  }
  
  @FunctionTemplate(name = "add", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Float4Add implements DrillSimpleFunc{
    
	@Param Float4Holder left;
	@Param Float4Holder right;
	@Output Float4Holder out;

	public void setup(RecordBatch b){}
    
	public void eval(){
  	out.value = left.value + right.value;
	}
  }
 
 
  @FunctionTemplate(name = "add", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Float8Add implements DrillSimpleFunc{
    
	@Param Float8Holder left;
	@Param Float8Holder right;
	@Output Float8Holder out;

	public void setup(RecordBatch b){}
    
	public void eval(){
  	out.value = left.value + right.value;
	}
  }
  
  @FunctionTemplate(name = "negative", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Negative implements DrillSimpleFunc{
    
    @Param BigIntHolder input;
    @Output BigIntHolder out;

    public void setup(RecordBatch b){}
    
    public void eval(){
      out.value = -input.value;
    }

  }
  
}
