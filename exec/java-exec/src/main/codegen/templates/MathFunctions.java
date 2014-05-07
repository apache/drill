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
<@pp.dropOutputFile />


<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/GMathFunctions.java" />

<#include "/@includes/license.ftl" />

/*
 * This class is automatically generated from AddTypes.tdd using FreeMarker.
 */


package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.fn.impl.StringFunctions;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

@SuppressWarnings("unused")

public class GMathFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GMathFunctions.class);
  
  private GMathFunctions(){}

  <#list mathFunc.unaryMathFunctions as func>

  <#list func.types as type>

  @FunctionTemplate(name = "${func.funcName}", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ${func.className}${type.input} implements DrillSimpleFunc {

    @Param ${type.input}Holder in;
    @Output ${type.outputType}Holder out;

    public void setup(RecordBatch b) {
    }

    public void eval() {
      out.value =(${type.castType}) ${func.javaFunc}(in.value);
    }
  }
  
  </#list>
  </#list>


  //////////////////////////////////////////////////////////////////////////////////////////////////
  //Math functions which take two arguments (of same type). 
  //////////////////////////////////////////////////////////////////////////////////////////////////
  
  <#list mathFunc.binaryMathFunctions as func>
  <#list func.types as type>

  @FunctionTemplate(name = "${func.funcName}", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ${func.className}${type.input} implements DrillSimpleFunc {

    @Param ${type.input}Holder input1;
    @Param ${type.input}Holder input2;
    @Output ${type.outputType}Holder out;

    public void setup(RecordBatch b) {
    }

    public void eval() {
      out.value =(${type.castType}) ( input1.value ${func.javaFunc} input2.value);
    }
  }
  </#list>
  </#list>
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//End of GMath Functions
//////////////////////////////////////////////////////////////////////////////////////////////////

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/TrigoMathFunctions.java" />
<#include "/@includes/license.ftl" />

//////////////////////////////////////////////////////////////////////////////////////////////////
//Functions for Trigo Math Functions
//////////////////////////////////////////////////////////////////////////////////////////////////


package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.fn.impl.StringFunctions;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

/*
 * This class is automatically generated from MathFunc.tdd using FreeMarker.
 */

@SuppressWarnings("unused")

public class TrigoMathFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TrigoMathFunctions.class);

  private TrigoMathFunctions(){}

  <#list mathFunc.trigoMathFunctions as func>

  <#list func.types as type>

  @FunctionTemplate(name = "${func.funcName}", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ${func.className}${type.input} implements DrillSimpleFunc {

    @Param ${type.input}Holder in;
    @Output ${func.outputType}Holder out;

    public void setup(RecordBatch b) {
    }

    public void eval() {
      out.value = ${func.javaFunc}(in.value);
    }
  }
 </#list>
 </#list>
}