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

<@pp.dropOutputFile />

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/DirectoryExplorers.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

/**
 * This file is generated with Freemarker using the template exec/java-exec/src/main/codegen/templates/DirectoryExplorers.java
 */
public class DirectoryExplorers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectoryExplorers.class);
  private static final String FILE_SEPARATOR = "/";

  <#list [ { "name" : "\"maxdir\"", "functionClassName" : "MaxDir", "comparison" : "compareTo(curr) < 0", "goal" : "maximum", "comparisonType" : "case-sensitive"},
           { "name" : "\"imaxdir\"", "functionClassName" : "IMaxDir", "comparison" : "compareToIgnoreCase(curr) < 0", "goal" : "maximum", "comparisonType" : "case-insensitive"},
           { "name" : "\"mindir\"", "functionClassName" : "MinDir", "comparison" : "compareTo(curr) > 0", "goal" : "minimum", "comparisonType" : "case-sensitive"},
           { "name" : "\"imindir\"", "functionClassName" : "IMinDir", "comparison" : "compareToIgnoreCase(curr) > 0", "goal" : "minimum", "comparisonType" : "case-insensitive"}
  ] as dirAggrProps>


  @FunctionTemplate(name = ${dirAggrProps.name}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class ${dirAggrProps.functionClassName} implements DrillSimpleFunc {

    @Param VarCharHolder schema;
    @Param  VarCharHolder table;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;
    @Inject org.apache.drill.exec.store.PartitionExplorer partitionExplorer;

    public void setup() {
    }

    public void eval() {
      Iterable<String> subPartitions;
      try {
        subPartitions = partitionExplorer.getSubPartitions(
            org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(schema),
            org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(table),
            new java.util.ArrayList<String>(),
            new java.util.ArrayList<String>());
      } catch (org.apache.drill.exec.store.PartitionNotFoundException e) {
        throw new RuntimeException(
            String.format("Error in %s function: Table %s does not exist in schema %s ",
                ${dirAggrProps.name},
                org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(table),
                org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(schema))
        );
      }
      java.util.Iterator partitionIterator = subPartitions.iterator();
      if (!partitionIterator.hasNext()) {
        throw new RuntimeException(
            String.format("Error in %s function: Table %s in schema %s does not contain sub-partitions.",
                ${dirAggrProps.name},
                org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(table),
                org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(schema)
            )
        );
      }
      String subPartitionStr = (String) partitionIterator.next();
      String curr;
      // find the ${dirAggrProps.goal} directory in the list using a ${dirAggrProps.comparisonType} string comparison
      while (partitionIterator.hasNext()){
        curr = (String) partitionIterator.next();
        if (subPartitionStr.${dirAggrProps.comparison}) {
          subPartitionStr = curr;
        }
      }
      String[] subPartitionParts = subPartitionStr.split(FILE_SEPARATOR);
      subPartitionStr = subPartitionParts[subPartitionParts.length - 1];
      byte[] result = subPartitionStr.getBytes();
      out.buffer = buffer = buffer.reallocIfNeeded(result.length);

      out.buffer.setBytes(0, subPartitionStr.getBytes(), 0, result.length);
      out.start = 0;
      out.end = result.length;
    }
  }
  </#list>
}
