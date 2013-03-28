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
package org.apache.drill.common.expression.fn;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.ArgumentValidators.AnyTypeAllowed;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;

public class AggregationFunctions implements CallProvider {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AggregationFunctions.class);

  @Override
  public FunctionDefinition[] getFunctionDefintions() {
    return new FunctionDefinition[] {
        FunctionDefinition.aggregator("count",  new AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput()),
        FunctionDefinition.aggregator("sum",  new AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput()),
        FunctionDefinition.aggregator("countDistinct",  new AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput())
    };
  }
}
