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
package org.apache.drill.exec.ref.eval;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BooleanEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.ConnectedEvaluator;

public abstract class EvaluatorFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EvaluatorFactory.class);

  /**
   * Provide an evaluator that returns a true/false value on evaluation.
   * @param e The base logical expression
   * @return true/false of expression.
   */
  public abstract BooleanEvaluator getBooleanEvaluator(RecordPointer record, LogicalExpression e);
  
  /**
   * Provide an evaluator that takes the output value of the named expression evaluation and place that value back into the simple record.
   * @param ne The named expression to work with.
   * @return
   */
  public abstract ConnectedEvaluator getConnectedEvaluator(RecordPointer record, NamedExpression ne);

  /**
   * Provide a basic evaluator that returns the data value interested in.
   * @param e
   * @return
   */
  public abstract BasicEvaluator getBasicEvaluator(RecordPointer record, LogicalExpression e);
  
  /**
   * Given a set of named expressions, please provide a single 
   * @param expressions
   * @return
   */
  public abstract AggregatingEvaluator getAggregatingOperator(RecordPointer record, LogicalExpression e);
  
}
