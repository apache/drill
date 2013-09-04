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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.BaseBasicEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.values.ValueReader;
import org.apache.drill.exec.ref.values.ScalarValues.BooleanScalar;

public class StringEvaluators {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StringEvaluators.class);

  @FunctionEvaluator("regex_like")
  public static class RegexEvaluator extends BaseBasicEvaluator{
    private final Matcher matcher;
    private final BasicEvaluator eval;
    
    public RegexEvaluator(RecordPointer record, FunctionArguments args){
      super(args.isOnlyConstants(), record);
      matcher = Pattern.compile(ValueReader.getString(args.getEvaluator("pattern").eval())).matcher("");
      eval = args.getEvaluator("value");
    }
    
    @Override
    public BooleanScalar eval() {
      matcher.reset(ValueReader.getChars(eval.eval()));
      return new BooleanScalar(matcher.find());
    }
  }
  
  

}
