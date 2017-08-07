/*
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

public class SqlPatternComplexMatcher implements SqlPatternMatcher {
  java.util.regex.Matcher matcher;
  CharSequence charSequenceWrapper;

  public SqlPatternComplexMatcher(String patternString, CharSequence charSequenceWrapper) {
    this.charSequenceWrapper = charSequenceWrapper;
    matcher = java.util.regex.Pattern.compile(patternString).matcher("");
    matcher.reset(charSequenceWrapper);
  }

  @Override
  public int match() {
    matcher.reset();
    return matcher.matches() ? 1 : 0;
  }

}
