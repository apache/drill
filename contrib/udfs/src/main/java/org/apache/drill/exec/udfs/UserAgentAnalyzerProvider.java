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

package org.apache.drill.exec.udfs;

import nl.basjes.parse.useragent.AnalyzerUtilities.ParsedArguments;
import nl.basjes.parse.useragent.UserAgentAnalyzer;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;

import java.util.ArrayList;
import java.util.List;

import static nl.basjes.parse.useragent.AnalyzerUtilities.parseArguments;
import static nl.basjes.parse.useragent.UserAgent.USERAGENT_HEADER;
import static org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder;

public class UserAgentAnalyzerProvider {

  public static UserAgentAnalyzer getInstance() {
    return UserAgentAnalyzerHolder.INSTANCE;
  }

  public static List<String> getAllFields() {
    return UserAgentAnalyzerHolder.INSTANCE.getAllPossibleFieldNamesSorted();
  }

  private static List<String> allHeaders = null;

  public static synchronized List<String> getAllHeaders() {
    if (allHeaders == null) {
      allHeaders = new ArrayList<>();
      allHeaders.add(USERAGENT_HEADER);
      allHeaders.addAll(getInstance().supportedClientHintHeaders());
    }
    return allHeaders;
  }

  private static class UserAgentAnalyzerHolder {
    private static final UserAgentAnalyzer INSTANCE = UserAgentAnalyzer.newBuilder()
            .dropTests()
            .hideMatcherLoadStats()
            .immediateInitialization()
            .build();
  }

  public static ParsedArguments parseArgumentArray(NullableVarCharHolder[] input) {
    List<String> inputList = new ArrayList<>();
    for (NullableVarCharHolder holder : input) {
      if (holder == null || holder.buffer == null) {
        inputList.add(null);
      } else {
        inputList.add(getStringFromVarCharHolder(holder));
      }
    }
    return parseArguments(inputList, getAllFields(), getAllHeaders());
  }
}
