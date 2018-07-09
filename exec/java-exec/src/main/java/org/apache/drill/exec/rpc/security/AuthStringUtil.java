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
package org.apache.drill.exec.rpc.security;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AuthStringUtil {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AuthStringUtil.class);

  // ignores case
  public static boolean listContains(List<String> list, String toCompare) {
    for (String string : list) {
      if (string.equalsIgnoreCase(toCompare)) {
        return true;
      }
    }
    return false;
  }

  // converts list if strings to set of uppercase strings
  public static Set<String> asSet(List<String> list) {
    if (list == null) {
      return new HashSet<>();
    }
    return list.stream()
        .map(input -> input == null ? null : input.toUpperCase())
        .collect(Collectors.toSet());
  }

  // prevent instantiation
  private AuthStringUtil() {
  }
}
