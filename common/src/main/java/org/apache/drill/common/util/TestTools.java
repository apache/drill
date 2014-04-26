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
package org.apache.drill.common.util;

import java.nio.file.Paths;

import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

public class TestTools {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTools.class);

  static final boolean IS_DEBUG = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments()
      .toString().indexOf("-agentlib:jdwp") > 0;
  static final String WORKING_PATH = Paths.get("").toAbsolutePath().toString();

  public static TestRule getTimeoutRule() {
    return getTimeoutRule(10000);
  }

  public static TestRule getTimeoutRule(int timeout) {
    return IS_DEBUG ? new TestName() : new Timeout(timeout);
  }

  public static String getWorkingPath() {
    return WORKING_PATH;
  }


}
