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
package org.apache.drill.exec.util;

public class AssertionUtil {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AssertionUtil.class);


  private static final boolean ASSERT_ENABLED;

  static{
    boolean isAssertEnabled = false;
    assert isAssertEnabled = true;
    ASSERT_ENABLED = isAssertEnabled;
  }

  public static boolean isAssertionsEnabled(){
    return ASSERT_ENABLED;
  }
}
