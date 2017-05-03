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
package org.apache.drill.exec.ops;

import java.util.Iterator;

import org.apache.drill.exec.physical.base.PhysicalOperator;

/**
 * Utility methods, formerly on the OperatorContext class, that work with
 * operators. The utilities here are available to operators at unit test
 * time, while methods in OperatorContext are available only in production
 * code.
 */

public class OperatorUtilities {

  private OperatorUtilities() { }

  public static int getChildCount(PhysicalOperator popConfig) {
    Iterator<PhysicalOperator> iter = popConfig.iterator();
    int count = 0;
    while (iter.hasNext()) {
      iter.next();
      count++;
    }

    if (count == 0) {
      count = 1;
    }
    return count;
  }
}
