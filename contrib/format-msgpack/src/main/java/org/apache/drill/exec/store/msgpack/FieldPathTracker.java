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
package org.apache.drill.exec.store.msgpack;

import java.util.Stack;

import org.apache.drill.shaded.guava.com.google.common.base.Joiner;

public class FieldPathTracker {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FieldPathTracker.class);

  /**
   * Keep track of the navigation inside the msgpack message. We use this to help
   * track down issues with the data files.
   */
  private final Stack<String> currentFieldPath = new Stack<>();

  public FieldPathTracker() {
  }

  public void reset() {
    // Remove all previous elements in case did not come out of the last message in
    // an orderly fashion.
    currentFieldPath.removeAllElements();
    // To start push a "root" in our field path.
    currentFieldPath.push("root");
  }

  /**
   * Add the field name to our current field path.
   */
  public void enter(String fieldName) {
    currentFieldPath.push(fieldName);
  }

  /**
   * When we return from writing a field we need to remove it from our current
   * field path.
   */
  public void leave() {
    currentFieldPath.pop();
  }

  @Override
  public String toString() {
    return Joiner.on(".").join(currentFieldPath);
  }
}