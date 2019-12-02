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
package org.apache.drill.exec.store.base;

public class PlanStringBuilder {

  private final StringBuilder buf = new StringBuilder();
  int fieldCount = 0;

  public PlanStringBuilder(Object node) {
    this(node.getClass().getSimpleName());
  }

  public PlanStringBuilder(String node) {
    buf.append(node).append(" [");
  }

  public PlanStringBuilder field(String key, String value) {
    if (value != null) {
      startField(key);
      buf.append("\"").append(value).append("\"");
    }
    return this;
  }

  public PlanStringBuilder unquotedField(String key, String value) {
    if (value != null) {
      startField(key);
      buf.append(value);
    }
    return this;
  }

  public PlanStringBuilder field(String key, Object value) {
    if (value != null) {
      startField(key);
      buf.append(value.toString());
    }
    return this;
  }

  public PlanStringBuilder field(String key, int value) {
    startField(key);
    buf.append(value);
    return this;
  }

  private void startField(String key) {
    if (fieldCount++ != 0) {
      buf.append(", ");
    }
    buf.append(key).append("=");
  }

  @Override
  public String toString() { return buf.append("]").toString(); }
}
