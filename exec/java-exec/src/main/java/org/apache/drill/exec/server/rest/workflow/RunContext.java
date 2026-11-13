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
package org.apache.drill.exec.server.rest.workflow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Facts about one run that conditions and templates can reference. */
public class RunContext {
  /** Returned by {@link #resolve} for a path that does not exist. */
  public static final Object MISSING = new Object();
  public static final Set<String> BASE_FIELDS = Set.of(
      "status", "rowCount", "rowCountDelta", "deltaPercent", "durationMs", "intervalMs", "error");
  static final String ROWS_PREFIX = "rows[*].";
  static final String ACTIONS_PREFIX = "actions.";

  /** success | error */
  public String status;
  public long rowCount;
  public Long rowCountDelta;
  public Double deltaPercent;
  public long durationMs;
  public long intervalMs;
  public String error;
  public List<String> columns = List.of();
  public List<Map<String, String>> rows = List.of();
  public final Map<String, Map<String, Object>> actionOutputs = new LinkedHashMap<>();

  public Object resolve(String path) {
    switch (path) {
      case "status": return status;
      case "rowCount": return rowCount;
      case "rowCountDelta": return rowCountDelta;
      case "deltaPercent": return deltaPercent;
      case "durationMs": return durationMs;
      case "intervalMs": return intervalMs;
      case "error": return error;
      default: break;
    }
    if (path.startsWith(ROWS_PREFIX)) {
      String column = path.substring(ROWS_PREFIX.length());
      List<Object> values = new ArrayList<>();
      for (Map<String, String> row : rows) {
        if (row.containsKey(column)) {
          values.add(row.get(column));
        }
      }
      return values;
    }
    if (path.startsWith(ACTIONS_PREFIX)) {
      String[] parts = path.split("\\.", 3);
      if (parts.length == 3) {
        Map<String, Object> outputs = actionOutputs.get(parts[1]);
        if (outputs != null && outputs.containsKey(parts[2])) {
          return outputs.get(parts[2]);
        }
      }
    }
    return MISSING;
  }
}
