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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ActionTypeRegistry {
  private final Map<String, ActionType> types = new LinkedHashMap<>();

  public ActionTypeRegistry(List<ActionType> types) {
    types.forEach(t -> this.types.put(t.id(), t));
  }

  public static ActionTypeRegistry defaults() {
    return new ActionTypeRegistry(List.of(new RecordAlertAction(), new SaveToTableAction(), new EmailAction()));
  }

  public ActionType get(String id) {
    return id == null ? null : types.get(id);
  }

  public Collection<ActionType> all() {
    return types.values();
  }
}
