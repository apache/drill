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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Sends an email through the global SMTP configuration. */
public class EmailAction implements ActionType {
  private static final String SCHEMA = "{\"type\":\"object\",\"required\":[\"to\",\"subject\",\"body\"],\"properties\":{"
      + "\"to\":{\"type\":\"array\",\"title\":\"Recipients\",\"minItems\":1,"
      + "\"items\":{\"type\":\"string\",\"format\":\"email\"}},"
      + "\"subject\":{\"type\":\"string\",\"title\":\"Subject\",\"x-template\":true},"
      + "\"body\":{\"type\":\"string\",\"title\":\"Body\",\"x-template\":true,\"x-multiline\":true}}}";

  @Override
  public String id() {
    return "email";
  }

  @Override
  public String displayName() {
    return "Send email";
  }

  @Override
  public String description() {
    return "Email the listed recipients using the server's SMTP settings.";
  }

  @Override
  public ObjectNode configSchema() {
    return RecordAlertAction.parse(SCHEMA);
  }

  @Override
  public Map<String, String> outputs() {
    return Map.of("sent", "boolean");
  }

  @Override
  public boolean hasSideEffects() {
    return true;
  }

  @Override
  public Map<String, Object> execute(ActionContext ctx, JsonNode config) throws Exception {
    List<String> to = new ArrayList<>();
    config.path("to").forEach(n -> to.add(ctx.render(n.asText())));
    ctx.services().sendMail(to, ctx.render(config.path("subject").asText()), ctx.render(config.path("body").asText()));
    return Map.of("sent", true);
  }
}
