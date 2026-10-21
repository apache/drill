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
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.rest.ProjectResources;
import org.apache.drill.exec.server.rest.SavedQueryResources;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import jakarta.ws.rs.WebApplicationException;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestWorkflowResources extends ClusterTest {
  private static final MediaType JSON = MediaType.parse("application/json");
  private static final ObjectMapper M = new ObjectMapper();
  private static final OkHttpClient HTTP = new OkHttpClient.Builder().readTimeout(60, TimeUnit.SECONDS).build();
  private static String base;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(new ClusterFixtureBuilder(dirTestWatcher)
        .configProperty(ExecConstants.HTTP_ENABLE, true)
        .configProperty(ExecConstants.HTTP_PORT_HUNT, true));
    Drillbit bit = cluster.drillbit();
    base = "http://localhost:" + bit.getWebServerPort() + "/api/v1/workflows";
    WorkflowStores stores = WorkflowRunner.getInstance().stores();
    stores.savedQueries().put("q1", M.convertValue(Map.of("id", "q1", "name", "Employees",
        "sql", "SELECT employee_id FROM cp.`employee.json` LIMIT 7;", "owner", "anonymous"),
        SavedQueryResources.SavedQuery.class));
    stores.projects().put("p1", M.convertValue(Map.of("id", "p1", "name", "P1", "owner", "anonymous",
        "savedQueryIds", List.of("q1", "q3")), ProjectResources.Project.class));
    stores.savedQueries().put("q3", M.convertValue(Map.of("id", "q3", "name", "Olivia's in P1",
        "sql", "SELECT 1 FROM (VALUES(1))", "owner", "olivia"), SavedQueryResources.SavedQuery.class));
    stores.savedQueries().put("q2", M.convertValue(Map.of("id", "q2", "name", "Olivia's",
        "sql", "SELECT 1 FROM (VALUES(1))", "owner", "olivia"), SavedQueryResources.SavedQuery.class));
    stores.projects().put("p2", M.convertValue(Map.of("id", "p2", "name", "P2", "owner", "olivia",
        "isPublic", true, "savedQueryIds", List.of("q2")), ProjectResources.Project.class));
  }

  private static JsonNode call(String method, String path, Object body, int expected) throws Exception {
    RequestBody rb = body == null ? null : RequestBody.create(M.writeValueAsString(body), JSON);
    Request req = new Request.Builder().url(base + path)
        .method(method, rb == null && method.equals("POST") ? RequestBody.create("", JSON) : rb).build();
    try (Response resp = HTTP.newCall(req).execute()) {
      String text = resp.body().string();
      assertEquals(expected, resp.code(), text);
      return text.isEmpty() ? null : M.readTree(text);
    }
  }

  private static Map<String, Object> workflow(String message) {
    return Map.of("projectId", "p1", "name", "Employees watch", "savedQueryId", "q1",
        "schedule", Map.of("frequency", "daily", "timeOfDay", "08:00", "timeoutSeconds", 60),
        "actions", List.of(
            Map.of("id", "big", "type", "record_alert",
                "condition", Map.of("expr", "rowCount >= 7"), "config", Map.of("message", message)),
            Map.of("id", "never", "type", "record_alert",
                "condition", Map.of("expr", "rowCount > 1000"), "config", Map.of("message", "x")),
            Map.of("id", "after", "type", "record_alert",
                "config", Map.of("message", "{{actions.never.message}}"))));
  }

  @Test
  public void crudValidateRunAndDryRun() throws Exception {
    JsonNode bad = call("POST", "", Map.of("projectId", "p1", "name", "", "savedQueryId", "q1",
        "actions", List.of(Map.of("id", "a", "type", "record_alert", "condition", Map.of("expr", "rowCount >"),
            "config", Map.of("message", "m")))), 422);
    assertTrue(bad.get("errors").toString().contains("actions[0].condition"));

    JsonNode created = call("POST", "", workflow("{{rowCount}} rows"), 201);
    String id = created.get("id").asText();
    assertEquals("anonymous", created.get("owner").asText());
    assertFalse(created.get("schedule").get("enabled").asBoolean());
    assertTrue(created.get("actions").get(0).get("condition").has("tree"));

    assertTrue(call("GET", "?projectId=p1", null, 200).findValuesAsText("id").contains(id));

    JsonNode dry = call("POST", "/" + id + "/dry-run", null, 200);
    assertEquals(7, dry.get("context").get("rowCount").asInt());
    assertEquals("fired", dry.get("actions").get(0).get("outcome").asText());

    JsonNode run = call("POST", "/" + id + "/run", null, 200);
    assertEquals("success", run.get("status").asText());
    assertEquals("7 rows", run.get("actions").get(0).get("outputs").get("message").asText());
    assertEquals("condition false", run.get("actions").get(1).get("reason").asText());
    assertEquals("upstream never skipped", run.get("actions").get(2).get("reason").asText());

    assertEquals(1, call("GET", "/" + id + "/runs", null, 200).size());

    call("POST", "/" + id + "/enabled", Map.of("enabled", true), 200);
    assertTrue(call("GET", "/" + id, null, 200).get("schedule").get("enabled").asBoolean());

    assertEquals(3, call("GET", "/action-types", null, 200).size());
    assertEquals("all", call("POST", "/parse-condition", Map.of("expr", "a > 1 && b"), 200)
        .get("tree").fieldNames().next());
    assertEquals(10, call("POST", "/parse-condition", Map.of("expr", "rowCount >"), 200).get("position").asInt());

    call("DELETE", "/" + id, null, 204);
    call("GET", "/" + id, null, 404);
  }

  @Test
  public void savedQueryMustBelongToProject() throws Exception {
    Map<String, Object> wf = new HashMap<>(workflow("m"));
    wf.put("savedQueryId", "other");
    assertTrue(call("POST", "", wf, 422).get("errors").toString().contains("savedQueryId"));
  }

  @Test
  public void savedQueryMustBeOwnedByTheRunAsUser() throws Exception {
    Map<String, Object> wf = new HashMap<>(workflow("m"));
    wf.put("savedQueryId", "q3");
    JsonNode errors = call("POST", "", wf, 422).get("errors");
    assertTrue(errors.toString().contains("savedQueryId"), errors.toString());
    assertTrue(errors.toString().contains("must be owned by anonymous"), errors.toString());

    String id = call("POST", "", workflow("m"), 201).get("id").asText();
    assertTrue(call("PUT", "/" + id, wf, 422).get("errors").toString().contains("savedQueryId"));
    call("DELETE", "/" + id, null, 204);
  }

  @Test
  public void adminOutsideTheProjectCannotCreate() {
    WorkflowResources admin = new WorkflowResources();
    admin.principal = new DrillUserPrincipal("boss", true);
    Workflow wf = M.convertValue(Map.of("projectId", "p2", "name", "x", "savedQueryId", "q2",
        "actions", List.of(Map.of("id", "a", "type", "record_alert", "config", Map.of("message", "m")))),
        Workflow.class);
    WebApplicationException e = assertThrows(WebApplicationException.class, () -> admin.create(wf));
    assertEquals(409, e.getResponse().getStatus());
    assertEquals(409, assertThrows(WebApplicationException.class, () -> admin.dryRunUnsaved(wf))
        .getResponse().getStatus());
  }

  @Test
  public void runNowIgnoresAStaleRunningFlagButNotALiveOne() throws Exception {
    String id = call("POST", "", workflow("m"), 201).get("id").asText();
    WorkflowStores stores = WorkflowRunner.getInstance().stores();
    Workflow wf = stores.workflows().get(id);
    wf.isRunning = true;
    wf.runStartedAt = Instant.now().toString();
    stores.workflows().put(id, wf);
    assertEquals("Workflow is already running", call("POST", "/" + id + "/run", null, 409).get("error").asText());

    wf.runStartedAt = Instant.now().minusSeconds(86_400).toString();
    stores.workflows().put(id, wf);
    assertEquals("success", call("POST", "/" + id + "/run", null, 200).get("status").asText());
    assertFalse(stores.workflows().get(id).isRunning);
    call("DELETE", "/" + id, null, 204);
  }

  private static Map<String, Object> forged(Map<String, Object> wf) {
    Map<String, Object> out = new HashMap<>(wf);
    out.put("id", "forged-id");
    out.put("owner", "mallory");
    out.put("status", "active");
    out.put("isRunning", true);
    out.put("lastStatus", "success");
    out.put("schedule", Map.of("frequency", "daily", "timeOfDay", "08:00", "enabled", true));
    return out;
  }

  @Test
  public void forgedServerFieldsAreDiscarded() throws Exception {
    JsonNode created = call("POST", "", forged(workflow("m")), 201);
    String id = created.get("id").asText();
    assertNotEquals("forged-id", id);
    assertEquals("anonymous", created.get("owner").asText());
    assertEquals("paused", created.get("status").asText());
    assertFalse(created.get("isRunning").asBoolean());
    assertFalse(created.has("lastStatus"));
    assertFalse(created.get("schedule").get("enabled").asBoolean());

    JsonNode updated = call("PUT", "/" + id, forged(workflow("m2")), 200);
    assertEquals(id, updated.get("id").asText());
    assertEquals("anonymous", updated.get("owner").asText());
    assertEquals("paused", updated.get("status").asText());
    assertFalse(updated.get("isRunning").asBoolean());
    assertFalse(updated.has("lastStatus"));
    assertFalse(updated.get("schedule").get("enabled").asBoolean());
    call("DELETE", "/" + id, null, 204);
  }

  @Test
  public void ownerWhoLeftTheProjectCannotBeRunOrReEnabled() throws Exception {
    String id = call("POST", "", workflow("m"), 201).get("id").asText();
    WorkflowStores stores = WorkflowRunner.getInstance().stores();
    Workflow wf = stores.workflows().get(id);
    wf.owner = "departed";
    wf.status = "owner_lost_access";
    stores.workflows().put(id, wf);

    assertTrue(call("POST", "/" + id + "/run", null, 409).get("error").asText().contains("click Take ownership"));
    assertTrue(call("POST", "/" + id + "/enabled", Map.of("enabled", true), 409)
        .get("error").asText().contains("click Take ownership"));

    // A workflow migrated with no owner is repaired to anonymous on start.
    wf = stores.workflows().get(id);
    wf.owner = null;
    stores.workflows().put(id, wf);
    WorkflowRunner.getInstance().fillMissingOwners();
    assertEquals("anonymous", stores.workflows().get(id).owner);
    assertEquals(0, call("GET", "/" + id + "/runs", null, 200).size());
    call("DELETE", "/" + id, null, 204);
  }

  @Test
  public void nullBodiesAreBadRequests() throws Exception {
    String id = call("POST", "", workflow("m"), 201).get("id").asText();
    call("POST", "/parse-condition", Map.of(), 400);
    call("POST", "/" + id + "/enabled", null, 400);
    call("DELETE", "/" + id, null, 204);
  }

  /** Auth is off in this cluster, so non-admin checks call the resource directly with a plain user. */
  private static WorkflowResources as(String user) {
    WorkflowResources r = new WorkflowResources();
    r.principal = new DrillUserPrincipal(user, false);
    return r;
  }

  private static int status(Runnable call) {
    return assertThrows(WebApplicationException.class, call::run).getResponse().getStatus();
  }

  @Test
  public void publicViewerSeesDefinitionButNotRunsAndCannotCreateOrValidate() throws Exception {
    Workflow wf = M.convertValue(Map.of("projectId", "p2", "name", "Olivia watch", "savedQueryId", "q2",
        "actions", List.of(Map.of("id", "a", "type", "record_alert", "config", Map.of("message", "m")))),
        Workflow.class);
    String id = ((Workflow) as("olivia").create(wf).getEntity()).id;

    WorkflowResources stranger = as("stranger");
    assertEquals(id, stranger.get(id).id);
    assertEquals(403, status(() -> stranger.runs(id)));
    assertEquals(403, status(() -> stranger.update(id, wf)));
    assertEquals(403, status(() -> stranger.delete(id)));
    assertEquals(403, status(() -> stranger.create(wf)));
    assertEquals(0, as("olivia").runs(id).size());

    Workflow other = M.convertValue(Map.of("projectId", "p1", "name", "x", "savedQueryId", "q1"), Workflow.class);
    assertEquals(403, status(() -> stranger.validateOnly(other)));
    as("olivia").delete(id);
  }

  @Test
  public void validateDoesNotTrustAnIdFromAnotherProject() throws Exception {
    String hidden = call("POST", "", workflow("m"), 201).get("id").asText();
    try {
      Workflow probe = M.convertValue(Map.of("id", hidden, "projectId", "p2", "name", "x", "savedQueryId", "q2",
          "actions", List.of(Map.of("id", "a", "type", "record_alert", "config", Map.of("message", "m")))),
          Workflow.class);
      String errors = M.writeValueAsString(as("stranger").validateOnly(probe).errors);
      assertTrue(errors.contains("runs as stranger"), errors);
      assertFalse(errors.contains("anonymous"), errors);
    } finally {
      call("DELETE", "/" + hidden, null, 204);
    }
  }

  @Test
  public void observabilityIsMembersOnly() throws Exception {
    WorkflowResources stranger = as("stranger");
    assertEquals(400, statusOf(() -> stranger.observabilitySummary(null)));
    assertEquals(403, statusOf(() -> stranger.observabilitySummary("p2")));

    Workflow wf = M.convertValue(Map.of("projectId", "p2", "name", "Olivia watch", "savedQueryId", "q2",
        "actions", List.of(Map.of("id", "a", "type", "record_alert", "config", Map.of("message", "m")))),
        Workflow.class);
    String id = ((Workflow) as("olivia").create(wf).getEntity()).id;
    try {
      assertEquals(403, statusOf(() -> stranger.health(id)));
      stranger.storageRegistry = cluster.drillbit().getContext().getStorage();
      assertFalse(stranger.observabilityStatus().containsKey("logDir"));
    } finally {
      as("olivia").delete(id);
    }
  }

  private static int statusOf(org.junit.jupiter.api.function.Executable call) {
    return assertThrows(WebApplicationException.class, call).getResponse().getStatus();
  }
}
