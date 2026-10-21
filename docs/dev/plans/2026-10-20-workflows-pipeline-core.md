# Workflows Pipeline Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the SQL Lab scheduler with project-scoped workflows: a saved query on a schedule followed by an ordered, conditional action pipeline, with REST, migration, run log, and a dedicated editor UI.

**Architecture:** A new Java package `org.apache.drill.exec.server.rest.workflow` holds pure, unit-tested pieces (model, condition parser/evaluator, templates, validator, schedule math, migration, permissions, action pipeline) plus two Drill-bound pieces (`WorkflowRunner` replacing `ScheduleManager`, `WorkflowResources` replacing `ScheduleResources` + `WorkflowConfigResources`). Action types are plugins behind one interface; sub-project 1 ships `record_alert`, `save_to_table`, `email`. The React SPA gets a rewritten Workflows list and a new editor page.

**Tech Stack:** Java 17, Jersey/HK2, Jackson, Drill `PersistentStore`, logback, Jakarta Mail (Eclipse Angus), JUnit 5 + Mockito, Drill `ClusterTest`; React 18, AntD 5, TanStack Query, ECharts, vitest.

**Spec:** [`docs/dev/specs/2026-10-20-workflows-design.md`](../specs/2026-10-20-workflows-design.md) — sections 1–4, 6 (first three action types), 7, 8. Sub-projects 2–4 are out of scope for this plan.

## Global Constraints

- Every new Java/TS/TSX/CSS file starts with the Apache 2.0 license header, copied verbatim from lines 1–17 of `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ScheduleManager.java` (Java) or lines 1–17 of `webapp/src/api/schedules.test.ts` (TS). Code blocks below write `/* Apache license header */` in its place.
- After every Java task: `mvn checkstyle:check -pl exec/java-exec` must pass (all `if` bodies braced, no unused imports).
- Java package for all new backend code: `org.apache.drill.exec.server.rest.workflow` at `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow/`. Tests at the mirrored path under `src/test/java`.
- Store names: `drill.sqllab.workflows`, `drill.sqllab.snapshots` (reused), `drill.sqllab.workflow_claims`, `drill.sqllab.workflow_config` (reused, key `default`), `drill.sqllab.schedules` (legacy, read-only), `drill.sqllab.smtp_config` (key `default`).
- Stores are created with `PersistentStoreConfig.newJacksonBuilder(workManager.getContext().getLpPersistence().getMapper(), X.class).name(NAME).build()` via `storeProvider.getOrCreateStore(...)`.
- Day-of-week convention everywhere: `0 = Sunday … 6 = Saturday`. Times use `ZoneId.systemDefault()`.
- REST errors: `{"error": "<message>"}`; validation failures: HTTP 422 `{"errors":[{"path","message"}]}`.
- Admin detection server-side: injected `DrillUserPrincipal principal; principal.isAdminUser()`. Client-side: `const isAdmin = user ? !user.authEnabled || user.isAdmin : false` from `useCurrentUser()`.
- Webapp commands run from `exec/java-exec/src/main/resources/webapp`: tests `npx vitest run <file>`, types `npx tsc --noEmit`, build `npm run build`.
- Commit messages imperative, **no Claude co-author / attribution lines**.

## Deviations from the spec (simplifications, flagged for the reviewer)

1. **No `/api/v1/schedules` deprecated alias.** The only client is this SPA, which is rewritten here.
2. **`exists` operator dropped**; use `field != null` (parser and builder support it).
3. **Expression editor is an AntD `Input` with live parse errors**, not Monaco (one-line expressions).
4. **Action reorder uses up/down buttons**, not drag-and-drop (no drag lib installed; buttons are keyboard-accessible).
5. **Condition builder is one level** (AND or OR over leaf rows). Deeper trees stay in expression mode.
6. **Health shows average duration, not p95.**
7. **Legacy disabled alert rules and the AI-summary stub are not migrated** (they never produced anything). Legacy `notifyEmails` **are** migrated to `email` actions and will start sending after upgrade; called out in `WORKFLOWS.md`.
8. Snapshots keep **20 stored preview rows** (ZooKeeper 1 MB node cap); actions still see 100 rows in memory.

## Review Focus

1. **Saved SQL ending in `;`** — wrapping it for `COUNT(*)` or CTAS must not produce a syntax error; the trailing semicolon is stripped. (Task 9)
2. **Two Drillbits ticking the same due workflow** — exactly one runs it; the other returns without executing or advancing state. (Task 9)
3. **Condition referencing an upstream action that errored or was skipped** — dependent action is skipped with reason `upstream <id> <outcome>`, never evaluated against missing outputs. (Task 4)
4. **Expression with a dangling operator or unknown field** (`rowCount >`, `rowcount > 5`) — save is rejected with a positioned/pathed error instead of silently never firing. (Tasks 2, 5)
5. **`save_to_table` location from user input** (`dfs.tmp; DROP TABLE x`) — rejected by schema pattern before any SQL is built. (Tasks 5, 8)

---

## File Structure

### Backend (new, `…/server/rest/workflow/`)

| File | Responsibility |
|---|---|
| `Workflow.java` | Workflow document POJO (+ nested `Schedule`, `ActionSpec`, `Condition`) |
| `RunContext.java` | Per-run facts + prior action outputs; path resolution |
| `ConditionParser.java` | Expression string → JSON condition tree |
| `ConditionEvaluator.java` | Condition tree × RunContext → boolean |
| `Templates.java` | `{{path}}` rendering and reference extraction |
| `ActionType.java`, `ActionContext.java`, `WorkflowServices.java`, `ActionOutcome.java`, `ActionTypeRegistry.java` | Action plugin contract |
| `ActionPipeline.java` | Ordered execution with skip propagation, timeouts, dry-run |
| `SchemaCheck.java` | Minimal JSON-schema validation for action configs |
| `WorkflowValidator.java` | Document validation + expr→tree normalization |
| `WorkflowSchedule.java` | Next-run, interval, expiry math |
| `WorkflowConfig.java` | Global settings POJO |
| `WorkflowMigration.java` | Legacy schedule → workflow conversion |
| `WorkflowRun.java` | Snapshot POJO (store `drill.sqllab.snapshots`) |
| `WorkflowRunLog.java` | JSONL run-log line via logger `workflow.runs` |
| `RecordAlertAction.java`, `SaveToTableAction.java`, `EmailAction.java` | Action types |
| `SmtpMailer.java` | Jakarta Mail sender |
| `WorkflowPermissions.java` | Pure permission rules |
| `WorkflowStores.java` | Lazy store accessors |
| `RunClaims.java` | Cluster-wide run claim via `putIfAbsent` |
| `WorkflowRunner.java` | Background ticker, query execution, recording, services impl |
| `WorkflowObservability.java` | Run-log workspace setup + SQL |
| `WorkflowResources.java` | REST `/api/v1/workflows` |

### Backend (modified / deleted)

- Modify `exec/java-exec/pom.xml` (angus-mail), `exec/jdbc-all/pom.xml` (exclude mail), `distribution/src/main/resources/logback.xml` (run-log appender), `server/Drillbit.java`, `server/rest/DrillRestServer.java`.
- Delete `ScheduleManager.java`, `ScheduleResources.java`, `WorkflowConfigResources.java`.

### Frontend (`exec/java-exec/src/main/resources/webapp/src/`)

| File | Responsibility |
|---|---|
| `types/workflow.ts` | All workflow TS types |
| `api/workflows.ts` (+ `.test.ts`) | REST client |
| `utils/conditionTree.ts` (+ `.test.ts`) | tree ⇄ expression text, builder model, refs |
| `components/workflow/TemplateInput.tsx` | Text field with "insert {{field}}" |
| `components/workflow/SchemaForm.tsx` | Form generated from action config schema |
| `components/workflow/ConditionEditor.tsx` | Always / Builder / Expression |
| `components/workflow/ActionCard.tsx` | One action: id, condition, config, reorder |
| `components/workflow/OutcomeList.tsx` | Per-action outcome list (dry-run + runs) |
| `components/workflow/RunsTab.tsx`, `HealthTab.tsx` | Detail tabs |
| `pages/WorkflowEditorPage.tsx` | Editor/detail page |
| `pages/WorkflowsPage.tsx` | Rewritten list (project + admin) |

Delete: `api/schedules.ts`, `api/schedules.test.ts`, `components/query-editor/ScheduleModal.tsx`; remove `Schedule*`, `AlertRule`, `TriggeredAlert`, `QuerySchedule`, `QuerySnapshot` types from `types/index.ts` once no importer remains.

---

### Task 1: Workflow model and RunContext

**Files:**
- Create: `…/workflow/Workflow.java`, `…/workflow/RunContext.java`
- Test: `…/workflow/RunContextTest.java`

**Interfaces:**
- Produces: `Workflow` (public fields), `Workflow.Schedule`, `Workflow.ActionSpec`, `Workflow.Condition` with `static Condition always()`; `RunContext` with public fields, `Object resolve(String path)`, `static final Object MISSING`, `static final Set<String> BASE_FIELDS`, `Map<String, Map<String,Object>> actionOutputs`.

- [ ] **Step 1: Write the failing test**

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class RunContextTest {

  private RunContext ctx() {
    RunContext c = new RunContext();
    c.status = "success";
    c.rowCount = 120;
    c.durationMs = 900;
    c.rows = List.of(Map.of("user", "root", "n", "3"), Map.of("user", "bob", "n", "7"));
    c.actionOutputs.put("judge", Map.of("verdict", true, "reason", "spike"));
    return c;
  }

  @Test
  public void resolvesBaseFields() {
    assertEquals("success", ctx().resolve("status"));
    assertEquals(120L, ctx().resolve("rowCount"));
    assertNull(ctx().resolve("rowCountDelta"));
  }

  @Test
  public void resolvesRowColumnAsList() {
    assertEquals(List.of("root", "bob"), ctx().resolve("rows[*].user"));
  }

  @Test
  public void resolvesActionOutputs() {
    assertEquals(true, ctx().resolve("actions.judge.verdict"));
    assertSame(RunContext.MISSING, ctx().resolve("actions.judge.nope"));
    assertSame(RunContext.MISSING, ctx().resolve("actions.other.verdict"));
  }

  @Test
  public void unknownPathIsMissing() {
    assertSame(RunContext.MISSING, ctx().resolve("rowcount"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl exec/java-exec -Dtest=RunContextTest -DfailIfNoTests=false`
Expected: compilation failure, `RunContext` not found.

- [ ] **Step 3: Implement**

`Workflow.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/**
 * A workflow: a saved query on a schedule followed by an ordered list of actions.
 * This one document is shared by the REST API, the UI and Prospector.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Workflow {
  public String id;
  public String projectId;
  public String name;
  public String description;
  public String owner;
  public String savedQueryId;
  public Schedule schedule = new Schedule();
  public List<ActionSpec> actions = new ArrayList<>();
  public int retentionCount = 30;
  /** active | paused | expired | owner_lost_access */
  public String status = "active";
  public String createdAt;
  public String renewedAt;
  public String lastRunAt;
  public String nextRunAt;
  /** success | error of the most recent run */
  public String lastStatus;
  public int consecutiveFailures;
  public boolean isRunning;

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Schedule {
    /** hourly | daily | weekly | monthly */
    public String frequency = "daily";
    /** HH:mm; for hourly only the minutes matter */
    public String timeOfDay = "08:00";
    /** 0 = Sunday … 6 = Saturday (weekly only) */
    public Integer dayOfWeek;
    /** 1–28 (monthly only) */
    public Integer dayOfMonth;
    public int timeoutSeconds = 300;
    public boolean enabled;
    public String expiresAt;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ActionSpec {
    public String id;
    public String type;
    public Condition condition = Condition.always();
    public ObjectNode config;
  }

  /** Exactly one of always / tree / expr is meaningful; expr is parsed into tree on save. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Condition {
    public Boolean always;
    public JsonNode tree;
    public String expr;

    public static Condition always() {
      Condition c = new Condition();
      c.always = true;
      return c;
    }

    public static Condition tree(JsonNode tree) {
      Condition c = new Condition();
      c.tree = tree;
      return c;
    }
  }
}
```

`RunContext.java`:

```java
/* Apache license header */
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl exec/java-exec -Dtest=RunContextTest -DfailIfNoTests=false`
Expected: 4 tests PASS.

- [ ] **Step 5: Checkstyle and commit**

```bash
mvn checkstyle:check -pl exec/java-exec
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/workflow
git commit -m "Add workflow document model and run context"
```

---

### Task 2: Condition expression parser

**Files:**
- Create: `…/workflow/ConditionParser.java`
- Test: `…/workflow/ConditionParserTest.java`

**Interfaces:**
- Produces: `static JsonNode ConditionParser.parse(String expr) throws ConditionParser.ParseException`; `ParseException` has `public final int position`. Tree nodes: `{"all":[…]}`, `{"any":[…]}`, `{"not":node}`, `{"field":"…","op":"…","value":…}`. Ops: `== != > >= < <= contains startsWith`. A bare path `x` becomes `{"field":"x","op":"==","value":true}`.

- [ ] **Step 1: Write the failing test**

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConditionParserTest {
  private static final ObjectMapper M = new ObjectMapper();

  private static JsonNode json(String s) throws Exception {
    return M.readTree(s.replace('\'', '"'));
  }

  @Test
  public void simpleComparison() throws Exception {
    assertEquals(json("{'field':'rowCount','op':'>','value':50}"),
        ConditionParser.parse("rowCount > 50"));
  }

  @Test
  public void andBindsTighterThanOr() throws Exception {
    assertEquals(json("{'any':[{'field':'a','op':'>','value':1},"
            + "{'all':[{'field':'b','op':'<','value':2},{'field':'c','op':'==','value':'x'}]}]}"),
        ConditionParser.parse("a > 1 || b < 2 && c == 'x'"));
  }

  @Test
  public void parensNotAndBarePath() throws Exception {
    assertEquals(json("{'all':[{'not':{'any':[{'field':'a','op':'==','value':true},"
            + "{'field':'b','op':'!=','value':null}]}},{'field':'actions.judge.verdict','op':'==','value':true}]}"),
        ConditionParser.parse("!(a || b != null) && actions.judge.verdict"));
  }

  @Test
  public void rowPathsWordOpsAndStrings() throws Exception {
    assertEquals(json("{'all':[{'field':'rows[*].user','op':'startsWith','value':'ro\\u0027o'},"
            + "{'field':'error','op':'contains','value':'timeout'}]}"),
        ConditionParser.parse("rows[*].user startsWith \"ro'o\" && error contains 'timeout'"));
  }

  @Test
  public void negativeAndDecimalNumbers() throws Exception {
    assertEquals(json("{'field':'deltaPercent','op':'<=','value':-12.5}"),
        ConditionParser.parse("deltaPercent <= -12.5"));
  }

  @Test
  public void errorsCarryPosition() {
    ConditionParser.ParseException e1 = assertThrows(ConditionParser.ParseException.class,
        () -> ConditionParser.parse("rowCount >"));
    assertEquals(10, e1.position);
    ConditionParser.ParseException e2 = assertThrows(ConditionParser.ParseException.class,
        () -> ConditionParser.parse("rowCount > 5 )"));
    assertEquals(13, e2.position);
    ConditionParser.ParseException e3 = assertThrows(ConditionParser.ParseException.class,
        () -> ConditionParser.parse("5 > rowCount"));
    assertEquals(0, e3.position);
    assertThrows(ConditionParser.ParseException.class, () -> ConditionParser.parse("   "));
    assertThrows(ConditionParser.ParseException.class, () -> ConditionParser.parse("a == 'open"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl exec/java-exec -Dtest=ConditionParserTest -DfailIfNoTests=false`
Expected: compilation failure.

- [ ] **Step 3: Implement**

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Recursive-descent parser for workflow condition expressions.
 * <pre>
 * expr    := or
 * or      := and ( "||" and )*
 * and     := unary ( "&&" unary )*
 * unary   := "!" unary | cmp
 * cmp     := "(" expr ")" | path ( OP literal )?
 * path    := ident ( "." ident | "[*]" )*
 * literal := number | 'string' | "string" | true | false | null
 * </pre>
 * The left side of a comparison is always a field path, which keeps the tree shape
 * identical to what the structured builder produces.
 */
public final class ConditionParser {

  public static class ParseException extends Exception {
    public final int position;

    public ParseException(String message, int position) {
      super(message + " at position " + position);
      this.position = position;
    }
  }

  private static final JsonNodeFactory F = JsonNodeFactory.instance;
  private static final String[] SYMBOL_OPS = {"==", "!=", ">=", "<=", ">", "<"};
  private static final String[] WORD_OPS = {"contains", "startsWith"};

  private final String src;
  private int pos;

  private ConditionParser(String src) {
    this.src = src;
  }

  public static JsonNode parse(String expr) throws ParseException {
    ConditionParser p = new ConditionParser(expr == null ? "" : expr);
    p.skipWs();
    if (p.atEnd()) {
      throw new ParseException("Empty expression", 0);
    }
    JsonNode node = p.or();
    p.skipWs();
    if (!p.atEnd()) {
      throw new ParseException("Unexpected '" + p.src.charAt(p.pos) + "'", p.pos);
    }
    return node;
  }

  private JsonNode or() throws ParseException {
    List<JsonNode> parts = new ArrayList<>();
    parts.add(and());
    while (eat("||")) {
      parts.add(and());
    }
    return group("any", parts);
  }

  private JsonNode and() throws ParseException {
    List<JsonNode> parts = new ArrayList<>();
    parts.add(unary());
    while (eat("&&")) {
      parts.add(unary());
    }
    return group("all", parts);
  }

  private JsonNode unary() throws ParseException {
    skipWs();
    if (peekChar('!') && !src.startsWith("!=", pos)) {
      pos++;
      ObjectNode not = F.objectNode();
      not.set("not", unary());
      return not;
    }
    return cmp();
  }

  private JsonNode cmp() throws ParseException {
    skipWs();
    if (eat("(")) {
      JsonNode inner = or();
      if (!eat(")")) {
        throw new ParseException("Expected ')'", pos);
      }
      return inner;
    }
    int start = pos;
    String path = path();
    if (path == null) {
      throw new ParseException("Expected a field name", start);
    }
    skipWs();
    String op = op();
    if (op == null) {
      return leaf(path, "==", F.booleanNode(true));
    }
    skipWs();
    return leaf(path, op, literal());
  }

  private String path() {
    int start = pos;
    if (!identStart()) {
      return null;
    }
    ident();
    while (true) {
      if (src.startsWith("[*]", pos)) {
        pos += 3;
      } else if (peekChar('.') && pos + 1 < src.length() && isIdentStart(src.charAt(pos + 1))) {
        pos++;
        ident();
      } else {
        break;
      }
    }
    String path = src.substring(start, pos);
    if (path.equals("true") || path.equals("false") || path.equals("null")) {
      pos = start;
      return null;
    }
    return path;
  }

  private String op() {
    for (String op : SYMBOL_OPS) {
      if (src.startsWith(op, pos)) {
        pos += op.length();
        return op;
      }
    }
    for (String op : WORD_OPS) {
      int end = pos + op.length();
      if (src.startsWith(op, pos) && (end >= src.length() || !isIdentPart(src.charAt(end)))) {
        pos = end;
        return op;
      }
    }
    return null;
  }

  private JsonNode literal() throws ParseException {
    int start = pos;
    if (atEnd()) {
      throw new ParseException("Expected a value", pos);
    }
    char c = src.charAt(pos);
    if (c == '\'' || c == '"') {
      return F.textNode(string(c));
    }
    if (c == '-' || Character.isDigit(c)) {
      pos++;
      while (!atEnd() && (Character.isDigit(src.charAt(pos)) || src.charAt(pos) == '.')) {
        pos++;
      }
      String num = src.substring(start, pos);
      try {
        return num.contains(".") ? F.numberNode(Double.parseDouble(num)) : F.numberNode(Long.parseLong(num));
      } catch (NumberFormatException e) {
        throw new ParseException("Invalid number '" + num + "'", start);
      }
    }
    for (String kw : new String[] {"true", "false", "null"}) {
      int end = pos + kw.length();
      if (src.startsWith(kw, pos) && (end >= src.length() || !isIdentPart(src.charAt(end)))) {
        pos = end;
        return kw.equals("null") ? F.nullNode() : F.booleanNode(kw.equals("true"));
      }
    }
    throw new ParseException("Expected a value", start);
  }

  private String string(char quote) throws ParseException {
    int start = pos;
    pos++;
    StringBuilder sb = new StringBuilder();
    while (!atEnd()) {
      char c = src.charAt(pos++);
      if (c == quote) {
        return sb.toString();
      }
      if (c == '\\' && !atEnd()) {
        c = src.charAt(pos++);
      }
      sb.append(c);
    }
    throw new ParseException("Unterminated string", start);
  }

  private static JsonNode group(String key, List<JsonNode> parts) {
    if (parts.size() == 1) {
      return parts.get(0);
    }
    ObjectNode node = F.objectNode();
    ArrayNode arr = node.putArray(key);
    parts.forEach(arr::add);
    return node;
  }

  private static JsonNode leaf(String field, String op, JsonNode value) {
    ObjectNode n = F.objectNode();
    n.put("field", field);
    n.put("op", op);
    n.set("value", value);
    return n;
  }

  private boolean eat(String token) {
    skipWs();
    if (src.startsWith(token, pos)) {
      pos += token.length();
      return true;
    }
    return false;
  }

  private void ident() {
    while (!atEnd() && isIdentPart(src.charAt(pos))) {
      pos++;
    }
  }

  private boolean identStart() {
    return !atEnd() && isIdentStart(src.charAt(pos));
  }

  private static boolean isIdentStart(char c) {
    return Character.isLetter(c) || c == '_';
  }

  private static boolean isIdentPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  private boolean peekChar(char c) {
    return !atEnd() && src.charAt(pos) == c;
  }

  private void skipWs() {
    while (!atEnd() && Character.isWhitespace(src.charAt(pos))) {
      pos++;
    }
  }

  private boolean atEnd() {
    return pos >= src.length();
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl exec/java-exec -Dtest=ConditionParserTest -DfailIfNoTests=false`
Expected: 6 tests PASS. If a position assertion is off by one, fix the parser (positions are 0-based indexes of the offending token's first character), not the test.

- [ ] **Step 5: Checkstyle and commit**

```bash
mvn checkstyle:check -pl exec/java-exec
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow/ConditionParser.java exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/workflow/ConditionParserTest.java
git commit -m "Add workflow condition expression parser"
```

---

### Task 3: Condition evaluator and templates

**Files:**
- Create: `…/workflow/ConditionEvaluator.java`, `…/workflow/Templates.java`
- Test: `…/workflow/ConditionEvaluatorTest.java`, `…/workflow/TemplatesTest.java`

**Interfaces:**
- Consumes: `RunContext`, `Workflow.Condition`, `Workflow.ActionSpec`, `ConditionParser`.
- Produces:
  - `static boolean ConditionEvaluator.evaluate(Workflow.Condition c, RunContext ctx)` (null or `always` → true)
  - `static boolean ConditionEvaluator.evaluate(JsonNode tree, RunContext ctx)`; throws `IllegalArgumentException` on unknown op
  - `static String Templates.render(String template, RunContext ctx)`
  - `static JsonNode Templates.renderAll(JsonNode config, RunContext ctx)` (deep copy with every text value rendered)
  - `static Set<String> Templates.references(JsonNode config)` (all `{{path}}` in text values)
  - `static List<String> Templates.conditionFields(JsonNode tree)`
  - `static Set<String> Templates.referencedActionIds(Workflow.ActionSpec action)` (from condition fields and config templates)

- [ ] **Step 1: Write the failing tests**

`ConditionEvaluatorTest.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConditionEvaluatorTest {

  private static RunContext ctx() {
    RunContext c = new RunContext();
    c.status = "success";
    c.rowCount = 120;
    c.rows = List.of(Map.of("user", "root", "failed", "3"), Map.of("user", "bob", "failed", "70"));
    c.actionOutputs.put("judge", Map.of("verdict", true));
    return c;
  }

  private static boolean eval(String expr) throws Exception {
    return ConditionEvaluator.evaluate(ConditionParser.parse(expr), ctx());
  }

  @Test
  public void numbersAndStrings() throws Exception {
    assertTrue(eval("rowCount > 100"));
    assertFalse(eval("rowCount >= 121"));
    assertTrue(eval("status == 'success'"));
    assertTrue(eval("status != 'error'"));
  }

  @Test
  public void rowColumnsMatchAnyRowAndCoerceNumbers() throws Exception {
    assertTrue(eval("rows[*].failed > 50"));
    assertFalse(eval("rows[*].failed > 100"));
    assertTrue(eval("rows[*].user startsWith 'ro'"));
  }

  @Test
  public void booleansNullsAndMissing() throws Exception {
    assertTrue(eval("actions.judge.verdict"));
    assertTrue(eval("rowCountDelta == null"));
    assertFalse(eval("rowCountDelta > 0"));
    assertFalse(eval("actions.missing.verdict"));
    assertTrue(eval("error == null && !(status == 'error')"));
  }

  @Test
  public void alwaysAndNullConditions() {
    assertTrue(ConditionEvaluator.evaluate((Workflow.Condition) null, ctx()));
    assertTrue(ConditionEvaluator.evaluate(Workflow.Condition.always(), ctx()));
  }

  @Test
  public void unknownOperatorThrows() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> ConditionEvaluator.evaluate(
        new com.fasterxml.jackson.databind.ObjectMapper().readTree(
            "{\"field\":\"rowCount\",\"op\":\"~\",\"value\":1}"), ctx()));
  }
}
```

`TemplatesTest.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TemplatesTest {
  private static final ObjectMapper M = new ObjectMapper();

  private static RunContext ctx() {
    RunContext c = new RunContext();
    c.rowCount = 7;
    c.rows = List.of(Map.of("user", "a"), Map.of("user", "b"));
    c.actionOutputs.put("sum", Map.of("text", "all good"));
    return c;
  }

  @Test
  public void rendersPathsListsAndMissing() {
    assertEquals("7 rows: all good [a, b] ()",
        Templates.render("{{rowCount}} rows: {{ actions.sum.text }} [{{rows[*].user}}] ({{nope}})", ctx()));
  }

  @Test
  public void collectsReferencesAndActionIds() throws Exception {
    Workflow.ActionSpec a = new Workflow.ActionSpec();
    a.config = (ObjectNode) M.readTree("{\"message\":\"{{actions.sum.text}} {{rowCount}}\",\"to\":[\"{{actions.x.y}}\"]}");
    a.condition = Workflow.Condition.tree(ConditionParser.parse("actions.judge.verdict && rowCount > 1"));
    assertEquals(Set.of("actions.sum.text", "rowCount", "actions.x.y"), Templates.references(a.config));
    assertEquals(Set.of("sum", "x", "judge"), Templates.referencedActionIds(a));
  }

  @Test
  public void renderAllRendersNestedText() throws Exception {
    ObjectNode cfg = (ObjectNode) M.readTree("{\"subject\":\"n={{rowCount}}\",\"to\":[\"x@y\"],\"n\":3}");
    assertEquals(M.readTree("{\"subject\":\"n=7\",\"to\":[\"x@y\"],\"n\":3}"), Templates.renderAll(cfg, ctx()));
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl exec/java-exec -Dtest='ConditionEvaluatorTest,TemplatesTest' -DfailIfNoTests=false`
Expected: compilation failure.

- [ ] **Step 3: Implement**

`ConditionEvaluator.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/** Evaluates a condition tree against a {@link RunContext}. */
public final class ConditionEvaluator {

  private ConditionEvaluator() { }

  public static boolean evaluate(Workflow.Condition condition, RunContext ctx) {
    if (condition == null || Boolean.TRUE.equals(condition.always) || condition.tree == null) {
      return true;
    }
    return evaluate(condition.tree, ctx);
  }

  public static boolean evaluate(JsonNode node, RunContext ctx) {
    if (node == null || node.isNull()) {
      return true;
    }
    if (node.has("all")) {
      for (JsonNode child : node.get("all")) {
        if (!evaluate(child, ctx)) {
          return false;
        }
      }
      return true;
    }
    if (node.has("any")) {
      for (JsonNode child : node.get("any")) {
        if (evaluate(child, ctx)) {
          return true;
        }
      }
      return false;
    }
    if (node.has("not")) {
      return !evaluate(node.get("not"), ctx);
    }
    String op = node.path("op").asText("==");
    Object actual = ctx.resolve(node.path("field").asText());
    Object expected = toJava(node.get("value"));
    if (actual instanceof List) {
      for (Object v : (List<?>) actual) {
        if (compare(v, op, expected)) {
          return true;
        }
      }
      return false;
    }
    return compare(actual == RunContext.MISSING ? null : actual, op, expected);
  }

  static boolean compare(Object a, String op, Object b) {
    if (a == null || b == null) {
      boolean bothNull = a == null && b == null;
      switch (op) {
        case "==": return bothNull;
        case "!=": return !bothNull;
        case ">": case ">=": case "<": case "<=": case "contains": case "startsWith": return false;
        default: throw new IllegalArgumentException("Unknown operator: " + op);
      }
    }
    Double an = number(a);
    Double bn = number(b);
    boolean numeric = an != null && bn != null;
    switch (op) {
      case "==": return numeric ? an.doubleValue() == bn.doubleValue() : a.toString().equals(b.toString());
      case "!=": return !compare(a, "==", b);
      case ">": return numeric && an > bn;
      case ">=": return numeric && an >= bn;
      case "<": return numeric && an < bn;
      case "<=": return numeric && an <= bn;
      case "contains": return a.toString().contains(b.toString());
      case "startsWith": return a.toString().startsWith(b.toString());
      default: throw new IllegalArgumentException("Unknown operator: " + op);
    }
  }

  private static Double number(Object o) {
    if (o instanceof Number) {
      return ((Number) o).doubleValue();
    }
    if (o instanceof String) {
      try {
        return Double.parseDouble(((String) o).trim());
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private static Object toJava(JsonNode v) {
    if (v == null || v.isNull()) {
      return null;
    }
    if (v.isBoolean()) {
      return v.booleanValue();
    }
    if (v.isNumber()) {
      return v.numberValue();
    }
    return v.asText();
  }
}
```

`Templates.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** {@code {{path}}} substitution. No logic, no escaping: plain text in, plain text out. */
public final class Templates {
  static final Pattern REF = Pattern.compile("\\{\\{\\s*([A-Za-z_][A-Za-z0-9_.]*(?:\\[\\*\\][A-Za-z0-9_.]*)?)\\s*\\}\\}");

  private Templates() { }

  public static String render(String template, RunContext ctx) {
    if (template == null) {
      return "";
    }
    Matcher m = REF.matcher(template);
    StringBuilder sb = new StringBuilder();
    while (m.find()) {
      m.appendReplacement(sb, Matcher.quoteReplacement(display(ctx.resolve(m.group(1)))));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  public static JsonNode renderAll(JsonNode node, RunContext ctx) {
    if (node == null) {
      return null;
    }
    if (node.isTextual()) {
      return TextNode.valueOf(render(node.asText(), ctx));
    }
    if (node.isObject()) {
      ObjectNode copy = ((ObjectNode) node).objectNode();
      Iterator<Map.Entry<String, JsonNode>> it = node.fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> e = it.next();
        copy.set(e.getKey(), renderAll(e.getValue(), ctx));
      }
      return copy;
    }
    if (node.isArray()) {
      ArrayNode copy = ((ArrayNode) node).arrayNode();
      node.forEach(child -> copy.add(renderAll(child, ctx)));
      return copy;
    }
    return node;
  }

  public static Set<String> references(JsonNode node) {
    Set<String> refs = new LinkedHashSet<>();
    collect(node, refs);
    return refs;
  }

  public static List<String> conditionFields(JsonNode tree) {
    List<String> fields = new ArrayList<>();
    collectFields(tree, fields);
    return fields;
  }

  public static Set<String> referencedActionIds(Workflow.ActionSpec action) {
    Set<String> paths = new LinkedHashSet<>(references(action.config));
    if (action.condition != null) {
      paths.addAll(conditionFields(action.condition.tree));
    }
    return paths.stream()
        .filter(p -> p.startsWith(RunContext.ACTIONS_PREFIX))
        .map(p -> p.split("\\.")[1])
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static void collect(JsonNode node, Set<String> refs) {
    if (node == null) {
      return;
    }
    if (node.isTextual()) {
      Matcher m = REF.matcher(node.asText());
      while (m.find()) {
        refs.add(m.group(1));
      }
    } else if (node.isContainerNode()) {
      node.forEach(child -> collect(child, refs));
    }
  }

  private static void collectFields(JsonNode node, List<String> fields) {
    if (node == null || node.isNull()) {
      return;
    }
    if (node.has("field")) {
      fields.add(node.get("field").asText());
    }
    for (String key : new String[] {"all", "any"}) {
      if (node.has(key)) {
        node.get(key).forEach(child -> collectFields(child, fields));
      }
    }
    if (node.has("not")) {
      collectFields(node.get("not"), fields);
    }
  }

  private static String display(Object value) {
    if (value == null || value == RunContext.MISSING) {
      return "";
    }
    if (value instanceof List) {
      return ((List<?>) value).stream().map(String::valueOf).collect(Collectors.joining(", "));
    }
    return String.valueOf(value);
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl exec/java-exec -Dtest='ConditionEvaluatorTest,TemplatesTest' -DfailIfNoTests=false`
Expected: 8 tests PASS.

- [ ] **Step 5: Checkstyle and commit**

```bash
mvn checkstyle:check -pl exec/java-exec
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/workflow
git commit -m "Add workflow condition evaluator and templates"
```

---

### Task 4: Action contract and pipeline

**Files:**
- Create: `ActionType.java`, `ActionContext.java`, `WorkflowServices.java`, `ActionOutcome.java`, `ActionTypeRegistry.java`, `ActionPipeline.java` (all in `…/workflow/`)
- Test: `…/workflow/ActionPipelineTest.java`

**Interfaces:**
- Consumes: `Workflow`, `RunContext`, `ConditionEvaluator`, `Templates`.
- Produces:
  - `interface ActionType { String id(); String displayName(); String description(); ObjectNode configSchema(); Map<String,String> outputs(); boolean hasSideEffects(); Map<String,Object> execute(ActionContext ctx, JsonNode config) throws Exception; default Map<String,Object> preview(ActionContext ctx, JsonNode config) }`
  - `class ActionContext(Workflow workflow, String sql, RunContext run, WorkflowServices services)` with `workflow()`, `sql()`, `run()`, `services()`, `render(String)`
  - `interface WorkflowServices { void executeStatements(List<String> sql, String userName, int timeoutSeconds) throws Exception; void sendMail(List<String> to, String subject, String body) throws Exception; }`
  - `class ActionOutcome` public fields `id, type, outcome, reason, outputs, error, durationMs`; constants `FIRED="fired"`, `SKIPPED="skipped"`, `ERRORED="errored"`
  - `class ActionTypeRegistry(List<ActionType>)` with `ActionType get(String id)`, `Collection<ActionType> all()`, `static ActionTypeRegistry defaults()`
  - `class ActionPipeline(ActionTypeRegistry registry, ExecutorService executor, long actionTimeoutMs)` with `List<ActionOutcome> run(Workflow wf, String sql, RunContext ctx, WorkflowServices services, boolean dryRun)`

- [ ] **Step 1: Write the failing test**

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ActionPipelineTest {
  private static final ObjectMapper M = new ObjectMapper();
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final List<String> calls = new ArrayList<>();

  /** Records calls; config {"fail":true} throws, {"sleepMs":n} sleeps, {"out":"x"} returns {"value":"x"}. */
  private class Recording implements ActionType {
    private final String id;
    private final boolean sideEffects;
    Recording(String id, boolean sideEffects) { this.id = id; this.sideEffects = sideEffects; }
    @Override public String id() { return id; }
    @Override public String displayName() { return id; }
    @Override public String description() { return id; }
    @Override public ObjectNode configSchema() { return M.createObjectNode(); }
    @Override public Map<String, String> outputs() { return Map.of("value", "string"); }
    @Override public boolean hasSideEffects() { return sideEffects; }
    @Override public Map<String, Object> execute(ActionContext ctx, JsonNode config) throws Exception {
      calls.add(id + ":" + ctx.render(config.path("msg").asText("")));
      if (config.path("fail").asBoolean()) {
        throw new IllegalStateException("boom");
      }
      if (config.has("sleepMs")) {
        Thread.sleep(config.get("sleepMs").asLong());
      }
      return Map.of("value", config.path("out").asText(""));
    }
  }

  private ActionPipeline pipeline() {
    return new ActionPipeline(new ActionTypeRegistry(List.of(
        new Recording("pure", false), new Recording("effect", true))), executor, 200);
  }

  private static Workflow.ActionSpec action(String id, String type, String expr, String config) throws Exception {
    Workflow.ActionSpec a = new Workflow.ActionSpec();
    a.id = id;
    a.type = type;
    a.condition = expr == null ? Workflow.Condition.always() : Workflow.Condition.tree(ConditionParser.parse(expr));
    a.config = (ObjectNode) M.readTree(config);
    return a;
  }

  private static RunContext ctx() {
    RunContext c = new RunContext();
    c.status = "success";
    c.rowCount = 10;
    return c;
  }

  @AfterEach
  public void shutdown() {
    executor.shutdownNow();
  }

  @Test
  public void runsInOrderAndChainsOutputs() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("a", "pure", null, "{\"out\":\"hello\"}"));
    wf.actions.add(action("b", "effect", "actions.a.value == 'hello'", "{\"msg\":\"got {{actions.a.value}}\"}"));
    List<ActionOutcome> out = pipeline().run(wf, "select 1", ctx(), null, false);
    assertEquals(List.of("a:", "b:got hello"), calls);
    assertEquals(ActionOutcome.FIRED, out.get(1).outcome);
  }

  @Test
  public void falseConditionSkipsAndDependentsAreSkipped() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("a", "pure", "rowCount > 100", "{}"));
    wf.actions.add(action("b", "pure", null, "{\"msg\":\"{{actions.a.value}}\"}"));
    wf.actions.add(action("c", "pure", null, "{}"));
    List<ActionOutcome> out = pipeline().run(wf, "select 1", ctx(), null, false);
    assertEquals("condition false", out.get(0).reason);
    assertEquals(ActionOutcome.SKIPPED, out.get(1).outcome);
    assertEquals("upstream a skipped", out.get(1).reason);
    assertEquals(ActionOutcome.FIRED, out.get(2).outcome);
    assertEquals(List.of("c:"), calls);
  }

  @Test
  public void errorIsIsolatedAndPropagatesAsSkip() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("a", "pure", null, "{\"fail\":true}"));
    wf.actions.add(action("b", "pure", "actions.a.value == 'x'", "{}"));
    wf.actions.add(action("c", "pure", null, "{}"));
    List<ActionOutcome> out = pipeline().run(wf, "select 1", ctx(), null, false);
    assertEquals(ActionOutcome.ERRORED, out.get(0).outcome);
    assertEquals("boom", out.get(0).error);
    assertEquals("upstream a errored", out.get(1).reason);
    assertEquals(ActionOutcome.FIRED, out.get(2).outcome);
  }

  @Test
  public void timeoutErrors() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("slow", "pure", null, "{\"sleepMs\":2000}"));
    List<ActionOutcome> out = pipeline().run(wf, "select 1", ctx(), null, false);
    assertEquals(ActionOutcome.ERRORED, out.get(0).outcome);
    assertTrue(out.get(0).error.contains("timed out"), out.get(0).error);
  }

  @Test
  public void dryRunPreviewsSideEffectsButExecutesPureActions() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("a", "pure", null, "{\"out\":\"v\"}"));
    wf.actions.add(action("b", "effect", null, "{\"msg\":\"n={{rowCount}}\"}"));
    List<ActionOutcome> out = pipeline().run(wf, "select 1", ctx(), null, true);
    assertEquals(List.of("a:"), calls);
    assertEquals(M.readTree("{\"msg\":\"n=10\"}"), out.get(1).outputs.get("wouldSend"));
  }

  @Test
  public void unknownTypeErrors() throws Exception {
    Workflow wf = new Workflow();
    wf.actions.add(action("x", "nope", null, "{}"));
    assertEquals("Unknown action type: nope", pipeline().run(wf, "s", ctx(), null, false).get(0).error);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl exec/java-exec -Dtest=ActionPipelineTest -DfailIfNoTests=false`
Expected: compilation failure.

- [ ] **Step 3: Implement**

`ActionType.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;

/** A kind of step a workflow can run after its query. */
public interface ActionType {
  String id();

  String displayName();

  String description();

  /** JSON Schema subset understood by {@link SchemaCheck}; also drives the editor form. */
  ObjectNode configSchema();

  /** Output name → "string" | "number" | "boolean". */
  Map<String, String> outputs();

  /** True if executing touches anything outside the run (tables, mail, HTTP). Dry runs call {@link #preview}. */
  boolean hasSideEffects();

  Map<String, Object> execute(ActionContext ctx, JsonNode config) throws Exception;

  default Map<String, Object> preview(ActionContext ctx, JsonNode config) {
    return Map.of("wouldSend", Templates.renderAll(config, ctx.run()));
  }
}
```

`ActionContext.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

/** Everything an action needs while executing. */
public class ActionContext {
  private final Workflow workflow;
  private final String sql;
  private final RunContext run;
  private final WorkflowServices services;

  public ActionContext(Workflow workflow, String sql, RunContext run, WorkflowServices services) {
    this.workflow = workflow;
    this.sql = sql;
    this.run = run;
    this.services = services;
  }

  public Workflow workflow() { return workflow; }

  public String sql() { return sql; }

  public RunContext run() { return run; }

  public WorkflowServices services() { return services; }

  public String render(String template) {
    return Templates.render(template, run);
  }
}
```

`WorkflowServices.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import java.util.List;

/** Side-effecting capabilities the runner lends to actions; faked in tests. */
public interface WorkflowServices {
  /** Runs the statements in order on one Drill session as the given user. */
  void executeStatements(List<String> sql, String userName, int timeoutSeconds) throws Exception;

  void sendMail(List<String> to, String subject, String body) throws Exception;
}
```

`ActionOutcome.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ActionOutcome {
  public static final String FIRED = "fired";
  public static final String SKIPPED = "skipped";
  public static final String ERRORED = "errored";

  public String id;
  public String type;
  public String outcome;
  public String reason;
  public Map<String, Object> outputs;
  public String error;
  public long durationMs;

  public ActionOutcome() { }

  public ActionOutcome(String id, String type) {
    this.id = id;
    this.type = type;
  }
}
```

`ActionTypeRegistry.java` (the `defaults()` list grows in Tasks 7–8):

```java
/* Apache license header */
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
    return new ActionTypeRegistry(List.of());
  }

  public ActionType get(String id) {
    return id == null ? null : types.get(id);
  }

  public Collection<ActionType> all() {
    return types.values();
  }
}
```

`ActionPipeline.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runs a workflow's actions in list order. An action is skipped when an action it
 * references did not fire or when its condition is false; an action that throws or
 * times out is recorded as errored and never stops the actions after it.
 */
public class ActionPipeline {
  private final ActionTypeRegistry registry;
  private final ExecutorService executor;
  private final long actionTimeoutMs;

  public ActionPipeline(ActionTypeRegistry registry, ExecutorService executor, long actionTimeoutMs) {
    this.registry = registry;
    this.executor = executor;
    this.actionTimeoutMs = actionTimeoutMs;
  }

  public List<ActionOutcome> run(Workflow wf, String sql, RunContext ctx, WorkflowServices services, boolean dryRun) {
    Map<String, String> outcomeById = new HashMap<>();
    List<ActionOutcome> results = new ArrayList<>();
    for (Workflow.ActionSpec action : wf.actions) {
      long start = System.currentTimeMillis();
      ActionOutcome o = new ActionOutcome(action.id, action.type);
      String blocker = firstUnfired(action, outcomeById);
      if (blocker != null) {
        o.outcome = ActionOutcome.SKIPPED;
        o.reason = "upstream " + blocker + " " + outcomeById.getOrDefault(blocker, "missing");
      } else {
        runOne(wf, sql, ctx, services, dryRun, action, o);
      }
      o.durationMs = System.currentTimeMillis() - start;
      outcomeById.put(action.id, o.outcome);
      results.add(o);
    }
    return results;
  }

  private void runOne(Workflow wf, String sql, RunContext ctx, WorkflowServices services, boolean dryRun,
      Workflow.ActionSpec action, ActionOutcome o) {
    ActionType type = registry.get(action.type);
    if (type == null) {
      o.outcome = ActionOutcome.ERRORED;
      o.error = "Unknown action type: " + action.type;
      return;
    }
    try {
      if (!ConditionEvaluator.evaluate(action.condition, ctx)) {
        o.outcome = ActionOutcome.SKIPPED;
        o.reason = "condition false";
        return;
      }
      ActionContext actx = new ActionContext(wf, sql, ctx, services);
      Map<String, Object> outputs = dryRun && type.hasSideEffects()
          ? type.preview(actx, action.config)
          : withTimeout(type, actx, action);
      o.outcome = ActionOutcome.FIRED;
      o.outputs = outputs;
      ctx.actionOutputs.put(action.id, outputs);
    } catch (Exception e) {
      o.outcome = ActionOutcome.ERRORED;
      o.error = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
    }
  }

  private Map<String, Object> withTimeout(ActionType type, ActionContext actx, Workflow.ActionSpec action)
      throws Exception {
    Future<Map<String, Object>> f = executor.submit(() -> type.execute(actx, action.config));
    try {
      return f.get(actionTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      f.cancel(true);
      throw new TimeoutException("Action timed out after " + actionTimeoutMs + " ms");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      throw cause instanceof Exception ? (Exception) cause : e;
    }
  }

  private static String firstUnfired(Workflow.ActionSpec action, Map<String, String> outcomeById) {
    for (String id : Templates.referencedActionIds(action)) {
      if (!ActionOutcome.FIRED.equals(outcomeById.get(id))) {
        return id;
      }
    }
    return null;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl exec/java-exec -Dtest=ActionPipelineTest -DfailIfNoTests=false`
Expected: 6 tests PASS.

- [ ] **Step 5: Checkstyle and commit**

```bash
mvn checkstyle:check -pl exec/java-exec
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/workflow
git commit -m "Add workflow action contract and pipeline"
```

---

### Task 5: Config schema check and workflow validator

**Files:**
- Create: `…/workflow/SchemaCheck.java`, `…/workflow/WorkflowValidator.java`
- Test: `…/workflow/WorkflowValidatorTest.java`

**Interfaces:**
- Consumes: `Workflow`, `ConditionParser`, `Templates`, `ActionTypeRegistry`, `RunContext.BASE_FIELDS`.
- Produces:
  - `static void SchemaCheck.check(JsonNode schema, JsonNode value, String path, List<WorkflowValidator.ValidationError> errors)` — supports `type` (object/string/number/integer/boolean/array), `required`, `properties`, `enum`, `items`, `minItems`, `pattern`, `format: "email"`; ignores keys starting `x-` and `title`/`description`/`default`.
  - `class WorkflowValidator.ValidationError { public String path; public String message; }` (no-arg + `(path, message)` constructors)
  - `static List<ValidationError> WorkflowValidator.validate(Workflow wf, ActionTypeRegistry registry)` — **mutates** each action condition: when `expr` is set and parses, sets `tree` and clears `always`.

- [ ] **Step 1: Write the failing test**

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkflowValidatorTest {
  private static final ObjectMapper M = new ObjectMapper();

  private static ActionType type(String id, String schema, Map<String, String> outputs) {
    return new ActionType() {
      @Override public String id() { return id; }
      @Override public String displayName() { return id; }
      @Override public String description() { return id; }
      @Override public ObjectNode configSchema() {
        try {
          return (ObjectNode) M.readTree(schema);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
      @Override public Map<String, String> outputs() { return outputs; }
      @Override public boolean hasSideEffects() { return false; }
      @Override public Map<String, Object> execute(ActionContext c, JsonNode cfg) { return Map.of(); }
    };
  }

  private static final ActionTypeRegistry REG = new ActionTypeRegistry(List.of(
      type("note", "{\"type\":\"object\",\"required\":[\"message\"],\"properties\":{"
          + "\"message\":{\"type\":\"string\"},"
          + "\"to\":{\"type\":\"array\",\"minItems\":1,\"items\":{\"type\":\"string\",\"format\":\"email\"}},"
          + "\"location\":{\"type\":\"string\",\"pattern\":\"^[A-Za-z0-9_]+(\\\\.[A-Za-z0-9_]+)?$\"},"
          + "\"mode\":{\"type\":\"string\",\"enum\":[\"a\",\"b\"]}}}", Map.of("message", "string"))));

  private static Workflow valid() throws Exception {
    Workflow wf = new Workflow();
    wf.name = "W";
    wf.savedQueryId = "q1";
    wf.actions.add(action("first", "note", "{\"message\":\"hi\"}"));
    return wf;
  }

  private static Workflow.ActionSpec action(String id, String type, String config) throws Exception {
    Workflow.ActionSpec a = new Workflow.ActionSpec();
    a.id = id;
    a.type = type;
    a.config = (ObjectNode) M.readTree(config);
    return a;
  }

  private static Map<String, String> errors(Workflow wf) {
    return WorkflowValidator.validate(wf, REG).stream()
        .collect(Collectors.toMap(e -> e.path, e -> e.message, (a, b) -> a + "; " + b));
  }

  @Test
  public void validWorkflowHasNoErrors() throws Exception {
    assertEquals(Map.of(), errors(valid()));
  }

  @Test
  public void requiredTopLevelAndSchedule() throws Exception {
    Workflow wf = valid();
    wf.name = " ";
    wf.savedQueryId = null;
    wf.schedule.frequency = "yearly";
    wf.schedule.timeOfDay = "25:00";
    wf.schedule.dayOfMonth = 31;
    Map<String, String> e = errors(wf);
    assertTrue(e.containsKey("name"));
    assertTrue(e.containsKey("savedQueryId"));
    assertTrue(e.containsKey("schedule.frequency"));
    assertTrue(e.containsKey("schedule.timeOfDay"));
    assertTrue(e.containsKey("schedule.dayOfMonth"));
  }

  @Test
  public void actionIdTypeAndConfig() throws Exception {
    Workflow wf = valid();
    wf.actions.add(action("first", "note", "{\"message\":\"x\"}"));
    wf.actions.add(action("bad id", "nope", "{}"));
    wf.actions.add(action("c", "note", "{\"to\":[\"not-an-email\"],\"location\":\"dfs.tmp; DROP TABLE x\",\"mode\":\"z\",\"extra\":1}"));
    Map<String, String> e = errors(wf);
    assertTrue(e.get("actions[1].id").contains("Duplicate"));
    assertTrue(e.containsKey("actions[2].id"));
    assertTrue(e.get("actions[2].type").contains("Unknown action type"));
    assertTrue(e.get("actions[3].config.message").contains("required"));
    assertTrue(e.containsKey("actions[3].config.to[0]"));
    assertTrue(e.containsKey("actions[3].config.location"));
    assertTrue(e.containsKey("actions[3].config.mode"));
    assertTrue(e.get("actions[3].config.extra").contains("Unknown"));
  }

  @Test
  public void expressionIsParsedIntoTree() throws Exception {
    Workflow wf = valid();
    wf.actions.get(0).condition = new Workflow.Condition();
    wf.actions.get(0).condition.expr = "rowCount > 5";
    assertEquals(Map.of(), errors(wf));
    assertNotNull(wf.actions.get(0).condition.tree);
  }

  @Test
  public void badExpressionReportsPosition() throws Exception {
    Workflow wf = valid();
    wf.actions.get(0).condition = new Workflow.Condition();
    wf.actions.get(0).condition.expr = "rowCount >";
    assertTrue(errors(wf).get("actions[0].condition").contains("position 10"));
  }

  @Test
  public void referencesMustPointBackwardsAtDeclaredOutputs() throws Exception {
    Workflow wf = valid();
    wf.actions.add(action("second", "note", "{\"message\":\"{{actions.first.message}} {{actions.first.nope}}\"}"));
    wf.actions.get(0).config = (ObjectNode) M.readTree("{\"message\":\"{{actions.second.message}} {{rowcount}}\"}");
    Map<String, String> e = errors(wf);
    assertTrue(e.get("actions[0].config").contains("actions.second.message"));
    assertTrue(e.get("actions[0].config").contains("rowcount"));
    assertTrue(e.get("actions[1].config").contains("actions.first.nope"));
    assertTrue(!e.get("actions[1].config").contains("actions.first.message"));
  }

  @Test
  public void conditionUnknownFieldRejected() throws Exception {
    Workflow wf = valid();
    wf.actions.get(0).condition = new Workflow.Condition();
    wf.actions.get(0).condition.expr = "rowcount > 5";
    assertTrue(errors(wf).get("actions[0].condition").contains("rowcount"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl exec/java-exec -Dtest=WorkflowValidatorTest -DfailIfNoTests=false`
Expected: compilation failure.

- [ ] **Step 3: Implement**

`SchemaCheck.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Validates action configs against the small JSON Schema subset action types declare.
 * ponytail: hand-rolled subset (type/required/properties/enum/items/minItems/pattern/email);
 * swap for a real JSON Schema library if action types need more.
 */
public final class SchemaCheck {
  private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");

  private SchemaCheck() { }

  public static void check(JsonNode schema, JsonNode value, String path,
      List<WorkflowValidator.ValidationError> errors) {
    if (schema == null || schema.isEmpty()) {
      return;
    }
    String type = schema.path("type").asText("");
    if (value == null || value.isMissingNode() || value.isNull()) {
      value = type.equals("object") ? com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode() : null;
      if (value == null) {
        return;
      }
    }
    if (!typeMatches(type, value)) {
      errors.add(new WorkflowValidator.ValidationError(path, "Expected " + type));
      return;
    }
    if (schema.has("enum")) {
      boolean ok = false;
      for (JsonNode allowed : schema.get("enum")) {
        ok |= allowed.equals(value);
      }
      if (!ok) {
        errors.add(new WorkflowValidator.ValidationError(path, "Must be one of " + schema.get("enum")));
      }
    }
    if (type.equals("string")) {
      if (schema.has("pattern") && !Pattern.compile(schema.get("pattern").asText()).matcher(value.asText()).find()) {
        errors.add(new WorkflowValidator.ValidationError(path, "Invalid format"));
      }
      if ("email".equals(schema.path("format").asText()) && !EMAIL.matcher(value.asText()).matches()) {
        errors.add(new WorkflowValidator.ValidationError(path, "Invalid email address"));
      }
    }
    if (type.equals("array")) {
      if (schema.has("minItems") && value.size() < schema.get("minItems").asInt()) {
        errors.add(new WorkflowValidator.ValidationError(path, "At least " + schema.get("minItems").asInt() + " required"));
      }
      for (int i = 0; i < value.size(); i++) {
        check(schema.get("items"), value.get(i), path + "[" + i + "]", errors);
      }
    }
    if (type.equals("object")) {
      JsonNode props = schema.path("properties");
      for (JsonNode req : schema.path("required")) {
        JsonNode v = value.get(req.asText());
        if (v == null || v.isNull() || (v.isTextual() && v.asText().trim().isEmpty())) {
          errors.add(new WorkflowValidator.ValidationError(path + "." + req.asText(), "This field is required"));
        }
      }
      Iterator<String> names = value.fieldNames();
      while (names.hasNext()) {
        String name = names.next();
        if (!props.has(name)) {
          errors.add(new WorkflowValidator.ValidationError(path + "." + name, "Unknown field"));
        } else {
          check(props.get(name), value.get(name), path + "." + name, errors);
        }
      }
    }
  }

  private static boolean typeMatches(String type, JsonNode v) {
    switch (type) {
      case "object": return v.isObject();
      case "string": return v.isTextual();
      case "number": return v.isNumber();
      case "integer": return v.isIntegralNumber();
      case "boolean": return v.isBoolean();
      case "array": return v.isArray();
      default: return true;
    }
  }
}
```

`WorkflowValidator.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Validates a workflow document and normalizes expression conditions into trees. */
public final class WorkflowValidator {
  private static final Set<String> FREQUENCIES = Set.of("hourly", "daily", "weekly", "monthly");
  private static final Pattern TIME = Pattern.compile("^([01]\\d|2[0-3]):[0-5]\\d$");
  private static final Pattern ACTION_ID = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

  public static class ValidationError {
    public String path;
    public String message;

    public ValidationError() { }

    public ValidationError(String path, String message) {
      this.path = path;
      this.message = message;
    }
  }

  private WorkflowValidator() { }

  public static List<ValidationError> validate(Workflow wf, ActionTypeRegistry registry) {
    List<ValidationError> errors = new ArrayList<>();
    if (wf.name == null || wf.name.trim().isEmpty()) {
      errors.add(new ValidationError("name", "Name is required"));
    }
    if (wf.savedQueryId == null || wf.savedQueryId.isEmpty()) {
      errors.add(new ValidationError("savedQueryId", "Choose a saved query"));
    }
    validateSchedule(wf.schedule, errors);

    Map<String, ActionType> earlier = new HashMap<>();
    Set<String> seen = new HashSet<>();
    for (int i = 0; i < wf.actions.size(); i++) {
      Workflow.ActionSpec a = wf.actions.get(i);
      String p = "actions[" + i + "]";
      if (a.id == null || !ACTION_ID.matcher(a.id).matches()) {
        errors.add(new ValidationError(p + ".id", "Use letters, digits and _; start with a letter"));
      } else if (!seen.add(a.id)) {
        errors.add(new ValidationError(p + ".id", "Duplicate action id '" + a.id + "'"));
      }
      ActionType type = registry.get(a.type);
      if (type == null) {
        errors.add(new ValidationError(p + ".type", "Unknown action type: " + a.type));
      } else {
        SchemaCheck.check(type.configSchema(), a.config, p + ".config", errors);
      }
      validateCondition(a, p + ".condition", earlier, errors);
      Set<String> bad = badRefs(Templates.references(a.config), earlier);
      if (!bad.isEmpty()) {
        errors.add(new ValidationError(p + ".config", "Unknown reference(s): " + String.join(", ", bad)));
      }
      if (a.id != null && type != null) {
        earlier.put(a.id, type);
      }
    }
    return errors;
  }

  private static void validateSchedule(Workflow.Schedule s, List<ValidationError> errors) {
    if (s == null) {
      errors.add(new ValidationError("schedule", "Schedule is required"));
      return;
    }
    if (!FREQUENCIES.contains(s.frequency)) {
      errors.add(new ValidationError("schedule.frequency", "Must be hourly, daily, weekly or monthly"));
    }
    if (s.timeOfDay == null || !TIME.matcher(s.timeOfDay).matches()) {
      errors.add(new ValidationError("schedule.timeOfDay", "Use HH:mm (24-hour)"));
    }
    if (s.dayOfWeek != null && (s.dayOfWeek < 0 || s.dayOfWeek > 6)) {
      errors.add(new ValidationError("schedule.dayOfWeek", "0 (Sunday) to 6 (Saturday)"));
    }
    if (s.dayOfMonth != null && (s.dayOfMonth < 1 || s.dayOfMonth > 28)) {
      errors.add(new ValidationError("schedule.dayOfMonth", "1 to 28"));
    }
    if (s.timeoutSeconds < 1 || s.timeoutSeconds > 86_400) {
      errors.add(new ValidationError("schedule.timeoutSeconds", "1 to 86400 seconds"));
    }
  }

  private static void validateCondition(Workflow.ActionSpec a, String p, Map<String, ActionType> earlier,
      List<ValidationError> errors) {
    Workflow.Condition c = a.condition;
    if (c == null) {
      a.condition = Workflow.Condition.always();
      return;
    }
    if (c.expr != null) {
      try {
        c.tree = ConditionParser.parse(c.expr);
        c.always = null;
      } catch (ConditionParser.ParseException e) {
        errors.add(new ValidationError(p, e.getMessage()));
        return;
      }
    }
    if (!Boolean.TRUE.equals(c.always) && c.tree == null) {
      errors.add(new ValidationError(p, "Condition needs always, tree or expr"));
      return;
    }
    Set<String> bad = badRefs(new LinkedHashSet<>(Templates.conditionFields(c.tree)), earlier);
    if (!bad.isEmpty()) {
      errors.add(new ValidationError(p, "Unknown field(s): " + String.join(", ", bad)));
    }
  }

  private static Set<String> badRefs(Set<String> refs, Map<String, ActionType> earlier) {
    Set<String> bad = new LinkedHashSet<>();
    for (String ref : refs) {
      if (RunContext.BASE_FIELDS.contains(ref) || ref.startsWith(RunContext.ROWS_PREFIX)) {
        continue;
      }
      String[] parts = ref.split("\\.", 3);
      boolean ok = ref.startsWith(RunContext.ACTIONS_PREFIX) && parts.length == 3
          && earlier.containsKey(parts[1]) && earlier.get(parts[1]).outputs().containsKey(parts[2]);
      if (!ok) {
        bad.add(ref);
      }
    }
    return bad;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl exec/java-exec -Dtest=WorkflowValidatorTest -DfailIfNoTests=false`
Expected: 7 tests PASS.

- [ ] **Step 5: Checkstyle and commit**

```bash
mvn checkstyle:check -pl exec/java-exec
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/workflow
git commit -m "Add workflow validation with config schema checks"
```

---

### Task 6: Schedule math, config, and legacy migration

**Files:**
- Create: `…/workflow/WorkflowSchedule.java`, `…/workflow/WorkflowConfig.java`, `…/workflow/WorkflowMigration.java`
- Test: `…/workflow/WorkflowScheduleTest.java`, `…/workflow/WorkflowMigrationTest.java`

**Interfaces:**
- Produces:
  - `static Instant WorkflowSchedule.nextRun(Workflow.Schedule s, Instant from, ZoneId zone)` — strictly after `from`
  - `static long WorkflowSchedule.intervalMillis(Workflow.Schedule s)`
  - `static boolean WorkflowSchedule.isExpired(Workflow wf, Instant now, WorkflowConfig cfg)`
  - `class WorkflowConfig { boolean expirationEnabled = true; int expirationDays = 90; int warningDaysBeforeExpiry = 14; int actionTimeoutSeconds = 60; boolean legacyMigrated; }` (public fields, `@JsonIgnoreProperties(ignoreUnknown = true)`)
  - `class WorkflowMigration.LegacySchedule` (public fields mirroring the old `QueryScheduleModel`, ignore unknown)
  - `static Workflow WorkflowMigration.convert(LegacySchedule old, String projectId, String savedQueryName)`

- [ ] **Step 1: Write the failing tests**

`WorkflowScheduleTest.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkflowScheduleTest {
  private static final ZoneId UTC = ZoneId.of("UTC");
  // Wednesday 2026-10-21 10:30 UTC
  private static final Instant NOW = Instant.parse("2026-10-21T10:30:00Z");

  private static Workflow.Schedule s(String freq, String time, Integer dow, Integer dom) {
    Workflow.Schedule s = new Workflow.Schedule();
    s.frequency = freq;
    s.timeOfDay = time;
    s.dayOfWeek = dow;
    s.dayOfMonth = dom;
    return s;
  }

  @Test
  public void hourly() {
    assertEquals(Instant.parse("2026-10-21T10:45:00Z"), WorkflowSchedule.nextRun(s("hourly", "00:45", null, null), NOW, UTC));
    assertEquals(Instant.parse("2026-10-21T11:15:00Z"), WorkflowSchedule.nextRun(s("hourly", "00:15", null, null), NOW, UTC));
  }

  @Test
  public void dailyTodayOrTomorrow() {
    assertEquals(Instant.parse("2026-10-21T14:00:00Z"), WorkflowSchedule.nextRun(s("daily", "14:00", null, null), NOW, UTC));
    assertEquals(Instant.parse("2026-10-22T08:00:00Z"), WorkflowSchedule.nextRun(s("daily", "08:00", null, null), NOW, UTC));
    assertEquals(Instant.parse("2026-10-22T10:30:00Z"), WorkflowSchedule.nextRun(s("daily", "10:30", null, null), NOW, UTC));
  }

  @Test
  public void weeklyUsesSundayZero() {
    assertEquals(Instant.parse("2026-10-25T09:00:00Z"), WorkflowSchedule.nextRun(s("weekly", "09:00", 0, null), NOW, UTC));
    assertEquals(Instant.parse("2026-10-28T09:00:00Z"), WorkflowSchedule.nextRun(s("weekly", "09:00", 3, null), NOW, UTC));
    assertEquals(Instant.parse("2026-10-21T11:00:00Z"), WorkflowSchedule.nextRun(s("weekly", "11:00", 3, null), NOW, UTC));
  }

  @Test
  public void monthly() {
    assertEquals(Instant.parse("2026-11-01T08:00:00Z"), WorkflowSchedule.nextRun(s("monthly", "08:00", null, 1), NOW, UTC));
    assertEquals(Instant.parse("2026-10-28T08:00:00Z"), WorkflowSchedule.nextRun(s("monthly", "08:00", null, 28), NOW, UTC));
  }

  @Test
  public void expiry() {
    WorkflowConfig cfg = new WorkflowConfig();
    Workflow wf = new Workflow();
    wf.schedule.expiresAt = "2026-10-20T00:00:00Z";
    assertTrue(WorkflowSchedule.isExpired(wf, NOW, cfg));
    cfg.expirationEnabled = false;
    assertFalse(WorkflowSchedule.isExpired(wf, NOW, cfg));
    cfg.expirationEnabled = true;
    wf.schedule.expiresAt = null;
    assertFalse(WorkflowSchedule.isExpired(wf, NOW, cfg));
  }
}
```

`WorkflowMigrationTest.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkflowMigrationTest {

  private static WorkflowMigration.LegacySchedule legacy() {
    WorkflowMigration.LegacySchedule o = new WorkflowMigration.LegacySchedule();
    o.id = "s-1";
    o.savedQueryId = "q-1";
    o.owner = "alice";
    o.frequency = "weekly";
    o.timeOfDay = "09:15";
    o.dayOfWeek = 2;
    o.enabled = true;
    o.expiresAt = "2027-01-01T00:00:00Z";
    o.timeoutSeconds = 120;
    o.retentionCount = 10;
    return o;
  }

  @Test
  public void scheduleFieldsCarryOver() {
    Workflow wf = WorkflowMigration.convert(legacy(), "p-1", "Failed logins");
    assertEquals("s-1", wf.id);
    assertEquals("p-1", wf.projectId);
    assertEquals("Failed logins", wf.name);
    assertEquals("alice", wf.owner);
    assertEquals("weekly", wf.schedule.frequency);
    assertEquals(2, wf.schedule.dayOfWeek);
    assertEquals(120, wf.schedule.timeoutSeconds);
    assertTrue(wf.schedule.enabled);
    assertEquals(10, wf.retentionCount);
    assertTrue(wf.actions.isEmpty());
  }

  @Test
  public void orphanIsDisabled() {
    Workflow wf = WorkflowMigration.convert(legacy(), null, "x");
    assertNull(wf.projectId);
    assertFalse(wf.schedule.enabled);
  }

  @Test
  public void persistAlertsAndEmailsBecomeActionsInOrder() throws Exception {
    WorkflowMigration.LegacySchedule o = legacy();
    o.persistResults = true;
    o.resultLocation = "dfs.tmp";
    o.resultFormat = "json";
    o.resultMode = "append";
    o.alertRules = "[{\"id\":\"r1\",\"type\":\"rowCountAbove\",\"threshold\":50,\"enabled\":true},"
        + "{\"id\":\"r2\",\"type\":\"durationAbove\",\"threshold\":30,\"enabled\":true},"
        + "{\"id\":\"r3\",\"type\":\"queryFailed\",\"enabled\":false},"
        + "{\"id\":\"r4\",\"type\":\"durationExceedsInterval\",\"enabled\":true}]";
    o.notifyOnFailure = true;
    o.notifyOnSuccess = true;
    o.notifyEmails = List.of("ops@example.com");

    Workflow wf = WorkflowMigration.convert(o, "p-1", "Q");
    List<String> ids = wf.actions.stream().map(a -> a.id).collect(java.util.stream.Collectors.toList());
    assertEquals(List.of("save_results", "alert_r1", "alert_r2", "alert_r4", "email_on_failure", "email_on_success"), ids);
    assertEquals("{\"location\":\"dfs.tmp\",\"format\":\"json\",\"mode\":\"append\"}", wf.actions.get(0).config.toString());
    assertEquals(ConditionParser.parse("rowCount > 50"), wf.actions.get(1).condition.tree);
    assertEquals(ConditionParser.parse("durationMs > 30000"), wf.actions.get(2).condition.tree);
    assertEquals(ConditionParser.parse("durationMs > 302400000"), wf.actions.get(3).condition.tree);
    assertEquals(ConditionParser.parse("status == 'error'"), wf.actions.get(4).condition.tree);
    assertEquals("ops@example.com", wf.actions.get(5).config.get("to").get(0).asText());

    assertTrue(WorkflowValidator.validate(wf, ActionTypeRegistry.defaults()).stream()
        .noneMatch(e -> e.path.endsWith(".condition")));
  }
}
```

(The last assertion only checks conditions; config/type errors are expected until Tasks 7–8 register the action types. Task 8 tightens it.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl exec/java-exec -Dtest='WorkflowScheduleTest,WorkflowMigrationTest' -DfailIfNoTests=false`
Expected: compilation failure.

- [ ] **Step 3: Implement**

`WorkflowConfig.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Global workflow settings, stored in drill.sqllab.workflow_config under key "default". */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowConfig {
  public boolean expirationEnabled = true;
  public int expirationDays = 90;
  public int warningDaysBeforeExpiry = 14;
  public int actionTimeoutSeconds = 60;
  /** Set once legacy schedules have been converted; prevents re-creating deleted workflows. */
  public boolean legacyMigrated;
}
```

`WorkflowSchedule.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjusters;

/** Schedule arithmetic. Day-of-week is 0 = Sunday … 6 = Saturday. */
public final class WorkflowSchedule {

  private WorkflowSchedule() { }

  public static Instant nextRun(Workflow.Schedule s, Instant from, ZoneId zone) {
    LocalTime t = parseTime(s.timeOfDay);
    ZonedDateTime now = from.atZone(zone);
    ZonedDateTime c;
    switch (s.frequency == null ? "daily" : s.frequency) {
      case "hourly":
        c = now.withMinute(t.getMinute()).withSecond(0).withNano(0);
        if (!c.isAfter(now)) {
          c = c.plusHours(1);
        }
        break;
      case "weekly":
        c = now.with(TemporalAdjusters.nextOrSame(dayOfWeek(s.dayOfWeek == null ? 1 : s.dayOfWeek))).with(t);
        if (!c.isAfter(now)) {
          c = c.plusWeeks(1);
        }
        break;
      case "monthly":
        int dom = s.dayOfMonth == null ? 1 : Math.max(1, Math.min(28, s.dayOfMonth));
        c = now.withDayOfMonth(dom).with(t);
        if (!c.isAfter(now)) {
          c = c.plusMonths(1);
        }
        break;
      default:
        c = now.with(t);
        if (!c.isAfter(now)) {
          c = c.plusDays(1);
        }
        break;
    }
    return c.toInstant();
  }

  public static long intervalMillis(Workflow.Schedule s) {
    switch (s.frequency == null ? "daily" : s.frequency) {
      case "hourly": return 3_600_000L;
      case "weekly": return 604_800_000L;
      case "monthly": return 2_592_000_000L;
      default: return 86_400_000L;
    }
  }

  public static boolean isExpired(Workflow wf, Instant now, WorkflowConfig cfg) {
    if (!cfg.expirationEnabled || wf.schedule.expiresAt == null || wf.schedule.expiresAt.isEmpty()) {
      return false;
    }
    return Instant.parse(wf.schedule.expiresAt).isBefore(now);
  }

  static DayOfWeek dayOfWeek(int d) {
    return d == 0 ? DayOfWeek.SUNDAY : DayOfWeek.of(d);
  }

  private static LocalTime parseTime(String hhmm) {
    try {
      return LocalTime.parse(hhmm == null ? "08:00" : hhmm);
    } catch (RuntimeException e) {
      return LocalTime.of(8, 0);
    }
  }
}
```

`WorkflowMigration.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/** Converts pre-workflow schedules (drill.sqllab.schedules) into workflows. */
public final class WorkflowMigration {
  private static final ObjectMapper M = new ObjectMapper();

  /** Shape of the old QueryScheduleModel; only fields the migration reads. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LegacySchedule {
    public String id;
    public String savedQueryId;
    public String owner;
    public String description;
    public String frequency;
    public boolean enabled;
    public String timeOfDay;
    public Integer dayOfWeek;
    public Integer dayOfMonth;
    public boolean notifyOnSuccess;
    public boolean notifyOnFailure;
    public List<String> notifyEmails;
    public int retentionCount = 30;
    public String nextRunAt;
    public String lastRunAt;
    public String createdAt;
    public String expiresAt;
    public String renewedAt;
    public boolean persistResults;
    public String resultLocation;
    public String resultFormat;
    public String resultMode;
    public String alertRules;
    public int timeoutSeconds = 300;
    public boolean paused;
  }

  private WorkflowMigration() { }

  public static Workflow convert(LegacySchedule o, String projectId, String savedQueryName) {
    Workflow wf = new Workflow();
    wf.id = o.id;
    wf.projectId = projectId;
    wf.name = savedQueryName != null ? savedQueryName : (o.description != null ? o.description : "Workflow " + o.id);
    wf.description = o.description;
    wf.owner = o.owner;
    wf.savedQueryId = o.savedQueryId;
    wf.schedule.frequency = o.frequency;
    wf.schedule.timeOfDay = o.timeOfDay != null ? o.timeOfDay : "08:00";
    wf.schedule.dayOfWeek = o.dayOfWeek;
    wf.schedule.dayOfMonth = o.dayOfMonth;
    wf.schedule.timeoutSeconds = o.timeoutSeconds > 0 ? o.timeoutSeconds : 300;
    // Orphans stay off until an admin assigns them to a project.
    wf.schedule.enabled = o.enabled && !o.paused && projectId != null;
    wf.schedule.expiresAt = o.expiresAt;
    wf.retentionCount = o.retentionCount > 0 ? o.retentionCount : 30;
    wf.status = wf.schedule.enabled ? "active" : "paused";
    wf.createdAt = o.createdAt;
    wf.renewedAt = o.renewedAt;
    wf.lastRunAt = o.lastRunAt;
    wf.nextRunAt = o.nextRunAt;

    if (o.persistResults) {
      ObjectNode cfg = M.createObjectNode();
      cfg.put("location", o.resultLocation != null ? o.resultLocation : "dfs.tmp");
      cfg.put("format", o.resultFormat != null ? o.resultFormat : "parquet");
      cfg.put("mode", o.resultMode != null ? o.resultMode : "overwrite");
      wf.actions.add(action("save_results", "save_to_table", null, cfg));
    }
    addAlertActions(o, wf);
    if (o.notifyEmails != null && !o.notifyEmails.isEmpty()) {
      if (o.notifyOnFailure) {
        wf.actions.add(action("email_on_failure", "email", "status == 'error'",
            email(o.notifyEmails, "Workflow failed: " + wf.name, "{{error}}")));
      }
      if (o.notifyOnSuccess) {
        wf.actions.add(action("email_on_success", "email", "status == 'success'",
            email(o.notifyEmails, "Workflow succeeded: " + wf.name, "{{rowCount}} rows")));
      }
    }
    return wf;
  }

  private static void addAlertActions(LegacySchedule o, Workflow wf) {
    if (o.alertRules == null || o.alertRules.isEmpty()) {
      return;
    }
    JsonNode rules;
    try {
      rules = M.readTree(o.alertRules);
    } catch (Exception e) {
      return;
    }
    long halfInterval = WorkflowSchedule.intervalMillis(wf.schedule) / 2;
    for (JsonNode r : rules) {
      if (!r.path("enabled").asBoolean(false)) {
        continue;
      }
      long t = r.path("threshold").asLong(0);
      String expr;
      String message;
      switch (r.path("type").asText()) {
        case "rowCountAbove": expr = "rowCount > " + t; message = "Row count {{rowCount}} exceeds " + t; break;
        case "rowCountBelow": expr = "rowCount < " + t; message = "Row count {{rowCount}} is below " + t; break;
        case "queryFailed": expr = "status == 'error'"; message = "Query failed: {{error}}"; break;
        case "durationAbove": expr = "durationMs > " + (t * 1000); message = "Took {{durationMs}} ms"; break;
        case "durationExceedsInterval":
          expr = "durationMs > " + halfInterval;
          message = "Took {{durationMs}} ms, over half the schedule interval";
          break;
        default: continue;
      }
      ObjectNode cfg = M.createObjectNode();
      cfg.put("message", message);
      wf.actions.add(action("alert_" + r.path("id").asText().replaceAll("[^A-Za-z0-9_]", "_"), "record_alert", expr, cfg));
    }
  }

  private static ObjectNode email(List<String> to, String subject, String body) {
    ObjectNode cfg = M.createObjectNode();
    ArrayNode arr = cfg.putArray("to");
    to.forEach(arr::add);
    cfg.put("subject", subject);
    cfg.put("body", body);
    return cfg;
  }

  private static Workflow.ActionSpec action(String id, String type, String expr, ObjectNode config) {
    Workflow.ActionSpec a = new Workflow.ActionSpec();
    a.id = id;
    a.type = type;
    a.config = config;
    if (expr != null) {
      try {
        a.condition = Workflow.Condition.tree(ConditionParser.parse(expr));
      } catch (ConditionParser.ParseException e) {
        throw new IllegalStateException("Bad generated expression: " + expr, e);
      }
    }
    return a;
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl exec/java-exec -Dtest='WorkflowScheduleTest,WorkflowMigrationTest' -DfailIfNoTests=false`
Expected: 8 tests PASS.

- [ ] **Step 5: Checkstyle and commit**

```bash
mvn checkstyle:check -pl exec/java-exec
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/workflow
git commit -m "Add workflow schedule math and legacy schedule migration"
```

---

### Task 7: record_alert and save_to_table actions

**Files:**
- Create: `…/workflow/RecordAlertAction.java`, `…/workflow/SaveToTableAction.java`
- Modify: `…/workflow/ActionTypeRegistry.java` (`defaults()`)
- Test: `…/workflow/BuiltInActionsTest.java`

**Interfaces:**
- Consumes: `ActionType`, `ActionContext`, `WorkflowServices.executeStatements`.
- Produces: `RecordAlertAction` (id `record_alert`, outputs `message`, no side effects); `SaveToTableAction` (id `save_to_table`, outputs `path`, side effects) with `static List<String> SaveToTableAction.statements(String workflowId, String sql, JsonNode config, long nowMillis)`; `static String SaveToTableAction.stripSemicolon(String sql)`.

- [ ] **Step 1: Write the failing test**

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BuiltInActionsTest {
  private static final ObjectMapper M = new ObjectMapper();

  @Test
  public void recordAlertRendersMessage() throws Exception {
    RunContext ctx = new RunContext();
    ctx.rowCount = 9;
    Map<String, Object> out = new RecordAlertAction().execute(
        new ActionContext(new Workflow(), "s", ctx, null), M.readTree("{\"message\":\"n={{rowCount}}\"}"));
    assertEquals(Map.of("message", "n=9"), out);
  }

  @Test
  public void saveToTableStatementsPerMode() throws Exception {
    assertEquals(List.of(
            "ALTER SESSION SET `store.format` = 'json'",
            "DROP TABLE IF EXISTS dfs.tmp.`schedules`.`wf_1`",
            "CREATE TABLE dfs.tmp.`schedules`.`wf_1` AS SELECT * FROM (select 1)"),
        SaveToTableAction.statements("wf-1", "select 1 ;  ", M.readTree("{\"location\":\"dfs.tmp\",\"format\":\"json\",\"mode\":\"overwrite\"}"), 5L));
    assertEquals(List.of(
            "ALTER SESSION SET `store.format` = 'parquet'",
            "INSERT INTO dfs.tmp.`schedules`.`wf_1` SELECT * FROM (select 1)"),
        SaveToTableAction.statements("wf-1", "select 1", M.readTree("{\"mode\":\"append\"}"), 5L));
    assertEquals(List.of(
            "ALTER SESSION SET `store.format` = 'parquet'",
            "CREATE TABLE dfs.tmp.`schedules`.`wf_1`.`run_5` AS SELECT * FROM (select 1)"),
        SaveToTableAction.statements("wf-1", "select 1", M.readTree("{\"mode\":\"newTablePerRun\"}"), 5L));
  }

  @Test
  public void saveToTableRunsStatementsAsOwnerOnOneSession() throws Exception {
    List<String> seen = new ArrayList<>();
    WorkflowServices services = new WorkflowServices() {
      @Override public void executeStatements(List<String> sql, String user, int timeout) {
        seen.add(user + ":" + sql.size() + ":" + timeout);
      }
      @Override public void sendMail(List<String> to, String s, String b) { }
    };
    Workflow wf = new Workflow();
    wf.id = "wf-1";
    wf.owner = "alice";
    wf.schedule.timeoutSeconds = 77;
    Map<String, Object> out = new SaveToTableAction().execute(
        new ActionContext(wf, "select 1", new RunContext(), services), M.readTree("{}"));
    assertEquals(List.of("alice:3:77"), seen);
    assertEquals("dfs.tmp.`schedules`.`wf_1`", out.get("path"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl exec/java-exec -Dtest=BuiltInActionsTest -DfailIfNoTests=false`
Expected: compilation failure.

- [ ] **Step 3: Implement**

`RecordAlertAction.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;

/** Records an alert on the run (shown as a badge in run history). */
public class RecordAlertAction implements ActionType {
  private static final String SCHEMA = "{\"type\":\"object\",\"required\":[\"message\"],\"properties\":{"
      + "\"message\":{\"type\":\"string\",\"title\":\"Alert message\",\"x-template\":true}}}";

  @Override public String id() { return "record_alert"; }

  @Override public String displayName() { return "Record alert"; }

  @Override public String description() { return "Flag the run with an alert message in run history."; }

  @Override public ObjectNode configSchema() { return parse(SCHEMA); }

  @Override public Map<String, String> outputs() { return Map.of("message", "string"); }

  @Override public boolean hasSideEffects() { return false; }

  @Override
  public Map<String, Object> execute(ActionContext ctx, JsonNode config) {
    return Map.of("message", ctx.render(config.path("message").asText()));
  }

  static ObjectNode parse(String json) {
    try {
      return (ObjectNode) new ObjectMapper().readTree(json);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
```

`SaveToTableAction.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Persists the full query result with CTAS / INSERT. */
public class SaveToTableAction implements ActionType {
  private static final String SCHEMA = "{\"type\":\"object\",\"properties\":{"
      + "\"location\":{\"type\":\"string\",\"title\":\"Workspace\",\"default\":\"dfs.tmp\","
      + "\"pattern\":\"^[A-Za-z0-9_]+(\\\\.[A-Za-z0-9_]+)?$\"},"
      + "\"format\":{\"type\":\"string\",\"title\":\"Format\",\"default\":\"parquet\",\"enum\":[\"parquet\",\"json\",\"csv\"]},"
      + "\"mode\":{\"type\":\"string\",\"title\":\"Mode\",\"default\":\"overwrite\","
      + "\"enum\":[\"overwrite\",\"append\",\"newTablePerRun\"]}}}";

  @Override public String id() { return "save_to_table"; }

  @Override public String displayName() { return "Save to table"; }

  @Override public String description() { return "Write the full query result to a table (CTAS or INSERT)."; }

  @Override public ObjectNode configSchema() { return RecordAlertAction.parse(SCHEMA); }

  @Override public Map<String, String> outputs() { return Map.of("path", "string"); }

  @Override public boolean hasSideEffects() { return true; }

  @Override
  public Map<String, Object> execute(ActionContext ctx, JsonNode config) throws Exception {
    Workflow wf = ctx.workflow();
    List<String> sql = statements(wf.id, ctx.sql(), config, System.currentTimeMillis());
    ctx.services().executeStatements(sql, wf.owner, wf.schedule.timeoutSeconds);
    return Map.of("path", tablePath(config.path("location").asText("dfs.tmp"), wf.id));
  }

  static List<String> statements(String workflowId, String sql, JsonNode config, long nowMillis) {
    String location = config.path("location").asText("dfs.tmp");
    String format = config.path("format").asText("parquet");
    String mode = config.path("mode").asText("overwrite");
    String query = stripSemicolon(sql);
    String path = tablePath(location, workflowId);
    List<String> out = new ArrayList<>();
    // Same session as the CTAS, so the format option actually applies.
    out.add("ALTER SESSION SET `store.format` = '" + format + "'");
    switch (mode) {
      case "append":
        out.add("INSERT INTO " + path + " SELECT * FROM (" + query + ")");
        break;
      case "newTablePerRun":
        out.add("CREATE TABLE " + path + ".`run_" + nowMillis + "` AS SELECT * FROM (" + query + ")");
        break;
      default:
        out.add("DROP TABLE IF EXISTS " + path);
        out.add("CREATE TABLE " + path + " AS SELECT * FROM (" + query + ")");
        break;
    }
    return out;
  }

  /** Keeps the legacy `schedules` folder so migrated workflows keep writing to the same tables. */
  static String tablePath(String location, String workflowId) {
    return location + ".`schedules`.`" + workflowId.replace('-', '_') + "`";
  }

  public static String stripSemicolon(String sql) {
    String s = sql.trim();
    while (s.endsWith(";")) {
      s = s.substring(0, s.length() - 1).trim();
    }
    return s;
  }
}
```

`ActionTypeRegistry.defaults()` becomes:

```java
  public static ActionTypeRegistry defaults() {
    return new ActionTypeRegistry(List.of(new RecordAlertAction(), new SaveToTableAction()));
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl exec/java-exec -Dtest='BuiltInActionsTest,WorkflowMigrationTest' -DfailIfNoTests=false`
Expected: PASS.

- [ ] **Step 5: Checkstyle and commit**

```bash
mvn checkstyle:check -pl exec/java-exec
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/workflow
git commit -m "Add record_alert and save_to_table workflow actions"
```

---

### Task 8: Email action and SMTP sender

**Files:**
- Modify: `exec/java-exec/pom.xml` (add dependency), `exec/jdbc-all/pom.xml` (exclude)
- Create: `…/workflow/SmtpMailer.java`, `…/workflow/EmailAction.java`
- Modify: `…/workflow/ActionTypeRegistry.java`, `…/workflow/WorkflowMigrationTest.java`
- Test: `…/workflow/SmtpMailerTest.java`, add to `BuiltInActionsTest.java`

**Interfaces:**
- Consumes: `org.apache.drill.exec.server.rest.smtp.SmtpConfig` (getters `getHost() getPort() getUsername() getPassword() getFromAddress() getFromName() getEncryption() isEnabled()`; verify exact getter names in that file before use).
- Produces: `static void SmtpMailer.send(SmtpConfig cfg, List<String> to, String subject, String body) throws Exception`; `static Properties SmtpMailer.properties(SmtpConfig cfg)`; `static MimeMessage SmtpMailer.message(Session s, SmtpConfig cfg, List<String> to, String subject, String body)`; `EmailAction` (id `email`, outputs `sent`, side effects).

- [ ] **Step 1: Add the dependency**

In `exec/java-exec/pom.xml` `<dependencies>` add:

```xml
    <dependency>
      <groupId>org.eclipse.angus</groupId>
      <artifactId>angus-mail</artifactId>
      <version>2.0.3</version>
    </dependency>
```

In `exec/jdbc-all/pom.xml`, inside the `maven-shade-plugin` `<artifactSet><excludes>` block next to the `org.graalvm.*` excludes (around line 469), add:

```xml
              <exclude>org.eclipse.angus:*</exclude>
              <exclude>jakarta.mail:*</exclude>
```

Run: `mvn dependency:tree -pl exec/java-exec -Dincludes=org.eclipse.angus,jakarta.mail,jakarta.activation`
Expected: `angus-mail:2.0.3` and `jakarta.mail-api` present; if a `jakarta.activation-api` version conflict is reported by the enforcer, add an `<exclusion>` for `jakarta.activation:jakarta.activation-api` on angus-mail and rely on the version already managed in the root pom.

- [ ] **Step 2: Write the failing tests**

`SmtpMailerTest.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.apache.drill.exec.server.rest.smtp.SmtpConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SmtpMailerTest {

  private static SmtpConfig cfg(String encryption) {
    SmtpConfig c = new SmtpConfig();
    c.setHost("mail.example.com");
    c.setPort(587);
    c.setUsername("u");
    c.setPassword("p");
    c.setFromAddress("drill@example.com");
    c.setFromName("Apache Drill");
    c.setEncryption(encryption);
    c.setEnabled(true);
    return c;
  }

  @Test
  public void propertiesPerEncryption() {
    Properties tls = SmtpMailer.properties(cfg("starttls"));
    assertEquals("true", tls.getProperty("mail.smtp.starttls.enable"));
    assertEquals("true", tls.getProperty("mail.smtp.auth"));
    assertEquals("true", SmtpMailer.properties(cfg("ssl")).getProperty("mail.smtp.ssl.enable"));
    assertNull(SmtpMailer.properties(cfg("none")).getProperty("mail.smtp.starttls.enable"));
  }

  @Test
  public void messageHeaders() throws Exception {
    MimeMessage m = SmtpMailer.message(Session.getInstance(new Properties()), cfg("starttls"),
        List.of("a@example.com", "b@example.com"), "Subj", "Body");
    assertEquals("Subj", m.getSubject());
    assertEquals(2, m.getAllRecipients().length);
    assertEquals("Apache Drill", ((InternetAddress) m.getFrom()[0]).getPersonal());
    assertEquals("Body", m.getContent());
  }
}
```

(If `SmtpConfig` has no no-arg constructor or setters with these names, construct it with its 8-arg `@JsonCreator` instead; check `smtp/SmtpConfig.java`.)

Add to `BuiltInActionsTest.java`:

```java
  @Test
  public void emailRendersAndSends() throws Exception {
    List<String> sent = new ArrayList<>();
    WorkflowServices services = new WorkflowServices() {
      @Override public void executeStatements(List<String> sql, String user, int timeout) { }
      @Override public void sendMail(List<String> to, String s, String b) { sent.add(to + "|" + s + "|" + b); }
    };
    RunContext ctx = new RunContext();
    ctx.rowCount = 4;
    Map<String, Object> out = new EmailAction().execute(new ActionContext(new Workflow(), "s", ctx, services),
        M.readTree("{\"to\":[\"a@example.com\"],\"subject\":\"{{rowCount}} rows\",\"body\":\"see {{rowCount}}\"}"));
    assertEquals(List.of("[a@example.com]|4 rows|see 4"), sent);
    assertEquals(Map.of("sent", true), out);
  }
```

Tighten the last assertion in `WorkflowMigrationTest.persistAlertsAndEmailsBecomeActionsInOrder` to:

```java
    assertTrue(WorkflowValidator.validate(wf, ActionTypeRegistry.defaults()).isEmpty());
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `mvn test -pl exec/java-exec -Dtest='SmtpMailerTest,BuiltInActionsTest,WorkflowMigrationTest' -DfailIfNoTests=false`
Expected: compilation failure (`SmtpMailer`, `EmailAction` missing).

- [ ] **Step 4: Implement**

`SmtpMailer.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.apache.drill.exec.server.rest.smtp.SmtpConfig;

import java.util.List;
import java.util.Properties;

/** Sends plain-text mail through the admin-configured SMTP server. */
public final class SmtpMailer {

  private SmtpMailer() { }

  public static void send(SmtpConfig cfg, List<String> to, String subject, String body) throws Exception {
    if (cfg == null || !cfg.isEnabled() || cfg.getHost() == null || cfg.getHost().isEmpty()) {
      throw new IllegalStateException("SMTP is not configured; an admin can set it up under SMTP settings");
    }
    boolean auth = cfg.getUsername() != null && !cfg.getUsername().isEmpty();
    Session session = Session.getInstance(properties(cfg), auth ? new Authenticator() {
      @Override
      protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication(cfg.getUsername(), cfg.getPassword());
      }
    } : null);
    Transport.send(message(session, cfg, to, subject, body));
  }

  static Properties properties(SmtpConfig cfg) {
    Properties p = new Properties();
    p.put("mail.smtp.host", cfg.getHost());
    p.put("mail.smtp.port", String.valueOf(cfg.getPort()));
    p.put("mail.smtp.connectiontimeout", "15000");
    p.put("mail.smtp.timeout", "30000");
    if (cfg.getUsername() != null && !cfg.getUsername().isEmpty()) {
      p.put("mail.smtp.auth", "true");
    }
    String enc = cfg.getEncryption() == null ? "starttls" : cfg.getEncryption();
    if (enc.equalsIgnoreCase("starttls")) {
      p.put("mail.smtp.starttls.enable", "true");
      p.put("mail.smtp.starttls.required", "true");
    } else if (enc.equalsIgnoreCase("ssl")) {
      p.put("mail.smtp.ssl.enable", "true");
    }
    return p;
  }

  static MimeMessage message(Session session, SmtpConfig cfg, List<String> to, String subject, String body)
      throws Exception {
    MimeMessage m = new MimeMessage(session);
    m.setFrom(new InternetAddress(cfg.getFromAddress(), cfg.getFromName()));
    for (String addr : to) {
      m.addRecipient(Message.RecipientType.TO, new InternetAddress(addr, true));
    }
    m.setSubject(subject, "UTF-8");
    m.setText(body, "UTF-8");
    return m;
  }
}
```

`EmailAction.java`:

```java
/* Apache license header */
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

  @Override public String id() { return "email"; }

  @Override public String displayName() { return "Send email"; }

  @Override public String description() { return "Email the listed recipients using the server's SMTP settings."; }

  @Override public ObjectNode configSchema() { return RecordAlertAction.parse(SCHEMA); }

  @Override public Map<String, String> outputs() { return Map.of("sent", "boolean"); }

  @Override public boolean hasSideEffects() { return true; }

  @Override
  public Map<String, Object> execute(ActionContext ctx, JsonNode config) throws Exception {
    List<String> to = new ArrayList<>();
    config.path("to").forEach(n -> to.add(ctx.render(n.asText())));
    ctx.services().sendMail(to, ctx.render(config.path("subject").asText()), ctx.render(config.path("body").asText()));
    return Map.of("sent", true);
  }
}
```

`ActionTypeRegistry.defaults()`:

```java
  public static ActionTypeRegistry defaults() {
    return new ActionTypeRegistry(List.of(new RecordAlertAction(), new SaveToTableAction(), new EmailAction()));
  }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn test -pl exec/java-exec -Dtest='SmtpMailerTest,BuiltInActionsTest,WorkflowMigrationTest' -DfailIfNoTests=false`
Expected: PASS.

- [ ] **Step 6: Checkstyle, jdbc-all size, commit**

```bash
mvn checkstyle:check -pl exec/java-exec
mvn install -pl exec/jdbc-all -am -DskipTests   # enforcer must still pass the size limit
git add exec/java-exec/pom.xml exec/jdbc-all/pom.xml exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/workflow
git commit -m "Add email workflow action backed by Jakarta Mail"
```

---

### Task 9: Run records, claims, stores, and WorkflowRunner

**Files:**
- Create: `…/workflow/WorkflowRun.java`, `…/workflow/WorkflowRunLog.java`, `…/workflow/RunClaims.java`, `…/workflow/WorkflowStores.java`, `…/workflow/WorkflowRunner.java`
- Modify: `distribution/src/main/resources/logback.xml`
- Test: `…/workflow/RunClaimsTest.java`, `…/workflow/WorkflowRunLogTest.java`

**Interfaces:**
- Consumes: everything above; `RestQueryRunner`, `QueryWrapper.RestQueryBuilder`, `WebUserConnection`, `WebSessionResources`, `SavedQueryResources.SavedQuery`, `ProjectResources.Project`, `ProjectResources.openStore(PersistentStoreProvider, WorkManager)`, `SmtpConfig`.
- Produces:
  - `WorkflowRun` public fields: `id, scheduleId (= workflow id; legacy JSON name), savedQueryId, executedAt, status ("success"|"error"|"skipped"), trigger ("schedule"|"manual"), rowCount (Long), duration (Long), errorMessage, previewRows (JSON string), previewColumns (JSON string), previousRowCount (Long), rowCountDelta (Long), actions (List<ActionOutcome>)`, plus legacy read-only `aiSummary, triggeredAlerts, resultPath`.
  - `static Map<String,Object> WorkflowRunLog.line(Workflow wf, WorkflowRun run, String drillbit)`; `static void WorkflowRunLog.log(Workflow, WorkflowRun, String drillbit)` (logger `workflow.runs`).
  - `RunClaims(PersistentStore<RunClaims.Claim> store)`: `boolean claim(String key, String drillbit)`, `void prune(Instant olderThan)`; `RunClaims.Claim { public String drillbit; public String claimedAt; }`.
  - `WorkflowStores(PersistentStoreProvider, WorkManager)`: `workflows()`, `runs()`, `claims()`, `legacySchedules()`, `savedQueries()`, `projects()`, `config()` (`PersistentStore<WorkflowConfig>`), `smtp()`; helpers `WorkflowConfig loadConfig()`, `void saveConfig(WorkflowConfig)`, `SavedQuery savedQuery(String id)` (null if missing/deleted), `Project project(String id)` (null if missing/deleted), `String projectForQuery(String savedQueryId)`.
  - `WorkflowRunner` singleton: `static WorkflowRunner getOrCreate(WorkManager, PersistentStoreProvider)`, `static WorkflowRunner getInstance()`, `void start()`, `void close()`, `WorkflowStores stores()`, `ActionTypeRegistry registry()`, `WorkflowRun runNow(Workflow wf)`, `DryRunResult dryRun(Workflow wf)`, `String drillbitId()`; nested `DryRunResult { public RunContext context; public List<ActionOutcome> actions; }`.

- [ ] **Step 1: Write the failing tests**

`RunClaimsTest.java` (Mockito-backed store; do **not** use `InMemoryStore`, whose `putIfAbsent` returns true for existing keys):

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import org.apache.drill.exec.store.sys.PersistentStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RunClaimsTest {

  @SuppressWarnings("unchecked")
  private static PersistentStore<RunClaims.Claim> store(Map<String, RunClaims.Claim> map) {
    PersistentStore<RunClaims.Claim> s = mock(PersistentStore.class);
    when(s.putIfAbsent(anyString(), any())).thenAnswer(i -> map.putIfAbsent(i.getArgument(0), i.getArgument(1)) == null);
    when(s.getAll()).thenAnswer(i -> map.entrySet().iterator());
    doAnswer(i -> map.remove(i.<String>getArgument(0))).when(s).delete(anyString());
    return s;
  }

  @Test
  public void onlyFirstDrillbitWinsASlot() {
    Map<String, RunClaims.Claim> map = new ConcurrentHashMap<>();
    RunClaims a = new RunClaims(store(map));
    RunClaims b = new RunClaims(store(map));
    assertTrue(a.claim("wf:2026-10-21T08:00:00Z", "node-a"));
    assertFalse(b.claim("wf:2026-10-21T08:00:00Z", "node-b"));
    assertTrue(b.claim("wf:2026-10-22T08:00:00Z", "node-b"));
    assertEquals("node-a", map.get("wf:2026-10-21T08:00:00Z").drillbit);
  }

  @Test
  public void pruneRemovesOldClaims() {
    Map<String, RunClaims.Claim> map = new ConcurrentHashMap<>();
    RunClaims c = new RunClaims(store(map));
    c.claim("old", "n");
    map.get("old").claimedAt = "2026-01-01T00:00:00Z";
    c.claim("new", "n");
    c.prune(Instant.parse("2026-06-01T00:00:00Z"));
    assertEquals(java.util.Set.of("new"), map.keySet());
  }
}
```

`WorkflowRunLogTest.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class WorkflowRunLogTest {

  @Test
  public void lineHasRunFactsButNoOutputs() {
    Workflow wf = new Workflow();
    wf.id = "wf-1";
    wf.projectId = "p-1";
    wf.owner = "alice";
    WorkflowRun run = new WorkflowRun();
    run.id = "r-1";
    run.executedAt = "2026-10-21T08:00:00Z";
    run.status = "success";
    run.trigger = "schedule";
    run.rowCount = 5L;
    run.duration = 120L;
    ActionOutcome o = new ActionOutcome("sum", "record_alert");
    o.outcome = ActionOutcome.FIRED;
    o.outputs = Map.of("message", "secret data");
    run.actions = List.of(o);

    Map<String, Object> line = WorkflowRunLog.line(wf, run, "host:31010");
    assertEquals("wf-1", line.get("workflowId"));
    assertEquals("p-1", line.get("projectId"));
    assertEquals(120L, line.get("durationMs"));
    @SuppressWarnings("unchecked")
    Map<String, Object> action = ((List<Map<String, Object>>) line.get("actions")).get(0);
    assertEquals("fired", action.get("outcome"));
    assertFalse(action.containsKey("outputs"));
    assertFalse(line.toString().contains("secret data"));
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl exec/java-exec -Dtest='RunClaimsTest,WorkflowRunLogTest' -DfailIfNoTests=false`
Expected: compilation failure.

- [ ] **Step 3: Implement the small classes**

`WorkflowRun.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * One recorded run, stored in drill.sqllab.snapshots. Field names stay compatible
 * with pre-workflow snapshots (scheduleId is the workflow id).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WorkflowRun {
  public String id;
  public String scheduleId;
  public String savedQueryId;
  public String executedAt;
  public String status;
  public String trigger;
  public Long rowCount;
  public Long duration;
  public String errorMessage;
  public String previewRows;
  public String previewColumns;
  public Long previousRowCount;
  public Long rowCountDelta;
  public List<ActionOutcome> actions;
  // Legacy, read-only
  public String aiSummary;
  public String triggeredAlerts;
  public String resultPath;
}
```

`WorkflowRunLog.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Appends one JSON line per run to workflow-runs.log (logback logger "workflow.runs").
 * Action outputs are deliberately left out: they may contain query data.
 */
public final class WorkflowRunLog {
  private static final Logger RUNS = LoggerFactory.getLogger("workflow.runs");
  private static final Logger logger = LoggerFactory.getLogger(WorkflowRunLog.class);
  private static final ObjectMapper M = new ObjectMapper();

  private WorkflowRunLog() { }

  public static void log(Workflow wf, WorkflowRun run, String drillbit) {
    try {
      RUNS.info(M.writeValueAsString(line(wf, run, drillbit)));
    } catch (Exception e) {
      logger.warn("Failed to write workflow run log line for {}", wf.id, e);
    }
  }

  static Map<String, Object> line(Workflow wf, WorkflowRun run, String drillbit) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("ts", run.executedAt);
    m.put("runId", run.id);
    m.put("workflowId", wf.id);
    m.put("projectId", wf.projectId);
    m.put("owner", wf.owner);
    m.put("drillbit", drillbit);
    m.put("trigger", run.trigger);
    m.put("status", run.status);
    m.put("rowCount", run.rowCount);
    m.put("durationMs", run.duration);
    List<Map<String, Object>> actions = new ArrayList<>();
    if (run.actions != null) {
      for (ActionOutcome o : run.actions) {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("id", o.id);
        a.put("type", o.type);
        a.put("outcome", o.outcome);
        a.put("reason", o.reason);
        a.put("durationMs", o.durationMs);
        a.put("error", o.error);
        actions.add(a);
      }
    }
    m.put("actions", actions);
    return m;
  }
}
```

`RunClaims.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.drill.exec.store.sys.PersistentStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Ensures one Drillbit runs each scheduled slot. Backed by putIfAbsent, which is an
 * atomic ZooKeeper create in clustered mode.
 */
public class RunClaims {

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Claim {
    public String drillbit;
    public String claimedAt;
  }

  private final PersistentStore<Claim> store;

  public RunClaims(PersistentStore<Claim> store) {
    this.store = store;
  }

  public boolean claim(String key, String drillbit) {
    Claim c = new Claim();
    c.drillbit = drillbit;
    c.claimedAt = Instant.now().toString();
    // ZooKeeper node names cannot contain '/'; ISO timestamps and UUIDs do not.
    return store.putIfAbsent(key.replace('/', '_'), c);
  }

  public void prune(Instant olderThan) {
    List<String> stale = new ArrayList<>();
    Iterator<Map.Entry<String, Claim>> it = store.getAll();
    while (it.hasNext()) {
      Map.Entry<String, Claim> e = it.next();
      String at = e.getValue() == null ? null : e.getValue().claimedAt;
      if (at == null || Instant.parse(at).isBefore(olderThan)) {
        stale.add(e.getKey());
      }
    }
    stale.forEach(store::delete);
  }
}
```

`WorkflowStores.java`:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import org.apache.drill.exec.server.rest.ProjectResources;
import org.apache.drill.exec.server.rest.SavedQueryResources;
import org.apache.drill.exec.server.rest.smtp.SmtpConfig;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Lazily opened persistent stores used by the runner and the REST resource. */
public class WorkflowStores {
  static final String WORKFLOWS = "drill.sqllab.workflows";
  static final String RUNS = "drill.sqllab.snapshots";
  static final String CLAIMS = "drill.sqllab.workflow_claims";
  static final String LEGACY_SCHEDULES = "drill.sqllab.schedules";
  static final String SAVED_QUERIES = "drill.sqllab.saved_queries";
  static final String CONFIG = "drill.sqllab.workflow_config";
  static final String SMTP = "drill.sqllab.smtp_config";
  static final String DEFAULT_KEY = "default";

  private final PersistentStoreProvider provider;
  private final WorkManager workManager;
  private final Map<String, PersistentStore<?>> open = new ConcurrentHashMap<>();

  public WorkflowStores(PersistentStoreProvider provider, WorkManager workManager) {
    this.provider = provider;
    this.workManager = workManager;
  }

  @SuppressWarnings("unchecked")
  private <V> PersistentStore<V> store(String name, Class<V> type) {
    return (PersistentStore<V>) open.computeIfAbsent(name, n -> {
      try {
        return provider.getOrCreateStore(PersistentStoreConfig.newJacksonBuilder(
            workManager.getContext().getLpPersistence().getMapper(), type).name(n).build());
      } catch (Exception e) {
        throw new IllegalStateException("Cannot open store " + n, e);
      }
    });
  }

  public PersistentStore<Workflow> workflows() { return store(WORKFLOWS, Workflow.class); }

  public PersistentStore<WorkflowRun> runs() { return store(RUNS, WorkflowRun.class); }

  public PersistentStore<RunClaims.Claim> claims() { return store(CLAIMS, RunClaims.Claim.class); }

  public PersistentStore<WorkflowMigration.LegacySchedule> legacySchedules() {
    return store(LEGACY_SCHEDULES, WorkflowMigration.LegacySchedule.class);
  }

  public PersistentStore<SavedQueryResources.SavedQuery> savedQueries() {
    return store(SAVED_QUERIES, SavedQueryResources.SavedQuery.class);
  }

  public PersistentStore<ProjectResources.Project> projects() {
    return ProjectResources.openStore(provider, workManager);
  }

  public PersistentStore<WorkflowConfig> config() { return store(CONFIG, WorkflowConfig.class); }

  public PersistentStore<SmtpConfig> smtp() { return store(SMTP, SmtpConfig.class); }

  public WorkflowConfig loadConfig() {
    WorkflowConfig c = config().get(DEFAULT_KEY);
    return c != null ? c : new WorkflowConfig();
  }

  public void saveConfig(WorkflowConfig c) {
    config().put(DEFAULT_KEY, c);
  }

  public SavedQueryResources.SavedQuery savedQuery(String id) {
    SavedQueryResources.SavedQuery q = id == null ? null : savedQueries().get(id);
    return q == null || q.getDeletedAt() > 0 ? null : q;
  }

  public ProjectResources.Project project(String id) {
    ProjectResources.Project p = id == null ? null : projects().get(id);
    return p == null || p.getDeletedAt() > 0 ? null : p;
  }

  /** ponytail: full scan of projects; fine for hundreds of projects, index if that grows. */
  public String projectForQuery(String savedQueryId) {
    Iterator<Map.Entry<String, ProjectResources.Project>> it = projects().getAll();
    while (it.hasNext()) {
      ProjectResources.Project p = it.next().getValue();
      if (p != null && p.getDeletedAt() == 0 && p.getSavedQueryIds().contains(savedQueryId)) {
        return p.getId();
      }
    }
    return null;
  }
}
```

`ProjectResources.openStore` is package-private in `org.apache.drill.exec.server.rest`; change its modifier to `public static` (one-word change at `ProjectResources.java` ~L1915). `ProjectResources.canRead` (~L1480) likewise becomes `public static`.

- [ ] **Step 4: Run small-class tests**

Run: `mvn test -pl exec/java-exec -Dtest='RunClaimsTest,WorkflowRunLogTest' -DfailIfNoTests=false`
Expected: PASS.

- [ ] **Step 5: Implement `WorkflowRunner`**

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.rest.ProjectResources;
import org.apache.drill.exec.server.rest.QueryWrapper;
import org.apache.drill.exec.server.rest.RestQueryRunner;
import org.apache.drill.exec.server.rest.SavedQueryResources;
import org.apache.drill.exec.server.rest.WebSessionResources;
import org.apache.drill.exec.server.rest.WebUserConnection;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Background service that runs due workflows: claim the slot, run the saved query,
 * run the action pipeline, record the run. Replaces ScheduleManager.
 */
public class WorkflowRunner implements AutoCloseable, WorkflowServices {
  private static final Logger logger = LoggerFactory.getLogger(WorkflowRunner.class);
  static final int PREVIEW_ROWS = 100;
  /** ponytail: snapshots live in ZooKeeper (1 MB node cap), so only 20 preview rows are stored. */
  static final int STORED_PREVIEW_ROWS = 20;
  private static volatile WorkflowRunner instance;

  public static class DryRunResult {
    public RunContext context;
    public List<ActionOutcome> actions;
  }

  private final WorkManager workManager;
  private final WorkflowStores stores;
  private final ActionTypeRegistry registry = ActionTypeRegistry.defaults();
  private final RunClaims claims;
  private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(daemon("drill-workflow-runner"));
  private final ExecutorService workers = Executors.newCachedThreadPool(daemon("drill-workflow-worker"));
  private final ObjectMapper mapper = new ObjectMapper();

  private WorkflowRunner(WorkManager workManager, PersistentStoreProvider provider) {
    this.workManager = workManager;
    this.stores = new WorkflowStores(provider, workManager);
    this.claims = new RunClaims(stores.claims());
  }

  public static WorkflowRunner getOrCreate(WorkManager wm, PersistentStoreProvider sp) {
    if (instance == null) {
      synchronized (WorkflowRunner.class) {
        if (instance == null) {
          instance = new WorkflowRunner(wm, sp);
        }
      }
    }
    return instance;
  }

  public static WorkflowRunner getInstance() {
    return instance;
  }

  public WorkflowStores stores() { return stores; }

  public ActionTypeRegistry registry() { return registry; }

  public void start() {
    migrateLegacy();
    ticker.scheduleAtFixedRate(this::tick, 30, 60, TimeUnit.SECONDS);
  }

  // ---------------------------------------------------------------- scheduling

  void tick() {
    Instant now = Instant.now();
    WorkflowConfig cfg = stores.loadConfig();
    try {
      claims.prune(now.minus(Duration.ofDays(7)));
    } catch (Exception e) {
      logger.debug("Claim pruning failed", e);
    }
    Iterator<Map.Entry<String, Workflow>> it = stores.workflows().getAll();
    while (it.hasNext()) {
      Workflow wf = it.next().getValue();
      try {
        processDue(wf, now, cfg);
      } catch (Exception e) {
        logger.warn("Workflow {} failed to process", wf.id, e);
      }
    }
  }

  private void processDue(Workflow wf, Instant now, WorkflowConfig cfg) {
    if (!wf.schedule.enabled) {
      return;
    }
    if (WorkflowSchedule.isExpired(wf, now, cfg)) {
      disable(wf, "expired");
      return;
    }
    if (ownerLostAccess(wf)) {
      disable(wf, "owner_lost_access");
      return;
    }
    if (wf.nextRunAt == null || Instant.parse(wf.nextRunAt).isAfter(now)) {
      return;
    }
    if (!claims.claim(wf.id + ":" + wf.nextRunAt, drillbitId())) {
      return; // another Drillbit owns this slot
    }
    if (wf.isRunning) {
      WorkflowRun skipped = newRun(wf, "schedule", now);
      skipped.status = "skipped";
      skipped.errorMessage = "Skipped: previous run still in progress";
      stores.runs().put(skipped.id, skipped);
      wf.nextRunAt = WorkflowSchedule.nextRun(wf.schedule, now, ZoneId.systemDefault()).toString();
      stores.workflows().put(wf.id, wf);
      return;
    }
    execute(wf, "schedule");
  }

  private void disable(Workflow wf, String status) {
    wf.schedule.enabled = false;
    wf.status = status;
    stores.workflows().put(wf.id, wf);
    logger.info("Workflow {} disabled: {}", wf.id, status);
  }

  private boolean ownerLostAccess(Workflow wf) {
    if (wf.projectId == null) {
      return false;
    }
    ProjectResources.Project p = stores.project(wf.projectId);
    return p == null || !WorkflowPermissions.isMember(p, wf.owner);
  }

  // ---------------------------------------------------------------- execution

  public WorkflowRun runNow(Workflow wf) {
    return execute(wf, "manual");
  }

  public DryRunResult dryRun(Workflow wf) {
    SavedQueryResources.SavedQuery q = stores.savedQuery(wf.savedQueryId);
    DryRunResult r = new DryRunResult();
    r.context = runQuery(wf, q);
    r.actions = pipeline().run(wf, q == null ? null : q.getSql(), r.context, this, true);
    return r;
  }

  private WorkflowRun execute(Workflow wf, String trigger) {
    Instant now = Instant.now();
    wf.isRunning = true;
    stores.workflows().put(wf.id, wf);
    WorkflowRun run = newRun(wf, trigger, now);
    try {
      SavedQueryResources.SavedQuery q = stores.savedQuery(wf.savedQueryId);
      RunContext ctx = runQuery(wf, q);
      run.actions = pipeline().run(wf, q == null ? null : q.getSql(), ctx, this, false);
      fillRun(run, ctx);
    } catch (Exception e) {
      run.status = "error";
      run.errorMessage = e.getMessage();
      logger.error("Workflow {} run failed", wf.id, e);
    } finally {
      Workflow fresh = stores.workflows().get(wf.id);
      Workflow target = fresh != null ? fresh : wf;
      target.isRunning = false;
      target.lastRunAt = now.toString();
      target.lastStatus = run.status;
      target.consecutiveFailures = "error".equals(run.status) ? target.consecutiveFailures + 1 : 0;
      if ("schedule".equals(trigger) || target.nextRunAt == null) {
        target.nextRunAt = WorkflowSchedule.nextRun(target.schedule, now, ZoneId.systemDefault()).toString();
      }
      if (fresh != null) {
        stores.workflows().put(target.id, target);
      }
    }
    stores.runs().put(run.id, run);
    WorkflowRunLog.log(wf, run, drillbitId());
    enforceRetention(wf.id, wf.retentionCount);
    return run;
  }

  private ActionPipeline pipeline() {
    return new ActionPipeline(registry, workers, stores.loadConfig().actionTimeoutSeconds * 1000L);
  }

  private RunContext runQuery(Workflow wf, SavedQueryResources.SavedQuery q) {
    RunContext ctx = new RunContext();
    ctx.intervalMs = WorkflowSchedule.intervalMillis(wf.schedule);
    long start = System.currentTimeMillis();
    int timeout = wf.schedule.timeoutSeconds > 0 ? wf.schedule.timeoutSeconds : 300;
    if (q == null) {
      ctx.status = "error";
      ctx.error = "Saved query not found: " + wf.savedQueryId;
    } else {
      try {
        String sql = SaveToTableAction.stripSemicolon(q.getSql());
        RestQueryRunner.QueryResult r = query(List.of(sql), PREVIEW_ROWS, timeout, wf.owner);
        ctx.rows = r.rows != null ? r.rows : List.of();
        ctx.columns = r.columns != null ? new ArrayList<>(r.columns) : List.of();
        ctx.rowCount = ctx.rows.size() < PREVIEW_ROWS ? ctx.rows.size() : countRows(sql, timeout, wf.owner);
        ctx.status = "success";
      } catch (TimeoutException e) {
        ctx.status = "error";
        ctx.error = "Query timed out after " + timeout + " seconds";
      } catch (Exception e) {
        ctx.status = "error";
        ctx.error = e.getMessage();
      }
    }
    ctx.durationMs = System.currentTimeMillis() - start;
    WorkflowRun prev = previousRun(wf.id);
    if ("success".equals(ctx.status) && prev != null && prev.rowCount != null) {
      ctx.rowCountDelta = ctx.rowCount - prev.rowCount;
      ctx.deltaPercent = prev.rowCount == 0 ? null : 100.0 * ctx.rowCountDelta / prev.rowCount;
    }
    return ctx;
  }

  /** Only runs when the preview hit its cap, so small results cost one query. */
  private long countRows(String sql, int timeout, String owner) throws Exception {
    RestQueryRunner.QueryResult r = query(List.of("SELECT COUNT(*) AS cnt FROM (" + sql + ")"), 1, timeout, owner);
    return Long.parseLong(r.rows.get(0).get("cnt"));
  }

  private void fillRun(WorkflowRun run, RunContext ctx) {
    run.status = ctx.status;
    run.errorMessage = ctx.error;
    run.rowCount = ctx.rowCount;
    run.duration = ctx.durationMs;
    run.rowCountDelta = ctx.rowCountDelta;
    run.previousRowCount = ctx.rowCountDelta == null ? null : ctx.rowCount - ctx.rowCountDelta;
    try {
      run.previewRows = mapper.writeValueAsString(ctx.rows.subList(0, Math.min(STORED_PREVIEW_ROWS, ctx.rows.size())));
      run.previewColumns = mapper.writeValueAsString(ctx.columns);
    } catch (Exception e) {
      logger.debug("Failed to serialize preview", e);
    }
  }

  private WorkflowRun newRun(Workflow wf, String trigger, Instant now) {
    WorkflowRun run = new WorkflowRun();
    run.id = UUID.randomUUID().toString();
    run.scheduleId = wf.id;
    run.savedQueryId = wf.savedQueryId;
    run.executedAt = now.toString();
    run.trigger = trigger;
    return run;
  }

  /** ponytail: full scan of runs per execution; index by workflow if run counts grow large. */
  private WorkflowRun previousRun(String workflowId) {
    WorkflowRun best = null;
    Iterator<Map.Entry<String, WorkflowRun>> it = stores.runs().getAll();
    while (it.hasNext()) {
      WorkflowRun r = it.next().getValue();
      if (r != null && workflowId.equals(r.scheduleId) && !"skipped".equals(r.status)
          && (best == null || r.executedAt.compareTo(best.executedAt) > 0)) {
        best = r;
      }
    }
    return best;
  }

  private void enforceRetention(String workflowId, int retentionCount) {
    int limit = retentionCount > 0 ? retentionCount : 30;
    List<WorkflowRun> runs = new ArrayList<>();
    stores.runs().getAll().forEachRemaining(e -> {
      if (e.getValue() != null && workflowId.equals(e.getValue().scheduleId)) {
        runs.add(e.getValue());
      }
    });
    runs.sort((a, b) -> b.executedAt.compareTo(a.executedAt));
    for (int i = limit; i < runs.size(); i++) {
      stores.runs().delete(runs.get(i).id);
    }
  }

  // ---------------------------------------------------------------- services

  @Override
  public void executeStatements(List<String> sql, String userName, int timeoutSeconds) throws Exception {
    query(sql, 1, timeoutSeconds, userName);
  }

  @Override
  public void sendMail(List<String> to, String subject, String body) throws Exception {
    SmtpMailer.send(stores.smtp().get(WorkflowStores.DEFAULT_KEY), to, subject, body);
  }

  /** Runs statements in order on one session; returns the last statement's result. */
  private RestQueryRunner.QueryResult query(List<String> statements, int rowLimit, int timeoutSeconds, String userName)
      throws Exception {
    Future<RestQueryRunner.QueryResult> f = workers.submit(() -> {
      WebUserConnection conn = connection(userName);
      try {
        RestQueryRunner.QueryResult last = null;
        for (String sql : statements) {
          QueryWrapper w = new QueryWrapper.RestQueryBuilder().query(sql).queryType("SQL").rowLimit(rowLimit).build();
          last = new RestQueryRunner(w, workManager, conn).run();
        }
        return last;
      } finally {
        conn.cleanupSession();
      }
    });
    try {
      return f.get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      f.cancel(true);
      throw e;
    } catch (ExecutionException e) {
      throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
    }
  }

  /** Ported from ScheduleManager.createSchedulerConnection. */
  private WebUserConnection connection(String userName) {
    DrillConfig config = workManager.getContext().getConfig();
    BufferAllocator allocator = workManager.getContext().getAllocator().newChildAllocator("WorkflowRunner:session",
        config.getLong(ExecConstants.HTTP_SESSION_MEMORY_RESERVATION),
        config.getLong(ExecConstants.HTTP_SESSION_MEMORY_MAXIMUM));
    String user = userName == null || userName.isEmpty() ? DrillUserPrincipal.ANONYMOUS_USER : userName;
    UserSession session = UserSession.Builder.newBuilder()
        .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(user).build())
        .withOptionManager(workManager.getContext().getOptionManager())
        .setSupportComplexTypes(config.getBoolean(ExecConstants.CLIENT_SUPPORT_COMPLEX_TYPES))
        .build();
    Promise<Void> closeFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
    WebSessionResources wsr = new WebSessionResources(allocator, null, session, closeFuture);
    return DrillUserPrincipal.ANONYMOUS_USER.equals(user)
        ? new WebUserConnection.AnonWebUserConnection(wsr) : new WebUserConnection(wsr);
  }

  public String drillbitId() {
    DrillbitEndpoint ep = workManager.getContext().getEndpoint();
    return ep.getAddress() + ":" + ep.getUserPort();
  }

  // ---------------------------------------------------------------- migration & lifecycle

  private void migrateLegacy() {
    WorkflowConfig cfg = stores.loadConfig();
    if (cfg.legacyMigrated) {
      return;
    }
    int count = 0;
    Iterator<Map.Entry<String, WorkflowMigration.LegacySchedule>> it = stores.legacySchedules().getAll();
    while (it.hasNext()) {
      WorkflowMigration.LegacySchedule old = it.next().getValue();
      if (old == null || stores.workflows().contains(old.id)) {
        continue;
      }
      SavedQueryResources.SavedQuery q = stores.savedQuery(old.savedQueryId);
      Workflow wf = WorkflowMigration.convert(old, stores.projectForQuery(old.savedQueryId), q == null ? null : q.getName());
      stores.workflows().put(wf.id, wf);
      count++;
    }
    cfg.legacyMigrated = true;
    stores.saveConfig(cfg);
    logger.info("Migrated {} legacy schedules to workflows", count);
  }

  @Override
  public void close() {
    ticker.shutdown();
    workers.shutdown();
    try {
      if (!ticker.awaitTermination(10, TimeUnit.SECONDS)) {
        ticker.shutdownNow();
      }
      if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
        workers.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    instance = null;
  }

  private static ThreadFactory daemon(String name) {
    return r -> {
      Thread t = new Thread(r, name);
      t.setDaemon(true);
      return t;
    };
  }
}
```

Verify each imported class's package against `ScheduleManager.java`'s imports (it uses the same set); adjust import lines, not logic, if a package differs. `WorkflowPermissions.isMember` is created in Task 10 — create it now with just this method so the runner compiles:

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import org.apache.drill.exec.server.rest.ProjectResources;

/** Who may see and change workflows. Pure functions; no I/O. */
public final class WorkflowPermissions {

  private WorkflowPermissions() { }

  /** Owner or explicitly shared member. Public visibility alone is not membership. */
  public static boolean isMember(ProjectResources.Project p, String user) {
    return p != null && user != null && (user.equals(p.getOwner()) || p.getSharedWith().contains(user));
  }
}
```

- [ ] **Step 6: Add the run-log appender**

In `distribution/src/main/resources/logback.xml`, after the `AI_EVENTS` appender:

```xml
  <!-- One JSON object per workflow run. Queried via the dfs.workflow_logs workspace. -->
  <appender name="WORKFLOW_RUNS" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>${DRILL_LOG_DIR:-./log}/workflow-runs.log</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy">
      <fileNamePattern>${DRILL_LOG_DIR:-./log}/workflow-runs-%d{yyyy-MM-dd}.%i.log</fileNamePattern>
      <maxFileSize>100MB</maxFileSize>
      <maxHistory>90</maxHistory>
      <totalSizeCap>5GB</totalSizeCap>
    </rollingPolicy>
    <encoder>
      <pattern>%msg%n</pattern>
    </encoder>
  </appender>
```

and after the `ai.events` logger:

```xml
  <logger name="workflow.runs" additivity="false" level="info">
    <appender-ref ref="WORKFLOW_RUNS" />
  </logger>
```

- [ ] **Step 7: Compile, checkstyle, commit**

```bash
mvn compile -pl exec/java-exec -q
mvn checkstyle:check -pl exec/java-exec
mvn test -pl exec/java-exec -Dtest='RunClaimsTest,WorkflowRunLogTest' -DfailIfNoTests=false
git add distribution/src/main/resources/logback.xml exec/java-exec/src/main/java/org/apache/drill/exec/server/rest exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/workflow
git commit -m "Add workflow runner with cluster-safe run claims and run log"
```

(The runner is exercised end-to-end through REST in Task 10.)

---

### Task 10: Permissions, REST resource, and Drillbit wiring

**Files:**
- Modify: `…/workflow/WorkflowPermissions.java`
- Create: `…/workflow/WorkflowResources.java`
- Modify: `server/Drillbit.java` (L23 import, L239–245 start, L330 close), `server/rest/DrillRestServer.java` (L121–122)
- Delete: `server/rest/ScheduleManager.java`, `server/rest/ScheduleResources.java`, `server/rest/WorkflowConfigResources.java`
- Test: `…/workflow/WorkflowPermissionsTest.java`, `…/workflow/TestWorkflowResources.java`

**Interfaces:**
- Produces:
  - `WorkflowPermissions`: `isMember(Project, user)`, `canView(Project p, String user, boolean admin)`, `canCreate(Project p, String user, boolean admin)`, `canEdit(Workflow wf, String user, boolean admin)`, `canManage(Workflow wf, Project p, String user, boolean admin)` (pause/enable/delete/run)
  - REST under `/api/v1/workflows` exactly as the table in step 4.

- [ ] **Step 1: Write the failing permissions test**

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.exec.server.rest.ProjectResources;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkflowPermissionsTest {

  private static ProjectResources.Project project(boolean isPublic) {
    return new ObjectMapper().convertValue(Map.of("id", "p", "name", "P", "owner", "olivia",
        "sharedWith", List.of("sam"), "isPublic", isPublic), ProjectResources.Project.class);
  }

  private static Workflow wf(String owner) {
    Workflow w = new Workflow();
    w.owner = owner;
    w.projectId = "p";
    return w;
  }

  @Test
  public void matrix() {
    ProjectResources.Project p = project(true);
    // view
    assertTrue(WorkflowPermissions.canView(p, "sam", false));
    assertTrue(WorkflowPermissions.canView(p, "stranger", false)); // public project
    assertFalse(WorkflowPermissions.canView(project(false), "stranger", false));
    assertTrue(WorkflowPermissions.canView(null, "admin", true));
    // create: members only, public is not enough
    assertTrue(WorkflowPermissions.canCreate(p, "sam", false));
    assertFalse(WorkflowPermissions.canCreate(p, "stranger", false));
    assertFalse(WorkflowPermissions.canCreate(null, "sam", false));
    // edit: own only (runs as owner), admin any
    assertTrue(WorkflowPermissions.canEdit(wf("sam"), "sam", false));
    assertFalse(WorkflowPermissions.canEdit(wf("sam"), "olivia", false));
    assertTrue(WorkflowPermissions.canEdit(wf("sam"), "root", true));
    // manage: own, project owner, admin
    assertTrue(WorkflowPermissions.canManage(wf("sam"), p, "olivia", false));
    assertTrue(WorkflowPermissions.canManage(wf("sam"), p, "sam", false));
    assertFalse(WorkflowPermissions.canManage(wf("olivia"), p, "sam", false));
  }
}
```

(If `Project`'s `@JsonCreator` uses a different JSON name for `isPublic`, e.g. `public`, adjust the map key to match the `@JsonProperty`.)

- [ ] **Step 2: Run to verify it fails, then implement**

Run: `mvn test -pl exec/java-exec -Dtest=WorkflowPermissionsTest -DfailIfNoTests=false` → compilation failure.

Add to `WorkflowPermissions`:

```java
  public static boolean canView(ProjectResources.Project p, String user, boolean admin) {
    return admin || (p != null && ProjectResources.canRead(p, user));
  }

  public static boolean canCreate(ProjectResources.Project p, String user, boolean admin) {
    return admin || isMember(p, user);
  }

  /** Edits change what runs under the owner's identity, so only the owner (or an admin) may edit. */
  public static boolean canEdit(Workflow wf, String user, boolean admin) {
    return admin || (user != null && user.equals(wf.owner));
  }

  public static boolean canManage(Workflow wf, ProjectResources.Project p, String user, boolean admin) {
    return canEdit(wf, user, admin) || (p != null && user != null && user.equals(p.getOwner()));
  }
```

Run again → PASS.

- [ ] **Step 3: Write the failing REST integration test**

`TestWorkflowResources.java` (JUnit4 `ClusterTest`, auth disabled → anonymous admin; seeds stores directly):

```java
/* Apache license header */
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
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        "savedQueryIds", List.of("q1")), ProjectResources.Project.class));
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

    assertEquals(1, call("GET", "?projectId=p1", null, 200).size());

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
    Map<String, Object> wf = new java.util.HashMap<>(workflow("m"));
    wf.put("savedQueryId", "other");
    assertTrue(call("POST", "", wf, 422).get("errors").toString().contains("savedQueryId"));
  }
}
```

- [ ] **Step 4: Implement `WorkflowResources`**

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.drill.exec.server.rest.ProjectResources;
import org.apache.drill.exec.server.rest.WebUserConnection;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.work.WorkManager;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Path("/api/v1/workflows")
@Tag(name = "Workflows", description = "Scheduled queries with action pipelines")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
@Produces(MediaType.APPLICATION_JSON)
public class WorkflowResources {

  @Inject WorkManager workManager;
  @Inject DrillUserPrincipal principal;
  @Inject WebUserConnection webUserConnection;
  @Inject StoragePluginRegistry storageRegistry;

  // ---------------------------------------------------------------- CRUD

  @GET
  public List<Workflow> list(@QueryParam("projectId") String projectId) {
    List<Workflow> out = new ArrayList<>();
    if (projectId != null && !WorkflowPermissions.canView(stores().project(projectId), user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "Access denied");
    }
    Iterator<Map.Entry<String, Workflow>> it = stores().workflows().getAll();
    while (it.hasNext()) {
      Workflow wf = it.next().getValue();
      boolean match = projectId == null ? canView(wf) : projectId.equals(wf.projectId);
      if (match) {
        out.add(wf);
      }
    }
    return out;
  }

  @GET
  @Path("/{id}")
  public Workflow get(@PathParam("id") String id) {
    return viewable(id);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response create(Workflow wf) {
    ProjectResources.Project p = stores().project(wf.projectId);
    if (!WorkflowPermissions.canCreate(p, user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "You must be a member of the project");
    }
    List<WorkflowValidator.ValidationError> errors = validate(wf, p);
    if (!errors.isEmpty()) {
      return unprocessable(errors);
    }
    Instant now = Instant.now();
    WorkflowConfig cfg = stores().loadConfig();
    wf.id = UUID.randomUUID().toString();
    wf.owner = user();
    wf.createdAt = now.toString();
    wf.status = "paused";
    wf.schedule.enabled = false;
    wf.isRunning = false;
    wf.consecutiveFailures = 0;
    wf.lastRunAt = null;
    wf.lastStatus = null;
    wf.schedule.expiresAt = cfg.expirationEnabled ? now.plus(Duration.ofDays(cfg.expirationDays)).toString() : null;
    wf.nextRunAt = WorkflowSchedule.nextRun(wf.schedule, now, ZoneId.systemDefault()).toString();
    stores().workflows().put(wf.id, wf);
    return Response.status(Response.Status.CREATED).entity(wf).build();
  }

  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response update(@PathParam("id") String id, Workflow wf) {
    Workflow existing = viewable(id);
    if (!WorkflowPermissions.canEdit(existing, user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "Only the workflow owner or an admin can edit it");
    }
    wf.projectId = existing.projectId;
    List<WorkflowValidator.ValidationError> errors = validate(wf, stores().project(existing.projectId));
    if (!errors.isEmpty()) {
      return unprocessable(errors);
    }
    wf.id = existing.id;
    wf.owner = existing.owner;
    wf.createdAt = existing.createdAt;
    wf.renewedAt = existing.renewedAt;
    wf.lastRunAt = existing.lastRunAt;
    wf.lastStatus = existing.lastStatus;
    wf.consecutiveFailures = existing.consecutiveFailures;
    wf.isRunning = existing.isRunning;
    wf.status = existing.status;
    wf.schedule.enabled = existing.schedule.enabled;
    wf.schedule.expiresAt = existing.schedule.expiresAt;
    wf.nextRunAt = WorkflowSchedule.nextRun(wf.schedule, Instant.now(), ZoneId.systemDefault()).toString();
    stores().workflows().put(id, wf);
    return Response.ok(wf).build();
  }

  @DELETE
  @Path("/{id}")
  public Response delete(@PathParam("id") String id) {
    Workflow wf = manageable(id);
    stores().workflows().delete(wf.id);
    List<String> runIds = new ArrayList<>();
    stores().runs().getAll().forEachRemaining(e -> {
      if (e.getValue() != null && id.equals(e.getValue().scheduleId)) {
        runIds.add(e.getKey());
      }
    });
    runIds.forEach(stores().runs()::delete);
    return Response.noContent().build();
  }

  public static class EnabledRequest {
    public boolean enabled;
  }

  @POST
  @Path("/{id}/enabled")
  @Consumes(MediaType.APPLICATION_JSON)
  public Workflow setEnabled(@PathParam("id") String id, EnabledRequest req) {
    Workflow wf = manageable(id);
    if (req.enabled && WorkflowSchedule.isExpired(wf, Instant.now(), stores().loadConfig())) {
      throw error(Response.Status.CONFLICT, "Workflow has expired; renew it first");
    }
    if (req.enabled && wf.projectId == null) {
      throw error(Response.Status.CONFLICT, "Assign the workflow to a project first");
    }
    wf.schedule.enabled = req.enabled;
    wf.status = req.enabled ? "active" : "paused";
    wf.nextRunAt = WorkflowSchedule.nextRun(wf.schedule, Instant.now(), ZoneId.systemDefault()).toString();
    stores().workflows().put(id, wf);
    return wf;
  }

  // ---------------------------------------------------------------- validation & dry run

  public static class ValidationResult {
    public List<WorkflowValidator.ValidationError> errors;
  }

  @POST
  @Path("/validate")
  @Consumes(MediaType.APPLICATION_JSON)
  public ValidationResult validateOnly(Workflow wf) {
    ValidationResult r = new ValidationResult();
    r.errors = validate(wf, stores().project(wf.projectId));
    return r;
  }

  public static class ParseRequest {
    public String expr;
  }

  @POST
  @Path("/parse-condition")
  @Consumes(MediaType.APPLICATION_JSON)
  public Map<String, Object> parseCondition(ParseRequest req) {
    Map<String, Object> out = new LinkedHashMap<>();
    try {
      out.put("tree", ConditionParser.parse(req.expr));
    } catch (ConditionParser.ParseException e) {
      out.put("error", e.getMessage());
      out.put("position", e.position);
    }
    return out;
  }

  @POST
  @Path("/dry-run")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response dryRunUnsaved(Workflow wf) {
    ProjectResources.Project p = stores().project(wf.projectId);
    if (!WorkflowPermissions.canCreate(p, user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "You must be a member of the project");
    }
    List<WorkflowValidator.ValidationError> errors = validate(wf, p);
    if (!errors.isEmpty()) {
      return unprocessable(errors);
    }
    // Dry runs of unsaved drafts execute as the person drafting them.
    wf.owner = user();
    return Response.ok(runner().dryRun(wf)).build();
  }

  @POST
  @Path("/{id}/dry-run")
  public WorkflowRunner.DryRunResult dryRunSaved(@PathParam("id") String id) {
    Workflow wf = viewable(id);
    if (!WorkflowPermissions.canEdit(wf, user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "Only the workflow owner or an admin can dry-run it");
    }
    return runner().dryRun(wf);
  }

  @POST
  @Path("/{id}/run")
  public WorkflowRun runNow(@PathParam("id") String id) {
    Workflow wf = manageable(id);
    if (wf.isRunning) {
      throw error(Response.Status.CONFLICT, "Workflow is already running");
    }
    return runner().runNow(wf);
  }

  @POST
  @Path("/{id}/renew")
  public Workflow renew(@PathParam("id") String id) {
    Workflow wf = manageable(id);
    WorkflowConfig cfg = stores().loadConfig();
    Instant now = Instant.now();
    wf.renewedAt = now.toString();
    wf.schedule.expiresAt = cfg.expirationEnabled ? now.plus(Duration.ofDays(cfg.expirationDays)).toString() : null;
    if ("expired".equals(wf.status)) {
      wf.status = "paused";
    }
    stores().workflows().put(id, wf);
    return wf;
  }

  @POST
  @Path("/{id}/take-ownership")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  public Workflow takeOwnership(@PathParam("id") String id) {
    Workflow wf = viewable(id);
    ProjectResources.Project p = stores().project(wf.projectId);
    if (p != null && !WorkflowPermissions.isMember(p, user())) {
      throw error(Response.Status.CONFLICT, "Add yourself to the project before taking ownership");
    }
    wf.owner = user();
    if ("owner_lost_access".equals(wf.status)) {
      wf.status = "paused";
    }
    stores().workflows().put(id, wf);
    return wf;
  }

  public static class AssignRequest {
    public String projectId;
  }

  @POST
  @Path("/{id}/assign-project")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  @Consumes(MediaType.APPLICATION_JSON)
  public Workflow assignProject(@PathParam("id") String id, AssignRequest req) {
    Workflow wf = viewable(id);
    ProjectResources.Project p = stores().project(req.projectId);
    if (p == null) {
      throw error(Response.Status.NOT_FOUND, "Project not found");
    }
    if (!p.getSavedQueryIds().contains(wf.savedQueryId)) {
      throw error(Response.Status.CONFLICT, "Add the workflow's saved query to that project first");
    }
    wf.projectId = p.getId();
    stores().workflows().put(id, wf);
    return wf;
  }

  // ---------------------------------------------------------------- runs, types, config

  @GET
  @Path("/{id}/runs")
  public List<WorkflowRun> runs(@PathParam("id") String id) {
    viewable(id);
    List<WorkflowRun> out = new ArrayList<>();
    stores().runs().getAll().forEachRemaining(e -> {
      if (e.getValue() != null && id.equals(e.getValue().scheduleId)) {
        out.add(e.getValue());
      }
    });
    out.sort((a, b) -> b.executedAt.compareTo(a.executedAt));
    return out;
  }

  @GET
  @Path("/{id}/runs/{runId}")
  public WorkflowRun run(@PathParam("id") String id, @PathParam("runId") String runId) {
    viewable(id);
    WorkflowRun r = stores().runs().get(runId);
    if (r == null || !id.equals(r.scheduleId)) {
      throw error(Response.Status.NOT_FOUND, "Run not found");
    }
    return r;
  }

  public static class ActionTypeInfo {
    public String id;
    public String displayName;
    public String description;
    public Object configSchema;
    public Map<String, String> outputs;
    public boolean hasSideEffects;
  }

  @GET
  @Path("/action-types")
  public List<ActionTypeInfo> actionTypes() {
    List<ActionTypeInfo> out = new ArrayList<>();
    for (ActionType t : runner().registry().all()) {
      ActionTypeInfo i = new ActionTypeInfo();
      i.id = t.id();
      i.displayName = t.displayName();
      i.description = t.description();
      i.configSchema = t.configSchema();
      i.outputs = t.outputs();
      i.hasSideEffects = t.hasSideEffects();
      out.add(i);
    }
    return out;
  }

  @GET
  @Path("/config")
  public WorkflowConfig getConfig() {
    return stores().loadConfig();
  }

  @PUT
  @Path("/config")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  @Consumes(MediaType.APPLICATION_JSON)
  public WorkflowConfig putConfig(WorkflowConfig cfg) {
    cfg.legacyMigrated = stores().loadConfig().legacyMigrated;
    stores().saveConfig(cfg);
    return cfg;
  }

  // ---------------------------------------------------------------- helpers

  private List<WorkflowValidator.ValidationError> validate(Workflow wf, ProjectResources.Project p) {
    if (wf.schedule == null) {
      wf.schedule = new Workflow.Schedule();
    }
    if (wf.actions == null) {
      wf.actions = new ArrayList<>();
    }
    List<WorkflowValidator.ValidationError> errors = WorkflowValidator.validate(wf, runner().registry());
    if (wf.savedQueryId != null && (p == null || !p.getSavedQueryIds().contains(wf.savedQueryId))) {
      errors.add(new WorkflowValidator.ValidationError("savedQueryId", "Saved query is not in this project"));
    }
    return errors;
  }

  private boolean canView(Workflow wf) {
    return WorkflowPermissions.canView(stores().project(wf.projectId), user(), admin());
  }

  private Workflow viewable(String id) {
    Workflow wf = stores().workflows().get(id);
    if (wf == null) {
      throw error(Response.Status.NOT_FOUND, "Workflow not found");
    }
    if (!canView(wf)) {
      throw error(Response.Status.FORBIDDEN, "Access denied");
    }
    return wf;
  }

  private Workflow manageable(String id) {
    Workflow wf = viewable(id);
    if (!WorkflowPermissions.canManage(wf, stores().project(wf.projectId), user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "Access denied");
    }
    return wf;
  }

  WorkflowRunner runner() {
    WorkflowRunner r = WorkflowRunner.getInstance();
    if (r == null) {
      throw error(Response.Status.SERVICE_UNAVAILABLE, "Workflow runner is not running");
    }
    return r;
  }

  WorkflowStores stores() {
    return runner().stores();
  }

  String user() {
    return principal.getName();
  }

  boolean admin() {
    return principal.isAdminUser();
  }

  static WebApplicationException error(Response.Status status, String message) {
    return new WebApplicationException(Response.status(status).entity(Map.of("error", message))
        .type(MediaType.APPLICATION_JSON).build());
  }

  private static Response unprocessable(List<WorkflowValidator.ValidationError> errors) {
    return Response.status(422).entity(Map.of("errors", errors)).build();
  }
}
```

Note: `WorkflowRunner.DryRunResult.context` serializes `RunContext`'s public fields (including `actionOutputs`); `resolve()` is not a bean getter so it is not serialized.

- [ ] **Step 5: Wire it up and delete the old classes**

`Drillbit.java`: replace the `ScheduleManager` import with `import org.apache.drill.exec.server.rest.workflow.WorkflowRunner;`, the start block (L239–245) with:

```java
    try {
      WorkflowRunner.getOrCreate(manager, storeProvider).start();
      logger.info("WorkflowRunner started");
    } catch (Exception e) {
      logger.warn("Failed to start WorkflowRunner (workflows will not run): {}", e.getMessage());
    }
```

and in `close()` replace `ScheduleManager.getInstance()` with `WorkflowRunner.getInstance()`.

`DrillRestServer.java` L121–122: replace `register(WorkflowConfigResources.class); register(ScheduleResources.class);` with `register(org.apache.drill.exec.server.rest.workflow.WorkflowResources.class);` (or add an import and use the simple name, matching neighboring style).

```bash
git rm exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ScheduleManager.java \
       exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ScheduleResources.java \
       exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/WorkflowConfigResources.java
grep -rn "ScheduleManager\|ScheduleResources\|WorkflowConfigResources" exec/java-exec/src
```

Expected: the grep finds nothing (fix any remaining reference).

- [ ] **Step 6: Run tests**

Run: `mvn test -pl exec/java-exec -Dtest='WorkflowPermissionsTest,TestWorkflowResources' -DfailIfNoTests=false`
Expected: PASS. If `/{id}/run` returns `rowCount` 7 but `status` is error, read `errorMessage` — the most likely cause is SQL wrapping; the test SQL deliberately ends in `;` (Review Focus 1).

- [ ] **Step 7: Checkstyle and commit**

```bash
mvn checkstyle:check -pl exec/java-exec
git add -A exec/java-exec/src
git commit -m "Replace schedules API with workflows REST resource"
```

---

### Task 11: Observability endpoints

**Files:**
- Create: `…/workflow/WorkflowObservability.java`
- Modify: `…/workflow/WorkflowResources.java`
- Test: `…/workflow/TestWorkflowObservability.java`

**Interfaces:**
- Produces:
  - `WorkflowObservability` constants `WORKSPACE = "workflow_logs"`, `FORMAT = "workflow_log"`, `TABLE = "dfs.workflow_logs.`workflow-runs*.log`"`; `static String summarySql(String projectId, String sinceIso)`, `static String dailySql(String workflowId, String sinceIso)`, `static String actionSql(String workflowId, String sinceIso)`, `static String escape(String)`; `static boolean logPresent(String logDir)`.
  - REST: `GET /observability/status` → `{logDir, logPresent, workspace, format, ready}`; `POST /observability/setup` (admin); `GET /observability/summary?projectId=` → `{setupRequired, rows:[{workflowId, runs, successes, avgDurationMs, lastTs}]}`; `GET /{id}/health` → `{setupRequired, daily:[{day, runs, successes, avgDurationMs}], actions:[{id, type, outcome, cnt}]}`; `GET /observability/running` (admin) → `List<Workflow>` with `isRunning`.

- [ ] **Step 1: Write the failing test** (SQL runs against a temp workspace holding a sample log)

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import org.apache.drill.exec.store.easy.json.JSONFormatConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestWorkflowObservability extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    File dir = dirTestWatcher.makeSubDir(java.nio.file.Paths.get("wflogs"));
    Files.write(new File(dir, "workflow-runs.log").toPath(), List.of(
        "{\"ts\":\"2026-10-20T08:00:00Z\",\"workflowId\":\"w1\",\"projectId\":\"p1\",\"status\":\"success\",\"durationMs\":100,"
            + "\"actions\":[{\"id\":\"a\",\"type\":\"record_alert\",\"outcome\":\"fired\"}]}",
        "{\"ts\":\"2026-10-21T08:00:00Z\",\"workflowId\":\"w1\",\"projectId\":\"p1\",\"status\":\"error\",\"durationMs\":300,"
            + "\"actions\":[{\"id\":\"a\",\"type\":\"record_alert\",\"outcome\":\"skipped\"}]}",
        "{\"ts\":\"2026-10-21T09:00:00Z\",\"workflowId\":\"w2\",\"projectId\":\"p2\",\"status\":\"success\",\"durationMs\":50,"
            + "\"actions\":[]}"));
    cluster.defineFormat("dfs", WorkflowObservability.FORMAT,
        new JSONFormatConfig(List.of("log"), null, null, null, null, null));
    cluster.defineWorkspace("dfs", WorkflowObservability.WORKSPACE, dir.getAbsolutePath(), WorkflowObservability.FORMAT);
  }

  @Test
  public void summaryFiltersByProject() throws Exception {
    DirectRowSet rows = client.queryBuilder().sql(WorkflowObservability.summarySql("p1", "2026-10-01T00:00:00Z")).rowSet();
    try {
      assertEquals(1, rows.rowCount());
      RowSetReader r = rows.reader();
      r.next();
      assertEquals("w1", r.scalar("workflowId").getString());
      assertEquals(2L, r.scalar("runs").getLong());
      assertEquals(1L, r.scalar("successes").getLong());
    } finally {
      rows.clear();
    }
  }

  @Test
  public void dailyAndActionBreakdown() throws Exception {
    DirectRowSet daily = client.queryBuilder().sql(WorkflowObservability.dailySql("w1", "2026-10-01T00:00:00Z")).rowSet();
    try {
      assertEquals(2, daily.rowCount());
    } finally {
      daily.clear();
    }
    DirectRowSet actions = client.queryBuilder().sql(WorkflowObservability.actionSql("w1", "2026-10-01T00:00:00Z")).rowSet();
    try {
      assertEquals(2, actions.rowCount());
    } finally {
      actions.clear();
    }
  }

  @Test
  public void escapeQuotes() {
    assertEquals("a''b", WorkflowObservability.escape("a'b"));
  }
}
```


- [ ] **Step 2: Run to verify it fails**

Run: `mvn test -pl exec/java-exec -Dtest=TestWorkflowObservability -DfailIfNoTests=false`
Expected: compilation failure.

- [ ] **Step 3: Implement `WorkflowObservability`**

```java
/* Apache license header */
package org.apache.drill.exec.server.rest.workflow;

import java.io.File;

/** SQL over the workflow run log (dfs.workflow_logs), mirroring the AI Analytics setup. */
public final class WorkflowObservability {
  public static final String WORKSPACE = "workflow_logs";
  public static final String FORMAT = "workflow_log";
  static final String TABLE = "dfs." + WORKSPACE + ".`workflow-runs*.log`";

  private WorkflowObservability() { }

  public static String summarySql(String projectId, String sinceIso) {
    return "SELECT workflowId, COUNT(*) AS runs, "
        + "SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes, "
        + "AVG(durationMs) AS avgDurationMs, MAX(ts) AS lastTs "
        + "FROM " + TABLE + " WHERE ts >= '" + escape(sinceIso) + "'"
        + (projectId == null ? "" : " AND projectId = '" + escape(projectId) + "'")
        + " GROUP BY workflowId";
  }

  public static String dailySql(String workflowId, String sinceIso) {
    return "SELECT SUBSTR(ts, 1, 10) AS `day`, COUNT(*) AS runs, "
        + "SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes, "
        + "AVG(durationMs) AS avgDurationMs "
        + "FROM " + TABLE + " WHERE workflowId = '" + escape(workflowId) + "' AND ts >= '" + escape(sinceIso) + "' "
        + "GROUP BY SUBSTR(ts, 1, 10) ORDER BY `day`";
  }

  public static String actionSql(String workflowId, String sinceIso) {
    return "SELECT t.a.id AS id, t.a.type AS type, t.a.outcome AS outcome, COUNT(*) AS cnt FROM "
        + "(SELECT FLATTEN(actions) AS a FROM " + TABLE + " WHERE workflowId = '" + escape(workflowId)
        + "' AND ts >= '" + escape(sinceIso) + "') t "
        + "GROUP BY t.a.id, t.a.type, t.a.outcome";
  }

  public static String escape(String s) {
    return s == null ? "" : s.replace("'", "''");
  }

  static boolean logPresent(String logDir) {
    File[] files = logDir == null ? null : new File(logDir).listFiles(
        (d, n) -> n.startsWith("workflow-runs") && n.endsWith(".log"));
    return files != null && files.length > 0;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl exec/java-exec -Dtest=TestWorkflowObservability -DfailIfNoTests=false`
Expected: 3 tests PASS.

- [ ] **Step 5: Add the REST endpoints**

Add to `WorkflowResources` (imports: `org.apache.drill.common.logical.FormatPluginConfig`, `org.apache.drill.common.logical.StoragePluginConfig`, `org.apache.drill.exec.store.dfs.FileSystemConfig`, `org.apache.drill.exec.store.dfs.WorkspaceConfig`, `org.apache.drill.exec.server.rest.QueryWrapper`, `org.apache.drill.exec.server.rest.RestQueryRunner`; verify packages against `AiAnalyticsResources.java` imports):

```java
  @GET
  @Path("/observability/status")
  public Map<String, Object> observabilityStatus() throws Exception {
    String logDir = System.getenv("DRILL_LOG_DIR");
    FileSystemConfig fs = (FileSystemConfig) storageRegistry.getStoredConfig("dfs");
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("logDir", logDir);
    out.put("logPresent", WorkflowObservability.logPresent(logDir));
    out.put("workspace", fs != null && fs.getWorkspaces().containsKey(WorkflowObservability.WORKSPACE));
    out.put("format", fs != null && fs.getFormats().containsKey(WorkflowObservability.FORMAT));
    out.put("ready", Boolean.TRUE.equals(out.get("workspace")) && Boolean.TRUE.equals(out.get("format")));
    return out;
  }

  /** Same steps as AiAnalyticsResources.setup, for the workflow run log. */
  @POST
  @Path("/observability/setup")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  public Map<String, Object> observabilitySetup() throws Exception {
    String logDir = System.getenv("DRILL_LOG_DIR");
    if (logDir == null) {
      throw error(Response.Status.BAD_REQUEST, "DRILL_LOG_DIR is not set");
    }
    StoragePluginConfig cfg = storageRegistry.getStoredConfig("dfs");
    if (!(cfg instanceof FileSystemConfig)) {
      throw error(Response.Status.CONFLICT, "dfs storage plugin is not a file system plugin");
    }
    FileSystemConfig fs = (FileSystemConfig) cfg;
    if (!fs.getWorkspaces().containsKey(WorkflowObservability.WORKSPACE)) {
      FileSystemConfig copy = fs.copy();
      copy.getWorkspaces().put(WorkflowObservability.WORKSPACE,
          new WorkspaceConfig(logDir, false, WorkflowObservability.FORMAT, false));
      storageRegistry.put("dfs", copy);
    }
    FileSystemConfig fs2 = (FileSystemConfig) storageRegistry.getStoredConfig("dfs");
    if (!fs2.getFormats().containsKey(WorkflowObservability.FORMAT)) {
      FormatPluginConfig fmt = storageRegistry.mapper().readValue(
          "{\"type\":\"json\",\"extensions\":[\"log\"]}", FormatPluginConfig.class);
      storageRegistry.putFormatPlugin("dfs", WorkflowObservability.FORMAT, fmt);
    }
    return observabilityStatus();
  }

  @GET
  @Path("/observability/summary")
  public Map<String, Object> observabilitySummary(@QueryParam("projectId") String projectId) throws Exception {
    if (projectId == null && !admin()) {
      throw error(Response.Status.BAD_REQUEST, "projectId is required");
    }
    if (projectId != null && !WorkflowPermissions.canView(stores().project(projectId), user(), admin())) {
      throw error(Response.Status.FORBIDDEN, "Access denied");
    }
    String since = Instant.now().minus(Duration.ofDays(30)).toString();
    return sqlResult(Map.of("rows", WorkflowObservability.summarySql(projectId, since)));
  }

  @GET
  @Path("/{id}/health")
  public Map<String, Object> health(@PathParam("id") String id) throws Exception {
    viewable(id);
    String since = Instant.now().minus(Duration.ofDays(30)).toString();
    Map<String, String> queries = new LinkedHashMap<>();
    queries.put("daily", WorkflowObservability.dailySql(id, since));
    queries.put("actions", WorkflowObservability.actionSql(id, since));
    return sqlResult(queries);
  }

  @GET
  @Path("/observability/running")
  @RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
  public List<Workflow> running() {
    List<Workflow> out = new ArrayList<>();
    stores().workflows().getAll().forEachRemaining(e -> {
      if (e.getValue() != null && e.getValue().isRunning) {
        out.add(e.getValue());
      }
    });
    return out;
  }

  /** Runs each named query; if the run-log workspace is missing, reports setupRequired instead. */
  private Map<String, Object> sqlResult(Map<String, String> queries) throws Exception {
    Map<String, Object> out = new LinkedHashMap<>();
    boolean ready = Boolean.TRUE.equals(observabilityStatus().get("ready"))
        && Boolean.TRUE.equals(observabilityStatus().get("logPresent"));
    out.put("setupRequired", !ready);
    for (Map.Entry<String, String> q : queries.entrySet()) {
      if (!ready) {
        out.put(q.getKey(), List.of());
        continue;
      }
      WebUserConnection conn = new WebUserConnection(webUserConnection.resources());
      QueryWrapper w = new QueryWrapper.RestQueryBuilder().query(q.getValue()).queryType("SQL").rowLimit("100000").build();
      out.put(q.getKey(), new RestQueryRunner(w, workManager, conn).run().rows);
    }
    return out;
  }
```

Numeric values come back as strings in `QueryResult.rows`; the frontend converts with `Number(...)`.

- [ ] **Step 6: Run all workflow tests, checkstyle, commit**

```bash
mvn test -pl exec/java-exec -Dtest='*Workflow*,ConditionParserTest,ConditionEvaluatorTest,TemplatesTest,ActionPipelineTest,RunClaimsTest,RunContextTest,BuiltInActionsTest,SmtpMailerTest' -DfailIfNoTests=false
mvn checkstyle:check -pl exec/java-exec
git add exec/java-exec/src
git commit -m "Add workflow observability endpoints over the run log"
```

---

### Task 12: Frontend types, API client, and condition utilities

**Files:**
- Create: `webapp/src/types/workflow.ts`, `webapp/src/api/workflows.ts`, `webapp/src/api/workflows.test.ts`, `webapp/src/utils/conditionTree.ts`, `webapp/src/utils/conditionTree.test.ts`

**Interfaces:**
- Produces (TS): types `Workflow, WorkflowSchedule, WorkflowAction, Condition, ConditionNode, ConditionLeaf, ValidationError, ActionOutcome, WorkflowRun, DryRunResult, ActionTypeInfo, JsonSchemaProp, WorkflowConfig, ObservabilitySummary, WorkflowHealth`; API functions listed in step 3; utils `BASE_FIELDS, OPS, isLeaf, treeToExpr, toBuilderModel, fromBuilderModel, coerceValue, availableRefs, summarizeCondition, newWorkflow`.

- [ ] **Step 1: Write the failing tests**

`api/workflows.test.ts`:

```ts
/* Apache license header */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const mockGet = vi.hoisted(() => vi.fn());
const mockPost = vi.hoisted(() => vi.fn());
const mockPut = vi.hoisted(() => vi.fn());
const mockDelete = vi.hoisted(() => vi.fn());
vi.mock('./client', () => ({ default: { get: mockGet, post: mockPost, put: mockPut, delete: mockDelete } }));

import { listWorkflows, dryRunWorkflow, validationErrors, setWorkflowEnabled } from './workflows';

describe('workflows api', () => {
  beforeEach(() => vi.clearAllMocks());

  it('lists by project', async () => {
    mockGet.mockResolvedValue({ data: [{ id: 'w1' }] });
    expect(await listWorkflows('p1')).toEqual([{ id: 'w1' }]);
    expect(mockGet).toHaveBeenCalledWith('/api/v1/workflows', { params: { projectId: 'p1' } });
  });

  it('dry-runs saved vs unsaved', async () => {
    mockPost.mockResolvedValue({ data: { actions: [] } });
    await dryRunWorkflow({ id: 'w1' } as never);
    expect(mockPost).toHaveBeenLastCalledWith('/api/v1/workflows/w1/dry-run');
    await dryRunWorkflow({ name: 'x' } as never);
    expect(mockPost).toHaveBeenLastCalledWith('/api/v1/workflows/dry-run', { name: 'x' });
  });

  it('toggles enabled', async () => {
    mockPost.mockResolvedValue({ data: {} });
    await setWorkflowEnabled('w1', true);
    expect(mockPost).toHaveBeenCalledWith('/api/v1/workflows/w1/enabled', { enabled: true });
  });

  it('extracts 422 errors', () => {
    const err = { isAxiosError: true, response: { status: 422, data: { errors: [{ path: 'name', message: 'x' }] } } };
    expect(validationErrors(err)).toEqual([{ path: 'name', message: 'x' }]);
    expect(validationErrors(new Error('no'))).toBeNull();
  });
});
```

`utils/conditionTree.test.ts`:

```ts
/* Apache license header */
import { describe, it, expect } from 'vitest';
import {
  treeToExpr, toBuilderModel, fromBuilderModel, coerceValue, availableRefs, summarizeCondition,
} from './conditionTree';
import type { ActionTypeInfo, WorkflowAction } from '../types/workflow';

describe('conditionTree', () => {
  it('renders trees to expressions with quoting and grouping', () => {
    expect(treeToExpr({ field: 'status', op: '==', value: "it's" })).toBe("status == 'it\\'s'");
    expect(treeToExpr({
      any: [{ field: 'a', op: '>', value: 1 }, { all: [{ field: 'b', op: '<', value: 2 }, { field: 'c', op: '==', value: null }] }],
    })).toBe('a > 1 || (b < 2 && c == null)');
    expect(treeToExpr({ not: { field: 'x', op: '==', value: true } })).toBe('!(x == true)');
  });

  it('round-trips one-level trees through the builder model', () => {
    const tree = { all: [{ field: 'rowCount', op: '>', value: 5 }, { field: 'status', op: '==', value: 'error' }] };
    const model = toBuilderModel(tree)!;
    expect(model.mode).toBe('all');
    expect(fromBuilderModel(model)).toEqual(tree);
    expect(toBuilderModel({ field: 'a', op: '>', value: 1 })).toEqual({ mode: 'all', leaves: [{ field: 'a', op: '>', value: 1 }] });
    expect(toBuilderModel({ any: [{ all: [] }] })).toBeNull();
    expect(toBuilderModel({ not: { field: 'a', op: '==', value: 1 } })).toBeNull();
  });

  it('coerces builder values', () => {
    expect(coerceValue('42')).toBe(42);
    expect(coerceValue('-1.5')).toBe(-1.5);
    expect(coerceValue('true')).toBe(true);
    expect(coerceValue('null')).toBeNull();
    expect(coerceValue('root')).toBe('root');
  });

  it('offers earlier action outputs only', () => {
    const types = [{ id: 'record_alert', outputs: { message: 'string' } }] as unknown as ActionTypeInfo[];
    const actions = [
      { id: 'a', type: 'record_alert' }, { id: 'b', type: 'record_alert' },
    ] as WorkflowAction[];
    const refs = availableRefs(actions, 1, types);
    expect(refs).toContain('rowCount');
    expect(refs).toContain('actions.a.message');
    expect(refs).not.toContain('actions.b.message');
  });

  it('summarizes conditions', () => {
    expect(summarizeCondition({ always: true })).toBe('Always');
    expect(summarizeCondition({ expr: 'rowCount > 1' })).toBe('rowCount > 1');
    expect(summarizeCondition({ tree: { field: 'a', op: '==', value: 1 } })).toBe('a == 1');
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npx vitest run src/api/workflows.test.ts src/utils/conditionTree.test.ts`
Expected: FAIL, modules not found.

- [ ] **Step 3: Implement**

`types/workflow.ts`:

```ts
/* Apache license header */
export type Frequency = 'hourly' | 'daily' | 'weekly' | 'monthly';

export interface ConditionLeaf { field: string; op: string; value: unknown }
export type ConditionNode =
  | { all: ConditionNode[] }
  | { any: ConditionNode[] }
  | { not: ConditionNode }
  | ConditionLeaf;

export interface Condition { always?: boolean; tree?: ConditionNode; expr?: string }

export interface WorkflowAction {
  id: string;
  type: string;
  condition: Condition;
  config: Record<string, unknown>;
}

export interface WorkflowSchedule {
  frequency: Frequency;
  timeOfDay: string;
  dayOfWeek?: number | null;
  dayOfMonth?: number | null;
  timeoutSeconds: number;
  enabled: boolean;
  expiresAt?: string | null;
}

export interface Workflow {
  id?: string;
  projectId: string | null;
  name: string;
  description?: string;
  owner?: string;
  savedQueryId: string;
  schedule: WorkflowSchedule;
  actions: WorkflowAction[];
  retentionCount: number;
  status?: 'active' | 'paused' | 'expired' | 'owner_lost_access';
  createdAt?: string;
  lastRunAt?: string;
  nextRunAt?: string;
  lastStatus?: 'success' | 'error';
  consecutiveFailures?: number;
  isRunning?: boolean;
}

export interface ValidationError { path: string; message: string }

export interface ActionOutcome {
  id: string;
  type: string;
  outcome: 'fired' | 'skipped' | 'errored';
  reason?: string;
  outputs?: Record<string, unknown>;
  error?: string;
  durationMs: number;
}

export interface WorkflowRun {
  id: string;
  scheduleId: string;
  executedAt: string;
  status: 'success' | 'error' | 'skipped';
  trigger?: 'schedule' | 'manual';
  rowCount?: number;
  duration?: number;
  errorMessage?: string;
  previewRows?: string;
  previewColumns?: string;
  rowCountDelta?: number;
  actions?: ActionOutcome[];
}

export interface DryRunResult {
  context: {
    status: string; rowCount: number; rowCountDelta?: number; durationMs: number; error?: string;
    columns: string[]; rows: Record<string, string>[];
  };
  actions: ActionOutcome[];
}

export interface JsonSchemaProp {
  type: 'string' | 'number' | 'integer' | 'boolean' | 'array';
  title?: string;
  description?: string;
  enum?: string[];
  default?: unknown;
  format?: string;
  items?: { type: 'string'; format?: string };
  'x-template'?: boolean;
  'x-multiline'?: boolean;
}

export interface ActionTypeInfo {
  id: string;
  displayName: string;
  description: string;
  configSchema: { type: 'object'; required?: string[]; properties?: Record<string, JsonSchemaProp> };
  outputs: Record<string, string>;
  hasSideEffects: boolean;
}

export interface WorkflowConfig {
  expirationEnabled: boolean;
  expirationDays: number;
  warningDaysBeforeExpiry: number;
  actionTimeoutSeconds: number;
}

export interface ObservabilitySummary {
  setupRequired: boolean;
  rows: { workflowId: string; runs: string; successes: string; avgDurationMs: string; lastTs: string }[];
}

export interface WorkflowHealth {
  setupRequired: boolean;
  daily: { day: string; runs: string; successes: string; avgDurationMs: string }[];
  actions: { id: string; type: string; outcome: string; cnt: string }[];
}

export interface ObservabilityStatus {
  logDir: string | null; logPresent: boolean; workspace: boolean; format: boolean; ready: boolean;
}
```

`api/workflows.ts`:

```ts
/* Apache license header */
import axios from 'axios';
import apiClient from './client';
import type {
  ActionTypeInfo, ConditionNode, DryRunResult, ObservabilityStatus, ObservabilitySummary, ValidationError,
  Workflow, WorkflowConfig, WorkflowHealth, WorkflowRun,
} from '../types/workflow';

const BASE = '/api/v1/workflows';

export async function listWorkflows(projectId?: string): Promise<Workflow[]> {
  return (await apiClient.get(BASE, { params: projectId ? { projectId } : {} })).data;
}
export async function getWorkflow(id: string): Promise<Workflow> {
  return (await apiClient.get(`${BASE}/${id}`)).data;
}
export async function createWorkflow(wf: Workflow): Promise<Workflow> {
  return (await apiClient.post(BASE, wf)).data;
}
export async function updateWorkflow(id: string, wf: Workflow): Promise<Workflow> {
  return (await apiClient.put(`${BASE}/${id}`, wf)).data;
}
export async function deleteWorkflow(id: string): Promise<void> {
  await apiClient.delete(`${BASE}/${id}`);
}
export async function setWorkflowEnabled(id: string, enabled: boolean): Promise<Workflow> {
  return (await apiClient.post(`${BASE}/${id}/enabled`, { enabled })).data;
}
export async function validateWorkflow(wf: Workflow): Promise<ValidationError[]> {
  return (await apiClient.post(`${BASE}/validate`, wf)).data.errors;
}
export async function parseCondition(expr: string): Promise<{ tree?: ConditionNode; error?: string; position?: number }> {
  return (await apiClient.post(`${BASE}/parse-condition`, { expr })).data;
}
export async function dryRunWorkflow(wf: Workflow): Promise<DryRunResult> {
  return (wf.id ? await apiClient.post(`${BASE}/${wf.id}/dry-run`) : await apiClient.post(`${BASE}/dry-run`, wf)).data;
}
export async function runWorkflowNow(id: string): Promise<WorkflowRun> {
  return (await apiClient.post(`${BASE}/${id}/run`)).data;
}
export async function renewWorkflow(id: string): Promise<Workflow> {
  return (await apiClient.post(`${BASE}/${id}/renew`)).data;
}
export async function takeOwnership(id: string): Promise<Workflow> {
  return (await apiClient.post(`${BASE}/${id}/take-ownership`)).data;
}
export async function assignProject(id: string, projectId: string): Promise<Workflow> {
  return (await apiClient.post(`${BASE}/${id}/assign-project`, { projectId })).data;
}
export async function listRuns(id: string): Promise<WorkflowRun[]> {
  return (await apiClient.get(`${BASE}/${id}/runs`)).data;
}
export async function getActionTypes(): Promise<ActionTypeInfo[]> {
  return (await apiClient.get(`${BASE}/action-types`)).data;
}
export async function getObservabilitySummary(projectId?: string): Promise<ObservabilitySummary> {
  return (await apiClient.get(`${BASE}/observability/summary`, { params: projectId ? { projectId } : {} })).data;
}
export async function getWorkflowHealth(id: string): Promise<WorkflowHealth> {
  return (await apiClient.get(`${BASE}/${id}/health`)).data;
}
export async function getRunningWorkflows(): Promise<Workflow[]> {
  return (await apiClient.get(`${BASE}/observability/running`)).data;
}
export async function getObservabilityStatus(): Promise<ObservabilityStatus> {
  return (await apiClient.get(`${BASE}/observability/status`)).data;
}
export async function setupObservability(): Promise<ObservabilityStatus> {
  return (await apiClient.post(`${BASE}/observability/setup`)).data;
}
export async function getWorkflowConfig(): Promise<WorkflowConfig> {
  return (await apiClient.get(`${BASE}/config`)).data;
}
export async function updateWorkflowConfig(cfg: WorkflowConfig): Promise<WorkflowConfig> {
  return (await apiClient.put(`${BASE}/config`, cfg)).data;
}

/** The server's 422 field errors, or null for any other failure. */
export function validationErrors(err: unknown): ValidationError[] | null {
  if (axios.isAxiosError(err) || (err as { isAxiosError?: boolean })?.isAxiosError) {
    const resp = (err as { response?: { status: number; data?: { errors?: ValidationError[] } } }).response;
    if (resp?.status === 422 && resp.data?.errors) {
      return resp.data.errors;
    }
  }
  return null;
}
```

`utils/conditionTree.ts`:

```ts
/* Apache license header */
import type {
  ActionTypeInfo, Condition, ConditionLeaf, ConditionNode, Workflow, WorkflowAction,
} from '../types/workflow';

export const BASE_FIELDS = ['status', 'rowCount', 'rowCountDelta', 'deltaPercent', 'durationMs', 'intervalMs', 'error'];
export const OPS = ['==', '!=', '>', '>=', '<', '<=', 'contains', 'startsWith'];

export function isLeaf(n: ConditionNode): n is ConditionLeaf {
  return 'field' in n;
}

function literal(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  if (typeof v === 'string') return `'${v.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`;
  return String(v);
}

export function treeToExpr(n: ConditionNode, nested = false): string {
  if (isLeaf(n)) return `${n.field} ${n.op} ${literal(n.value)}`;
  if ('not' in n) return `!(${treeToExpr(n.not)})`;
  const key = 'all' in n ? 'all' : 'any';
  const parts = (key === 'all' ? (n as { all: ConditionNode[] }).all : (n as { any: ConditionNode[] }).any)
    .map((c) => treeToExpr(c, true));
  const s = parts.join(key === 'all' ? ' && ' : ' || ');
  return nested && parts.length > 1 ? `(${s})` : s;
}

export interface BuilderModel { mode: 'all' | 'any'; leaves: ConditionLeaf[] }

/** One level of AND/OR over leaves; anything deeper returns null (edit as an expression). */
export function toBuilderModel(tree: ConditionNode | undefined): BuilderModel | null {
  if (!tree) return { mode: 'all', leaves: [] };
  if (isLeaf(tree)) return { mode: 'all', leaves: [tree] };
  if ('not' in tree) return null;
  const mode = 'all' in tree ? 'all' : 'any';
  const children = mode === 'all' ? (tree as { all: ConditionNode[] }).all : (tree as { any: ConditionNode[] }).any;
  return children.every(isLeaf) ? { mode, leaves: children as ConditionLeaf[] } : null;
}

export function fromBuilderModel(m: BuilderModel): ConditionNode {
  return m.mode === 'all' ? { all: m.leaves } : { any: m.leaves };
}

export function coerceValue(s: string): unknown {
  const t = s.trim();
  if (t === 'true') return true;
  if (t === 'false') return false;
  if (t === 'null') return null;
  if (t !== '' && !Number.isNaN(Number(t))) return Number(t);
  return s;
}

export function availableRefs(actions: WorkflowAction[], index: number, types: ActionTypeInfo[]): string[] {
  const refs = [...BASE_FIELDS];
  actions.slice(0, index).forEach((a) => {
    const t = types.find((x) => x.id === a.type);
    Object.keys(t?.outputs ?? {}).forEach((o) => refs.push(`actions.${a.id}.${o}`));
  });
  return refs;
}

export function summarizeCondition(c: Condition | undefined): string {
  if (!c || c.always) return 'Always';
  if (c.expr) return c.expr;
  return c.tree ? treeToExpr(c.tree) : 'Always';
}

export function newWorkflow(projectId: string, savedQueryId = ''): Workflow {
  return {
    projectId,
    name: '',
    savedQueryId,
    schedule: { frequency: 'daily', timeOfDay: '08:00', timeoutSeconds: 300, enabled: false },
    actions: [],
    retentionCount: 30,
  };
}
```

- [ ] **Step 4: Run tests and type-check**

Run: `npx vitest run src/api/workflows.test.ts src/utils/conditionTree.test.ts && npx tsc --noEmit`
Expected: PASS; no type errors.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src/types/workflow.ts exec/java-exec/src/main/resources/webapp/src/api/workflows.ts exec/java-exec/src/main/resources/webapp/src/api/workflows.test.ts exec/java-exec/src/main/resources/webapp/src/utils/conditionTree.ts exec/java-exec/src/main/resources/webapp/src/utils/conditionTree.test.ts
git commit -m "Add workflow API client and condition tree utilities"
```

---

### Task 13: Editor building blocks

**Files:**
- Create: `webapp/src/components/workflow/TemplateInput.tsx`, `SchemaForm.tsx`, `ConditionEditor.tsx`, `ActionCard.tsx`, `OutcomeList.tsx`
- Test: `webapp/src/components/workflow/ConditionEditor.test.tsx`

**Interfaces:**
- Consumes: Task 12 types, `parseCondition`, utils.
- Produces:
  - `TemplateInput({ value, onChange, refs, multiline? })`
  - `SchemaForm({ schema, value, onChange, refs, errors })` where `errors: Record<string,string>` keyed by config property
  - `ConditionEditor({ value, onChange, refs, error? })`
  - `ActionCard({ action, index, count, type, refs, errors, onChange, onMove, onDelete })` where `errors: ValidationError[]` already filtered to this action
  - `OutcomeList({ outcomes })`

- [ ] **Step 1: Write the failing test**

```tsx
/* Apache license header */
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';

const parse = vi.hoisted(() => vi.fn());
vi.mock('../../api/workflows', () => ({ parseCondition: parse }));

import ConditionEditor from './ConditionEditor';

describe('ConditionEditor', () => {
  it('shows a positioned parse error in expression mode', async () => {
    parse.mockResolvedValue({ error: 'Expected a value at position 10', position: 10 });
    const onChange = vi.fn();
    render(<ConditionEditor value={{ expr: 'rowCount >' }} onChange={onChange} refs={['rowCount']} />);
    await waitFor(() => expect(screen.getByText(/position 10/)).toBeTruthy());
  });

  it('builder edits emit a tree', () => {
    const onChange = vi.fn();
    render(<ConditionEditor value={{ tree: { all: [{ field: 'rowCount', op: '>', value: 5 }] } }}
      onChange={onChange} refs={['rowCount']} />);
    fireEvent.change(screen.getByDisplayValue('5'), { target: { value: '9' } });
    expect(onChange).toHaveBeenCalledWith({ tree: { all: [{ field: 'rowCount', op: '>', value: 9 }] } });
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npx vitest run src/components/workflow/ConditionEditor.test.tsx`
Expected: FAIL, module not found.

- [ ] **Step 3: Implement the components**

`TemplateInput.tsx`:

```tsx
/* Apache license header */
import { useRef } from 'react';
import { Button, Dropdown, Input, Space } from 'antd';
import type { InputRef } from 'antd';
import type { TextAreaRef } from 'antd/es/input/TextArea';

interface Props {
  value?: string;
  onChange: (v: string) => void;
  refs: string[];
  multiline?: boolean;
  placeholder?: string;
}

/** Text field with an "Insert field" menu that drops {{path}} at the cursor. */
export default function TemplateInput({ value = '', onChange, refs, multiline, placeholder }: Props) {
  const inputRef = useRef<InputRef>(null);
  const areaRef = useRef<TextAreaRef>(null);

  const insert = (path: string) => {
    const el = multiline ? areaRef.current?.resizableTextArea?.textArea : inputRef.current?.input;
    const pos = el?.selectionStart ?? value.length;
    onChange(`${value.slice(0, pos)}{{${path}}}${value.slice(pos)}`);
  };

  const menu = { items: refs.map((r) => ({ key: r, label: r })), onClick: ({ key }: { key: string }) => insert(key) };

  return (
    <Space direction="vertical" style={{ width: '100%' }} size={4}>
      {multiline ? (
        <Input.TextArea ref={areaRef} rows={4} value={value} placeholder={placeholder}
          onChange={(e) => onChange(e.target.value)} />
      ) : (
        <Input ref={inputRef} value={value} placeholder={placeholder} onChange={(e) => onChange(e.target.value)} />
      )}
      <Dropdown menu={menu} trigger={['click']}>
        <Button size="small" type="link" style={{ padding: 0 }}>Insert field</Button>
      </Dropdown>
    </Space>
  );
}
```

`SchemaForm.tsx`:

```tsx
/* Apache license header */
import { Form, Input, InputNumber, Select, Switch } from 'antd';
import type { ActionTypeInfo, JsonSchemaProp } from '../../types/workflow';
import TemplateInput from './TemplateInput';

interface Props {
  schema: ActionTypeInfo['configSchema'];
  value: Record<string, unknown>;
  onChange: (v: Record<string, unknown>) => void;
  refs: string[];
  errors: Record<string, string>;
}

/** Renders an action's config form from its JSON schema subset. */
export default function SchemaForm({ schema, value, onChange, refs, errors }: Props) {
  const set = (key: string, v: unknown) => onChange({ ...value, [key]: v });
  const required = new Set(schema.required ?? []);

  const control = (key: string, p: JsonSchemaProp) => {
    const v = value[key] ?? p.default;
    if (p.enum) {
      return <Select value={v as string} options={p.enum.map((o) => ({ value: o, label: o }))} onChange={(x) => set(key, x)} />;
    }
    switch (p.type) {
      case 'boolean':
        return <Switch checked={Boolean(v)} onChange={(x) => set(key, x)} />;
      case 'number':
      case 'integer':
        return <InputNumber value={v as number} onChange={(x) => set(key, x)} style={{ width: '100%' }} />;
      case 'array':
        return <Select mode="tags" value={(v as string[]) ?? []} onChange={(x) => set(key, x)} tokenSeparators={[',', ' ']}
          open={false} placeholder="Type and press Enter" />;
      default:
        return p['x-template']
          ? <TemplateInput value={v as string} onChange={(x) => set(key, x)} refs={refs} multiline={p['x-multiline']} />
          : <Input value={v as string} onChange={(e) => set(key, e.target.value)} />;
    }
  };

  return (
    <Form layout="vertical" component="div">
      {Object.entries(schema.properties ?? {}).map(([key, p]) => (
        <Form.Item key={key} label={p.title ?? key} required={required.has(key)} tooltip={p.description}
          validateStatus={errors[key] ? 'error' : undefined} help={errors[key]}>
          {control(key, p)}
        </Form.Item>
      ))}
    </Form>
  );
}
```

`ConditionEditor.tsx`:

```tsx
/* Apache license header */
import { useEffect, useState } from 'react';
import { AutoComplete, Button, Input, Segmented, Select, Space, Typography, message } from 'antd';
import { DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import type { Condition } from '../../types/workflow';
import { parseCondition } from '../../api/workflows';
import { OPS, coerceValue, fromBuilderModel, toBuilderModel, treeToExpr } from '../../utils/conditionTree';

interface Props {
  value: Condition;
  onChange: (c: Condition) => void;
  refs: string[];
  error?: string;
}

type Mode = 'always' | 'builder' | 'expr';

const modeOf = (c: Condition): Mode => (c.always ? 'always' : c.expr !== undefined ? 'expr' : 'builder');

export default function ConditionEditor({ value, onChange, refs, error }: Props) {
  const mode = modeOf(value);
  const [parseError, setParseError] = useState<string | null>(null);

  useEffect(() => {
    if (mode !== 'expr' || !value.expr?.trim()) {
      setParseError(null);
      return undefined;
    }
    const t = setTimeout(() => {
      parseCondition(value.expr!).then((r) => setParseError(r.error ?? null)).catch(() => setParseError(null));
    }, 400);
    return () => clearTimeout(t);
  }, [mode, value.expr]);

  const switchMode = async (next: Mode) => {
    if (next === 'always') {
      onChange({ always: true });
    } else if (next === 'expr') {
      onChange({ expr: value.tree ? treeToExpr(value.tree) : '' });
    } else if (mode === 'expr' && value.expr?.trim()) {
      const r = await parseCondition(value.expr);
      if (r.tree && toBuilderModel(r.tree)) {
        onChange({ tree: r.tree });
      } else {
        message.warning(r.error ?? 'This expression is too complex for the builder');
      }
    } else {
      onChange({ tree: { all: [] } });
    }
  };

  const model = mode === 'builder' ? toBuilderModel(value.tree) : null;
  const setModel = (m: NonNullable<typeof model>) => onChange({ tree: fromBuilderModel(m) });

  return (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Segmented<Mode> value={mode} onChange={switchMode}
        options={[{ label: 'Always', value: 'always' }, { label: 'Builder', value: 'builder' }, { label: 'Expression', value: 'expr' }]} />
      {mode === 'expr' && (
        <>
          <Input value={value.expr} status={parseError || error ? 'error' : undefined}
            placeholder="rowCount > 50 && actions.judge.verdict" onChange={(e) => onChange({ expr: e.target.value })}
            style={{ fontFamily: 'monospace' }} />
          {(parseError || error) && <Typography.Text type="danger">{parseError ?? error}</Typography.Text>}
        </>
      )}
      {mode === 'builder' && model && (
        <>
          <Select value={model.mode} style={{ width: 200 }} onChange={(m) => setModel({ ...model, mode: m })}
            options={[{ value: 'all', label: 'All of these (AND)' }, { value: 'any', label: 'Any of these (OR)' }]} />
          {model.leaves.map((leaf, i) => {
            const update = (patch: Partial<typeof leaf>) =>
              setModel({ ...model, leaves: model.leaves.map((l, j) => (j === i ? { ...l, ...patch } : l)) });
            return (
              <Space.Compact key={i} style={{ width: '100%' }}>
                <AutoComplete value={leaf.field} style={{ width: '40%' }} onChange={(f) => update({ field: f })}
                  options={[...refs, 'rows[*].'].map((r) => ({ value: r }))} />
                <Select value={leaf.op} style={{ width: 130 }} onChange={(op) => update({ op })}
                  options={OPS.map((o) => ({ value: o, label: o }))} />
                <Input value={leaf.value === null ? 'null' : String(leaf.value ?? '')}
                  onChange={(e) => update({ value: coerceValue(e.target.value) })} />
                <Button icon={<DeleteOutlined />} aria-label="Remove condition"
                  onClick={() => setModel({ ...model, leaves: model.leaves.filter((_, j) => j !== i) })} />
              </Space.Compact>
            );
          })}
          <Button icon={<PlusOutlined />} size="small"
            onClick={() => setModel({ ...model, leaves: [...model.leaves, { field: 'rowCount', op: '>', value: 0 }] })}>
            Add condition
          </Button>
          {error && <Typography.Text type="danger">{error}</Typography.Text>}
        </>
      )}
      {mode === 'builder' && !model && (
        <Typography.Text type="warning">Too complex for the builder; switch to Expression to edit.</Typography.Text>
      )}
    </Space>
  );
}
```

`OutcomeList.tsx`:

```tsx
/* Apache license header */
import { List, Tag, Typography } from 'antd';
import type { ActionOutcome } from '../../types/workflow';

const COLORS: Record<ActionOutcome['outcome'], string> = { fired: 'green', skipped: 'default', errored: 'red' };

export default function OutcomeList({ outcomes }: { outcomes: ActionOutcome[] }) {
  return (
    <List size="small" dataSource={outcomes} renderItem={(o) => (
      <List.Item>
        <List.Item.Meta
          title={<>{o.id} <Tag>{o.type}</Tag><Tag color={COLORS[o.outcome]}>{o.outcome}</Tag>
            <Typography.Text type="secondary">{o.durationMs} ms</Typography.Text></>}
          description={<>
            {o.reason && <div>{o.reason}</div>}
            {o.error && <Typography.Text type="danger">{o.error}</Typography.Text>}
            {o.outputs && <pre style={{ whiteSpace: 'pre-wrap', margin: 0 }}>{JSON.stringify(o.outputs, null, 2)}</pre>}
          </>} />
      </List.Item>
    )} />
  );
}
```

`ActionCard.tsx`:

```tsx
/* Apache license header */
import { Button, Card, Input, Space, Tag, Tooltip, Typography } from 'antd';
import { ArrowDownOutlined, ArrowUpOutlined, DeleteOutlined } from '@ant-design/icons';
import type { ActionTypeInfo, ValidationError, WorkflowAction } from '../../types/workflow';
import ConditionEditor from './ConditionEditor';
import SchemaForm from './SchemaForm';

interface Props {
  action: WorkflowAction;
  index: number;
  count: number;
  type?: ActionTypeInfo;
  refs: string[];
  errors: ValidationError[];
  onChange: (a: WorkflowAction) => void;
  onMove: (delta: -1 | 1) => void;
  onDelete: () => void;
}

export default function ActionCard({ action, index, count, type, refs, errors, onChange, onMove, onDelete }: Props) {
  const prefix = `actions[${index}]`;
  const errorAt = (p: string) => errors.find((e) => e.path === `${prefix}${p}`)?.message;
  const configErrors: Record<string, string> = {};
  errors.forEach((e) => {
    const m = e.path.match(/^actions\[\d+\]\.config\.([^.[]+)/);
    if (m) configErrors[m[1]] = e.message;
  });
  const generalConfigError = errorAt('.config');

  return (
    <Card size="small"
      title={<Space>{type?.displayName ?? action.type}<Tag>{action.id}</Tag>
        {type?.hasSideEffects && <Tooltip title="Suppressed during dry runs"><Tag color="orange">side effects</Tag></Tooltip>}</Space>}
      extra={<Space>
        <Button size="small" icon={<ArrowUpOutlined />} aria-label="Move up" disabled={index === 0} onClick={() => onMove(-1)} />
        <Button size="small" icon={<ArrowDownOutlined />} aria-label="Move down" disabled={index === count - 1} onClick={() => onMove(1)} />
        <Button size="small" danger icon={<DeleteOutlined />} aria-label="Delete action" onClick={onDelete} />
      </Space>}>
      <Space direction="vertical" style={{ width: '100%' }}>
        <div>
          <Typography.Text strong>Action id</Typography.Text>
          <Input value={action.id} status={errorAt('.id') ? 'error' : undefined}
            onChange={(e) => onChange({ ...action, id: e.target.value })} />
          {errorAt('.id') && <Typography.Text type="danger">{errorAt('.id')}</Typography.Text>}
          {errorAt('.type') && <Typography.Text type="danger">{errorAt('.type')}</Typography.Text>}
        </div>
        <div>
          <Typography.Text strong>Run when</Typography.Text>
          <ConditionEditor value={action.condition} refs={refs} error={errorAt('.condition')}
            onChange={(condition) => onChange({ ...action, condition })} />
        </div>
        {type && (
          <SchemaForm schema={type.configSchema} value={action.config} refs={refs} errors={configErrors}
            onChange={(config) => onChange({ ...action, config })} />
        )}
        {generalConfigError && <Typography.Text type="danger">{generalConfigError}</Typography.Text>}
      </Space>
    </Card>
  );
}
```

- [ ] **Step 4: Run tests and type-check**

Run: `npx vitest run src/components/workflow && npx tsc --noEmit`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src/components/workflow
git commit -m "Add workflow editor building blocks"
```

---

### Task 14: Workflow editor page with dry run, runs, and health

**Files:**
- Create: `webapp/src/pages/WorkflowEditorPage.tsx`, `webapp/src/components/workflow/RunsTab.tsx`, `webapp/src/components/workflow/HealthTab.tsx`
- Modify: `webapp/src/App.tsx` (lazy import + two routes inside `/projects/:id`)

**Interfaces:**
- Consumes: Tasks 12–13; `useProjectContext()` (`project`, `projectId`, `savedQueryIdSet`), `getSavedQueries()`, `useCurrentUser()`.
- Produces: routes `/projects/:id/workflows/new` and `/projects/:id/workflows/:wid`.

- [ ] **Step 1: Implement `RunsTab.tsx`**

```tsx
/* Apache license header */
import { useQuery } from '@tanstack/react-query';
import { Table, Tag } from 'antd';
import dayjs from 'dayjs';
import type { WorkflowRun } from '../../types/workflow';
import { listRuns } from '../../api/workflows';
import OutcomeList from './OutcomeList';

function parseJson<T>(s: string | undefined, fallback: T): T {
  try {
    return s ? (JSON.parse(s) as T) : fallback;
  } catch {
    return fallback;
  }
}

export default function RunsTab({ workflowId }: { workflowId: string }) {
  const { data = [], isLoading } = useQuery({ queryKey: ['workflow-runs', workflowId], queryFn: () => listRuns(workflowId) });
  const count = (r: WorkflowRun, o: string) => (r.actions ?? []).filter((a) => a.outcome === o).length;
  return (
    <Table<WorkflowRun> rowKey="id" loading={isLoading} dataSource={data} size="small"
      columns={[
        { title: 'When', dataIndex: 'executedAt', render: (v: string) => dayjs(v).format('YYYY-MM-DD HH:mm') },
        { title: 'Trigger', dataIndex: 'trigger' },
        { title: 'Status', dataIndex: 'status',
          render: (s: string) => <Tag color={s === 'success' ? 'green' : s === 'error' ? 'red' : 'default'}>{s}</Tag> },
        { title: 'Rows', dataIndex: 'rowCount' },
        { title: 'Duration', dataIndex: 'duration', render: (d?: number) => (d == null ? '' : `${d} ms`) },
        { title: 'Actions', render: (_, r) => `${count(r, 'fired')} fired · ${count(r, 'skipped')} skipped · ${count(r, 'errored')} errored` },
      ]}
      expandable={{
        expandedRowRender: (r) => {
          const columns = parseJson<string[]>(r.previewColumns, []);
          const rows = parseJson<Record<string, string>[]>(r.previewRows, []);
          return (
            <>
              {r.errorMessage && <Tag color="red">{r.errorMessage}</Tag>}
              <Table size="small" pagination={false} rowKey={(_, i) => String(i)} dataSource={rows}
                columns={columns.map((c) => ({ title: c, dataIndex: c }))} scroll={{ x: true }} />
              <OutcomeList outcomes={r.actions ?? []} />
            </>
          );
        },
      }} />
  );
}
```

- [ ] **Step 2: Implement `HealthTab.tsx`**

```tsx
/* Apache license header */
import { useQuery } from '@tanstack/react-query';
import { Alert, Table } from 'antd';
import ReactECharts from 'echarts-for-react';
import { getWorkflowHealth } from '../../api/workflows';

export default function HealthTab({ workflowId }: { workflowId: string }) {
  const { data } = useQuery({ queryKey: ['workflow-health', workflowId], queryFn: () => getWorkflowHealth(workflowId) });
  if (!data) return null;
  if (data.setupRequired) {
    return <Alert type="info" showIcon message="Run history is not set up"
      description="An admin can enable it from the Workflows page under Administration." />;
  }
  const days = data.daily.map((d) => d.day);
  const option = {
    tooltip: { trigger: 'axis' },
    legend: { data: ['Success rate %', 'Avg duration (ms)'] },
    xAxis: { type: 'category', data: days },
    yAxis: [{ type: 'value', max: 100, name: '%' }, { type: 'value', name: 'ms' }],
    series: [
      { name: 'Success rate %', type: 'line',
        data: data.daily.map((d) => Math.round((100 * Number(d.successes)) / Math.max(1, Number(d.runs)))) },
      { name: 'Avg duration (ms)', type: 'bar', yAxisIndex: 1, data: data.daily.map((d) => Math.round(Number(d.avgDurationMs))) },
    ],
  };
  return (
    <>
      <ReactECharts option={option} style={{ height: 280 }} />
      <Table size="small" rowKey={(r) => `${r.id}-${r.outcome}`} dataSource={data.actions} pagination={false}
        columns={[
          { title: 'Action', dataIndex: 'id' }, { title: 'Type', dataIndex: 'type' },
          { title: 'Outcome', dataIndex: 'outcome' }, { title: 'Count (30 d)', dataIndex: 'cnt' },
        ]} />
    </>
  );
}
```

- [ ] **Step 3: Implement `WorkflowEditorPage.tsx`**

```tsx
/* Apache license header */
import { useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  Alert, Button, Drawer, Dropdown, Form, Input, InputNumber, Select, Space, Spin, Switch, Table, Tabs, Typography, message,
} from 'antd';
import { PlayCircleOutlined, PlusOutlined, ExperimentOutlined } from '@ant-design/icons';
import type { ActionTypeInfo, DryRunResult, ValidationError, Workflow, WorkflowAction } from '../types/workflow';
import {
  createWorkflow, dryRunWorkflow, getActionTypes, getWorkflow, runWorkflowNow, setWorkflowEnabled, updateWorkflow,
  validateWorkflow, validationErrors,
} from '../api/workflows';
import { getErrorMessage } from '../api/client';
import { getSavedQueries } from '../api/savedQueries';
import { useProjectContext } from '../contexts/ProjectContext';
import { availableRefs, newWorkflow } from '../utils/conditionTree';
import ActionCard from '../components/workflow/ActionCard';
import OutcomeList from '../components/workflow/OutcomeList';
import RunsTab from '../components/workflow/RunsTab';
import HealthTab from '../components/workflow/HealthTab';

const DAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

function defaultsFor(t: ActionTypeInfo): Record<string, unknown> {
  const cfg: Record<string, unknown> = {};
  Object.entries(t.configSchema.properties ?? {}).forEach(([k, p]) => {
    if (p.default !== undefined) cfg[k] = p.default;
  });
  return cfg;
}

function uniqueId(base: string, actions: WorkflowAction[]): string {
  let i = 1;
  while (actions.some((a) => a.id === `${base}_${i}`)) i += 1;
  return `${base}_${i}`;
}

export default function WorkflowEditorPage() {
  const { wid } = useParams<{ wid: string }>();
  const [search] = useSearchParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { projectId, savedQueryIdSet } = useProjectContext();
  const [wf, setWf] = useState<Workflow | null>(wid ? null : newWorkflow(projectId!, search.get('query') ?? ''));
  const [errors, setErrors] = useState<ValidationError[]>([]);
  const [dryRun, setDryRun] = useState<DryRunResult | null>(null);
  const [busy, setBusy] = useState<'save' | 'dry' | 'run' | null>(null);

  const { data: types = [] } = useQuery({ queryKey: ['workflow-action-types'], queryFn: getActionTypes });
  const { data: queries = [] } = useQuery({ queryKey: ['savedQueries'], queryFn: getSavedQueries });
  const projectQueries = useMemo(() => queries.filter((q) => savedQueryIdSet.has(q.id)), [queries, savedQueryIdSet]);

  useEffect(() => {
    if (wid) getWorkflow(wid).then(setWf).catch((e) => message.error(String(e)));
  }, [wid]);

  useEffect(() => {
    if (!wf) return undefined;
    const t = setTimeout(() => { validateWorkflow(wf).then(setErrors).catch(() => undefined); }, 600);
    return () => clearTimeout(t);
  }, [wf]);

  if (!wf) return <Spin />;

  const set = (patch: Partial<Workflow>) => setWf({ ...wf, ...patch });
  const setSchedule = (patch: Partial<Workflow['schedule']>) => set({ schedule: { ...wf.schedule, ...patch } });
  const errorAt = (p: string) => errors.find((e) => e.path === p)?.message;
  const setAction = (i: number, a: WorkflowAction) => set({ actions: wf.actions.map((x, j) => (j === i ? a : x)) });
  const move = (i: number, d: -1 | 1) => {
    const next = [...wf.actions];
    [next[i], next[i + d]] = [next[i + d], next[i]];
    set({ actions: next });
  };

  const save = async () => {
    setBusy('save');
    try {
      const saved = wf.id ? await updateWorkflow(wf.id, wf) : await createWorkflow(wf);
      setWf(saved);
      queryClient.invalidateQueries({ queryKey: ['workflows'] });
      message.success('Saved');
      if (!wf.id) navigate(`/projects/${projectId}/workflows/${saved.id}`, { replace: true });
    } catch (e) {
      const v = validationErrors(e);
      if (v) setErrors(v);
      else message.error(getErrorMessage(e));
    } finally {
      setBusy(null);
    }
  };

  const runDry = async () => {
    setBusy('dry');
    try {
      setDryRun(await dryRunWorkflow(wf));
    } catch (e) {
      const v = validationErrors(e);
      if (v) setErrors(v);
      else message.error(getErrorMessage(e));
    } finally {
      setBusy(null);
    }
  };

  const runNow = async () => {
    setBusy('run');
    try {
      const run = await runWorkflowNow(wf.id!);
      message.info(`Run ${run.status}`);
      queryClient.invalidateQueries({ queryKey: ['workflow-runs', wf.id] });
    } catch (e) {
      message.error(getErrorMessage(e));
    } finally {
      setBusy(null);
    }
  };

  const toggle = async (enabled: boolean) => {
    try {
      setWf(await setWorkflowEnabled(wf.id!, enabled));
    } catch (e) {
      message.error(getErrorMessage(e));
    }
  };

  const definition = (
    <Space direction="vertical" style={{ width: '100%', maxWidth: 900 }} size="large">
      <Form layout="vertical" component="div">
        <Form.Item label="Name" required validateStatus={errorAt('name') ? 'error' : undefined} help={errorAt('name')}>
          <Input value={wf.name} onChange={(e) => set({ name: e.target.value })} />
        </Form.Item>
        <Form.Item label="Description">
          <Input.TextArea rows={2} value={wf.description} onChange={(e) => set({ description: e.target.value })} />
        </Form.Item>
        <Form.Item label="Saved query" required validateStatus={errorAt('savedQueryId') ? 'error' : undefined}
          help={errorAt('savedQueryId')}>
          <Select showSearch optionFilterProp="label" value={wf.savedQueryId || undefined}
            onChange={(v) => set({ savedQueryId: v })}
            options={projectQueries.map((q) => ({ value: q.id, label: q.name }))} />
        </Form.Item>
        <Space wrap>
          <Form.Item label="Frequency">
            <Select value={wf.schedule.frequency} style={{ width: 140 }} onChange={(frequency) => setSchedule({ frequency })}
              options={['hourly', 'daily', 'weekly', 'monthly'].map((f) => ({ value: f, label: f }))} />
          </Form.Item>
          <Form.Item label={wf.schedule.frequency === 'hourly' ? 'Minute (HH:mm, hour ignored)' : 'Time'}
            validateStatus={errorAt('schedule.timeOfDay') ? 'error' : undefined} help={errorAt('schedule.timeOfDay')}>
            <input type="time" value={wf.schedule.timeOfDay} onChange={(e) => setSchedule({ timeOfDay: e.target.value })}
              className="ant-input" style={{ width: 130 }} />
          </Form.Item>
          {wf.schedule.frequency === 'weekly' && (
            <Form.Item label="Day">
              <Select value={wf.schedule.dayOfWeek ?? 1} style={{ width: 140 }} onChange={(dayOfWeek) => setSchedule({ dayOfWeek })}
                options={DAYS.map((d, i) => ({ value: i, label: d }))} />
            </Form.Item>
          )}
          {wf.schedule.frequency === 'monthly' && (
            <Form.Item label="Day of month" validateStatus={errorAt('schedule.dayOfMonth') ? 'error' : undefined}>
              <InputNumber min={1} max={28} value={wf.schedule.dayOfMonth ?? 1} onChange={(v) => setSchedule({ dayOfMonth: v })} />
            </Form.Item>
          )}
          <Form.Item label="Timeout (s)">
            <InputNumber min={1} max={86400} value={wf.schedule.timeoutSeconds}
              onChange={(v) => setSchedule({ timeoutSeconds: v ?? 300 })} />
          </Form.Item>
        </Space>
      </Form>

      <Typography.Title level={5}>Actions</Typography.Title>
      {wf.actions.length === 0 && <Typography.Text type="secondary">No actions yet. The query will run and its results will be recorded.</Typography.Text>}
      {wf.actions.map((a, i) => (
        <ActionCard key={i} action={a} index={i} count={wf.actions.length} type={types.find((t) => t.id === a.type)}
          refs={availableRefs(wf.actions, i, types)} errors={errors.filter((e) => e.path.startsWith(`actions[${i}]`))}
          onChange={(x) => setAction(i, x)} onMove={(d) => move(i, d)}
          onDelete={() => set({ actions: wf.actions.filter((_, j) => j !== i) })} />
      ))}
      <Dropdown trigger={['click']} menu={{
        items: types.map((t) => ({ key: t.id, label: t.displayName, title: t.description })),
        onClick: ({ key }) => {
          const t = types.find((x) => x.id === key)!;
          set({ actions: [...wf.actions, { id: uniqueId(t.id, wf.actions), type: t.id, condition: { always: true }, config: defaultsFor(t) }] });
        },
      }}>
        <Button icon={<PlusOutlined />}>Add action</Button>
      </Dropdown>
    </Space>
  );

  return (
    <div style={{ padding: 16 }}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 12 }} wrap>
        <Typography.Title level={4} style={{ margin: 0 }}>{wf.id ? wf.name : 'New workflow'}</Typography.Title>
        <Space wrap>
          {wf.id && (
            <Space>
              <Typography.Text>Enabled</Typography.Text>
              <Switch checked={wf.schedule.enabled} onChange={toggle} />
            </Space>
          )}
          <Button icon={<ExperimentOutlined />} loading={busy === 'dry'} onClick={runDry}>Dry run</Button>
          {wf.id && <Button icon={<PlayCircleOutlined />} loading={busy === 'run'} onClick={runNow}>Run now</Button>}
          <Button type="primary" loading={busy === 'save'} disabled={errors.length > 0} onClick={save}>Save</Button>
        </Space>
      </Space>
      {wf.status && wf.status !== 'active' && wf.status !== 'paused' && (
        <Alert type="warning" showIcon style={{ marginBottom: 12 }} message={`Status: ${wf.status.replace(/_/g, ' ')}`} />
      )}
      <Tabs items={[
        { key: 'def', label: 'Definition', children: definition },
        ...(wf.id ? [
          { key: 'runs', label: 'Runs', children: <RunsTab workflowId={wf.id} /> },
          { key: 'health', label: 'Health', children: <HealthTab workflowId={wf.id} /> },
        ] : []),
      ]} />
      <Drawer open={!!dryRun} onClose={() => setDryRun(null)} width={640} title="Dry run">
        {dryRun && (
          <Space direction="vertical" style={{ width: '100%' }}>
            <Typography.Text>
              Query {dryRun.context.status} · {dryRun.context.rowCount} rows · {dryRun.context.durationMs} ms
            </Typography.Text>
            {dryRun.context.error && <Alert type="error" message={dryRun.context.error} />}
            <Table size="small" pagination={false} scroll={{ x: true }} rowKey={(_, i) => String(i)}
              dataSource={dryRun.context.rows.slice(0, 10)}
              columns={dryRun.context.columns.map((c) => ({ title: c, dataIndex: c }))} />
            <Typography.Text type="secondary">Side-effect actions show what they would send; nothing was sent.</Typography.Text>
            <OutcomeList outcomes={dryRun.actions} />
          </Space>
        )}
      </Drawer>
    </div>
  );
}
```

- [ ] **Step 4: Add routes**

In `App.tsx`, next to `ProjectWorkflowsPage`'s lazy import add:

```tsx
const WorkflowEditorPage = lazy(() => import('./pages/WorkflowEditorPage'));
```

and inside `<Route path="/projects/:id" …>` after `workflows`:

```tsx
                <Route path="workflows/new" element={<WorkflowEditorPage />} />
                <Route path="workflows/:wid" element={<WorkflowEditorPage />} />
```

- [ ] **Step 5: Type-check, test, manual check, commit**

```bash
npx tsc --noEmit && npx vitest run src/components/workflow src/utils/conditionTree.test.ts src/api/workflows.test.ts
```

Manual: run a Drillbit (`bin/drillbit.sh start` from a built distribution) and `npm run dev`; open `/projects/<id>/workflows/new`, pick a query, add a Record alert with expression `rowCount > 0`, Dry run (outcome `fired`), Save, Run now, check the Runs tab.

```bash
git add exec/java-exec/src/main/resources/webapp/src
git commit -m "Add workflow editor page with dry run, runs and health tabs"
```

---

### Task 15: Workflows list (project + admin), navigation, and removal of the old UI

**Files:**
- Rewrite: `webapp/src/pages/WorkflowsPage.tsx`, `webapp/src/pages/ProjectWorkflowsPage.tsx`
- Modify: `webapp/src/components/shell/Sidebar.tsx`, `webapp/src/pages/SavedQueriesPage.tsx`, `webapp/src/components/schema-explorer/SchemaExplorer.tsx`, `webapp/src/types/index.ts`
- Delete: `webapp/src/api/schedules.ts`, `webapp/src/api/schedules.test.ts`, `webapp/src/components/query-editor/ScheduleModal.tsx`

**Interfaces:**
- Consumes: Task 12 API; `getProjects()`; `useCurrentUser()`.
- Produces: `WorkflowsPage({ projectId?: string })`; `describeSchedule(s: WorkflowSchedule): string` exported from `WorkflowsPage.tsx`.

- [ ] **Step 1: Rewrite `WorkflowsPage.tsx`**

```tsx
/* Apache license header */
import { useMemo, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  Alert, Button, Card, Col, InputNumber, Modal, Row, Select, Space, Statistic, Switch, Table, Tag, Typography, message,
} from 'antd';
import { PlusOutlined, SettingOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import type { Workflow, WorkflowConfig, WorkflowSchedule } from '../types/workflow';
import {
  assignProject, getObservabilityStatus, getObservabilitySummary, getRunningWorkflows, getWorkflowConfig,
  listWorkflows, renewWorkflow, setupObservability, updateWorkflowConfig,
} from '../api/workflows';
import { getProjects } from '../api/projects';
import { useCurrentUser } from '../hooks/useCurrentUser';

const DAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

export function describeSchedule(s: WorkflowSchedule): string {
  switch (s.frequency) {
    case 'hourly': return `Hourly at :${s.timeOfDay.slice(3)}`;
    case 'weekly': return `${DAYS[s.dayOfWeek ?? 1]} ${s.timeOfDay}`;
    case 'monthly': return `Day ${s.dayOfMonth ?? 1} ${s.timeOfDay}`;
    default: return `Daily ${s.timeOfDay}`;
  }
}

export default function WorkflowsPage({ projectId }: { projectId?: string }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { user } = useCurrentUser();
  const isAdmin = user ? !user.authEnabled || user.isAdmin : false;
  const adminView = !projectId;
  const [settingsOpen, setSettingsOpen] = useState(false);

  const { data: workflows = [], isLoading } = useQuery({
    queryKey: ['workflows', projectId ?? 'all'], queryFn: () => listWorkflows(projectId), enabled: !adminView || isAdmin,
  });
  const { data: summary } = useQuery({
    queryKey: ['workflow-summary', projectId ?? 'all'], queryFn: () => getObservabilitySummary(projectId),
    enabled: !adminView || isAdmin,
  });
  const { data: config } = useQuery({ queryKey: ['workflow-config'], queryFn: getWorkflowConfig });
  const { data: projects = [] } = useQuery({ queryKey: ['projects'], queryFn: getProjects, enabled: adminView && isAdmin });
  const { data: running = [] } = useQuery({
    queryKey: ['workflow-running'], queryFn: getRunningWorkflows, enabled: adminView && isAdmin, refetchInterval: 15_000,
  });
  const { data: obsStatus } = useQuery({
    queryKey: ['workflow-obs-status'], queryFn: getObservabilityStatus, enabled: adminView && isAdmin,
  });

  const refresh = () => queryClient.invalidateQueries({ queryKey: ['workflows'] });
  const renew = useMutation({ mutationFn: renewWorkflow, onSuccess: refresh });
  const assign = useMutation({
    mutationFn: ({ id, pid }: { id: string; pid: string }) => assignProject(id, pid),
    onSuccess: refresh, onError: (e) => message.error(String(e)),
  });
  const setup = useMutation({
    mutationFn: setupObservability,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['workflow-obs-status'] }),
  });

  const stats = useMemo(() => {
    const byId = new Map((summary?.rows ?? []).map((r) => [r.workflowId, r]));
    const warnBefore = dayjs().add(config?.warningDaysBeforeExpiry ?? 14, 'day');
    const expiring = (w: Workflow) => !!w.schedule.expiresAt && dayjs(w.schedule.expiresAt).isBefore(warnBefore);
    const failing = (w: Workflow) => (w.consecutiveFailures ?? 0) >= 2;
    const rate = (w: Workflow) => {
      const r = byId.get(w.id!);
      return r && Number(r.runs) > 0 ? Math.round((100 * Number(r.successes)) / Number(r.runs)) : null;
    };
    const sorted = [...workflows].sort((a, b) =>
      Number(failing(b)) - Number(failing(a)) || Number(expiring(b)) - Number(expiring(a)) || a.name.localeCompare(b.name));
    return {
      sorted, expiring, failing, rate,
      active: workflows.filter((w) => w.schedule.enabled).length,
      failingCount: workflows.filter(failing).length,
      expiringCount: workflows.filter(expiring).length,
    };
  }, [workflows, summary, config]);

  if (adminView && user && !isAdmin) {
    return <Alert type="warning" showIcon message="Admin access required" style={{ margin: 16 }} />;
  }

  const projectName = (id: string | null) => projects.find((p) => p.id === id)?.name ?? '';
  const editPath = (w: Workflow) => `/projects/${w.projectId}/workflows/${w.id}`;
  const orphans = workflows.filter((w) => !w.projectId);

  return (
    <div style={{ padding: 16 }}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 12 }} wrap>
        <Typography.Title level={4} style={{ margin: 0 }}>{adminView ? 'All workflows' : 'Workflows'}</Typography.Title>
        <Space>
          {adminView && <Button icon={<SettingOutlined />} onClick={() => setSettingsOpen(true)}>Settings</Button>}
          {!adminView && (
            <Button type="primary" icon={<PlusOutlined />} onClick={() => navigate(`/projects/${projectId}/workflows/new`)}>
              New workflow
            </Button>
          )}
        </Space>
      </Space>

      <Row gutter={12} style={{ marginBottom: 12 }}>
        <Col xs={12} md={6}><Card size="small"><Statistic title="Workflows" value={workflows.length} /></Card></Col>
        <Col xs={12} md={6}><Card size="small"><Statistic title="Active" value={stats.active} /></Card></Col>
        <Col xs={12} md={6}><Card size="small"><Statistic title="Failing" value={stats.failingCount}
          valueStyle={stats.failingCount ? { color: '#cf1322' } : undefined} /></Card></Col>
        <Col xs={12} md={6}><Card size="small"><Statistic title="Expiring soon" value={stats.expiringCount} /></Card></Col>
      </Row>

      {adminView && obsStatus && !obsStatus.ready && (
        <Alert type="info" showIcon style={{ marginBottom: 12 }} message="Run history analytics are not set up"
          description={obsStatus.logDir ? 'Creates the dfs.workflow_logs workspace over the Drill log directory.' : 'DRILL_LOG_DIR is not set on this Drillbit.'}
          action={obsStatus.logDir && <Button size="small" loading={setup.isPending} onClick={() => setup.mutate()}>Set up</Button>} />
      )}

      {adminView && running.length > 0 && (
        <Alert type="info" style={{ marginBottom: 12 }} message={`Running now: ${running.map((w) => w.name).join(', ')}`} />
      )}

      <Table<Workflow> rowKey="id" loading={isLoading} dataSource={stats.sorted.filter((w) => w.projectId)} size="middle"
        columns={[
          { title: 'Name', render: (_, w) => <Link to={editPath(w)}>{w.name}</Link> },
          ...(adminView ? [
            { title: 'Project', render: (_: unknown, w: Workflow) => projectName(w.projectId) },
            { title: 'Owner', dataIndex: 'owner' },
          ] : []),
          { title: 'Schedule', render: (_, w) => describeSchedule(w.schedule) },
          { title: 'State', render: (_, w) => (
            <Space size={4} wrap>
              <Tag color={w.schedule.enabled ? 'green' : 'default'}>{w.schedule.enabled ? 'on' : (w.status ?? 'paused').replace(/_/g, ' ')}</Tag>
              {stats.failing(w) && <Tag color="red">{w.consecutiveFailures} failures</Tag>}
              {stats.expiring(w) && <Tag color="orange">expires {dayjs(w.schedule.expiresAt).format('MMM D')}
                <Button type="link" size="small" onClick={() => renew.mutate(w.id!)}>Renew</Button></Tag>}
            </Space>
          ) },
          { title: 'Last run', render: (_, w) => (w.lastRunAt
            ? <Tag color={w.lastStatus === 'error' ? 'red' : 'green'}>{dayjs(w.lastRunAt).format('MMM D HH:mm')}</Tag> : '—') },
          { title: 'Success (30 d)', render: (_, w) => { const r = stats.rate(w); return r == null ? '—' : `${r}%`; } },
          { title: 'Next run', render: (_, w) => (w.schedule.enabled && w.nextRunAt ? dayjs(w.nextRunAt).format('MMM D HH:mm') : '—') },
          { title: 'Actions', render: (_, w) => w.actions.map((a) => <Tag key={a.id}>{a.type}</Tag>) },
        ]} />

      {adminView && orphans.length > 0 && (
        <Card size="small" title="Workflows without a project" style={{ marginTop: 16 }}>
          <Typography.Paragraph type="secondary">
            Migrated schedules whose query is not in any project. They stay disabled until assigned.
          </Typography.Paragraph>
          <Table<Workflow> rowKey="id" size="small" pagination={false} dataSource={orphans} columns={[
            { title: 'Name', dataIndex: 'name' },
            { title: 'Owner', dataIndex: 'owner' },
            { title: 'Assign to', render: (_, w) => (
              <Select style={{ width: 240 }} placeholder="Choose project"
                options={projects.filter((p) => p.savedQueryIds.includes(w.savedQueryId)).map((p) => ({ value: p.id, label: p.name }))}
                notFoundContent="Add the saved query to a project first"
                onChange={(pid: string) => assign.mutate({ id: w.id!, pid })} />
            ) },
          ]} />
        </Card>
      )}

      {config && <SettingsModal open={settingsOpen} config={config} onClose={() => setSettingsOpen(false)} />}
    </div>
  );
}

function SettingsModal({ open, config, onClose }: { open: boolean; config: WorkflowConfig; onClose: () => void }) {
  const queryClient = useQueryClient();
  const [cfg, setCfg] = useState(config);
  const save = useMutation({
    mutationFn: () => updateWorkflowConfig(cfg),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['workflow-config'] }); onClose(); },
  });
  return (
    <Modal open={open} title="Workflow settings" onCancel={onClose} onOk={() => save.mutate()} confirmLoading={save.isPending}>
      <Space direction="vertical">
        <Space><Switch checked={cfg.expirationEnabled} onChange={(v) => setCfg({ ...cfg, expirationEnabled: v })} /> Workflows expire</Space>
        <Space>Expire after <InputNumber min={1} value={cfg.expirationDays} onChange={(v) => setCfg({ ...cfg, expirationDays: v ?? 90 })} /> days</Space>
        <Space>Warn <InputNumber min={0} value={cfg.warningDaysBeforeExpiry} onChange={(v) => setCfg({ ...cfg, warningDaysBeforeExpiry: v ?? 14 })} /> days before</Space>
        <Space>Action timeout <InputNumber min={1} value={cfg.actionTimeoutSeconds} onChange={(v) => setCfg({ ...cfg, actionTimeoutSeconds: v ?? 60 })} /> seconds</Space>
      </Space>
    </Modal>
  );
}
```

- [ ] **Step 2: Rewrite `ProjectWorkflowsPage.tsx`**

```tsx
/* Apache license header */
import { useProjectContext } from '../contexts/ProjectContext';
import WorkflowsPage from './WorkflowsPage';

export default function ProjectWorkflowsPage() {
  const { projectId } = useProjectContext();
  return projectId ? <WorkflowsPage projectId={projectId} /> : null;
}
```

- [ ] **Step 3: Sidebar**

In `components/shell/Sidebar.tsx`:
1. Remove the `{ to: '/workflows', … }` entry from `LIBRARY` (L126) and append `{ to: '/workflows', label: 'All workflows', icon: <FieldTimeOutlined />, matchPrefix: '/workflows' }` to `ADMIN` (L130–139).
2. In `sectionsFor(project)` (L549–557), always include `WORKFLOWS_SECTION`; delete the `scheduledQueryIds` intersection check.
3. Delete the `useQuery({ queryKey: ['schedules'], queryFn: getSchedules, … })` at L516, the `getSchedules` import at L48, and any now-unused variables (`tsc` will list them).

- [ ] **Step 4: SavedQueriesPage**

In `pages/SavedQueriesPage.tsx`:
1. Replace `import { getSchedules } from '../api/schedules';` with `import { listWorkflows } from '../api/workflows';` and delete the `ScheduleModal` import (L67).
2. Replace the schedules query (L139–147) with:

```tsx
  const { data: workflows } = useQuery({ queryKey: ['workflows', 'all'], queryFn: () => listWorkflows() });
  const scheduledIds = useMemo(
    () => new Set((workflows ?? []).filter((w) => w.schedule.enabled).map((w) => w.savedQueryId)),
    [workflows],
  );
```

3. Replace the `onSchedule` handler body (L480–484) with:

```tsx
                const owning = (projects ?? []).find((p) => p.savedQueryIds.includes(selected.id));
                if (owning) {
                  navigate(`/projects/${owning.id}/workflows/new?query=${selected.id}`);
                } else {
                  message.info('Add this query to a project to schedule it.');
                }
```

(Ensure `navigate = useNavigate()` and `message` from antd are in scope; add imports if not.)
4. Delete the `scheduleQueryId/Name/Sql` state (L122–124) and the `<ScheduleModal …/>` element (L544–553).

- [ ] **Step 5: SchemaExplorer**

In `components/schema-explorer/SchemaExplorer.tsx`: replace `import { getSchedules } from '../../api/schedules';` with `import { listWorkflows } from '../../api/workflows';`; change the query at L365 to `useReactQuery({ queryKey: ['workflows', 'all'], queryFn: () => listWorkflows() })` named `workflows` (default `[]`); and the mapping at L403–410 to:

```tsx
    const workflowQueries = (projectId ? [] : workflows)
      .filter((w) => w.schedule.enabled)
      .slice(0, 10)
      .map((w) => ({
        id: `workflow-${w.id}`,
        name: w.name,
        description: `Every ${w.schedule.frequency}`,
      }));
```

Update the `useMemo` dependency `schedules` → `workflows`.

- [ ] **Step 6: Delete the old UI and types**

```bash
cd exec/java-exec/src/main/resources/webapp
git rm src/api/schedules.ts src/api/schedules.test.ts src/components/query-editor/ScheduleModal.tsx
grep -rn "api/schedules\|ScheduleModal\|QuerySchedule\|QuerySnapshot\|AlertRule\|TriggeredAlert" src
```

Remove the types `ScheduleFrequency, DayOfWeek, ResultMode, RefreshMode, ScheduleStatus, AlertRule, QuerySchedule, TriggeredAlert, QuerySnapshot` from `types/index.ts` (L469–547) **only where the grep shows no remaining importer**. `components/dashboard/AiAlertsPanel.tsx` parses `config.alertRules` from a different object; leave it and any type it still imports.

- [ ] **Step 7: Verify and commit**

```bash
npx tsc --noEmit && npx vitest run && npm run build
```

Expected: no type errors, all tests pass, build succeeds.

```bash
git add -A exec/java-exec/src/main/resources/webapp/src
git commit -m "Rewrite workflows list for projects and admins and remove schedule modal"
```

---

### Task 16: Documentation

**Files:**
- Create: `docs/dev/WORKFLOWS.md`, `docs/dev/ui/pages/workflow-editor.md`
- Rewrite: `docs/dev/SCHEDULER.md` (pointer), `docs/dev/ui/pages/workflows.md`, `docs/dev/ui/pages/project-workflows.md`
- Modify: `CLAUDE.md` (docs table), `docs/dev/DevDocs.md` (index), `docs/dev/ui/README.md` (page list, if it enumerates pages)

- [ ] **Step 1: Write `docs/dev/WORKFLOWS.md`** covering, with the exact names from this plan:
  - Concepts: workflow document (JSON example from spec §1), conditions (three forms, grammar, operators, field paths incl. `intervalMs`, `rows[*].col` any-match, missing values), templates.
  - Action types table (`record_alert`, `save_to_table`, `email`) with config fields and outputs; how to add a type (implement `ActionType`, add to `ActionTypeRegistry.defaults()`).
  - Runner: tick, claim key `<id>:<nextRunAt>`, query with 100-row preview + `COUNT(*)` when capped, pipeline semantics (skip reasons), recording (20 stored preview rows), retention, expiry, `owner_lost_access`.
  - Permissions matrix (spec §3) and the run-as-owner rule.
  - REST reference (Task 10/11 endpoints).
  - Storage: store names; `workflow-runs.log` appender; observability setup.
  - Migration: field mapping table; `legacyMigrated` flag; **upgrade note: legacy `notifyEmails` become live `email` actions and start sending once SMTP is configured**; orphans disabled until assigned; disabled alert rules and AI-summary stub not migrated.
  - SMTP: uses `/api/v1/smtp/config`; `angus-mail` dependency, excluded from jdbc-all.
- [ ] **Step 2: Replace `SCHEDULER.md` body** with: "Replaced by workflows. See [WORKFLOWS.md](WORKFLOWS.md)." (keep license header if the file has one).
- [ ] **Step 3: UI docs**: `ui/pages/workflows.md` (list page: routes, props `projectId?`, data sources, admin extras, settings), `ui/pages/workflow-editor.md` (routes, tabs, condition editor modes, dry run, validation flow), `ui/pages/project-workflows.md` (thin wrapper; tab always visible now). Follow the format of the existing page docs.
- [ ] **Step 4: Index**: add a `**Workflows** — scheduled queries with action pipelines, conditions, run log | [docs/dev/WORKFLOWS.md](docs/dev/WORKFLOWS.md)` row to the `CLAUDE.md` table and to `docs/dev/DevDocs.md`.
- [ ] **Step 5: Commit**

```bash
git add docs/dev CLAUDE.md
git commit -m "Document workflows backend, editor and list pages"
```

---

## Final verification (after all tasks)

```bash
mvn checkstyle:check -pl exec/java-exec
mvn test -pl exec/java-exec -Dtest='*Workflow*,ConditionParserTest,ConditionEvaluatorTest,TemplatesTest,ActionPipelineTest,RunClaimsTest,RunContextTest,BuiltInActionsTest,SmtpMailerTest' -DfailIfNoTests=false
mvn install -pl exec/jdbc-all -am -DskipTests
cd exec/java-exec/src/main/resources/webapp && npx tsc --noEmit && npx vitest run && npm run build
```

Then the manual pass from Task 14 Step 5, plus: with two schedules in the legacy store before first start, confirm both appear as workflows, an orphan shows under "Workflows without a project", and restarting does not re-create a deleted migrated workflow.
