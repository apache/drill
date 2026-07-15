# Prospector Interaction Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the AI Analytics dashboard show every Prospector interaction exactly once, attributed to the feature that made it, with real token counts.

**Architecture:** The server becomes the sole recorder of AI events. The browser stops logging entirely and instead tags each request with a `feature` string on `ChatContext`; `ProspectorResources` reads that tag when it writes its event. Client-side duplicate logging is deleted, blind spots (config test, early-return guards) gain events, and the dashboard stops reporting "no usage" when it is merely misconfigured.

**Tech Stack:** Java 17, JAX-RS (Jersey), Jackson, JUnit 5 (jupiter), SLF4J/Logback, React + TypeScript, Vitest.

**Spec:** `docs/superpowers/specs/2026-07-15-prospector-logging-and-context-design.md` (Part 1)

## Global Constraints

- Every new/modified source file (Java, TS, TSX) must carry the Apache 2.0 license header. See `docs/dev/LicenseHeadersAndNotices.md`.
- After modifying anything in `exec/java-exec`, run `mvn checkstyle:check -pl exec/java-exec`. It must pass. Common failures: `if` without braces, unused imports, missing license header.
- Do not add Claude as a git co-author. Commit messages are imperative ("Add X", not "Added X").
- Java tests in this area use **JUnit 5** (`org.junit.jupiter.api.Test`), matching `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ai/HttpClientFactoryTest.java`.
- `feature` values are lower_snake_case and must match the table in Task 3 exactly — the dashboard groups by this string.
- The default feature when a client sends none is `prospector_chat` (preserves old-client behaviour).

---

### Task 1: Make event construction testable and fix `success`

Today `recordEvent` is `private static` and its only output is a side effect (an SLF4J line), so its logic cannot be asserted. Split the pure part (`buildEvent`) from the side effect (`recordEvent`). This is a prerequisite for every later task's tests.

This task also fixes the correctness bug: `event.success = failure == null`. The streaming lambda catches `Exception`, not `Throwable`, so an `Error` (e.g. `OutOfMemoryError`) leaves `failure` null while `finally` still runs — recording a **successful, zero-token call**.

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java:497-527`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProspectorEventTest.java` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: `static AiEvent ProspectorResources.buildEvent(LlmConfig config, String username, String feature, String userMessage, String fullPrompt, LlmCallResult callResult, Exception failure, long durationMs)` — package-private, returns the populated event without logging. `recordEvent` keeps its behaviour and gains a `String feature` parameter after `username`.

- [ ] **Step 1: Write the failing test**

Create `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProspectorEventTest.java`:

```java
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
package org.apache.drill.exec.server.rest;

import org.apache.drill.exec.server.rest.ai.AiEvent;
import org.apache.drill.exec.server.rest.ai.LlmCallResult;
import org.apache.drill.exec.server.rest.ai.LlmConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the AI event built for each Prospector chat call.
 */
public class ProspectorEventTest {

  private static LlmConfig config() {
    LlmConfig config = new LlmConfig();
    config.setProvider("openai");
    config.setModel("gpt-4o");
    return config;
  }

  private static LlmCallResult result() {
    LlmCallResult result = new LlmCallResult();
    result.setPromptTokens(10);
    result.setResponseTokens(5);
    result.appendResponseText("hello");
    return result;
  }

  @Test
  public void testFeatureIsRecorded() {
    AiEvent event = ProspectorResources.buildEvent(
        config(), "alice", "dashboard_qna", "hi", "prompt", result(), null, 42L);
    assertEquals("dashboard_qna", event.feature);
    assertEquals("server", event.source);
    assertEquals("alice", event.user);
    assertEquals("openai", event.provider);
    assertEquals("gpt-4o", event.model);
    assertEquals(42L, event.durationMs);
  }

  @Test
  public void testFeatureDefaultsWhenAbsent() {
    AiEvent nullFeature = ProspectorResources.buildEvent(
        config(), "alice", null, "hi", "prompt", result(), null, 1L);
    assertEquals("prospector_chat", nullFeature.feature);

    AiEvent blankFeature = ProspectorResources.buildEvent(
        config(), "alice", "   ", "hi", "prompt", result(), null, 1L);
    assertEquals("prospector_chat", blankFeature.feature);
  }

  @Test
  public void testSuccessWhenResultPresent() {
    AiEvent event = ProspectorResources.buildEvent(
        config(), "alice", "sql_lab_chat", "hi", "prompt", result(), null, 1L);
    assertTrue(event.success);
    assertEquals(10, event.promptTokens);
    assertEquals(5, event.responseTokens);
    assertEquals("hello", event.response);
    assertNull(event.errorClass);
  }

  /**
   * An Error (not Exception) escapes the streaming catch block, leaving failure
   * null while finally still runs. Without a callResult this is not a success.
   */
  @Test
  public void testNotSuccessfulWhenResultMissingAndNoFailure() {
    AiEvent event = ProspectorResources.buildEvent(
        config(), "alice", "sql_lab_chat", "hi", "prompt", null, null, 1L);
    assertFalse(event.success);
    assertNull(event.totalTokens);
  }

  @Test
  public void testFailureIsRecorded() {
    AiEvent event = ProspectorResources.buildEvent(
        config(), "alice", "sql_lab_chat", "hi", "prompt", null,
        new IllegalStateException("boom"), 1L);
    assertFalse(event.success);
    assertEquals("IllegalStateException", event.errorClass);
    assertEquals("boom", event.error);
    assertFalse(event.cancelled);
  }

  @Test
  public void testClientCancellationIsFlagged() {
    AiEvent event = ProspectorResources.buildEvent(
        config(), "alice", "sql_lab_chat", "hi", "prompt", null,
        new IOException("Broken pipe"), 1L);
    assertTrue(event.cancelled);
    assertEquals("ClientCancelled", event.errorClass);
    assertFalse(event.success);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o test -pl exec/java-exec -Dtest=ProspectorEventTest -DfailIfNoTests=false`
Expected: FAIL — compilation error, `buildEvent` does not exist / has private access.

- [ ] **Step 3: Write minimal implementation**

In `ProspectorResources.java`, replace the body of `recordEvent` (lines 497-527) with a delegating pair. `DEFAULT_FEATURE` goes next to the other constants near line 71.

```java
  private static final String DEFAULT_FEATURE = "prospector_chat";

  private static void recordEvent(LlmConfig config, String username, String feature,
      String userMessage, String fullPrompt, LlmCallResult callResult, Exception failure,
      long durationMs) {
    try {
      AiEventLogger.log(buildEvent(config, username, feature, userMessage, fullPrompt,
          callResult, failure, durationMs));
    } catch (Exception logErr) {
      logger.warn("Failed to record AI usage event", logErr);
    }
  }

  /**
   * Builds the event for a single LLM call. Package-private and side-effect free
   * so the recorded fields can be asserted directly.
   */
  static AiEvent buildEvent(LlmConfig config, String username, String feature,
      String userMessage, String fullPrompt, LlmCallResult callResult, Exception failure,
      long durationMs) {
    AiEvent event = new AiEvent();
    event.ts = AiEventLogger.nowIso();
    event.user = username;
    event.feature = (feature == null || feature.trim().isEmpty()) ? DEFAULT_FEATURE : feature;
    event.source = "server";
    event.provider = config != null ? config.getProvider() : null;
    event.model = config != null ? config.getModel() : null;
    event.durationMs = durationMs;
    // A missing callResult means no completed call, even when nothing was thrown:
    // an Error escapes the streaming catch(Exception) but still runs its finally.
    event.success = failure == null && callResult != null;
    event.cancelled = AiEventLogger.isClientCancellation(failure);
    if (failure != null) {
      event.errorClass = event.cancelled
          ? "ClientCancelled"
          : failure.getClass().getSimpleName();
      event.error = failure.getMessage();
    }
    if (callResult != null) {
      event.promptTokens = callResult.getPromptTokens();
      event.responseTokens = callResult.getResponseTokens();
      event.totalTokens = callResult.getTotalTokens();
      event.response = AiEventLogger.truncate(callResult.getResponseText());
    }
    event.userMessage = AiEventLogger.truncate(userMessage);
    event.prompt = AiEventLogger.truncate(fullPrompt);
    return event;
  }
```

Update the single existing caller at line 398 to pass the feature through. For now pass `null` (Task 3 supplies the real value):

```java
        } finally {
          recordEvent(snapshot, username, null, userMessage, fullPrompt,
              callResult, failure, System.currentTimeMillis() - start);
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -o test -pl exec/java-exec -Dtest=ProspectorEventTest -DfailIfNoTests=false`
Expected: PASS — 6 tests.

Run: `mvn -o checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProspectorEventTest.java
git commit -m "Separate AI event construction from logging and fix success flag

An Error escapes the streaming catch(Exception) but still runs its finally,
so a call that produced no result was recorded as a successful zero-token
call. Require a callResult for success, and extract buildEvent so the
recorded fields can be asserted without capturing log output."
```

---

### Task 2: Carry `feature` on the wire

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java` (`ChatContext`, ~line 121; `chat`, ~line 398)
- Modify: `exec/java-exec/src/main/resources/webapp/src/types/ai.ts:39`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProspectorEventTest.java` (extend)

**Interfaces:**
- Consumes: `ProspectorResources.buildEvent(...)` from Task 1.
- Produces: `ChatContext.feature` (`String`, Java; `feature?: string`, TS). `chat()` reads `request.context.feature` and passes it to `recordEvent`.

- [ ] **Step 1: Write the failing test**

Append to `ProspectorEventTest.java`:

```java
  @Test
  public void testChatContextCarriesFeature() throws Exception {
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    ProspectorResources.ChatContext ctx = mapper.readValue(
        "{\"feature\":\"executive_summary\"}", ProspectorResources.ChatContext.class);
    assertEquals("executive_summary", ctx.feature);
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o test -pl exec/java-exec -Dtest=ProspectorEventTest -DfailIfNoTests=false`
Expected: FAIL — compilation error, `ChatContext.feature` does not exist.

- [ ] **Step 3: Write minimal implementation**

In `ProspectorResources.ChatContext` (after `currentSql`, ~line 123) add:

```java
    /** Which UI feature originated this call; used for analytics attribution. */
    @JsonProperty
    public String feature;
```

In `chat()`, replace the `null` placeholder from Task 1 with the context value. Read it into a local before the lambda, because `request` must be effectively final inside `StreamingOutput`:

```java
      String feature = request.context != null ? request.context.feature : null;
```

and in the `finally`:

```java
        } finally {
          recordEvent(snapshot, username, feature, userMessage, fullPrompt,
              callResult, failure, System.currentTimeMillis() - start);
        }
```

In `webapp/src/types/ai.ts`, add to `ChatContext` (after `currentSchema`):

```ts
  /** Which UI feature originated this call; used for analytics attribution. */
  feature?: string;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -o test -pl exec/java-exec -Dtest=ProspectorEventTest -DfailIfNoTests=false`
Expected: PASS — 7 tests.

Run: `cd exec/java-exec/src/main/resources/webapp && npx tsc --noEmit`
Expected: exit 0, no output.

Run: `mvn -o checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java \
        exec/java-exec/src/main/resources/webapp/src/types/ai.ts \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProspectorEventTest.java
git commit -m "Carry originating feature on ChatContext for AI analytics"
```

---

### Task 3: Tag every call site

Thirteen call sites currently send no feature, so the dashboard cannot distinguish them. Set `feature` on each `ChatContext`.

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/hooks/useProspector.ts` (~line 301)
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/SqlLabPage.tsx` (`aiContext` memo ~line 381; optimize call ~line 528)
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/shell/GlobalProspectorTab.tsx:30`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/project/QuerySuggestions.tsx` (~line 358)
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/ai/AiAssistantModal.tsx` (explain ~line 90; optimize ~line 210)
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/dashboard/AiQnAPanel.tsx` (~line 97)
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/dashboard/NlFilterPanel.tsx` (~line 85)
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/dashboard/AiAlertsPanel.tsx` (~line 180)
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/dashboard/ExecutiveSummaryPanel.tsx` (~line 249)
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/ProjectWikiPage.tsx` (~line 207)
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/ProfileDetailPage.tsx` (~line 364)
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/storage/FileSystemForm.tsx` (~line 937)

**Interfaces:**
- Consumes: `ChatContext.feature` from Task 2.
- Produces: the exact feature vocabulary below. Task 6 documents it; the dashboard groups by it.

Feature vocabulary (use verbatim):

| File | `feature` |
|---|---|
| `useProspector.ts` | `sql_lab_chat` |
| `GlobalProspectorTab.tsx` | `global_chat` |
| `QuerySuggestions.tsx` | `query_suggestions` |
| `AiAssistantModal.tsx` (explain) | `explain_query` |
| `AiAssistantModal.tsx` (optimize) | `optimize_query` |
| `SqlLabPage.tsx:528` | `sql_lab_optimize` |
| `AiQnAPanel.tsx` | `dashboard_qna` |
| `ExecutiveSummaryPanel.tsx` | `executive_summary` |
| `NlFilterPanel.tsx` | `nl_filter` |
| `AiAlertsPanel.tsx` | `ai_alerts` |
| `ProjectWikiPage.tsx` | `wiki_generation` |
| `ProfileDetailPage.tsx` | `profile_analysis` |
| `FileSystemForm.tsx` | `filesystem_form` |

**Note on `useProspector.ts`:** it does not build the context itself — it receives it from the caller and forwards it at line 301-302. `SqlLabPage`'s `aiContext` memo is the SQL Lab source, so set `feature: 'sql_lab_chat'` there. `GlobalProspectorTab.tsx:30` currently sends `{}`; make it `{ feature: 'global_chat' }`. Do not set a default inside `useProspector` — the server already defaults, and a hook-level default would mask an untagged caller.

- [ ] **Step 1: Write the failing test**

Create `exec/java-exec/src/main/resources/webapp/src/components/shell/__tests__/GlobalProspectorTab.context.test.ts`:

```ts
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
import { describe, expect, it } from 'vitest';
import { readFileSync } from 'fs';
import { join } from 'path';

/**
 * Every streamChat caller must tag its ChatContext with a feature, otherwise the
 * call is attributed to the prospector_chat default and the analytics dashboard
 * cannot tell the features apart. This guards the vocabulary as a whole.
 */
const SRC = join(__dirname, '..', '..', '..');

const EXPECTED: Array<[string, string]> = [
  ['pages/SqlLabPage.tsx', 'sql_lab_chat'],
  ['pages/SqlLabPage.tsx', 'sql_lab_optimize'],
  ['components/shell/GlobalProspectorTab.tsx', 'global_chat'],
  ['components/project/QuerySuggestions.tsx', 'query_suggestions'],
  ['components/ai/AiAssistantModal.tsx', 'explain_query'],
  ['components/ai/AiAssistantModal.tsx', 'optimize_query'],
  ['components/dashboard/AiQnAPanel.tsx', 'dashboard_qna'],
  ['components/dashboard/ExecutiveSummaryPanel.tsx', 'executive_summary'],
  ['components/dashboard/NlFilterPanel.tsx', 'nl_filter'],
  ['components/dashboard/AiAlertsPanel.tsx', 'ai_alerts'],
  ['pages/ProjectWikiPage.tsx', 'wiki_generation'],
  ['pages/ProfileDetailPage.tsx', 'profile_analysis'],
  ['components/storage/FileSystemForm.tsx', 'filesystem_form'],
];

describe('AI feature tagging', () => {
  it.each(EXPECTED)('%s tags feature %s', (file, feature) => {
    const source = readFileSync(join(SRC, file), 'utf8');
    expect(source).toContain(`feature: '${feature}'`);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd exec/java-exec/src/main/resources/webapp && npx vitest run src/components/shell/__tests__/GlobalProspectorTab.context.test.ts`
Expected: FAIL — 13 assertions fail, no file contains a `feature:` tag.

- [ ] **Step 3: Write minimal implementation**

Add the tag to each context object literal per the table. Examples:

`GlobalProspectorTab.tsx:30`:

```ts
  const context: ChatContext = useMemo(() => ({ feature: 'global_chat' }), []);
```

`SqlLabPage.tsx` `aiContext` memo — add as the first property:

```ts
      feature: 'sql_lab_chat',
```

`SqlLabPage.tsx:528` (optimize call) — its inline context becomes:

```ts
        context: { feature: 'sql_lab_optimize' },
```

`QuerySuggestions.tsx:358` — replace `context: {}`:

```ts
        context: { feature: 'query_suggestions' },
```

`ProjectWikiPage.tsx:207` — replace `context: {}`:

```ts
        context: { feature: 'wiki_generation' },
```

Apply the same shape to `AiAssistantModal.tsx` (explain → `explain_query`, optimize → `optimize_query`), `AiQnAPanel.tsx` (`dashboard_qna`), `ExecutiveSummaryPanel.tsx` (`executive_summary`), `NlFilterPanel.tsx` (`nl_filter`), `AiAlertsPanel.tsx` (`ai_alerts`), `ProfileDetailPage.tsx` (`profile_analysis`), `FileSystemForm.tsx` (`filesystem_form`). Where the component already builds a populated `ChatContext` (the dashboard panels set `dashboardQnAMode` etc.), add `feature` alongside the existing fields rather than replacing them.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd exec/java-exec/src/main/resources/webapp && npx vitest run src/components/shell/__tests__/GlobalProspectorTab.context.test.ts`
Expected: PASS — 13 assertions.

Run: `npx tsc --noEmit`
Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src
git commit -m "Tag every AI call site with its originating feature

All thirteen streamChat callers previously landed in the analytics dashboard
as prospector_chat, so dashboard Q&A, wiki generation and SQL Lab chat could
not be told apart."
```

---

### Task 4: Delete client-side logging

`query_suggestions`, `explain_query`, and `optimize_query` log in the browser **and** trigger a server `prospector_chat` event for the same LLM call, so `totalCalls`, `totalTokens`, and the cost projection are inflated and mix real tokens with `Math.ceil(len/4)` estimates. Task 3 gave the server the attribution these events existed to provide, so they are now pure duplicates.

Verified before writing this plan: those two components are the only importers of `aiObservability`, the service is the only caller of `POST /api/v1/ai/logs`, and its `getLogs()` localStorage accessor has no consumers. The deletion is clean.

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/project/QuerySuggestions.tsx` (remove import line 25; remove calls at 375, 383, 392)
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/ai/AiAssistantModal.tsx` (remove import line 23; remove calls at 107, 112, 228, 233)
- Delete: `exec/java-exec/src/main/resources/webapp/src/services/aiObservability.ts` (354 lines)
- Delete: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ai/AiLogsResources.java`
- Test: `exec/java-exec/src/main/resources/webapp/src/components/shell/__tests__/GlobalProspectorTab.context.test.ts` (extend)

**Interfaces:**
- Consumes: feature tagging from Task 3 (this deletion is only safe once the server records attribution).
- Produces: no client-side AI logging. `source` on every event is now always `server`.

- [ ] **Step 1: Write the failing test**

Append to `GlobalProspectorTab.context.test.ts`:

```ts
describe('client-side AI logging is removed', () => {
  it.each([
    'components/project/QuerySuggestions.tsx',
    'components/ai/AiAssistantModal.tsx',
  ])('%s does not log AI calls from the browser', (file) => {
    const source = readFileSync(join(SRC, file), 'utf8');
    expect(source).not.toContain('aiObservability');
    expect(source).not.toContain('logAICall');
  });

  it('the aiObservability service no longer exists', () => {
    expect(existsSync(join(SRC, 'services/aiObservability.ts'))).toBe(false);
  });
});
```

Add `existsSync` to the fs import at the top of the file:

```ts
import { existsSync, readFileSync } from 'fs';
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd exec/java-exec/src/main/resources/webapp && npx vitest run src/components/shell/__tests__/GlobalProspectorTab.context.test.ts`
Expected: FAIL — 3 assertions fail; the service exists and both components reference it.

- [ ] **Step 3: Write minimal implementation**

Remove the import and every `aiObservability.logAICall(...)` call from both components. In `QuerySuggestions.tsx` the calls sit inside the `onDone`/`onError` callbacks alongside `duration` bookkeeping — delete only the logging call; if a `duration`/`startTime` local becomes unused afterwards, delete it too (`tsc` will flag it).

Then delete both files:

```bash
git rm exec/java-exec/src/main/resources/webapp/src/services/aiObservability.ts
git rm exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ai/AiLogsResources.java
```

`AiLogsResources` is a JAX-RS resource discovered by package scanning, so no registration list needs editing. Confirm nothing else references it:

```bash
grep -rn --include='*.java' --include='*.ts' --include='*.tsx' -E "AiLogsResources|ai/logs|aiObservability" exec/java-exec/src
```

Expected: no matches.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd exec/java-exec/src/main/resources/webapp && npx vitest run src/components/shell/__tests__/GlobalProspectorTab.context.test.ts`
Expected: PASS.

Run: `npx tsc --noEmit`
Expected: exit 0.

Run: `mvn -o test-compile -pl exec/java-exec -DskipTests` then `mvn -o checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS for both.

- [ ] **Step 5: Commit**

```bash
git add -A exec/java-exec/src
git commit -m "Remove client-side AI logging in favour of server events

Three call sites logged in the browser while the server logged the same LLM
call, inflating totalCalls, totalTokens and the cost projection and mixing
real token counts with char/4 estimates. The server now records the feature
these events existed to supply, leaving them pure duplicates."
```

---

### Task 5: Log the blind spots

Three paths in `chat()` return before `StreamingOutput` is built, so nothing is recorded: the disabled guard (`:347`), the unknown-provider guard (`:355`), and the outer catch (`:409`). Separately, `POST /api/v1/ai/config/test` issues a real billable LLM call (`provider.validateConfig()` → `probe()`, `max_tokens:1`) and is recorded nowhere.

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java:344-415`
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/AiConfigResources.java` (~line 604, `testConfig`)
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProspectorEventTest.java` (extend)

**Interfaces:**
- Consumes: `ProspectorResources.buildEvent(...)` (Task 1), `ChatContext.feature` (Task 2).
- Produces: `static AiEvent ProspectorResources.buildSetupFailureEvent(LlmConfig config, String username, String feature, String errorClass, String message)` — package-private, side-effect free. Events carry `errorClass` of `ProspectorDisabled` / `UnknownProvider`; `AiConfigResources` emits feature `config_test`.

- [ ] **Step 1: Write the failing test**

Append to `ProspectorEventTest.java`:

```java
  @Test
  public void testSetupFailureEvent() {
    AiEvent event = ProspectorResources.buildSetupFailureEvent(
        null, "alice", "sql_lab_chat", "ProspectorDisabled", "Prospector is not enabled");
    assertFalse(event.success);
    assertFalse(event.cancelled);
    assertEquals("sql_lab_chat", event.feature);
    assertEquals("ProspectorDisabled", event.errorClass);
    assertEquals("Prospector is not enabled", event.error);
    assertEquals("server", event.source);
    // A setup failure never reached the provider, so it burned no tokens.
    assertNull(event.totalTokens);
    assertEquals(0L, event.durationMs);
  }

  @Test
  public void testSetupFailureEventDefaultsFeature() {
    AiEvent event = ProspectorResources.buildSetupFailureEvent(
        null, "alice", null, "UnknownProvider", "Unknown LLM provider: bogus");
    assertEquals("prospector_chat", event.feature);
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o test -pl exec/java-exec -Dtest=ProspectorEventTest -DfailIfNoTests=false`
Expected: FAIL — compilation error, `buildSetupFailureEvent` does not exist.

- [ ] **Step 3: Write minimal implementation**

In `chat()`, record an event before each early return. Add the builder and its logging wrapper next to `recordEvent`:

```java
  /**
   * Builds the event for a failure that never reached the provider. Package-private
   * and side-effect free so the recorded fields can be asserted directly.
   */
  static AiEvent buildSetupFailureEvent(LlmConfig config, String username, String feature,
      String errorClass, String message) {
    AiEvent event = buildEvent(config, username, feature, null, null, null, null, 0L);
    event.success = false;
    event.cancelled = false;
    event.errorClass = errorClass;
    event.error = message;
    return event;
  }

  /** Records a setup failure that never reached the provider. */
  private static void recordSetupFailure(LlmConfig config, String username, String feature,
      String errorClass, String message) {
    try {
      AiEventLogger.log(buildSetupFailureEvent(config, username, feature, errorClass, message));
    } catch (Exception logErr) {
      logger.warn("Failed to record AI setup failure", logErr);
    }
  }
```

Read the username and feature at the top of `chat()`, before the guards:

```java
  public Response chat(ChatRequest request, @Context SecurityContext sc) {
    String username = sc != null && sc.getUserPrincipal() != null
        ? sc.getUserPrincipal().getName() : null;
    String feature = request != null && request.context != null ? request.context.feature : null;
    try {
      LlmConfig config = getConfig();
      if (config == null || !config.isEnabled()) {
        recordSetupFailure(config, username, feature,
            "ProspectorDisabled", "Prospector is not enabled");
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
            .entity(new ErrorResponse("Prospector is not enabled"))
            .type(MediaType.APPLICATION_JSON)
            .build();
      }

      LlmProvider provider = LlmProviderRegistry.get(config.getProvider());
      if (provider == null) {
        recordSetupFailure(config, username, feature,
            "UnknownProvider", "Unknown LLM provider: " + config.getProvider());
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(new ErrorResponse("Unknown LLM provider: " + config.getProvider()))
            .type(MediaType.APPLICATION_JSON)
            .build();
      }
```

**Important:** `username` is currently derived further down in `chat()`. Move that derivation to the top as shown and delete the later duplicate; do not introduce a second local with the same name (it will not compile).

In the outer catch (`:409`):

```java
    } catch (Exception e) {
      logger.error("Error initiating AI chat", e);
      recordSetupFailure(null, username, feature, e.getClass().getSimpleName(), e.getMessage());
      return Response.serverError()
          .entity(new ErrorResponse("Failed to initiate chat: " + e.getMessage()))
          .type(MediaType.APPLICATION_JSON)
          .build();
    }
```

In `AiConfigResources.testConfig`, wrap the `validateConfig` call so the billable probe is recorded. Time it and log via `AiEventLogger` directly (this class has no `recordEvent`):

```java
    long start = System.currentTimeMillis();
    Exception failure = null;
    try {
      // ... existing provider.validateConfig(...) call and result handling
    } catch (Exception e) {
      failure = e;
      throw e;
    } finally {
      try {
        AiEvent event = new AiEvent();
        event.ts = AiEventLogger.nowIso();
        event.user = username;
        event.feature = "config_test";
        event.source = "server";
        event.provider = config != null ? config.getProvider() : null;
        event.model = config != null ? config.getModel() : null;
        event.durationMs = System.currentTimeMillis() - start;
        event.success = failure == null;
        if (failure != null) {
          event.errorClass = failure.getClass().getSimpleName();
          event.error = failure.getMessage();
        }
        AiEventLogger.log(event);
      } catch (Exception logErr) {
        logger.warn("Failed to record AI config test event", logErr);
      }
    }
```

Adapt `username` to however `testConfig` obtains its `SecurityContext`; if it has none, pass `null`.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -o test -pl exec/java-exec -Dtest=ProspectorEventTest -DfailIfNoTests=false`
Expected: PASS — 9 tests.

Run: `mvn -o checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java \
        exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/AiConfigResources.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProspectorEventTest.java
git commit -m "Record AI setup failures and config-test calls

The chat guards and outer catch returned before any event was written, so a
misconfigured Prospector looked identical to an unused one. /ai/config/test
issues a real billable probe and was recorded nowhere."
```

---

### Task 6: Make the dashboard honest

Two dashboard defects. `isReady()` gates `/summary` and `/events` and yields **HTTP 200 with empty arrays** when `DRILL_LOG_DIR` is unset or `ai-events.log` is absent — indistinguishable from "no AI usage". It also stats only the active file, so once logs roll, the dashboard reports zero while the read glob (`ai-events*.log`) would still match archives.

And `totalCalls` is a bare `COUNT(*)` while failures exclude cancellations, so `successCount + failureCount != totalCalls` whenever a user cancels — which reads as missing data.

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/AiAnalyticsResources.java` (`isReady` ~line 436; `/summary` ~line 224; `/events` ~line 315)
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/AiAnalyticsPage.tsx` (surface `notConfigured` and `cancelledCount`)
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/AiAnalyticsReadinessTest.java` (create)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `/summary` and `/events` payloads gain `notConfigured: boolean`; `/summary` gains `cancelledCount: long`.

**Do not** make `totalCalls` exclude cancelled rows. A cancelled call can still have consumed tokens upstream, so suppressing it would hide real usage and under-report cost — the opposite of this plan's goal. Report the count instead, so `success + failure + cancelled == total` holds.

- [ ] **Step 1: Write the failing test**

Create `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/AiAnalyticsReadinessTest.java` with the standard Apache header, then:

```java
package org.apache.drill.exec.server.rest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dashboard must distinguish "not configured" from "no usage", and must not
 * report zero merely because the active log has rolled into an archive.
 */
public class AiAnalyticsReadinessTest {

  @Test
  public void testEventLogPresentWhenActiveFileExists(@TempDir Path dir) throws IOException {
    new File(dir.toFile(), "ai-events.log").createNewFile();
    assertTrue(AiAnalyticsResources.hasEventLog(dir.toString()));
  }

  @Test
  public void testEventLogPresentWhenOnlyArchivesRemain(@TempDir Path dir) throws IOException {
    new File(dir.toFile(), "ai-events-2026-07-01.0.log").createNewFile();
    assertTrue(AiAnalyticsResources.hasEventLog(dir.toString()));
  }

  @Test
  public void testNoEventLogWhenDirectoryEmpty(@TempDir Path dir) {
    assertFalse(AiAnalyticsResources.hasEventLog(dir.toString()));
  }

  @Test
  public void testNoEventLogWhenDirectoryMissing() {
    assertFalse(AiAnalyticsResources.hasEventLog(null));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o test -pl exec/java-exec -Dtest=AiAnalyticsReadinessTest -DfailIfNoTests=false`
Expected: FAIL — compilation error, `hasEventLog` does not exist.

- [ ] **Step 3: Write minimal implementation**

Extract the file check from `isReady()` into a testable static that matches the read glob rather than only the active file:

```java
  /**
   * True when the log directory holds any AI event log, including rolled
   * archives. Mirrors the ai-events*.log glob used when querying.
   */
  static boolean hasEventLog(String logDir) {
    if (logDir == null) {
      return false;
    }
    File dir = new File(logDir);
    if (!dir.isDirectory()) {
      return false;
    }
    File[] matches = dir.listFiles((d, name) ->
        name.startsWith("ai-events") && name.endsWith(".log"));
    return matches != null && matches.length > 0;
  }
```

and use it:

```java
  private boolean isReady() {
    String logDir = System.getenv("DRILL_LOG_DIR");
    if (!hasEventLog(logDir)) {
      return false;
    }
    // ... unchanged workspace/format check
  }
```

In `/summary` and `/events`, when `isReady()` is false, return the existing empty payload **plus** `notConfigured: true`; when ready, set `notConfigured: false`. In `/summary`, add `cancelledCount` using the same predicate the failure clause uses, inverted:

```sql
SUM(CASE WHEN cancelled = true THEN 1 ELSE 0 END) AS cancelledCount
```

In `AiAnalyticsPage.tsx`, when `notConfigured` is true render an explicit "AI analytics is not configured — run Setup" state instead of empty charts, and display `cancelledCount` alongside success/failure.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -o test -pl exec/java-exec -Dtest=AiAnalyticsReadinessTest -DfailIfNoTests=false`
Expected: PASS — 4 tests.

Run: `mvn -o checkstyle:check -pl exec/java-exec` and `cd exec/java-exec/src/main/resources/webapp && npx tsc --noEmit`
Expected: BUILD SUCCESS / exit 0.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/AiAnalyticsResources.java \
        exec/java-exec/src/main/resources/webapp/src/pages/AiAnalyticsPage.tsx \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/AiAnalyticsReadinessTest.java
git commit -m "Distinguish unconfigured AI analytics from no usage

isReady returned empty 200s that read as zero usage, and stat'd only the
active log so rolled archives reported nothing. Also report cancelledCount so
success + failure + cancelled reconciles with totalCalls."
```

---

### Task 7: Document the feature vocabulary

**Files:**
- Modify: `docs/dev/ui/pages/ai-analytics.md` (line 43 lists a `format_sql` label that no code emits)
- Modify: `docs/dev/PROSPECTOR.md` (document that callers must tag `feature`)

**Interfaces:**
- Consumes: the vocabulary from Task 3.
- Produces: documentation only.

- [ ] **Step 1: Update the analytics page doc**

In `docs/dev/ui/pages/ai-analytics.md`, remove `format_sql` and replace the feature list with the thirteen values from Task 3's table. Add a note that events are recorded server-side only, that `source` is always `server`, and that `success + failure + cancelled == totalCalls`.

- [ ] **Step 2: Update the Prospector doc**

In `docs/dev/PROSPECTOR.md`, add to the chat endpoint section:

> `context.feature` tags which UI feature originated the call and drives the
> analytics dashboard's per-feature breakdown. Every caller must set it; calls
> without one are recorded as `prospector_chat`. Do not log AI calls from the
> browser — the server records every call to `/api/v1/ai/chat` with real token
> counts.

- [ ] **Step 3: Verify no stale references remain**

Run: `grep -rn "format_sql\|aiObservability\|ai/logs" docs/`
Expected: no matches.

- [ ] **Step 4: Commit**

```bash
git add docs/dev/ui/pages/ai-analytics.md docs/dev/PROSPECTOR.md
git commit -m "Document the AI analytics feature vocabulary"
```

---

## Verification

After all tasks, confirm end to end — compile/typecheck do not prove the events land:

1. Build and start a Drillbit with `DRILL_LOG_DIR` set and Prospector configured.
2. Open the AI Analytics dashboard, run Setup if prompted.
3. From SQL Lab, ask Prospector a question. Confirm **one** new event with feature `sql_lab_chat` and non-zero, non-estimated tokens.
4. Run Explain Query. Confirm **one** event with feature `explain_query` — not two.
5. Disable Prospector, attempt a chat, confirm an event with `errorClass=ProspectorDisabled`.
6. Unset `DRILL_LOG_DIR`, reload the dashboard, confirm it says "not configured" rather than showing zeros.
