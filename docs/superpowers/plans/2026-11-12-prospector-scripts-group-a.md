# Prospector Scripts — Group A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the no-Python half of the design — an `ai_summary` workflow action, and a PMML scoring UDF so a trained model can be applied in SQL and scheduled with Workflows as they already stand.

**Architecture:** `ai_summary` is one new `ActionType` plus one new method on `WorkflowServices`, the seam the runner already uses for side effects and tests already fake. The scoring half is a new `contrib/ml` Maven module holding a port of `gtkcyber/drill-ml-functions`: a var-arg `predict()` UDF over `pmml4s`, loading models through `DrillFileSystem` so S3 works.

**Tech Stack:** Java 17, Drill 1.23.0-SNAPSHOT, `pmml4s_2.13` 1.5.8, JUnit 4 with `ClusterTest`, Jackson.

**Spec:** [`../specs/2026-11-12-prospector-scripts-design.md`](../specs/2026-11-12-prospector-scripts-design.md)

## Global Constraints

- Apache 2.0 licence header on every new source file.
- `mvn checkstyle:check -pl exec/java-exec` after any `exec/java-exec` change; braces on every `if`, no unused imports.
- Commit messages imperative; do not add Claude as a co-author.
- New developer docs go in `docs/dev/`, and the table in `CLAUDE.md` plus `docs/dev/DevDocs.md` are updated in the same change.
- `pmml4s_2.13` version is `1.5.8`. The upstream repo's `0.9.15` is not used.
- Model loading accepts storage-plugin paths only. The upstream `http(s)` URL path is removed — it is a server-side request forgery surface reachable from any query.
- The upstream PMML *format plugin* (`PMMLFormatPlugin`, `PMMLBatchReader`, `PMMLFormatConfig`) is out of scope and is not ported.
- `drill-udfs` is referenced only by `distribution/pom.xml`, never by `exec/jdbc-all`, so a new contrib module needs no `jdbc-all` exclusions. Confirm this still holds in Task 5 rather than assuming it.

## Review Focus

1. **AI provider unreachable or slow during a scheduled run** — `ai_summary` must fail its own action with a stated reason, leaving the rest of the pipeline's outcome intact; it must not hang the run past the pipeline timeout. Pinned in Task 2.
2. **No AI configuration stored at all** — a workflow carrying `ai_summary` on a cluster where Prospector was never configured must say exactly that, not throw a `NullPointerException`. Pinned in Task 2.
3. **A summary long enough to break downstream actions** — the output is templated into `email` bodies and `record_alert` messages, and run records live in a 1 MB ZooKeeper-backed store. The action caps its own output. Pinned in Task 2.
4. **A model path that is missing, corrupt, or not a file-system plugin** — `predict()` must raise a `UserException` naming the path, not an NPE or a class-cast error. Pinned in Task 3.
5. **Feature count or type disagreeing with the model** — calling `predict()` with the wrong number of features must produce a clear function error rather than a silent wrong prediction. Pinned in Task 4.

---

## File Structure

| File | Responsibility |
|---|---|
| `exec/java-exec/.../workflow/WorkflowServices.java` | gains `summarize(...)`, the seam actions use to reach the LLM |
| `exec/java-exec/.../workflow/WorkflowRunner.java` | implements `summarize` against the configured provider |
| `exec/java-exec/.../rest/AiConfigResources.java` | gains a static accessor for the stored `LlmConfig` |
| `exec/java-exec/.../workflow/AiSummaryAction.java` | new `ActionType` |
| `exec/java-exec/.../workflow/ActionTypeRegistry.java` | registers it |
| `contrib/ml/pom.xml` | new module, `pmml4s` dependency |
| `contrib/ml/.../ml/MLUtils.java` | model loading and prediction mapping |
| `contrib/ml/.../ml/MLFromPMML.java` | the `predict()` UDF |
| `contrib/pom.xml`, `distribution/pom.xml` | module wiring |
| `docs/dev/MachineLearning.md` | the user-facing story for both halves |

---

### Task 1: The summarization seam

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow/WorkflowServices.java`
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow/WorkflowRunner.java`
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/AiConfigResources.java`
- Modify: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/workflow/BuiltInActionsTest.java` (three anonymous `WorkflowServices` fakes gain the new method)

**Interfaces:**
- Produces: `String WorkflowServices.summarize(String prompt, int maxChars) throws Exception` — returns the model's text, never null; throws when no provider is configured or the call fails.
- Produces: `static LlmConfig AiConfigResources.current(PersistentStoreProvider provider)` — returns the stored config under key `default` in store `drill.sqllab.ai_config`, or `null` when unset. Mirrors `UploadConfigResources.current`.

- [ ] **Step 1: Add `summarize` to `WorkflowServices`** as a third abstract method, with the signature above. Update the three anonymous implementations in `BuiltInActionsTest` to return a fixed string, so the existing suite still compiles.

- [ ] **Step 2: Run the workflow tests to confirm they still pass**

Run: `mvn -q test -pl exec/java-exec -Dtest='*Workflow*,BuiltInActionsTest,ActionPipelineTest'`
Expected: PASS — this step only proves the interface change broke nothing.

- [ ] **Step 3: Add `AiConfigResources.current(PersistentStoreProvider)`**, extracting the store lookup the instance method `getStore()` already performs so both paths share it.

- [ ] **Step 4: Implement `summarize` in `WorkflowRunner`.** It resolves the provider through `LlmProviderRegistry` from the stored `LlmConfig`, calls `streamChatCompletion` with a single user `ChatMessage`, an empty tool list, a `ByteArrayOutputStream` and `UsageObserver.NOOP`, and returns `LlmCallResult.getResponseText()` truncated to `maxChars`. When `AiConfigResources.current(...)` returns null it throws with the message `Prospector is not configured; an administrator must set it up before ai_summary can run.`

- [ ] **Step 5: Run checkstyle**

Run: `mvn checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add exec/java-exec/src
git commit -m "Add a summarization seam to workflow services"
```

---

### Task 2: The `ai_summary` action

**Files:**
- Create: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow/AiSummaryAction.java`
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/workflow/ActionTypeRegistry.java:34` (the `defaults()` list)
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/workflow/BuiltInActionsTest.java`

**Interfaces:**
- Consumes: `WorkflowServices.summarize(String, int)` from Task 1.
- Produces: action id `ai_summary`, outputs `Map.of("summary", "string")`, so later actions template `{{actions.ai_summary.summary}}`.

Config schema: one optional `instructions` string with `x-template: true` and `x-multiline: true`, and one optional `maxChars` integer defaulting to **2000**. `hasSideEffects()` is `true` — it calls a paid external provider, so dry runs must preview rather than execute.

`execute` builds a prompt from the rendered `instructions` plus the run's `status`, `rowCount`, `durationMs`, `columns` and up to the first 20 rows of `ctx.run()`, calls `summarize`, and returns the text under `summary`.

- [ ] **Step 1: Write the failing tests** in `BuiltInActionsTest`:

```java
@Test public void aiSummaryReturnsTheProvidersTextUnderSummary()
@Test public void aiSummaryTruncatesToMaxChars()          // maxChars=10, provider returns 50 chars
@Test public void aiSummaryFailsWithAStatedReasonWhenTheProviderThrows()
@Test public void aiSummaryIncludesRowCountAndColumnsInThePrompt()
@Test public void aiSummaryPreviewDoesNotCallTheProvider()  // hasSideEffects() is true
```

Each builds a `WorkflowServices` fake whose `summarize` records its prompt and returns, or throws, as the case requires.

- [ ] **Step 2: Run them to verify they fail**

Run: `mvn -q test -pl exec/java-exec -Dtest=BuiltInActionsTest`
Expected: FAIL — `AiSummaryAction` does not exist.

- [ ] **Step 3: Implement `AiSummaryAction implements ActionType`** in the file above, following `RecordAlertAction` for shape (including its `parse(String)` helper for the schema constant).

- [ ] **Step 4: Register it** in `ActionTypeRegistry.defaults()`, after `EmailAction`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `mvn -q test -pl exec/java-exec -Dtest=BuiltInActionsTest,TestWorkflowResources`
Expected: PASS. `TestWorkflowResources` is included because `GET /action-types` now returns a fourth type.

- [ ] **Step 6: Run checkstyle, then commit**

```bash
mvn checkstyle:check -pl exec/java-exec
git add exec/java-exec/src
git commit -m "Add an AI summary workflow action"
```

---

### Task 3: The `contrib/ml` module and model loading

**Files:**
- Create: `contrib/ml/pom.xml`
- Create: `contrib/ml/src/main/java/org/apache/drill/exec/udfs/ml/MLUtils.java`
- Create: `contrib/ml/src/test/java/org/apache/drill/exec/udfs/ml/TestMLUtils.java`
- Create: `contrib/ml/src/test/resources/models/iris_rf.pmml` (a small classifier, committed as a fixture)
- Modify: `contrib/pom.xml` (add `<module>ml</module>`)

**Interfaces:**
- Produces: `static org.pmml4s.model.Model MLUtils.getModel(String location, DrillbitContext context)` — resolves `<plugin>.<workspace>.<path>` through the registry and `DrillFileSystem`, throws `UserException.functionError()` otherwise.
- Produces: `static Map<String, Object> MLUtils.predict(Model model, FieldReader[] features)`.
- Produces: `static DrillBuf MLUtils.mapFields(Map<String, Object> predictions, ComplexWriter writer, DrillBuf buffer)`.

Port from `github.com/gtkcyber/drill-ml-functions` (`src/main/java/com/datadistillr/ml/udf/MLUtils.java`, 218 lines), renaming the package and dropping `getModelFromURL` and its callers entirely.

- [ ] **Step 1: Write the failing tests** in `TestMLUtils`:

```java
@Test public void loadsAModelFromAFileSystemPlugin()
@Test public void missingModelFileFailsWithThePathInTheMessage()   // UserException, message contains the path
@Test public void aNonFileSystemPluginIsRejected()                 // UserException, not ClassCastException
@Test public void aCorruptModelFileFailsWithTheModelPath()         // feed it a text file
@Test public void anHttpUrlIsRejected()                            // the SSRF path stays removed
```

- [ ] **Step 2: Create the module** — `contrib/ml/pom.xml` with parent `drill-contrib-parent`, artifactId `drill-ml`, dependencies on `drill-java-exec` (provided, with its test-jar for tests) and `org.pmml4s:pmml4s_2.13:1.5.8`; add `<module>ml</module>` to `contrib/pom.xml`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `mvn -q test -pl contrib/ml`
Expected: FAIL — `MLUtils` does not exist.

- [ ] **Step 4: Implement `MLUtils`** with the three signatures above.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `mvn -q test -pl contrib/ml`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add contrib/ml contrib/pom.xml
git commit -m "Add a machine learning contrib module with PMML model loading"
```

---

### Task 4: The `predict()` UDF

**Files:**
- Create: `contrib/ml/src/main/java/org/apache/drill/exec/udfs/ml/MLFromPMML.java`
- Create: `contrib/ml/src/test/java/org/apache/drill/exec/udfs/ml/TestMLFunctions.java`

**Interfaces:**
- Consumes: all three `MLUtils` signatures from Task 3.
- Produces: SQL function `predict(<model path>, <feature>...)`, `FunctionScope.SIMPLE`, `isVarArg = true`, first parameter `@Param(constant = true) VarCharHolder`, remaining `@Param FieldReader[]`, `@Output ComplexWriter`, with `@Inject DrillBuf` and `@Inject DrillbitContext`, and the parsed model held in `@Workspace` so it is parsed once per fragment.

Port from the upstream `MLFromPMML.java` (66 lines), which already has this shape.

- [ ] **Step 1: Write the failing tests** in `TestMLFunctions`, extending `ClusterTest`:

```java
@Test public void predictsAClassFromAPmmlModel()          // asserts the predicted label for a known row
@Test public void returnsProbabilitiesAlongsideThePrediction()
@Test public void theWrongNumberOfFeaturesFailsTheQuery()  // UserException naming the mismatch
@Test public void aMissingModelFailsTheQuery()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `mvn -q test -pl contrib/ml -Dtest=TestMLFunctions`
Expected: FAIL — no `predict` function registered.

- [ ] **Step 3: Implement `MLFromPMML`** with the signature above. A UDF body may only call fully-qualified static methods, as the upstream version does.

- [ ] **Step 4: Run the whole module's tests**

Run: `mvn -q test -pl contrib/ml`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add contrib/ml/src
git commit -m "Add a predict function for PMML models"
```

---

### Task 5: Distribution wiring and documentation

**Files:**
- Modify: `distribution/pom.xml:469-472` (add `org.apache.drill.contrib:drill-ml` beside `drill-udfs`)
- Modify: `distribution/src/assemble/component.xml:63` (add `<include>org.apache.drill.contrib:drill-ml:jar</include>` — contrib jars are listed one by one, so the module ships only if named here)
- Modify: `distribution/src/main/resources/LICENSE` (entries for `drill-ml`, `pmml4s` and `scala-library`, per [`../../dev/LicenseHeadersAndNotices.md`](../../dev/LicenseHeadersAndNotices.md))
- Create: `docs/dev/MachineLearning.md`
- Modify: `docs/dev/DevDocs.md`, `CLAUDE.md`, `docs/dev/WORKFLOWS.md` (the Action types table gains `ai_summary`)

- [ ] **Step 1: Add the module to the distribution** — the pom dependency, the assembly include, and the LICENSE entries — then build it.

Run: `mvn -q -DskipTests install -pl contrib/ml -am && mvn -q -DskipTests package -pl distribution`
Expected: BUILD SUCCESS

- [ ] **Step 2: Confirm `jdbc-all` is unaffected**

Run: `mvn -q verify -pl exec/jdbc-all`
Expected: BUILD SUCCESS. If the enforcer fails on size, `scala-library` and `pmml4s` need exclusions in `exec/jdbc-all/pom.xml`; the expectation from the spec is that it does not, because `jdbc-all` does not depend on contrib UDF modules.

- [ ] **Step 3: Write `docs/dev/MachineLearning.md`** covering: training a model and exporting PMML with `nyoka` from the SQL Lab notebook, where model files live, `predict()` syntax and its output map, the storage-plugin-only model path rule and why the URL form was removed, `ai_summary` configuration and its 2000-character default, and the limits — PMML covers trees, ensembles, regression and clustering well and time series poorly.

- [ ] **Step 4: Add the `ai_summary` row** to the Action types table in `docs/dev/WORKFLOWS.md`, and index the new doc in `docs/dev/DevDocs.md` and the table in `CLAUDE.md`.

- [ ] **Step 5: Commit**

```bash
git add distribution docs CLAUDE.md
git commit -m "Ship the machine learning module and document it"
```

---

## Out of scope for Group A

Stated here so a reviewer does not look for them: the script object, the script editor, the `create_script` Prospector tool, server-side Python, `run_script`, `save_result`, the query-less workflow mode, and script version control. Groups B and C.
