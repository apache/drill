# Machine learning and AI summaries

Drill has two related features for this: the `predict()` SQL function (the `contrib/ml` module), which scores rows against a PMML model, and the `ai_summary` workflow action, which asks the configured Prospector LLM to summarise a scheduled query run.

## Training a model

Training happens outside Drill. Train in the SQL Lab notebook (see [Notebooks.md](Notebooks.md)), or anywhere else, and export the fitted pipeline to PMML.

From the notebook, use `nyoka`. It is pure Python and needs only `lxml`, and it runs under the notebook's Pyodide runtime with scikit-learn 1.4.2. `sklearn2pmml` does not work there, because it shells out to a bundled Java jar at runtime.

Write the resulting `.pmml` file into a Drill workspace (the notebook can write files back to a workspace, see Notebooks.md), or copy it there by any other means.

PMML covers trees, ensembles, regression and clustering well. It covers time series poorly, so forecasting is not a good fit for this path.

## `predict()`

```sql
SELECT predict('dfs.models.`churn.pmml`', tenure, monthly_charges, contract) AS p
FROM dfs.data.`customers.parquet`;
```

Syntax: `predict('<plugin>.<workspace>.<path>', feature1, feature2, ...)`.

* The first argument must be a constant.
* The result is a map holding the prediction plus the probability fields the model defines. Values are plain `Double` and `String`.
* The model is parsed once per fragment and cached. Each call site caches independently.
* Models load through the Hadoop FileSystem API, so a model held in S3 works like one on local disk.

### Feature types

A feature argument may be `VARCHAR`, `VARDECIMAL`, `FLOAT4`, `FLOAT8`, `INT` or `BIGINT`. Anything else fails the query, naming the column and its type. `VARCHAR` reaches the model as a string; every numeric type reaches it as a double.

A NULL feature is passed to the model as a missing value, so the model's own missing-value handling decides what the prediction is. One missing value does not fail the query.

On the way out, `mapFields` writes `Double` values as `FLOAT8` and `String` values as `VARCHAR`. A prediction field of any other type is skipped, so it does not appear in the result map at all.

### Where models may live, and who reads them

Model paths accept storage-plugin locations only. `http` and `https` URLs are refused as the first argument, so a model is never fetched from a URL a query names.

The resolved path is confined to its workspace: paths that resolve outside the workspace are refused, whether they get there through `..` segments or by being absolute. An absolute path that lands *inside* the workspace is accepted. The confinement is lexical only. It compares resolved path strings and does not resolve symlinks, so a symlink inside the workspace that points outside it is followed.

The model file is read as the query user when `drill.exec.impersonation.enabled` is on, so `predict()` cannot reach a file the user could not select directly. With impersonation off, the read happens as the Drillbit process user, which is the same identity any other query on that cluster reads files with.

Model file *content* is screened before it is parsed. pmml4s parses PMML with a StAX factory it builds itself, with default settings and no hook to harden; on Drill's classpath that factory resolves external entities and external DTD subsets. A model file carrying a `<!DOCTYPE` could therefore read local files, probe for the existence of absolute paths through the resulting error messages, and make the Drillbit fetch a URL chosen by whoever wrote the file. Since the file-upload feature puts arbitrary user bytes into a `dfs` workspace by design, that is the expected input. So a model whose bytes contain a `<!DOCTYPE` declaration is refused outright, with no parsing attempted; PMML has no legitimate use for a DTD. With no DOCTYPE accepted, there is no server-side request forgery surface in model content either.

## The `pmml` format plugin

`predict()` takes its features positionally, in the order the model declares them, so the first
question anyone has is "what does this model expect?". The `pmml` format plugin answers it by
making a model file queryable:

```sql
SELECT * FROM dfs.models.`churn.pmml`
```

One row per file, with this schema:

| Column | Type | Contents |
|---|---|---|
| `model_name` | VARCHAR | the model's declared name, null if it has none |
| `model_type` | VARCHAR | the PMML model element, for example `TreeModel` |
| `classification` | BIT | true for a classifier |
| `clustering` | BIT | true for a clusterer |
| `regression` | BIT | true for a regression model |
| `input_fields` | repeated VARCHAR | the active input fields, **in the order `predict()` expects them** |
| `prediction_classes` | repeated VARCHAR | the labels a classifier can emit; empty for regression |
| `input_schema` | repeated map | `field_name` and `data_type` per input field |

Nothing is scored by this plugin. It reads metadata only; scoring is `predict()`.

The `pmml` extension is registered for `dfs` and `cp` out of the box, so no configuration is
needed. The same DOCTYPE screening described above applies here, because the plugin parses with
the same library — a model declaring a DOCTYPE is refused as a read error rather than a function
error. Unlike `predict()`, this plugin reads through Drill's normal scan path, so workspace
resolution and impersonation are handled by the framework.

Model files are XML metadata rather than data, so a file larger than 64 MB is refused rather than
buffered.

## `ai_summary` workflow action

`ai_summary` asks the configured LLM to summarise a run. See [WORKFLOWS.md](WORKFLOWS.md) for the action framework.

| Config | Meaning |
|---|---|
| `instructions` | Optional template for what the summary should cover. |
| `maxChars` | Optional. Maximum summary length, default 2000. |

Output: `summary`, referenced downstream as `{{actions.ai_summary.summary}}`, for example in an `email` action body.

Behaviour:

* It has side effects, so a dry run previews the action and does not call the provider.
* Every call is recorded as an AI usage event under the feature `workflow_ai_summary`, against the workflow's owner, so its tokens and cost show up in `/ai-analytics` like any other provider call.
* It refuses to run when Prospector is unconfigured or disabled.
* It honours the `sendDataToAi` setting. When that setting is off, the prompt carries only `status`, `rowCount` and `durationMs`. The user's own `instructions` are rendered against a blanked context in that case, so `{{rows[*].col}}` and `{{error}}` cannot leak data either. This is a privacy guarantee: with `sendDataToAi` off, no row data or error text reaches the provider.
