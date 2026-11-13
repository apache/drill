# Machine learning and AI summaries

This branch adds two related features: the `predict()` SQL function (the `contrib/ml` module), which scores rows against a PMML model, and the `ai_summary` workflow action, which asks the configured Prospector LLM to summarise a scheduled query run.

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

### Where models may live

Model paths accept storage-plugin locations only. `http` and `https` URLs are refused by design: fetching a model over HTTP from the Drillbit would be a server-side request forgery surface reachable from any query.

The resolved path is confined to its workspace. `..` segments and absolute paths are refused.

## `ai_summary` workflow action

`ai_summary` asks the configured LLM to summarise a run. See [WORKFLOWS.md](WORKFLOWS.md) for the action framework.

| Config | Meaning |
|---|---|
| `instructions` | Optional template for what the summary should cover. |
| `maxChars` | Optional. Maximum summary length, default 2000. |

Output: `summary`, referenced downstream as `{{actions.ai_summary.summary}}`, for example in an `email` action body.

Behaviour:

* It has side effects, so a dry run previews the action and does not call the provider.
* It refuses to run when Prospector is unconfigured or disabled.
* It honours the `sendDataToAi` setting. When that setting is off, the prompt carries only `status`, `rowCount` and `durationMs`. The user's own `instructions` are rendered against a blanked context in that case, so `{{rows[*].col}}` and `{{error}}` cannot leak data either. This is a privacy guarantee: with `sendDataToAi` off, no row data or error text reaches the provider.
