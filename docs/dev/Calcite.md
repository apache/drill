# Drill-specific commits in Apache Calcite

Currently, Drill uses Apache Calcite with additional changes, required for Drill. All the commits were left after 
update from Calcite `1.4.0` to Calcite `1.15.0` (3 commits) and from Calcite `1.18.0` to Calcite `1.20.0` (1 commit) and weren't merged to the Calcite's master yet since there is no consensus on them in Calcite community.

List of Jiras with Drill-specific commits:

|Jira|Summary|The reason why it wasn't merged|
|----|-------|-------------------------------|
|[CALCITE-2018](https://issues.apache.org/jira/browse/CALCITE-2018)|Queries failed with AssertionError: rel has lower cost than best cost of subset|Pull request with the fix was created ([PR-552](https://github.com/apache/calcite/pull/552)), but [CALCITE-2166](https://issues.apache.org/jira/browse/CALCITE-2166) which blocks it was found and is not resolved yet.|
|[CALCITE-2087](https://issues.apache.org/jira/browse/CALCITE-2087)|Add new method to ViewExpander interface to allow passing SchemaPlus.|Pull request into Apache Calcite was created, but it was declined. See conversation in Jira.|
|[CALCITE-1178](https://issues.apache.org/jira/browse/CALCITE-1178)|Allow SqlBetweenOperator to compare DATE and TIMESTAMP|SQL spec does not allow to compare datetime types if they have different `<primary datetime field>`s. Therefore Calcite community won’t accept these changes. Similar issues were reported in [CALCITE-2829](https://issues.apache.org/jira/browse/CALCITE-2829) and in [CALCITE-2745](https://issues.apache.org/jira/browse/CALCITE-2745).|
|[CALCITE-3121](https://issues.apache.org/jira/browse/CALCITE-3121)|VolcanoPlanner hangs due to removing ORDER BY from sub-query|Pull request was open to revert changes ([PR-1264](https://github.com/apache/calcite/pull/1264)) which remove ORDER BY clause; it wasn't merged, because aforementioned changes only unveiled the issue and no proper solution is available yet.|

# Drill-Calcite repository

Repository with source code is placed [here](https://github.com/vvysotskyi/drill-calcite). For backward 
compatibility, a couple of previous Drill Calcite branches were pushed into this repo.

Drill committers who need write permissions to the repository, should notify its owner.

# Process of updating Calcite version

- Push required changes to the existing branch, or create new branch. *Though this repository contains Drill specific commits, it is forbidden to add additional specific commits which were not merged to Apache Calcite master if it wasn't discussed in Drill community first.*
- The last commit must be a commit which updates the version number.
- Create and push tag for commit with the version update. Tag name should match the version, for example, `1.18.0-drill-r0`, `1.18.0-drill-r1`, `1.18.0-drill-r2` and so on.
- Bump-up Drill Calcite version in Drill pom.xml file.