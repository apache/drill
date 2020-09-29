# Drill-Calcite repository

Currently, Drill uses Apache Calcite with additional changes, required for Drill.

Repository with source code is placed [here](https://github.com/vvysotskyi/drill-calcite). For backward 
compatibility, a couple of previous Drill Calcite branches were pushed into this repo.

Drill committers who need write permissions to the repository, should notify its owner.

# Process of updating Calcite version

- Push required changes to the existing branch, or create new branch.
 *Though this repository contains Drill specific commits, it is forbidden to add additional specific commits which
  were not merged to Apache Calcite master if it wasn't discussed in Drill community first.*
- The last commit must be a commit which updates the version number.
- Create and push tag for commit with the version update. Tag name should match the version, for example,
 `1.18.0-drill-r0`, `1.18.0-drill-r1`, `1.18.0-drill-r2` and so on.
- Bump-up Drill Calcite version in Drill pom.xml file.
- Update [wiki](https://github.com/vvysotskyi/drill-calcite/wiki) to contain relevant information after update
 (specific commits, current version, etc.)

For more info about the update process or Drill-specific commits, please refer to
 [drill-calcite wiki](https://github.com/vvysotskyi/drill-calcite/wiki).
