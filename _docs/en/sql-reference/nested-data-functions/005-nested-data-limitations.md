---
title: "Nested Data Limitations"
slug: "Nested Data Limitations"
parent: "Nested Data Functions"
---
Do not use Map, Array, and repeated scalar types in GROUP BY or ORDER BY clauses or in a comparison operator. Drill does not support comparisons between VARCHAR:REPEATED and VARCHAR:REPEATED.
