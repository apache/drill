---
title: "Nested Data Functions"
parent: "SQL Reference"
---
This section contains descriptions of SQL functions that you can use to
analyze nested data:

  * [FLATTEN Function]({{ site.baseurl }}/docs/flatten)
  * [KVGEN Function]({{ site.baseurl }}/docs/kvgen)
  * [REPEATED_COUNT Function]({{ site.baseurl }}/docs/repeated-count)
  * [REPEATED_CONTAINS Function]({{ site.baseurl }}/docs/repeated-contains)

## Limitations
Map, Array, or repeated scalar types should not be used in GROUP BY or ORDER BY clauses or in a comparison operator. Drill does not support comparisons between VARCHAR:REPEATED and VARCHAR:REPEATED.