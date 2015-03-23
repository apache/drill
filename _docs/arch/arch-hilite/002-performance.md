---
title: "Performance"
parent: "Architectural Highlights"
---
[Previous](/docs/flexibility)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Back to Table of Contents](/docs)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Next](/docs/apache-drill-tutorial)

Drill is designed from the ground up for high performance on large datasets.
The following core elements of Drill processing are responsible for Drill's
performance:

**_Distributed engine_**

Drill provides a powerful distributed execution engine for processing queries.
Users can submit requests to any node in the cluster. You can simply add new
nodes to the cluster to scale for larger volumes of data, support more users
or to improve performance.

**_Columnar execution_**

Drill optimizes for both columnar storage and execution by using an in-memory
data model that is hierarchical and columnar. When working with data stored in
columnar formats such as Parquet, Drill avoids disk access for columns that
are not involved in an analytic query. Drill also provides an execution layer
that performs SQL processing directly on columnar data without row
materialization. The combination of optimizations for columnar storage and
direct columnar execution significantly lowers memory footprints and provides
faster execution of BI/Analytic type of workloads.

**_Vectorization_**

Rather than operating on single values from a single table record at one time,
vectorization in Drill allows the CPU to operate on vectors, referred to as a
Record Batches. Record Batches are arrays of values from many different
records. The technical basis for efficiency of vectorized processing is modern
chip technology with deep-pipelined CPU designs. Keeping all pipelines full to
achieve efficiency near peak performance is something impossible to achieve in
traditional database engines, primarily due to code complexity.

**_Runtime compilation_**

Runtime compilation is faster compared to the interpreted execution. Drill
generates highly efficient custom code for every single query for every single
operator. Here is a quick overview of the Drill compilation/code generation
process at a glance.

![drill compiler]({{ site.baseurl }}/docs/img/58.png)

**Optimistic and pipelined query execution**

Drill adopts an optimistic execution model to process queries. Drill assumes
that failures are infrequent within the short span of a query and therefore
does not spend time creating boundaries or checkpoints to minimize recovery
time. Failures at node level are handled gracefully. In the instance of a
single query failure, the query is rerun. Drill execution uses a pipeline
model where all tasks are scheduled at once. The query execution happens in-
memory as much as possible to move data through task pipelines, persisting to
disk only if there is memory overflow.