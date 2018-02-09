---
title: "Drill Plan Syntax"
date: 2018-02-09 00:15:59 UTC
parent: "Design Docs"
---
## Whats the plan?

This section is about the end-to-end plan flow for Drill. The incoming query
to Drill can be a SQL 2003 query/DrQL or MongoQL. The query is converted to a
_Logical Plan_ that is a Drill's internal representation of the query
(language-agnostic). Drill then uses its optimization rules over the Logical
Plan to optimize it for best performance and crafts out a _Physical Plan_. The
Physical Plan is the actual plan the Drill then executes for the final data
processing. Below is a diagram to illustrate the flow:

![drill query flow]({{ site.baseurl }}/docs/img/slide-15-638.png)

**The Logical Plan** describes the abstract data flow of a language independent query i.e. it would be a representation of the input query which would not be dependent on the actual input query language. It generally tries to work with primitive operations without focus on optimization. This makes it more verbose than traditional query languages. This is to allow a substantial level of flexibility in defining higher-level query language features. It would be forwarded to the optimizer to get a physical plan.

**The Physical Plan** is often called the execution plan, since it is the input to the execution engine. Its a description of the physical operations the execution engine will undertake to get the desired result. It is the output of the query planner and is a transformation of the logical plan after applying the optimization rules.

Typically, the physical and execution plans will be represented using the same
JSON format as the logical plan.

**Detailed document**: Here is a document that explains the Drill logical & physical plans in full detail. [Drill detailed plan syntax document](https://docs.google.com/document/d/1QTL8warUYS2KjldQrGUse7zp8eA72VKtLOHwfXy6c7I/edit).

