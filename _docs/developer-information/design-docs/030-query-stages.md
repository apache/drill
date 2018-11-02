---
title: "Query Stages"
date: 2018-11-02
parent: "Design Docs"
---
## Overview

Apache Drill is a system for interactive analysis of large-scale datasets. It
was designed to allow users to query across multiple large big data systems
using traditional query technologies such as SQL. It is built as a flexible
framework to support a wide variety of data operations, query languages and
storage engines.

## Query Parsing

A Drillbit is capable of parsing a provided query into a logical plan. In
theory, Drill is capable of parsing a large range of query languages. At
launch, this will likely be restricted to an enhanced SQL2003 language.

## Physical Planning

Once a query is parsed into a logical plan, a Drillbit will then translate the
plan into a physical plan. The physical plan will then be optimized for
performance. Since plan optimization can be computationally intensive, a
distributed in-memory cache will provide LRU retrieval of previously generated
optimized plans to speed query execution.

## Execution Planning

Once a physical plan is generated, the physical plan is then rendered into a
set of detailed executional plan fragments (EPFs). This rendering is based on
available resources, cluster load, query priority and detailed information
about data distribution. In the case of large clusters, a subset of nodes will
be responsible for rendering the EPFs. Shared state will be managed through
the use of a distributed in-memory cache.

## Execution Operation

Query execution starts with each Drillbit being provided with one or more EPFs
associated with query execution. A portion of these EPFs may be identified as
initial EPFs and thus they are executed immediately. Other EPFs are executed
as data flows into them.

