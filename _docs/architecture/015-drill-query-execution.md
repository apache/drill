---
title: "Drill Query Execution"
date: 2018-12-08
parent: "Architecture"
---

When you submit a Drill query, a client or an application sends the query in the form of an SQL statement to a Drillbit in the Drill cluster. A Drillbit is the process running on each active Drill node that coordinates, plans, and executes queries, as well as distributes query work across the cluster to maximize data locality.

The following image represents the communication between clients, applications, and Drillbits:

![]({{ site.baseurl }}/docs/img/query-flow-client.png)

The Drillbit that receives the query from a client or application becomes the Foreman for the query and drives the entire query. A parser in the Foreman parses the SQL, applying custom rules to convert specific SQL operators into a specific logical operator syntax that Drill understands. This collection of logical operators forms a logical plan. The logical plan describes the work required to generate the query results and defines which data sources and operations to apply.

The Foreman sends the logical plan into a cost-based optimizer to optimize the order of SQL operators in a statement and read the logical plan. The optimizer applies various types of rules to rearrange operators and functions into an optimal plan. The optimizer converts the logical plan into a physical plan that describes how to execute the query.

![]({{ site.baseurl }}/docs/img/client-phys-plan.png)

A parallelizer in the Foreman transforms the physical plan into multiple phases, called major and minor fragments. These fragments create a multi-level execution tree that rewrites the query and executes it in parallel against the configured data sources, sending the results back to the client or application.

![]({{ site.baseurl }}/docs/img/execution-tree.PNG)  


## Major Fragments
A major fragment is a concept that represents a phase of the query execution. A phase can consist of one or multiple operations that Drill must perform to execute the query. Drill assigns each major fragment a MajorFragmentID.

For example, to perform a hash aggregation of two files, Drill may create a plan with two major phases (major fragments) where the first phase is dedicated to scanning the two files and the second phase is dedicated to the aggregation of the data.  

![]({{ site.baseurl }}/docs/img/ex-operator.png)

Drill uses an exchange operator to separate major fragments. An exchange is a change in data location and/or parallelization of the physical plan. An exchange is composed of a sender and a receiver to allow data to move between nodes. 

Major fragments do not actually perform any query tasks. Each major fragment is divided into one or multiple minor fragments (discussed in the next section) that actually execute the operations required to complete the query and return results back to the client.

You can work with major fragments within the physical plan by capturing a JSON representation of the plan in a file, manually modifying it, and then submitting it back to Drill using the SUBMIT PLAN command. You can also view major fragments in the query profile, which is visible in the Drill Web UI. See [EXPLAIN ]({{ site.baseurl }}/docs/explain/)and [Query Profiles]({{ site.baseurl }}/docs/query-profiles/) for more information.

## Minor Fragments
Each major fragment is parallelized into minor fragments. A minor fragment is a logical unit of work that runs inside a thread. A logical unit of work in Drill is also referred to as a slice. The execution plan that Drill creates is composed of minor fragments. Drill assigns each minor fragment a MinorFragmentID.  

![]({{ site.baseurl }}/docs/img/min-frag.png)

The parallelizer in the Foreman creates one or more minor fragments from a major fragment at execution time, by breaking a major fragment into as many minor fragments as it can usefully run at the same time on the cluster.

Drill executes each minor fragment in its own thread as quickly as possible based on its upstream data requirements. Drill schedules the minor fragments on nodes with data locality. Otherwise, Drill schedules them in a round-robin fashion on the existing, available Drillbits.

Minor fragments contain one or more relational operators. An operator performs a relational operation, such as scan, filter, join, or group by. Each operator has a particular operator type and an OperatorID. Each OperatorID defines its relationship within the minor fragment to which it belongs. See [Physical Operators]({{ site.baseurl }}/docs/physical-operators/).

![]({{ site.baseurl }}/docs/img/operators.png)

For example, when performing a hash aggregation of two files, Drill breaks the first phase dedicated to scanning into two minor fragments. Each minor fragment contains scan operators that scan the files. Drill breaks the second phase dedicated to aggregation into four minor fragments. Each of the four minor fragments contain hash aggregate operators that perform the hash  aggregation operations on the data. 

You cannot modify the number of minor fragments within the execution plan. However, you can view the query profile in the Drill Web UI and modify some configuration options that change the behavior of minor fragments, such as the maximum number of slices. See [Configuration Options]({{ site.baseurl }}/docs/configuration-options-introduction/).

### Execution of Minor Fragments
Minor fragments can run as root, intermediate, or leaf fragments. An execution tree contains only one root fragment. The coordinates of the execution tree are numbered from the root, with the root being zero. Data flows downstream from the leaf fragments to the root fragment.
 
The root fragment runs in the Foreman and receives incoming queries, reads metadata from tables, rewrites the queries and routes them to the next level in the serving tree. The other fragments become intermediate or leaf fragments.  

Intermediate fragments start work when data is available or fed to them from other fragments. They perform operations on the data and then send the data downstream. They also pass the aggregated results to the root fragment, which performs further aggregation and provides the query results to the client or application.

The leaf fragments scan tables in parallel and communicate with the storage layer or access data on local disk. The leaf fragments pass partial results to the intermediate fragments, which perform parallel operations on intermediate results.  

![]({{ site.baseurl }}/docs/img/leaf-frag.png)    

Drill only plans queries that have concurrent running fragments. For example, if 20 available slices exist in the cluster, Drill plans a query that runs no more than 20 minor fragments in a particular major fragment. Drill is optimistic and assumes that it can complete all of the work in parallel. All minor fragments for a particular major fragment start at the same time based on their upstream data dependency.

