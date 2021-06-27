---
title: "为什么选择 Drill"
slug: "Why Drill"
parent: "新手开始"
lang: "zh"
---

## 选择 Apache Drill 的十大理由

## 1. 分钟级的上手速度

几分钟即可入门 Apache Drill。您仅需要在 Linux、Mac 或 Windows笔记本上解压发行程序，使用本地文件进行即可查询。不需要设置任何的基础服务，也不用事先定义表对象（下文称 Schema）。通过SQL直接指向数据位置进行查询：

    $ tar -xvf apache-drill-<version>.tar.gz
    $ <install directory>/bin/drill-embedded
    0: jdbc:drill:zk=local> SELECT * FROM cp.`employee.json` LIMIT 5;
    |--------------|----------------------------|---------------------|---------------|--------------|----------------------------|-----------|----------------|-------------|------------------------|----------|----------------|----------------------|-----------------|---------|-----------------------|
    | employee_id  |         full_name          |     first_name      |   last_name   | position_id  |       position_title       | store_id  | department_id  | birth_date  |       hire_date        |  salary  | supervisor_id  |   education_level    | marital_status  | gender  |    management_role    |
    |--------------|----------------------------|---------------------|---------------|--------------|----------------------------|-----------|----------------|-------------|------------------------|----------|----------------|----------------------|-----------------|---------|-----------------------|
    | 1            | Sheri Nowmer               | Sheri               | Nowmer        | 1            | President                  | 0         | 1              | 1961-08-26  | 1994-12-01 00:00:00.0  | 80000.0  | 0              | Graduate Degree      | S               | F       | Senior Management     |
    | 2            | Derrick Whelply            | Derrick             | Whelply       | 2            | VP Country Manager         | 0         | 1              | 1915-07-03  | 1994-12-01 00:00:00.0  | 40000.0  | 1              | Graduate Degree      | M               | M       | Senior Management     |
    | 4            | Michael Spence             | Michael             | Spence        | 2            | VP Country Manager         | 0         | 1              | 1969-06-20  | 1998-01-01 00:00:00.0  | 40000.0  | 1              | Graduate Degree      | S               | M       | Senior Management     |
    | 5            | Maya Gutierrez             | Maya                | Gutierrez     | 2            | VP Country Manager         | 0         | 1              | 1951-05-10  | 1998-01-01 00:00:00.0  | 35000.0  | 1              | Bachelors Degree     | M               | F       | Senior Management     |


## 2. 无模式（Schema-free）JSON 模型
Drill是世界上第一个也是唯一一个不要求Schema的分布式SQL引擎。它与 MongoDB 和 ElasticSearch 共享相同的 JSON 模型。不用事先创建Schema和依赖ETL工具，因为Drill能够理解其中的数据结构（模式发现特性）。

## 3. 原地查询复杂的，半结构化数据
基于Drill的无模式特性，您可以原地查询复杂的，半结构化数据，无需在执行查询前展平（Flatten）或转换（ETL）数据内容。Drill 还为 SQL 提供了灵活的扩展来处理嵌套数据结构。这里有一个示例，通过一个简单的SQL来查询JSON文件中的嵌套元素和数组：

    SELECT * FROM (SELECT t.trans_id,
                          t.trans_info.prod_id[0] AS prod_id,
                          t.trans_info.purch_flag AS purchased
                   FROM `clicks/clicks.json` t) sq
    WHERE sq.prod_id BETWEEN 700 AND 750 AND
          sq.purchased = 'true'
    ORDER BY sq.prod_id;


## 4. 真正的SQL，非 “类SQL”
Drill支持标准的SQL语法（SQL 2003）。您不需要再学习新的“类SQL”语言，也不用为“半功能”的BI工具而生气。Drill不仅支持丰富的数据类型，如 DATE, INTERVAL, TIMESTAMP, 和 VARCHAR等，还支持复杂的查询语法，如 关联子查询和WHERE子句连接，这里有一个示例，在Drill中运行 TPC-H 标准查询：

### TPC-H 第四个语句

    SELECT  o.o_orderpriority, COUNT(*) AS order_count
    FROM orders o
    WHERE o.o_orderdate >= DATE '1996-10-01'
          AND o.o_orderdate < DATE '1996-10-01' + INTERVAL '3' month
          AND EXISTS(
                     SELECT * FROM lineitem l
                     WHERE l.l_orderkey = o.o_orderkey
                     AND l.l_commitdate < l.l_receiptdate
                     )
          GROUP BY o.o_orderpriority
          ORDER BY o.o_orderpriority;

## 5. 支持标准的BI工具
Drill能够在标准的BI工具中运行。您可以使用现有的工具，如 Tableau, MicroStrategy, QlikView 和 Excel。Drill提供JDBC，但是不直接提供ODBC驱动。

## 6. 交互式查询Hive表
Drill可以充分利用Hive中存在的资源。您可以使用Drill在Hive表上运行交互式查询和分析，并且能够访问Hive的所有输入和输出格式（包含自定义 SerDes）。不仅可以连接不同的Hive元存储所包含的表，还可以将异构数据源的表进行连接（联邦查询特性），比如将Hive表关联（Join）Hbase表或文件系统中的日志目录等。这里有一个示例，使用Drill查询Hive表：

    SELECT `month`, state, sum(order_total) AS sales
    FROM hive.orders
    GROUP BY `month`, state
    ORDER BY 3 DESC LIMIT 5;


## 7. 联邦查询
Drill天生是为可扩展而设计。您可以开箱即用地连接文件系统（本地或分布式存储，如 S3或HDFS），HBase 和 Hive。当然，您也可以实现一个自定义的存储或数据格式插件来连接任意的数据源类型。Drill能够在单个查询中动态组合多个数据源（联邦查询），且不需要中心化的元存储。这里有一个示例，将Hive表，HBase表（视图）和JSON文件进行组合查询：

    SELECT custview.membership, sum(orders.order_total) AS sales
    FROM hive.orders, custview, dfs.`clicks/clicks.json` c
    WHERE orders.cust_id = custview.cust_id AND orders.cust_id = c.user_info.cust_id
    GROUP BY custview.membership
    ORDER BY 2;

## 8. 用户自定义函数（UDFs）
Drill提供了简单的，高性能的 Java API 构建 [用户自定义函数 UDFs]({{ site.baseurl }}/docs/develop-custom-functions/)，所以允许在Drill中添加您的业务逻辑。Drill还支持 Hive UDFs，如果您在Hive中创建了UDFs，那么可以在Drill中直接使用它们而无需修改。


## 9. 高性能分析
Drill专为高吞吐和低延迟而设计。它不使用 MapReduce、Tez 和 Spark 等类似的通用型计算框架。所以，它更灵活（支持无模式 JSON 模型）和更高性能。Drill的优化器会利用基于规则（RBO）和成本（CBO）技术，以及数据局部性（Data Locality）和算子下推（Pushdown），自动将查询片段下推到后端数据源。不仅于此，Drill提供了列式数据和矢量化执行引擎，进一步提升了内存利用率和CPU运行效率。

## 10. 水平扩展（从一台笔记本到一千多个节点）
Drill可以很方便地下载和部署，即使您用的是笔记本也如此。当有更大的数据集需要分析时，也可以快速在Hadoop集群上来部署（支持多达1000多个节点）。Drill会利用集群的聚合内存在高效的流水线模型下执行查询。当内存不足时，Drill会自动溢写到磁盘上。重要的是，Drill操作的数据不论是在内存中还是在磁盘上，数据结构完全一致，减少了大量的序列化和反序列化时间。
