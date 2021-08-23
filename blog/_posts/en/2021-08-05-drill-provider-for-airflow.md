---
layout: post
title: "Drill provider for Airflow"
code: drill-provider-for-airflow
excerpt: In its provider package release this month, the Apache Airflow project added a provider for interacting with Apache Drill.  This allows data engineers and data scientists to incorporate Drill queries in their Airflow DAGs, enabling the automation of big data and data science workflows.

authors: ["jturton"]
---

You're building a new report, visualisation or ML model.  Most of the data involved comes from sources well known to you but a new source has become available, allowing your team to measure and model new variables.  Eager to get to a prototype and an early sense of what the new analytics look like, you head straight for the first order of business and start to construct a first version of the dataset upon which your final output will be based.

The data sources you need to combine are immediately accessible but heteregenous: transactional data in PostgreSQL must be combined with data from another team that uses Splunk, lookup data maintained by operational team in an Excel spreadsheet, thousands of XML exports received from a partner and some Parquet files already in your big data environment just for good measure.

Using Drill iteratively you query and join in each data source one at a time, applying grouping, filtering and other intensive transformations as you go, finally producing a dataset with the fields and grain you need.  You store it by adding CREATE TABLE AS in front of your final SELECT then write a few counting and summing queries against the original data sources and your transformed dataset to check that your code produces the expected outputs.

Apart from possibly configuring some new storage plugins in the Drill web UI, you have so far not left your SQL editor.  The onerous data exploration and plumbing parts of your project have flashed by in a blaze of SQL, and you move your dataset into the next tool for visualisation or modelling.  The results are good and you know that your users will immediately ask for the outputs to incorporate new data on a regular schedule.

While Drill can assemble your dataset on the fly, as it did while you prototyped,  doing that for the full set takes over 40 minutes, places more load than you'd like in office hours on to your data sources and limits you to the history that the sources keep, in some cases only a few weeks.

It's time for ETL, you concede.  In the past that meant you had to choose between keeping your working Drill SQL and scheduling it using 70s Unix tools like Cron and Bash, seeing what you could jury-rig using some ETL software and interfaces to Drill like like ODBC and JDBC, or recreating your Drill SQL entirely in (perhaps multiple) other tools and languages, perhaps Spark and Python.  But this time you can do things differently...

[Apache Airflow](https://airflow.apache.org) is a workflow engine built in the Python programming ecosystem that has grown into a leading choice for orchestrating big data pipelines, amongst its other applications.  Perhaps the first point to understand about Airflow in the context of ETL is that it is designed only for workflow _control_, and not for data flow.  This makes it different from some of the ETL tools you might have encountered like Microsoft's SSIS or Pentaho's PDI which handle both control and data flow: these ETL tools will _themselves_ connect to a data source and stream data records from it.

In contrast Airflow is, unless you're doing it wrong, used only to instruct other software like Spark, Beam, PostgreSQL, Bash, Celery, Scikit-learn scripts, Slack, (... the list of providers is long and varied) to kick off actions at scheduled times.  While Airflow does load its schedules from the crontab format, a comparison to cron stops there.  Airflow can resolve and execute complex job DAGs with options for clustering, parallelism, retries, backfilling and performance monitoring.

The exciting news for Drill users is that [a new provider package adding support for Drill](https://pypi.org/project/apache-airflow-providers-apache-drill/) was added to Airflow this month.  This provider is based on the [sqlalchemy-drill package](https://pypi.org/project/sqlalchemy-drill/) which provides Drill connectivity for Python programs.  This means that you can add tasks which execute queries on Drill to your Airflow DAGs without any hacky intermediate shell scripts, or build new Airflow operators that use the Drill hook.

A new tutorial that walks through the development of a simple Airflow DAG that uses the Drill provider [is available here]({{site.baseurl}}/docs/orchestrating-queries-with-airflow/).
