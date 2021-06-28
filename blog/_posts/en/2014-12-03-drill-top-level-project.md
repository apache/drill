---
layout: post
title: "Apache Drill Graduates to a Top-Level Project"
code: drill-top-level-project
excerpt: Drill has graduated to a Top-Level Project at Apache. This marks a significant accomplishment for the Drill community, which now includes dozens of developers working at a variety of companies.
date: 2014-12-02 08:00
authors: ["tshiran"]
---
The Apache Software Foundation has just announced that it has promoted Drill to a top-level project at Apache, similar to other well-known projects like Apache Hadoop and httpd (the world's most popular Web server). This marks a significant accomplishment for the Drill community, and I wanted to personally thank everyone who has contributed to the project. It takes many people, and countless hours, to develop something as complex and innovative as Drill.

In this post I wanted to reflect on the past and future of Drill.

## Why We Started Drill

### The Evolution of Application Development and Data

Over the last decade, organizations have been striving to become more agile and data-driven, seeking to gain competitive advantage in their markets. This trend has led to dramatic changes in the way applications are built and delivered, and in the type and volume of data that is being leveraged.

**Applications**: In previous decades, software development was a carefully orchestrated and planned process. The release cycles were often measured in years, and upgrades were infrequent. Today, Web and mobile applications are developed in a much more iterative fashion. The release cycles are measured in days or weeks, and upgrades are a non-issue. (What version of Salesforce.com or Google Maps are you using?)

**Data**: In previous decades, data was measured in MBs or GBs, and it was highly structured and denormalized. Today's data is often measured in TBs or PBs, and it tends to be multi-structured — a combination of unstructured, semi-structured and structured. The data comes from many different sources, including a variety of applications, devices and services, and its structure changes much more frequently.

### A New Generation of Datastores

The relational database, which was invented in 1970, was not designed for these new processes and data volumes and structures. As a result, a new generation of datastores has emerged, including HDFS, NoSQL (HBase, MongoDB, etc.) and search (Elasticsearch, Solr).  These systems are schema-free (also known as "dynamic schema"). Applications, as opposed to DBAs, control the data structure, enabling more agility and flexibility. For example, an application developer can independently evolve the data structure with each application release (which could be daily or weekly) without filing a ticket with IT and waiting for the schema of the databae to be modified.

## The Need for a New Query Engine

With data increasingly being stored in schema-free datastores (HDFS, HBase, MongoDB, etc.) and a variety of cloud services, users need a way to explore and analyze this data, and a way to visualize it with BI tools (reports, dashboards, etc.). In 2012 we decided to embark on a journey to create the world's next-generation SQL engine. We had several high-level requirements in mind:

* **A schema-free data model.** Schema-free datastores (HDFS, NoSQL, search) need a schema-free SQL engine. These datastores became popular for a reason, and we shouldn't expect organizations to sacrifice those advantages in order to enjoy SQL-based analytics and BI. Today's organizations need agility and flexibility to cope with the volume, variety and velocity associated with modern applications and data.  
* **A standalone query engine that supports multiple data sources.** Most companies now use a variety of best-of-breed datastores and services to store data. This is true not just for large Global 2000 companies, but also for small startups. For example, it is not uncommon for a startup to have data in MySQL, MongoDB, HBase and HDFS, as well as a variety of online services. ETL was hard even 10 years ago when data was static and 100x smaller than it is today, and in today's era of Big Data it's often impractical or impossible to ingest all the data into a single system.
* **Ease of use.** The SQL engine can't be hard to setup and use. Analysts and developers should be able to download and use it without deploying any complex infrastructure such as Hadoop.
* **Scalability and performance.** The SQL engine must support interactive queries. It can't be batch-oriented like Hive. In addition, it must be able to scale linearly from a small laptop or virtual machine to a large cluster with hundreds or thousands of powerful servers.

With these requirements in mind, we decided to incubate a new project in 2012 in the Apache Software Foundation so that a community of vendors and developers could come together and develop the technology. (One little known fact is that the name "Drill" was actually suggested by Google engineers due to its inspiration from Google's Dremel execution engine.)

After almost two years of research and development, we released Drill 0.4 in August, and continued with monthly releases since then.

## What's Next

Graduating to a top-level project is a significant milestone, but it's really just the beginning of the journey. In fact, we're currently wrapping up Drill 0.7, which includes hundreds of fixes and enhancements, and we expect to release that in the next couple weeks.

Drill is currently being used by dozens of organizations, ranging from small startups to some of the largest Fortune 100s. These organizations are already gaining tremendous business value with Drill. As we march towards a 1.0 release early next year, these organizations are helping us shape the project and ensure that it meets the needs of a broad range of organizations as well as users (business analysts, technical analysts, data scientists and application developers). I would like to encourage you to join the ride today by [downloading Drill](http://drill.apache.org/download/) and [letting us know](mailto:user@drill.apache.org) what you think.

Happy Drilling!  
Tomer Shiran