---
layout: post
title: "Schema-free JSON Data Infrastructure"
code: schema-free-json-data-infrastructure
excerpt: JSON has emerged as the de-facto standard data exchange format. Data infrastructure technologies such as Apache Drill, MongoDB and Elasticsearch are embracing JSON as their native data models, bringing game-changing ease-of-use and agility to developers and analysts.
date: 2015-1-27 8:50:01
authors: [tshiran]
---

JSON has emerged in recent years as the de-facto standard data exchange format. It is being used everywhere. Front-end Web applications use JSON to maintain data and communicate with back-end applications. Web APIs are JSON-based (eg, [Twitter REST APIs](https://dev.twitter.com/rest/public), [Marketo REST APIs](http://developers.marketo.com/documentation/rest/), [GitHub API](https://developer.github.com/v3/)). It's the format of choice for public datasets, operational log files and more.

# Why is JSON a Convenient Data Exchange Format?

While I won't dive into the historical roots of JSON (JavaScript Object Notation, [`eval()`](http://en.wikipedia.org/wiki/JSON#JavaScript_eval.28.29), etc.), I do want to highlight several attributes of JSON that make it a convenient data exchange format:

* **JSON is self-describing**. You can look at a JSON document and understand what it represents. The field names are included in the document. You don't need an external schema or definition to interpret JSON-encoded data. This makes life easier for anyone who wants to deal with the data, and it also means that a collection of JSON documents represents what many people call a "schema-less dataset" (where structure can evolve, and different records can have different fields).
* **JSON is simple**. Other self-describing formats such as XML are much more complicated. A JSON document is made up of arrays and maps (or objects, in JSON terminology), and that's about it.
* **JSON can naturally represent real-world objects**. Try representing your application's `Customer` object (with the person's address, order history, etc.) in a CSV file or a relational database. It's hard. In fact, ORM systems were invented to help alleviate this issue.
* **JSON libraries are available in virtually every programming language**. Take a look at [the list of supported languages on JSON.org](http://www.json.org/). I counted 15 languages that start with the letters A, B or C.
* **JSON is idiomatic in loosely typed languages**. Many loosely typed languages, such as Python, Ruby and JavaScript, have data structures that are similar to JSON objects, making it very natural to handle JSON data in those languages. For example, a Python dictionary looks just like a JSON object. This makes it easy for developers to utilize JSON in their applications.

#  JSON Data Infrastructure

Traditional data infrastructure, such as relational databases, has some features that make it easier to store and process JSON-encoded data. For example, Oracle has [a JSON data type and a set of functions for handling JSON data](https://docs.oracle.com/database/121/ADXDB/json.htm).

However, a new class of data infrastructure is providing a much more seamless experience via a full-fledged JSON data model. For example:

* Drill is a SQL engine in which each record is conceptually a JSON document.
* Elasticsearch is a search engine in which each indexed document is conceptually a JSON document.
* MongoDB is an operational database in which each record is conceptually a JSON document.

These systems view JSON as a data model as opposed to one of many data types, realizing that JSON offers a simple way to represent real-world objects.

| | Traditional Infrastructure | JSON Infrastructure |
| --- | --- | --- |
| **Examples:** | Oracle, SQL Server | Drill, Elasticsearch, MongoDB |
| **Record:** | Tuple | JSON document |
| **Variable schema:** | No | Yes |

If you happen to be in the Bay Area tomorrow, please join Gaurav Gupta (VP Product Management, Elasticsearch), Paul Pedersen (Deputy CTO, MongoDB), Robert Greene (Senior Principal Product Manager, Oracle), Sukanta Ganguly (VP Solutions Architecture, Aerospike) and me for a panel moderated by Gartner's Nick Heudecker on this new world of schema-free JSON. Check out [The Hive Big Data Think Tank](http://www.meetup.com/SF-Bay-Areas-Big-Data-Think-Tank/) for more information.
