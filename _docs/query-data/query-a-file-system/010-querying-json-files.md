---
title: "Querying JSON Files"
date: 2016-11-21 22:14:43 UTC
parent: "Querying a File System"
---
To query complex JSON files, you need to understand the ["JSON Data Model"]({{site.baseurl}}/docs/json-data-model/). This section provides a trivial example of querying a sample file that Drill installs. 

## About the employee.json File
The sample file, `employee.json`, is packaged in the Foodmart data JAR in Drill's
classpath:  

`./jars/3rdparty/foodmart-data-json.0.4.jar`

The file contains fictitious employee
data. Here is a snippet of the file:

```
{"employee_id":1,"full_name":"Sheri Nowmer","first_name":"Sheri","last_name":"Nowmer","position_id":1,"position_title":"President","store_id":0,"department_id":1,"birth_date":"1961-08-26","hire_date":"1994-12-01 00:00:00.0","end_date":null,"salary":80000.0000,"supervisor_id":0,"education_level":"Graduate Degree","marital_status":"S","gender":"F","management_role":"Senior Management"}
```
To query a file in a JAR file in the Drill classpath, you need to use the [cp (classpath) storage plugin]({{site.baseurl}}/docs/storage-plugin-registration/) configuration, as shown in the sample query.

## Sample Query

Start the Drill shell, and select five rows of data from the `employee.json` file installed with Drill.

``SELECT * FROM cp.`employee.json` LIMIT 5;``

The query returns the following results:

    +--------------+----------------------------+---------------------+---------------+--------------+----------------------------+-----------+----------------+-------------+------------------------+----------+----------------+----------------------+-----------------+---------+-----------------------+
    | employee_id  |         full_name          |     first_name      |   last_name   | position_id  |       position_title       | store_id  | department_id  | birth_date  |       hire_date        |  salary  | supervisor_id  |   education_level    | marital_status  | gender  |    management_role    |
    +--------------+----------------------------+---------------------+---------------+--------------+----------------------------+-----------+----------------+-------------+------------------------+----------+----------------+----------------------+-----------------+---------+-----------------------+
    | 1            | Sheri Nowmer               | Sheri               | Nowmer        | 1            | President                  | 0         | 1              | 1961-08-26  | 1994-12-01 00:00:00.0  | 80000.0  | 0              | Graduate Degree      | S               | F       | Senior Management     |
    | 2            | Derrick Whelply            | Derrick             | Whelply       | 2            | VP Country Manager         | 0         | 1              | 1915-07-03  | 1994-12-01 00:00:00.0  | 40000.0  | 1              | Graduate Degree      | M               | M       | Senior Management     |
    | 4            | Michael Spence             | Michael             | Spence        | 2            | VP Country Manager         | 0         | 1              | 1969-06-20  | 1998-01-01 00:00:00.0  | 40000.0  | 1              | Graduate Degree      | S               | M       | Senior Management     |
    | 5            | Maya Gutierrez             | Maya                | Gutierrez     | 2            | VP Country Manager         | 0         | 1              | 1951-05-10  | 1998-01-01 00:00:00.0  | 35000.0  | 1              | Bachelors Degree     | M               | F       | Senior Management     |
