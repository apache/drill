---
title: "Querying JSON Files"
parent: "Querying a File System"
---
Your Drill installation includes a sample JSON file located in Drill's
classpath. The sample JSON file, `employee.json`, contains fictitious employee
data. Use SQL syntax to query the sample `JSON` file.

To view the data in the `employee.json` file, submit the following SQL query
to Drill:

         0: jdbc:drill:zk=local> SELECT * FROM cp.`employee.json` LIMIT 5;

The query returns the following results:

    +--------------+----------------------------+---------------------+---------------+--------------+----------------------------+-----------+----------------+-------------+------------------------+----------+----------------+----------------------+-----------------+---------+-----------------------+
    | employee_id  |         full_name          |     first_name      |   last_name   | position_id  |       position_title       | store_id  | department_id  | birth_date  |       hire_date        |  salary  | supervisor_id  |   education_level    | marital_status  | gender  |    management_role    |
    +--------------+----------------------------+---------------------+---------------+--------------+----------------------------+-----------+----------------+-------------+------------------------+----------+----------------+----------------------+-----------------+---------+-----------------------+
    | 1            | Sheri Nowmer               | Sheri               | Nowmer        | 1            | President                  | 0         | 1              | 1961-08-26  | 1994-12-01 00:00:00.0  | 80000.0  | 0              | Graduate Degree      | S               | F       | Senior Management     |
    | 2            | Derrick Whelply            | Derrick             | Whelply       | 2            | VP Country Manager         | 0         | 1              | 1915-07-03  | 1994-12-01 00:00:00.0  | 40000.0  | 1              | Graduate Degree      | M               | M       | Senior Management     |
    | 4            | Michael Spence             | Michael             | Spence        | 2            | VP Country Manager         | 0         | 1              | 1969-06-20  | 1998-01-01 00:00:00.0  | 40000.0  | 1              | Graduate Degree      | S               | M       | Senior Management     |
    | 5            | Maya Gutierrez             | Maya                | Gutierrez     | 2            | VP Country Manager         | 0         | 1              | 1951-05-10  | 1998-01-01 00:00:00.0  | 35000.0  | 1              | Bachelors Degree     | M               | F       | Senior Management     |

    0: jdbc:drill:zk=local>