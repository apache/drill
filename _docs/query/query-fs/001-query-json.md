---
title: "Querying JSON Files"
parent: "Querying a File System"
---
Your Drill installation includes a sample JSON file located in Drill's
classpath. The sample JSON file, `employee.json`, contains fictitious employee
data. Use SQL syntax to query the sample `JSON` file.

To view the data in the `employee.json` file, submit the following SQL query
to Drill:

         0: jdbc:drill:zk=local> SELECT * FROM cp.`employee.json`;

The query returns the following results:

**Example of partial output**

    +-------------+------------+------------+------------+-------------+-----------+
    | employee_id | full_name  | first_name | last_name  | position_id | position_ |
    +-------------+------------+------------+------------+-------------+-----------+
    | 1101        | Steve Eurich | Steve      | Eurich     | 16          | Store T |
    | 1102        | Mary Pierson | Mary       | Pierson    | 16          | Store T |
    | 1103        | Leo Jones  | Leo        | Jones      | 16          | Store Tem |
    | 1104        | Nancy Beatty | Nancy      | Beatty     | 16          | Store T |
    | 1105        | Clara McNight | Clara      | McNight    | 16          | Store  |
    | 1106        | Marcella Isaacs | Marcella   | Isaacs     | 17          | Stor |
    | 1107        | Charlotte Yonce | Charlotte  | Yonce      | 17          | Stor |
    | 1108        | Benjamin Foster | Benjamin   | Foster     | 17          | Stor |
    | 1109        | John Reed  | John       | Reed       | 17          | Store Per |
    | 1110        | Lynn Kwiatkowski | Lynn       | Kwiatkowski | 17          | St |
    | 1111        | Donald Vann | Donald     | Vann       | 17          | Store Pe |
    | 1112        | William Smith | William    | Smith      | 17          | Store  |
    | 1113        | Amy Hensley | Amy        | Hensley    | 17          | Store Pe |
    | 1114        | Judy Owens | Judy       | Owens      | 17          | Store Per |
    | 1115        | Frederick Castillo | Frederick  | Castillo   | 17          | S |
    | 1116        | Phil Munoz | Phil       | Munoz      | 17          | Store Per |
    | 1117        | Lori Lightfoot | Lori       | Lightfoot  | 17          | Store |
    ...
    +-------------+------------+------------+------------+-------------+-----------+
    1,155 rows selected (0.762 seconds)
    0: jdbc:drill:zk=local>