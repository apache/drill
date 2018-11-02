---
title: "CASE"
date: 2018-11-02
parent: "SQL Conditional Expressions"
---
Executes statements based on one or more equality conditions.

## Syntax

    CASE
        WHEN expression [, expression [ ... ]] THEN
          statements
      [ WHEN expression [, expression [ ... ]] THEN
          statements
        ... ]
      [ ELSE
          statements ]
    END;

## Usage Notes
Drill processes the CASE expression as follows:

1. Evaluates the Boolean expression in each WHEN clause that returns true, or falls through to the ELSE statement for each WHEN clause that returns false.If ELSE is not present and a WHEN clause returns false, an exception occurs.
2. Executes each statement in the WHEN clause that returns true or executes each ELSE statement for a WHEN clause that returns false. 
3. Exits the CASE statement.

## Example

The Drill installation includes [`employee.json`]({{site.baseurl}}/docs/querying-json-files/) in the Drill classpath that this example queries. You use the classpath (cp) storage plugin point to this file. 

The employee having an ID of 99 is Elizabeth Horne. The employee having a ID of 100 is Mary Hunt. The example CASE statement gets the first name of the employee 99 and the last name of employee 100. Any other employee ID does not meet the condition; the ID is too high.

    USE cp;  
    SELECT employee_id, 
      CASE 
        WHEN employee_id < 100 THEN first_name 
        WHEN employee_id = 100 THEN last_name 
          ELSE 'ID too high' 
      END from cp.`employee.json` 
      WHERE employee_id = 99 
        OR employee_id = 100 
        OR employee_id = 101;
    +-------------+------------+
    | employee_id |   EXPR$1   |
    +-------------+------------+
    | 99          | Elizabeth  |
    | 100         | Hunt       |
    | 101         | ID too high |
    +-------------+------------+
    3 rows selected (0.199 seconds)
