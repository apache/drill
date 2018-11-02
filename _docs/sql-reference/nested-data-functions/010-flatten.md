---
title: "FLATTEN"
date: 2018-11-02
parent: "Nested Data Functions"
---
FLATTEN separates the elements in a repeated field into individual records.

## Syntax

    FLATTEN(z)

*z* is a JSON array.

## Usage Notes

The FLATTEN function is useful for flexible exploration of repeated data.

To maintain the association between each flattened value and the other fields in
the record, the FLATTEN function copies all of the other columns into each new record. 

A very simple example would turn this data (one record):

    {
      "x" : 5,
      "y" : "a string",
      "z" : [ 1,2,3]
    }

into three distinct records:

    SELECT FLATTEN(z) FROM table;
    | x           | y              | z         |
    +-------------+----------------+-----------+
    | 5           | "a string"     | 1         |
    | 5           | "a string"     | 2         |
    | 5           | "a string"     | 3         |

The function takes a single argument, which must be an array (the `z` column
in this example).

Using the all (*) wildcard as the argument to flatten is not supported and returns an error.

## Examples

For a more interesting example, consider the JSON data in the publicly
available [Yelp](https://www.yelp.com/dataset_challenge/dataset) data set. The
first query below returns three columns from the
`yelp_academic_dataset_business.json` file: `name`, `hours`, and `categories`.
The query is restricted to distinct rows where the name is `z``pizza`. The
query returns only one row that meets those criteria; however, note that this
row contains an array of four categories:

    0: jdbc:drill:zk=local> select distinct name, hours, categories 
    from dfs.yelp.`yelp_academic_dataset_business.json` 
    where name ='zpizza';
    +------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+
    |    name    |   hours                                                                                                                                                                                                                                                                                                         | categories                                    |
    +------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+
    | zpizza     | {"Tuesday":{"close":"22:00","open":"10:00"},"Friday":{"close":"23:00","open":"10:00"},"Monday":{"close":"22:00","open":"10:00"},"Wednesday":{"close":"22:00","open":"10:00"},"Thursday":{"close":"22:00","open":"10:00"},"Sunday":{"close":"22:00","open":"10:00"},"Saturday":{"close":"23:00","open":"10:00"}} | ["Gluten-Free","Pizza","Vegan","Restaurants"] |

The FLATTEN function can operate on this single row and return multiple rows,
one for each category:

    0: jdbc:drill:zk=local> select distinct name, flatten(categories) as categories 
    from dfs.yelp.`yelp_academic_dataset_business.json` 
    where name ='zpizza' order by 2;
    +------------+-------------+
    |    name    | categories  |
    +------------+-------------+
    | zpizza     | Gluten-Free |
    | zpizza     | Pizza       |
    | zpizza     | Restaurants |
    | zpizza     | Vegan       |
    +------------+-------------+
    4 rows selected (2.797 seconds)

Having used the FLATTEN function to break down arrays into distinct rows, you
can run queries that do deeper analysis on the flattened result set. For
example, you can use FLATTEN in a subquery, then apply WHERE clause
constraints or aggregate functions to the results in the outer query.

The following query uses the same data file as the previous query to flatten
the categories array, then run a COUNT function on the flattened result:

    select celltbl.catl, count(celltbl.catl) catcount 
    from (select flatten(categories) catl 
    from dfs.yelp.`yelp_academic_dataset_business.json`) celltbl 
    group by celltbl.catl 
    order by count(celltbl.catl) desc limit 5;
 
    +---------------+------------+
    |    catl       |  catcount  |
    +---------------+------------+
    | Restaurants   | 14303      |
    | Shopping      | 6428       |
    | Food          | 5209       |
    | Beauty & Spas | 3421       |
    | Nightlife     | 2870       |
    +---------------|------------+

A common use case for FLATTEN is its use in conjunction with the
KVGEN function as shown in the section, ["JSON Data Model"]({{ site.baseurl }}/docs/json-data-model/).

