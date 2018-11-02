---
title: "Analyzing Data Using Window Functions"
date: 2018-11-02
parent: "Tutorials"
---

This tutorial briefly introduces the analytics in Drill 1.2, namely ANSI SQL-compliant analytic and window functions. Drill supports the following SQL window functions:

* PARTITION BY and OVER clauses
* A variety of aggregated window functions for Sum, Max, Min, Count, Avg
* Analytic functions such as First_Value, Last_Value, Lead, Lag, NTile, Row_Number, and Rank

Window functions are highly versatile. You can reduce the joins, subqueries, and explicit cursors that you need to write. Window functions solve a variety of use cases with minimal coding effort.

This tutorial builds on previous tutorials, [Analyzing the Yelp Academic Dataset]({{site.baseurl}}/docs/analyzing-the-yelp-academic-dataset/) and [Analyzing Highly Dynamic Datasets]({{site.baseurl}}/docs/analyzing-highly-dynamic-datasets/), and uses the same Yelp dataset. 

----------

## Getting Started

1. To get started, download the [Yelp](http://www.yelp.com/dataset_challenge) (business reviews) now. 

2. [Install and start Drill]({{site.baseurl}}/docs/analyzing-the-yelp-academic-dataset/#installing-and-starting-drill). 

3. List the available schemas in Drill.

        SHOW schemas;
        +---------------------+
        |     SCHEMA_NAME     |
        +---------------------+
        | INFORMATION_SCHEMA  |
        | cp.default          |
        | dfs.default         |
        | dfs.root            |
        | dfs.tmp             |
        | dfs.yelp            |
        | sys                 |
        +---------------------+

        7 rows selected (1.755 seconds)

4. Switch to using the workspace in which Yelp data is loaded.

        USE dfs.yelp;

        +-------+---------------------------------------+
        |  ok   |                summary                |
        +-------+---------------------------------------+
        | true  | Default schema changed to [dfs.yelp]  |
        +-------+---------------------------------------+

        1 row selected (0.129 seconds)

5. Start with exploring one of the datasets available in Yelp dataset - the business information.

        SELECT * FROM `business.json` LIMIT 1;

        +------------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+--------------------------------+---------+--------------+-------------------+-------------+-------+-------+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+
        | business_id            | full_address                                        | hours                                                                                                                                                                                                                                               | open |         categories             |   city  | review_count |        name       |  longitude  | state | stars |  latitude | attributes                                                                                                                                          |    type  | neighborhoods |
        +------------------------+--------------+------+-------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+--------------------------------+---------+--------------+-------------------+-------------+-------+-------+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+
        | vcNAWiLM4dR7D2nwwJ7nCA | 4840 E Indian School Rd Ste 101 Phoenix, AZ 85018   | {"Tuesday":{"close":"17:00","open":"08:00"},"Friday":{"close":"17:00","open":"08:00"},"Monday":{"close":"17:00","open":"08:00"},"Wednesday":{"close":"17:00","open":"08:00"},"Thursday":{"close":"17:00","open":"08:00"},"Sunday":{},"Saturday":{}} | true | ["Doctors","Health & Medical"] | Phoenix |      7       | Eric Goldberg, MD | -111.983758 |   AZ  |  3.5  | 33.499313 | {"By Appointment Only":true,"Good Ambience":{},"Parking":{},"Music":{},"Hair Types Specialized In":{},"Payment Types":{},"Dietary Restrictions":{}} | business |      []       |
        +-------------+--------------+-------+------+------------+------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+--------------------------------+---------+--------------+-------------------+-------------+-------+-------+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+
        1 row selected (0.514 seconds)

----------

## Use Window Functions for Simple Queries

1. Get the top Yelp businesses based on the number reviews in each city and the row number of the business.

        SELECT name, city, review_count, row_number()
        OVER (PARTITION BY city ORDER BY review_count DESC) AS rownum 
        FROM `business.json` LIMIT 15;  

        +----------------------------------------+------------+---------------+---------+
        |                  name                  |    city    | review_count  | rownum  |
        +----------------------------------------+------------+---------------+---------+
        | Cupz N' Crepes                         | Ahwatukee  | 124           | 1       |
        | My Wine Cellar                         | Ahwatukee  | 98            | 2       |
        | Kathy's Alterations                    | Ahwatukee  | 12            | 3       |
        | McDonald's                             | Ahwatukee  | 7             | 4       |
        | U-Haul                                 | Ahwatukee  | 5             | 5       |
        | Hi-Health                              | Ahwatukee  | 4             | 6       |
        | Healthy and Clean Living Environments  | Ahwatukee  | 4             | 7       |
        | Active Kids Pediatrics                 | Ahwatukee  | 4             | 8       |
        | Roberto's Authentic Mexican Food       | Anthem     | 117           | 1       |
        | Q to U BBQ                             | Anthem     | 74            | 2       |
        | Outlets At Anthem                      | Anthem     | 64            | 3       |
        | Dara Thai                              | Anthem     | 56            | 4       |
        | Cafe Provence                          | Anthem     | 53            | 5       |
        | Shanghai Club                          | Anthem     | 50            | 6       |
        | Two Brothers Kitchen                   | Anthem     | 43            | 7       |
        +----------------------------------------+------------+---------------+---------+
        15 rows selected (0.67 seconds)

2. Check the number reviews for each business compared to the average number of reviews across all business in the city.

        SELECT name, city,review_count,
        Avg(review_count) OVER (PARTITION BY City) AS city_reviews_avg
        FROM `business.json` LIMIT 15;

        +----------------------------------------+------------+---------------+---------------------+
        |                  name                  |    city    | review_count  |  city_reviews_avg   |
        +----------------------------------------+------------+---------------+---------------------+
        | Hi-Health                              | Ahwatukee  | 4             | 32.25               |
        | My Wine Cellar                         | Ahwatukee  | 98            | 32.25               |
        | U-Haul                                 | Ahwatukee  | 5             | 32.25               |
        | Cupz N' Crepes                         | Ahwatukee  | 124           | 32.25               |
        | McDonald's                             | Ahwatukee  | 7             | 32.25               |
        | Kathy's Alterations                    | Ahwatukee  | 12            | 32.25               |
        | Healthy and Clean Living Environments  | Ahwatukee  | 4             | 32.25               |
        | Active Kids Pediatrics                 | Ahwatukee  | 4             | 32.25               |
        | Anthem Community Center                | Anthem     | 4             | 14.492063492063492  |
        | Scrapbooks To Remember                 | Anthem     | 4             | 14.492063492063492  |
        | Hungry Howie's Pizza                   | Anthem     | 7             | 14.492063492063492  |
        | Pinata Nueva                           | Anthem     | 3             | 14.492063492063492  |
        | Starbucks Coffee Company               | Anthem     | 13            | 14.492063492063492  |
        | Pizza Hut                              | Anthem     | 6             | 14.492063492063492  |
        | Rays Pizza                             | Anthem     | 19            | 14.492063492063492  |
        +----------------------------------------+------------+---------------+---------------------+
        15 rows selected (0.395 seconds)

3. Check how the number of reviews for each business contribute to the total number of reviews for all businesses in the city.

        SELECT name, city,review_count,
        Sum(review_count) OVER (PARTITION BY City) AS city_reviews_sum
        FROM `business.json`limit 15;

        +----------------------------------------+------------+---------------+-------------------+
        |                  name                  |    city    | review_count  | city_reviews_sum  |
        +----------------------------------------+------------+---------------+-------------------+
        | Hi-Health                              | Ahwatukee  | 4             | 258               |
        | My Wine Cellar                         | Ahwatukee  | 98            | 258               |
        | U-Haul                                 | Ahwatukee  | 5             | 258               |
        | Cupz N' Crepes                         | Ahwatukee  | 124           | 258               |
        | McDonald's                             | Ahwatukee  | 7             | 258               |
        | Kathy's Alterations                    | Ahwatukee  | 12            | 258               |
        | Healthy and Clean Living Environments  | Ahwatukee  | 4             | 258               |
        | Active Kids Pediatrics                 | Ahwatukee  | 4             | 258               |
        | Anthem Community Center                | Anthem     | 4             | 913               |
        | Scrapbooks To Remember                 | Anthem     | 4             | 913               |
        | Hungry Howie's Pizza                   | Anthem     | 7             | 913               |
        | Pinata Nueva                           | Anthem     | 3             | 913               |
        | Starbucks Coffee Company               | Anthem     | 13            | 913               |
        | Pizza Hut                              | Anthem     | 6             | 913               |
        | Rays Pizza                             | Anthem     | 19            | 913               |
        +----------------------------------------+------------+---------------+-------------------+
        15 rows selected (0.543 seconds)


----------

## Use Window Functions for Complex Queries

1. List Top 10 cities and their highest ranked businesses in terms of number of reviews. Use Drill window functions such as rank, dense_rank in these queries.

        WITH X
        AS
        (SELECT name, city, review_count,
        RANK()
        OVER (PARTITION BY city
        ORDER BY review_count DESC) AS review_rank
        FROM `business.json`)
        SELECT X.name, X.city, X.review_count
        FROM X
        WHERE X.review_rank =1 ORDER BY review_count DESC LIMIT 10;

        +-------------------------------------------+-------------+---------------+
        |                   name                    |    city     | review_count  |
        +-------------------------------------------+-------------+---------------+
        | Mon Ami Gabi                              | Las Vegas   | 4084          |
        | Studio B                                  | Henderson   | 1336          |
        | Phoenix Sky Harbor International Airport  | Phoenix     | 1325          |
        | Four Peaks Brewing Co                     | Tempe       | 1110          |
        | The Mission                               | Scottsdale  | 783           |
        | Joe's Farm Grill                          | Gilbert     | 770           |
        | The Old Fashioned                         | Madison     | 619           |
        | Cornish Pasty Company                     | Mesa        | 578           |
        | SanTan Brewing Company                    | Chandler    | 469           |
        | Yard House                                | Glendale    | 321           |
        +-------------------------------------------+-------------+---------------+
        10 rows selected (0.49 seconds)

2. Compare the number of reviews for each business with the top and bottom review counts in the city.

        SELECT name, city, review_count,
        FIRST_VALUE(review_count)
        OVER(PARTITION BY city ORDER BY review_count DESC) AS top_review_count,
        LAST_VALUE(review_count)
        OVER(PARTITION BY city ORDER BY review_count DESC) AS bottom_review_count
        FROM `business.json` limit 15;

        +----------------------------------------+------------+---------------+-------------------+----------------------+
        |                  name                  |    city    | review_count  | top_review_count  | bottom_review_count  |
        +----------------------------------------+------------+---------------+-------------------+----------------------+
        | My Wine Cellar                         | Ahwatukee  | 98            | 124               | 12                   |
        | McDonald's                             | Ahwatukee  | 7             | 124               | 12                   |
        | U-Haul                                 | Ahwatukee  | 5             | 124               | 12                   |
        | Hi-Health                              | Ahwatukee  | 4             | 124               | 12                   |
        | Healthy and Clean Living Environments  | Ahwatukee  | 4             | 124               | 12                   |
        | Active Kids Pediatrics                 | Ahwatukee  | 4             | 124               | 12                   |
        | Cupz N' Crepes                         | Ahwatukee  | 124           | 124               | 12                   |
        | Kathy's Alterations                    | Ahwatukee  | 12            | 124               | 12                   |
        | Q to U BBQ                             | Anthem     | 74            | 117               | 117                  |
        | Dara Thai                              | Anthem     | 56            | 117               | 117                  |
        | Cafe Provence                          | Anthem     | 53            | 117               | 117                  |
        | Shanghai Club                          | Anthem     | 50            | 117               | 117                  |
        | Two Brothers Kitchen                   | Anthem     | 43            | 117               | 117                  |
        | The Tennessee Grill                    | Anthem     | 32            | 117               | 117                  |
        | Dollyrockers Boutique and Salon        | Anthem     | 30            | 117               | 117                  |
        +----------------------------------------+------------+---------------+-------------------+----------------------+
        15 rows selected (0.516 seconds)


3. Compare the number of reviews with the number of reviews for the previous and following businesses.

        SELECT city, review_count, name,
        LAG(review_count, 1) OVER(PARTITION BY city ORDER BY review_count DESC) 
        AS preceding_count,
        LEAD(review_count, 1) OVER(PARTITION BY city ORDER BY review_count DESC) 
        AS following_count
        FROM `business.json` limit 15;

        +------------+---------------+----------------------------------------+------------------+------------------+
        |    city    | review_count  |                  name                  | preceding_count  | following_count  |
        +------------+---------------+----------------------------------------+------------------+------------------+
        | Ahwatukee  | 124           | Cupz N' Crepes                         | null             | 98               |
        | Ahwatukee  | 98            | My Wine Cellar                         | 124              | 12               |
        | Ahwatukee  | 12            | Kathy's Alterations                    | 98               | 7                |
        | Ahwatukee  | 7             | McDonald's                             | 12               | 5                |
        | Ahwatukee  | 5             | U-Haul                                 | 7                | 4                |
        | Ahwatukee  | 4             | Hi-Health                              | 5                | 4                |
        | Ahwatukee  | 4             | Healthy and Clean Living Environments  | 4                | 4                |
        | Ahwatukee  | 4             | Active Kids Pediatrics                 | 4                | null             |
        | Anthem     | 117           | Roberto's Authentic Mexican Food       | null             | 74               |
        | Anthem     | 74            | Q to U BBQ                             | 117              | 64               |
        | Anthem     | 64            | Outlets At Anthem                      | 74               | 56               |
        | Anthem     | 56            | Dara Thai                              | 64               | 53               |
        | Anthem     | 53            | Cafe Provence                          | 56               | 50               |
        | Anthem     | 50            | Shanghai Club                          | 53               | 43               |
        | Anthem     | 43            | Two Brothers Kitchen                   | 50               | 32               |
        +------------+---------------+----------------------------------------+------------------+------------------+
        15 rows selected (0.518 seconds)
