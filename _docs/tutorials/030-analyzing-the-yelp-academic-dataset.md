---
title: "Analyzing the Yelp Academic Dataset"
date: 2018-11-02
parent: "Tutorials"
---
Apache Drill is one of the fastest growing open source projects, with the community making rapid progress with monthly releases. The key difference is Drill’s agility and flexibility.
Along with meeting the table stakes for SQL-on-Hadoop, which is to achieve low
latency performance at scale, Drill allows users to analyze the data without
any ETL or up-front schema definitions. The data can be in any file format
such as text, JSON, or Parquet. Data can have simple types such as strings,
integers, dates, or more complex multi-structured data, such as nested maps and
arrays. Data can exist in any file system, local or distributed, such as HDFS or S3. Drill, has a “no schema” approach, which enables you to get
value from your data in just a few minutes.

Let’s quickly walk through the steps required to install Drill and run it
against the Yelp data set. The publicly available data set used for this
example is downloadable from [Yelp](http://www.yelp.com/dataset_challenge)
(business reviews) and is in JSON format.

----------

## Installing and Starting Drill

### Download Apache Drill onto your local machine

To experiment with Drill locally, follow the installation instructions in [Drill in 10 Minutes]({{site.baseurl}}/docs/drill-in-10-minutes/).

Alternatively, you can [install Drill in distributed mode]({{ site.baseurl }}/docs/installing-drill-in-distributed-mode) if you
want to scale your environment.

Let’s try out some SQL examples to understand how Drill makes the raw data
analysis extremely easy.

{% include startnote.html %}You need to substitute your local path to the Yelp data set in the angle-bracketed portion of the FROM clause of each query you run.{% include endnote.html %}

----------

## Querying Data with Drill   
   
### 1\. View the contents of the Yelp business data

    0: jdbc:drill:zk=local> !set maxwidth 10000

    0: jdbc:drill:zk=local> select * from
        dfs.`<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json`
        limit 1;

    +------------------------+----------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+--------------------------------+---------+--------------+-------------------+-------------+-------+-------+-----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+
    | business_id            | full_address                                       | hours                                                                                                                                                                                                                                                      | open | categories                     | city    | review_count | name              | longitude   | state | stars | latitude  | attributes                                                                                                                                                   | type     | neighborhoods |
    +------------------------+----------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+--------------------------------+---------+--------------+-------------------+-------------+-------+-------+-----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+
    | vcNAWiLM4dR7D2nwwJ7nCA | 4840 E Indian School Rd Ste 101, Phoenix, AZ 85018 | fill in{"Tuesday":{"close":"17:00","open":"08:00"},"Friday":{"close":"17:00","open":"08:00"},"Monday":{"close":"17:00","open":"08:00"},"Wednesday":{"close":"17:00","open":"08:00"},"Thursday":{"close":"17:00","open":"08:00"},"Sunday":{},"Saturday":{}} | true | ["Doctors","Health & Medical"] | Phoenix | 7            | Eric Goldberg, MD | -111.983758 | AZ    | 3.5   | 33.499313 | {"By Appointment Only":true,"Good For":{},"Ambience":{},"Parking":{},"Music":{},"Hair Types Specialized In":{},"Payment Types":{},"Dietary Restrictions":{}} | business | []            |
    +------------------------+----------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+--------------------------------+---------+--------------+-------------------+-------------+-------+-------+-----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+

{% include startnote.html %}This document aligns Drill output for example purposes. Drill output is not aligned in this case.{% include endnote.html %}

You can directly query self-describing files such as JSON, Parquet, and text. There is no need to create metadata definitions in the Hive metastore.

### 2\. Explore the business data set further

#### Total reviews in the data set

    0: jdbc:drill:zk=local> select sum(review_count) as totalreviews 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json`;

    +--------------+
    | totalreviews |
    +--------------+
    | 1236445      |
    +--------------+

#### Top states and cities in total number of reviews

    0: jdbc:drill:zk=local> select state, city, count(*) totalreviews 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` 
    group by state, city order by count(*) desc limit 10;

    +------------+------------+--------------+
    |   state    |    city    | totalreviews |
    +------------+------------+--------------+
    | NV         | Las Vegas  | 12021        |
    | AZ         | Phoenix    | 7499         |
    | AZ         | Scottsdale | 3605         |
    | EDH        | Edinburgh  | 2804         |
    | AZ         | Mesa       | 2041         |
    | AZ         | Tempe      | 2025         |
    | NV         | Henderson  | 1914         |
    | AZ         | Chandler   | 1637         |
    | WI         | Madison    | 1630         |
    | AZ         | Glendale   | 1196         |
    +------------+------------+--------------+

#### Average number of reviews per business star rating

    0: jdbc:drill:zk=local> select stars,trunc(avg(review_count)) reviewsavg 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json`
    group by stars order by stars desc;

    +------------+------------+
    |   stars    | reviewsavg |
    +------------+------------+
    | 5.0        | 8.0        |
    | 4.5        | 28.0       |
    | 4.0        | 48.0       |
    | 3.5        | 35.0       |
    | 3.0        | 26.0       |
    | 2.5        | 16.0       |
    | 2.0        | 11.0       |
    | 1.5        | 9.0        |
    | 1.0        | 4.0        |
    +------------+------------+

#### Top businesses with high review counts (> 1000)

    0: jdbc:drill:zk=local> select name, state, city, `review_count` from
    dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json`
    where review_count > 1000 order by `review_count` desc limit 10;

    +-------------------------------+-------------+------------+---------------+
    |           name                |   state     |    city    |  review_count |
    +-------------------------------+-------------+------------+---------------+
    | Mon Ami Gabi                  | NV          | Las Vegas  | 4084          |
    | Earl of Sandwich              | NV          | Las Vegas  | 3655          |
    | Wicked Spoon                  | NV          | Las Vegas  | 3408          |
    | The Buffet                    | NV          | Las Vegas  | 2791          |
    | Serendipity 3                 | NV          | Las Vegas  | 2682          |
    | Bouchon                       | NV          | Las Vegas  | 2419          |
    | The Buffet at Bellagio        | NV          | Las Vegas  | 2404          |
    | Bacchanal Buffet              | NV          | Las Vegas  | 2369          |
    | The Cosmopolitan of Las Vegas | NV          | Las Vegas  | 2253          |
    | Aria Hotel & Casino           | NV          | Las Vegas  | 2224          |
    +-------------------------------+-------------+----------------------------+

#### Saturday open and close times for a few businesses

    0: jdbc:drill:zk=local> select b.name, b.hours.Saturday.`open`,
    b.hours.Saturday.`close`  
    from
    dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json`
    b limit 10;

    +----------------------------+------------+------------+
    |    name                    |   EXPR$1   |   EXPR$2   |
    +----------------------------+------------+------------+
    | Eric Goldberg, MD          | 08:00      | 17:00      |
    | Pine Cone Restaurant       | null       | null       |
    | Deforest Family Restaurant | 06:00      | 22:00      |
    | Culver's                   | 10:30      | 22:00      |
    | Chang Jiang Chinese Kitchen| 11:00      | 22:00      |
    | Charter Communications     | null       | null       |
    | Air Quality Systems        | null       | null       |
    | McFarland Public Library   | 09:00      | 20:00      |
    | Green Lantern Restaurant   | 06:00      | 02:00      |
    | Spartan Animal Hospital    | 07:30      | 18:00      |
    +----------------------------+------------+------------+

Note how Drill can traverse and refer through multiple levels of nesting.

### 3\. Get the amenities of each business in the data set

Note that the attributes column in the Yelp business data set has a different
element for every row, representing that businesses can have separate
amenities. Drill makes it easy to quickly access data sets with changing
schemas.

First, change Drill to work in all text mode (so we can take a look at all of
the data).

    0: jdbc:drill:zk=local> alter system set `store.json.all_text_mode` = true;
    +------------+-----------------------------------+
    |     ok     |  summary                          |
    +------------+-----------------------------------+
    | true       | store.json.all_text_mode updated. |
    +------------+-----------------------------------+

Then, query the attribute’s data.

    0: jdbc:drill:zk=local> select attributes from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` limit 10;

    +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |                                                     attributes                                                                                                                    |
    +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | {"By Appointment Only":"true","Good For":{},"Ambience":{},"Parking":{},"Music":{},"Hair Types Specialized In":{},"Payment Types":{},"Dietary Restrictions":{}}                    |
    | {"Take-out":"true","Good For":{"dessert":"false","latenight":"false","lunch":"true","dinner":"false","breakfast":"false","brunch":"false"},"Caters":"false","Noise Level":"averag |
    | {"Take-out":"true","Good For":{"dessert":"false","latenight":"false","lunch":"false","dinner":"false","breakfast":"false","brunch":"true"},"Caters":"false","Noise Level":"quiet" |
    | {"Take-out":"true","Good For":{},"Takes Reservations":"false","Delivery":"false","Ambience":{},"Parking":{"garage":"false","street":"false","validated":"false","lot":"true","val |
    | {"Take-out":"true","Good For":{},"Ambience":{},"Parking":{},"Has TV":"false","Outdoor Seating":"false","Attire":"casual","Music":{},"Hair Types Specialized In":{},"Payment Types |
    | {"Good For":{},"Ambience":{},"Parking":{},"Music":{},"Hair Types Specialized In":{},"Payment Types":{},"Dietary Restrictions":{}}                                                 |
    | {"Good For":{},"Ambience":{},"Parking":{},"Music":{},"Hair Types Specialized In":{},"Payment Types":{},"Dietary Restrictions":{}}                                                 |
    | {"Good For":{},"Ambience":{},"Parking":{},"Wi-Fi":"free","Music":{},"Hair Types Specialized In":{},"Payment Types":{},"Dietary Restrictions":{}}                                  |
    | {"Take-out":"true","Good For":{"dessert":"false","latenight":"false","lunch":"false","dinner":"true","breakfast":"false","brunch":"false"},"Noise Level":"average"                |
    | {"Good For":{},"Ambience":{},"Parking":{},"Music":{},"Hair Types Specialized In":{},"Payment Types":{},"Dietary Restrictions":{}}                                                 |
    +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

{% include startnote.html %}This document aligns Drill output for example purposes. Drill output is not aligned in this case.{% include endnote.html %}

Turn off the all text mode so we can continue to perform arithmetic operations
on data.

    0: jdbc:drill:zk=local> alter system set `store.json.all_text_mode` = false;
    +-------+------------------------------------+
    |  ok   |              summary               |
    +-------+------------------------------------+
    | true  | store.json.all_text_mode updated.  |
    +-------+------------------------------------+

### 4\. Explore the restaurant businesses in the data set

#### Number of restaurants in the data set

    0: jdbc:drill:zk=local> select count(*) as TotalRestaurants from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` where true=repeated_contains(categories,'Restaurants');
    +------------------+
    | TotalRestaurants |
    +------------------+
    | 14303            |
    +------------------+

#### Top restaurants in number of reviews

    0: jdbc:drill:zk=local> select name,state,city,`review_count` from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` where true=repeated_contains(categories,'Restaurants') order by `review_count` desc limit 10;

    +------------------------+-------+-----------+--------------+
    |          name          | state |    city   | review_count |
    +------------------------+-------+-----------+--------------+
    | Mon Ami Gabi           | NV    | Las Vegas | 4084         |
    | Earl of Sandwich       | NV    | Las Vegas | 3655         |
    | Wicked Spoon           | NV    | Las Vegas | 3408         |
    | The Buffet             | NV    | Las Vegas | 2791         |
    | Serendipity 3          | NV    | Las Vegas | 2682         |
    | Bouchon                | NV    | Las Vegas | 2419         |
    | The Buffet at Bellagio | NV    | Las Vegas | 2404         |
    | Bacchanal Buffet       | NV    | Las Vegas | 2369         |
    | Hash House A Go Go     | NV    | Las Vegas | 2201         |
    | Mesa Grill             | NV    | Las Vegas | 2004         |
    +------------------------+-------+-----------+--------------+

#### Top restaurants in number of listed categories

    0: jdbc:drill:zk=local> select name,repeated_count(categories) as categorycount, categories from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` where true=repeated_contains(categories,'Restaurants') order by repeated_count(categories) desc limit 10;

    +---------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------+
    | name                            | categorycount | categories                                                                                                                                        |
    +---------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------+
    | Binion's Hotel & Casino         | 10            | ["Arts &,Entertainment","Restaurants","Bars","Casinos","Event,Planning &,Services","Lounges","Nightlife","Hotels &,Travel","American]             |
    | Stage Deli                      | 10            | ["Arts &,Entertainment","Food","Hotels","Desserts","Delis","Casinos","Sandwiches","Hotels,& Travel","Restaurants","Event Planning &,Services"]    |
    | Jillian's                       | 9             | ["Arts &,Entertainment","American (Traditional)","Music,Venues","Bars","Dance,Clubs","Nightlife","Bowling","Active,Life","Restaurants"]           |
    | Hotel Chocolat                  | 9             | ["Coffee &,Tea","Food","Cafes","Chocolatiers &,Shops","Specialty Food","Event Planning &,Services","Hotels & Travel","Hotels","Restaurants"]      |
    | Hotel du Vin & Bistro Edinburgh | 9             | ["Modern,European","Bars","French","Wine,Bars","Event Planning &,Services","Nightlife","Hotels &,Travel","Hotels","Restaurants"]                  |
    | Elixir                          | 9             | ["Arts &,Entertainment","American (Traditional)","Music,Venues","Bars","Cocktail,Bars","Nightlife","American (New)","Local,Flavor","Restaurants"] |
    | Tocasierra Spa and Fitness      | 8             | ["Beauty &,Spas","Gyms","Medical Spas","Health &,Medical","Fitness & Instruction","Active,Life","Day Spas","Restaurants"]                         |
    | Costa Del Sol At Sunset Station | 8             | ["Steakhouses","Mexican","Seafood","Event,Planning & Services","Hotels &,Travel","Italian","Restaurants","Hotels"]                                |
    | Scottsdale Silverado Golf Club  | 8             | ["Fashion","Shopping","Sporting,Goods","Active Life","Golf","American,(New)","Sports Wear","Restaurants"]                                         |
    | House of Blues                  | 8             | ["Arts & Entertainment","Music Venues","Restaurants","Hotels","Event Planning & Services","Hotels & Travel","American (New)","Nightlife"]         |
    +---------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------+

{% include startnote.html %}This document aligns Drill output for example purposes. Drill output is not aligned in this case.{% include endnote.html %}

#### Top first categories in number of review counts

    0: jdbc:drill:zk=local> select categories[0], count(categories[0]) as categorycount 
    from dfs.`/<path-to-yelp-dataset>/yelp_academic_dataset_business.json` 
    group by categories[0] 
    order by count(categories[0]) desc limit 10;

    +----------------------+---------------+
    | EXPR$0               | categorycount |
    +----------------------+---------------+
    | Food                 | 4294          |
    | Shopping             | 1885          |
    | Active Life          | 1676          |
    | Bars                 | 1366          |
    | Local Services       | 1351          |
    | Mexican              | 1284          |
    | Hotels & Travel      | 1283          |
    | Fast Food            | 963           |
    | Arts & Entertainment | 906           |
    | Hair Salons          | 901           |
    +----------------------+---------------+

### 5\. Explore the Yelp reviews dataset and combine with the businesses.

#### Take a look at the contents of the Yelp reviews dataset.

    0: jdbc:drill:zk=local> select * 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_review.json` limit 1;
    +---------------------------------+------------------------+------------------------+-------+------------+----------------------------------------------------------------------+--------+------------------------+
    | votes                           | user_id                | review_id              | stars | date       | text                                                                 | type   | business_id            |
    +---------------------------------+------------------------+------------------------+-------+------------+----------------------------------------------------------------------+--------+------------------------+
    | {"funny":0,"useful":2,"cool":1} | Xqd0DzHaiyRqVH3WRG7hzg | 15SdjuK7DmYqUAj6rjGowg | 5     | 2007-05-17 | dr. goldberg offers everything i look for in a general practitioner. | review | vcNAWiLM4dR7D2nwwJ7nCA |
    +---------------------------------+------------------------+------------------------+-------+------------+----------------------------------------------------------------------+--------+------------------------+

#### Top businesses with cool rated reviews

Note that we are combining the Yelp business data set that has the overall
review_count to the Yelp review data, which holds additional details on each
of the reviews themselves.

    0: jdbc:drill:zk=local> Select b.name 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` b 
    where b.business_id in (SELECT r.business_id 
    FROM dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_review.json` r
    GROUP BY r.business_id having sum(r.votes.cool) > 2000 
    order by sum(r.votes.cool)  desc);
    +-------------------------------+
    |             name              |
    +-------------------------------+
    | Earl of Sandwich              |
    | XS Nightclub                  |
    | The Cosmopolitan of Las Vegas |
    | Wicked Spoon                  |
    +-------------------------------+

#### Create a view with the combined business and reviews data sets

Note that Drill views are lightweight, and can just be created in the local
file system. Drill in standalone mode comes with a dfs.tmp workspace, which we
can use to create views (or you can can define your own workspaces on a local
or distributed file system). If you want to persist the data physically
instead of in a logical view, you can use CREATE TABLE AS syntax.

    0: jdbc:drill:zk=local> create or replace view dfs.tmp.businessreviews as 
    Select b.name,b.stars,b.state,b.city,r.votes.funny,r.votes.useful,r.votes.cool, r.`date` 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` b, dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_review.json` r 
    where r.business_id=b.business_id
    +------------+-----------------------------------------------------------------+
    |     ok     |                           summary                               |
    +------------+-----------------------------------------------------------------+
    | true       | View 'businessreviews' created successfully in 'dfs.tmp' schema |
    +------------+-----------------------------------------------------------------+

Let’s get the total number of records from the view.

    0: jdbc:drill:zk=local> select count(*) as Total from dfs.tmp.businessreviews;
    +------------+
    |   Total    |
    +------------+
    | 1125458    |
    +------------+

In addition to these queries, you can get many deep insights using
Drill’s [SQL functionality]({{ site.baseurl }}/docs/sql-reference). If you are not comfortable with writing queries manually, you
can use a BI/Analytics tools such as Tableau/MicroStrategy to query raw
files/Hive/HBase data or Drill-created views directly using Drill [ODBC/JDBC
drivers]({{ site.baseurl }}/docs/odbc-jdbc-interfaces).

The goal of Apache Drill is to provide the freedom and flexibility in
exploring data in ways we have never seen before with SQL technologies. The
community is working on more exciting features around nested data and
supporting data with changing schemas in upcoming releases.

The FLATTEN function can be used to dynamically rationalize semi-structured
data so you can apply even deeper SQL functionality. Here is a sample query:

#### Get a flattened list of categories for each business

    0: jdbc:drill:zk=local> select name, flatten(categories) as category 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json`  limit 20;
    +-----------------------------+---------------------------------+
    | name                        | category                        |
    +-----------------------------+---------------------------------+
    | Eric Goldberg, MD           | Doctors                         |
    | Eric Goldberg, MD           | Health & Medical                |
    | Pine Cone Restaurant        | Restaurants                     |
    | Deforest Family Restaurant  | American (Traditional)          |
    | Deforest Family Restaurant  | Restaurants                     |
    | Culver's                    | Food                            |
    | Culver's                    | Ice Cream & Frozen Yogurt       |
    | Culver's                    | Fast Food                       |
    | Culver's                    | Restaurants                     |
    | Chang Jiang Chinese Kitchen | Chinese                         |
    | Chang Jiang Chinese Kitchen | Restaurants                     |
    | Charter Communications      | Television Stations             |
    | Charter Communications      | Mass Media                      |
    | Air Quality Systems         | Home Services                   |
    | Air Quality Systems         | Heating & Air Conditioning/HVAC |
    | McFarland Public Library    | Libraries                       |
    | McFarland Public Library    | Public Services & Government    |
    | Green Lantern Restaurant    | American (Traditional)          |
    | Green Lantern Restaurant    | Restaurants                     |
    | Spartan Animal Hospital     | Veterinarians                   |
    +-----------------------------+---------------------------------+

#### Top categories used in business reviews

    0: jdbc:drill:zk=local> select celltbl.catl, count(celltbl.catl) categorycnt 
    from (select flatten(categories) catl from dfs.`/yelp_academic_dataset_business.json` ) celltbl 
    group by celltbl.catl 
    order by count(celltbl.catl) desc limit 10 ;
    +------------------+-------------+
    | catl             | categorycnt |
    +------------------+-------------+
    | Restaurants      | 14303       |
    | Shopping         | 6428        |
    | Food             | 5209        |
    | Beauty & Spas    | 3421        |
    | Nightlife        | 2870        |
    | Bars             | 2378        |
    | Health & Medical | 2351        |
    | Automotive       | 2241        |
    | Home Services    | 1957        |
    | Fashion          | 1897        |
    +------------------+-------------+

Stay tuned for more features and upcoming activities in the Drill community.

To learn more about Drill, please refer to the following resources:

  * Download Drill here: <http://getdrill.org/drill/download>
  * [10 reasons we think Drill is cool]({{site.baseurl}}/docs/why-drill)
  * [A simple 10-minute tutorial]({{ site.baseurl }}/docs/drill-in-10-minutes>)
  * [More tutorials]({{ site.baseurl }}/docs/tutorials-introduction/)

