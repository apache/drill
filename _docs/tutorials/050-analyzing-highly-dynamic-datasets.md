---
title: "Analyzing Highly Dynamic Datasets"
date: 2017-12-16 06:24:23 UTC
parent: "Tutorials"
---

Today’s data is dynamic and application-driven. The growth of a new era of business applications driven by industry trends such as web, social, mobile, and Internet of Things are generating datasets with new data types and new data models. These applications are iterative, and the associated data models typically are semi-structured, schema-less and constantly evolving. Semi-structured data models can be complex/nested, schema-less, and capable of having varying fields in every single row and of constantly evolving as fields get added and removed frequently to meet business requirements. 

This tutorial shows you how to natively query dynamic datasets, such as JSON, and derive insights from any type of data in minutes. The dataset used in the example is from the Yelp check-ins dataset, which has the following structure:

    check-in
    {
        'type': 'checkin',
        'business_id': (encrypted business id),
        'checkin_info': {
            '0-0': (number of checkins from 00:00 to 01:00 on all Sundays),
            '1-0': (number of checkins from 01:00 to 02:00 on all Sundays),
            ...
            '14-4': (number of checkins from 14:00 to 15:00 on all Thursdays),
            ...
            '23-6': (number of checkins from 23:00 to 00:00 on all Saturdays)
        }, # if there was no checkin for a hour-day block it will not be in the dataset
    }

It is worth repeating the comment at the bottom of this snippet:

     # if there was no checkin for a hour-day block it will not be in the dataset. 

The element names that you see in the `checkin_info` are unknown upfront and can vary for every row. The data, although simple, is highly dynamic data. To analyze the data there is no need to first represent this dataset in a flattened relational structure, as you would using any other SQL on Hadoop technology.

----------

Step 1: First download Drill, if you have not yet done so, onto your machine

    http://drill.apache.org/download/
    tar -xvf apache-drill-1.12.0.tar

Install Drill locally on your desktop (embedded mode). You don’t need Hadoop.

----------

Step 2: Start the Drill shell.

    bin/drill-embedded

----------

Step 3: Start analyzing the data using SQL

First, let’s take a look at the dataset:

    0: jdbc:drill:zk=local> SELECT * FROM dfs.`/users/nrentachintala/Downloads/yelp/yelp_academic_dataset_checkin.json` limit 2;
    +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+------------------------+
    |                                                                 checkin_info                                                                                                                                                             |    type    |      business_id       |
    +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+------------------------+
    | {"3-4":1,"13-5":1,"6-6":1,"14-5":1,"14-6":1,"14-2":1,"14-3":1,"19-0":1,"11-5":1,"13-2":1,"11-6":2,"11-3":1,"12-6":1,"6-5":1,"5-5":1,"9-2":1,"9-5":1,"9-6":1,"5-2":1,"7-6":1,"7-5":1,"7-4":1,"17-5":1,"8-5":1,"10-2":1,"10-5":1,"10-6":1} | checkin    | JwUE5GmEO-sH1FuwJgKBlQ |
    | {"6-6":2,"6-5":1,"7-6":1,"7-5":1,"8-5":2,"10-5":1,"9-3":1,"12-5":1,"15-3":1,"15-5":1,"15-6":1,"16-3":1,"10-0":1,"15-4":1,"10-4":1,"8-2":1}                                                                                               | checkin    | uGykseHzyS5xAMWoN6YUqA |
    +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+------------------------+

{% include startnote.html %}This document aligns Drill output for example purposes. Drill output is not aligned in this case.{% include endnote.html %}

You query the data in JSON files directly. Schema definitions in Hive store are not necessary. The names of the elements within the `checkin_info` column are different between the first and second row.

Drill provides a function called KVGEN (Key Value Generator) which is useful when working with complex data that contains arbitrary maps consisting of dynamic and unknown element names such as checkin_info. KVGEN turns the dynamic map into an array of key-value pairs where keys represent the dynamic element names.

Let’s apply KVGEN on the `checkin_info` element to generate key-value pairs.

    0: jdbc:drill:zk=local> SELECT KVGEN(checkin_info) checkins FROM dfs.`/users/nrentachintala/Downloads/yelp/yelp_academic_dataset_checkin.json` LIMIT 2;
    +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |                                                                    checkins                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | [{"key":"3-4","value":1},{"key":"13-5","value":1},{"key":"6-6","value":1},{"key":"14-5","value":1},{"key":"14-6","value":1},{"key":"14-2","value":1},{"key":"14-3","value":1},{"key":"19-0","value":1},{"key":"11-5","value":1},{"key":"13-2","value":1},{"key":"11-6","value":2},{"key":"11-3","value":1},{"key":"12-6","value":1},{"key":"6-5","value":1},{"key":"5-5","value":1},{"key":"9-2","value":1},{"key":"9-5","value":1},{"key":"9-6","value":1},{"key":"5-2","value":1},{"key":"7-6","value":1},{"key":"7-5","value":1},{"key":"7-4","value":1},{"key":"17-5","value":1},{"key":"8-5","value":1},{"key":"10-2","value":1},{"key":"10-5","value":1},{"key":"10-6","value":1}] |
    | [{"key":"6-6","value":2},{"key":"6-5","value":1},{"key":"7-6","value":1},{"key":"7-5","value":1},{"key":"8-5","value":2},{"key":"10-5","value":1},{"key":"9-3","value":1},{"key":"12-5","value":1},{"key":"15-3","value":1},{"key":"15-5","value":1},{"key":"15-6","value":1},{"key":"16-3","value":1},{"key":"10-0","value":1},{"key":"15-4","value":1},{"key":"10-4","value":1},{"key":"8-2","value":1}]                                                                                                                                                                                                                                                                               |
    +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Drill provides another function to operate on complex data called ‘Flatten’ to break the list of key-value pairs resulting from ‘KVGen’ into separate rows to further apply analytic functions on it.

    0: jdbc:drill:zk=local> SELECT FLATTEN(KVGEN(checkin_info)) checkins FROM dfs.`/users/nrentachintala/Downloads/yelp/yelp_academic_dataset_checkin.json` LIMIT 20;
    +--------------------------+
    |         checkins         |
    +--------------------------+
    | {"key":"3-4","value":1}  |
    | {"key":"13-5","value":1} |
    | {"key":"6-6","value":1}  |
    | {"key":"14-5","value":1} |
    | {"key":"14-6","value":1} |
    | {"key":"14-2","value":1} |
    | {"key":"14-3","value":1} |
    | {"key":"19-0","value":1} |
    | {"key":"11-5","value":1} |
    | {"key":"13-2","value":1} |
    | {"key":"11-6","value":2} |
    | {"key":"11-3","value":1} |
    | {"key":"12-6","value":1} |
    | {"key":"6-5","value":1}  |
    | {"key":"5-5","value":1}  |
    | {"key":"9-2","value":1}  |
    | {"key":"9-5","value":1}  |
    | {"key":"9-6","value":1}  |
    | {"key":"5-2","value":1}  |
    | {"key":"7-6","value":1}  |
    +--------------------------+

You can get value from the data quickly by applying both KVGEN and FLATTEN functions on the datasets on the fly--no need for time-consuming schema definitions and data storage in intermediate formats.

On the output of flattened data, you use standard SQL functionality such as filters , aggregates, and sort. Let’s see a few examples.

### Get the total number of check-ins recorded in the Yelp dataset

    0: jdbc:drill:zk=local> SELECT SUM(checkintbl.checkins.`value`) AS TotalCheckins FROM (
    . . . . . . . . . . . >  SELECT FLATTEN(KVGEN(checkin_info)) checkins FROM dfs.`/users/nrentachintala/Downloads/yelp/yelp_academic_dataset_checkin.json` ) checkintbl
    . . . . . . . . . . . >  ;
    +---------------+
    | TotalCheckins |
    +---------------+
    | 4713811       |
    +---------------+

### Get the number of check-ins specifically for Sunday midnights

    0: jdbc:drill:zk=local> SELECT SUM(checkintbl.checkins.`value`) AS SundayMidnightCheckins FROM (
    . . . . . . . . . . . >  SELECT FLATTEN(KVGEN(checkin_info)) checkins FROM dfs.`/users/nrentachintala/Downloads/yelp/yelp_academic_dataset_checkin.json` ) checkintbl WHERE checkintbl.checkins.key='23-0';
    +------------------------+
    | SundayMidnightCheckins |
    +------------------------+
    | 8575                   |
    +------------------------+

### Get the number of check-ins per day of the week

    0: jdbc:drill:zk=local> SELECT `right`(checkintbl.checkins.key,1) WeekDay,sum(checkintbl.checkins.`value`) TotalCheckins from (
    . . . . . . . . . . . >  select flatten(kvgen(checkin_info)) checkins FROM dfs.`/users/nrentachintala/Downloads/yelp/yelp_academic_dataset_checkin.json`  ) checkintbl GROUP BY `right`(checkintbl.checkins.key,1) ORDER BY TotalCheckins;
    +------------+---------------+
    |  WeekDay   | TotalCheckins |
    +------------+---------------+
    | 1          | 545626        |
    | 0          | 555038        |
    | 2          | 555747        |
    | 3          | 596296        |
    | 6          | 735830        |
    | 4          | 788073        |
    | 5          | 937201        |
    +------------+---------------+

### Get the number of check-ins per hour of the day

    0: jdbc:drill:zk=local> SELECT SUBSTR(checkintbl.checkins.key,1,strpos(checkintbl.checkins.key,'-')-1) AS HourOfTheDay ,SUM(checkintbl.checkins.`value`) TotalCheckins FROM (
    . . . . . . . . . . . >  SELECT FLATTEN(KVGEN(checkin_info)) checkins FROM dfs.`/users/nrentachintala/Downloads/yelp/yelp_academic_dataset_checkin.json` ) checkintbl GROUP BY SUBSTR(checkintbl.checkins.key,1,strpos(checkintbl.checkins.key,'-')-1) ORDER BY TotalCheckins;
    +--------------+---------------+
    | HourOfTheDay | TotalCheckins |
    +--------------+---------------+
    | 3            | 20357         |
    | 4            | 21076         |
    | 2            | 28116         |
    | 5            | 33842         |
    | 1            | 45467         |
    | 6            | 54174         |
    | 0            | 74127         |
    | 7            | 96329         |
    | 23           | 102009        |
    | 8            | 130091        |
    | 22           | 140338        |
    | 9            | 162913        |
    | 21           | 211949        |
    | 10           | 220687        |
    | 15           | 261384        |
    | 14           | 276188        |
    | 16           | 292547        |
    | 20           | 293783        |
    | 13           | 328373        |
    | 11           | 338675        |
    | 17           | 374186        |
    | 19           | 385381        |
    | 12           | 399797        |
    | 18           | 422022        |
    +--------------+---------------+

----------

## Summary
In this tutorial, you surf both structured and semi-structured data without any upfront schema management or ETL.
