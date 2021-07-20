---
title: "分析 Yelp 学术数据集"
slug: "Analyzing the Yelp Academic Dataset"
parent: "教程"
lang: "zh"
---

Drill 的与众不同之处在于敏捷性和灵活性。
为了满足 SQL 查询 Hadoop，并规模化减少延迟，Drill 允许用户不必进行 ETL 流程或者 预先定义 schema。文件可以是任意格式，比如：纯文本，JSON 或者 Parquet。
数据可以是简单的字符串，整数，日期，也可以是更复杂的多结构数据，比如嵌套Map和数组。数据可以保存在任意文件系统，本地或者分布式，比如 HDFS 或者 S3。Drill 具备 “no schema” 方法，
使你可以在几分钟内从你的数据中得到数值。

让我们一起快速的安装 Drill 并加载 Yelp 数据集的步骤。示例中所用的公开数据集下载自[Yelp](http://www.yelp.com/dataset_challenge)（商家回顾），数据格式为 JSON。

----------

## 安装并运行 Drill

### 下载 Drill 到你的电脑中

本地试用 Drill，请遵循此安装指南[10分钟了解 Drill]({{site.baseurl}}/docs/drill-in-10-minutes/)。

另一种方案，如果你想扩展你的环境，你可以[分布式安装 Drill]({{site.baseurl}}/docs/installing-drill-in-distributed-mode)。

我们一起尝试一些 SQL 示例来了解 Drill 如何将原始数据分析变得如此简单。

{% include startnote.html %}你需要将每次查询语句中 FROM 部分尖括号中的内容替换成你本地存储 Yelp 数据的路径。{% include endnote.html %}

----------

## 通过 Drill 查询数据

### 1\. 查看 Yelp 商家数据的内容 

    0: jdbc:drill:zk=local> !set maxwidth 10000

    0: jdbc:drill:zk=local> select * from
        dfs.`<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json`
        limit 1;

    |------------------------|----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|--------------------------------|---------|--------------|-------------------|-------------|-------|-------|-----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
    | business_id            | full_address                                       | hours                                                                                                                                                                                                                                                      | open | categories                     | city    | review_count | name              | longitude   | state | stars | latitude  | attributes                                                                                                                                                   | type     | neighborhoods |
    |------------------------|----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|--------------------------------|---------|--------------|-------------------|-------------|-------|-------|-----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
    | vcNAWiLM4dR7D2nwwJ7nCA | 4840 E Indian School Rd Ste 101, Phoenix, AZ 85018 | fill in{"Tuesday":{"close":"17:00","open":"08:00"},"Friday":{"close":"17:00","open":"08:00"},"Monday":{"close":"17:00","open":"08:00"},"Wednesday":{"close":"17:00","open":"08:00"},"Thursday":{"close":"17:00","open":"08:00"},"Sunday":{},"Saturday":{}} | true | ["Doctors","Health & Medical"] | Phoenix | 7            | Eric Goldberg, MD | -111.983758 | AZ    | 3.5   | 33.499313 | {"By Appointment Only":true,"Good For":{},"Ambience":{},"Parking":{},"Music":{},"Hair Types Specialized In":{},"Payment Types":{},"Dietary Restrictions":{}} | business | []            |
    |------------------------|----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|--------------------------------|---------|--------------|-------------------|-------------|-------|-------|-----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|

{% include startnote.html %}本文档为了展示目的对齐了 Drill 的输出。实际上 Drill 的输出不会这样对齐。{% include endnote.html %}

你可以直接查询自我描述的文件格式，如 JSON，Parquet，和纯文本。不必为 Hive 元存储创建元数据。

### 2\. 更进一步探索商家数据集

#### 数据集中的总评价数

    0: jdbc:drill:zk=local> select sum(review_count) as totalreviews 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json`;

    |--------------|
    | totalreviews |
    |--------------|
    | 1236445      |
    |--------------|

#### 评论数最高的州和城市

    0: jdbc:drill:zk=local> select state, city, count(*) totalreviews 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` 
    group by state, city order by count(*) desc limit 10;

    |------------|------------|--------------|
    |   state    |    city    | totalreviews |
    |------------|------------|--------------|
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
    |------------|------------|--------------|

#### 每个评分星级下的平均评论数

    0: jdbc:drill:zk=local> select stars,trunc(avg(review_count)) reviewsavg 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json`
    group by stars order by stars desc;

    |------------|------------|
    |   stars    | reviewsavg |
    |------------|------------|
    | 5.0        | 8.0        |
    | 4.5        | 28.0       |
    | 4.0        | 48.0       |
    | 3.5        | 35.0       |
    | 3.0        | 26.0       |
    | 2.5        | 16.0       |
    | 2.0        | 11.0       |
    | 1.5        | 9.0        |
    | 1.0        | 4.0        |
    |------------|------------|

#### 评论数最多的商家 (> 1000)

    0: jdbc:drill:zk=local> select name, state, city, `review_count` from
    dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json`
    where review_count > 1000 order by `review_count` desc limit 10;

    |-------------------------------|-------------|------------|---------------|
    |           name                |   state     |    city    |  review_count |
    |-------------------------------|-------------|------------|---------------|
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
    |-------------------------------|-------------|----------------------------|

#### 一些商家周六的营业时间

    0: jdbc:drill:zk=local> select b.name, b.hours.Saturday.`open`,
    b.hours.Saturday.`close`  
    from
    dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json`
    b limit 10;

    |----------------------------|------------|------------|
    |    name                    |   EXPR$1   |   EXPR$2   |
    |----------------------------|------------|------------|
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
    |----------------------------|------------|------------|

请注意 Drill 如何遍历和引用多层级的嵌套数据。


### 3\. 从数据集中得到每个商家的便利设施情况

请注意 Yelp 商家数据集中，属性列的每一行都有不同的元素，代表商家有不同的便利设施。Drill 通过改变 schema 更简单的快速访问数据集。

首先，更改配置使 Drill 可以识别所有的文本格式（我们便可查看所有的数据）。

    0: jdbc:drill:zk=local> alter system set `store.json.all_text_mode` = true;
    |------------|-----------------------------------|
    |     ok     |  summary                          |
    |------------|-----------------------------------|
    | true       | store.json.all_text_mode updated. |
    |------------|-----------------------------------|

接下来，查询 "attribute" 所对应的数据。

    0: jdbc:drill:zk=local> select attributes from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` limit 10;

    |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    |                                                     attributes                                                                                                                    |
    |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
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
    |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|

{% include startnote.html %}本文档为了展示目的对齐了 Drill 的输出。实际上 Drill 的输出不会这样对齐。{% include endnote.html %}

关闭所有的文本格式，我们可以继续对数据进行算数操作。

    0: jdbc:drill:zk=local> alter system set `store.json.all_text_mode` = false;
    |-------|------------------------------------|
    |  ok   |              summary               |
    |-------|------------------------------------|
    | true  | store.json.all_text_mode updated.  |
    |-------|------------------------------------|

### 4\. 探索数据集中所有的餐饮商家

#### 数据集中所有的餐厅

    0: jdbc:drill:zk=local> select count(*) as TotalRestaurants from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` where true=repeated_contains(categories,'Restaurants');
    |------------------|
    | TotalRestaurants |
    |------------------|
    | 14303            |
    |------------------|

#### 评论最多的餐厅

    0: jdbc:drill:zk=local> select name,state,city,`review_count` from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` where true=repeated_contains(categories,'Restaurants') order by `review_count` desc limit 10;

    |------------------------|-------|-----------|--------------|
    |          name          | state |    city   | review_count |
    |------------------------|-------|-----------|--------------|
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
    |------------------------|-------|-----------|--------------|

#### 商品种类最多的餐厅

    0: jdbc:drill:zk=local> select name,repeated_count(categories) as categorycount, categories from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` where true=repeated_contains(categories,'Restaurants') order by repeated_count(categories) desc limit 10;

    |---------------------------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
    | name                            | categorycount | categories                                                                                                                                        |
    |---------------------------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
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
    |---------------------------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------|

{% include startnote.html %}本文档为了展示目的对齐了 Drill 的输出。实际上 Drill 的输出不会这样对齐。{% include endnote.html %}

#### 评论最多的商品种类

    0: jdbc:drill:zk=local> select categories[0], count(categories[0]) as categorycount 
    from dfs.`/<path-to-yelp-dataset>/yelp_academic_dataset_business.json` 
    group by categories[0] 
    order by count(categories[0]) desc limit 10;

    |----------------------|---------------|
    | EXPR$0               | categorycount |
    |----------------------|---------------|
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
    |----------------------|---------------|

### 5\. 探索 Yelp 评论数据集和商家信息。

#### 查看 Yelp 评论数据集。

    0: jdbc:drill:zk=local> select * 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_review.json` limit 1;
    |---------------------------------|------------------------|------------------------|-------|------------|----------------------------------------------------------------------|--------|------------------------|
    | votes                           | user_id                | review_id              | stars | date       | text                                                                 | type   | business_id            |
    |---------------------------------|------------------------|------------------------|-------|------------|----------------------------------------------------------------------|--------|------------------------|
    | {"funny":0,"useful":2,"cool":1} | Xqd0DzHaiyRqVH3WRG7hzg | 15SdjuK7DmYqUAj6rjGowg | 5     | 2007-05-17 | dr. goldberg offers everything i look for in a general practitioner. | review | vcNAWiLM4dR7D2nwwJ7nCA |
    |---------------------------------|------------------------|------------------------|-------|------------|----------------------------------------------------------------------|--------|------------------------|

#### 拥有最多好评的商家

注意，我们连接了 Yelp 商户数据集和评论数据集，所以具有了 Yelp 评论数据的整体评论数，使每一个评论具有了更详细的信息。

    0: jdbc:drill:zk=local> Select b.name 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` b 
    where b.business_id in (SELECT r.business_id 
    FROM dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_review.json` r
    GROUP BY r.business_id having sum(r.votes.cool) > 2000 
    order by sum(r.votes.cool)  desc);
    |-------------------------------|
    |             name              |
    |-------------------------------|
    | Earl of Sandwich              |
    | XS Nightclub                  |
    | The Cosmopolitan of Las Vegas |
    | Wicked Spoon                  |
    |-------------------------------|

#### 创建了连接商户和评论数据集后的视图

Drill 的视图是轻量级的，且只是创建在本地文件系统。
Drill 在独立模式运行下，会有一个 dfs.tmp 工作空间，我们会用来创建视图（或者你可以在本地文件系统或者分布式文件系统上自定义工作空间）。
如果你想保存这些视图，可以使用 CREATE TABLE AS 语法。

    0: jdbc:drill:zk=local> create or replace view dfs.tmp.businessreviews as 
    Select b.name,b.stars,b.state,b.city,r.votes.funny,r.votes.useful,r.votes.cool, r.`date` 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json` b, dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_review.json` r 
    where r.business_id=b.business_id
    |------------|-----------------------------------------------------------------|
    |     ok     |                           summary                               |
    |------------|-----------------------------------------------------------------|
    | true       | View 'businessreviews' created successfully in 'dfs.tmp' schema |
    |------------|-----------------------------------------------------------------|

我们得到视图中的总记录数

    0: jdbc:drill:zk=local> select count(*) as Total from dfs.tmp.businessreviews;
    |------------|
    |   Total    |
    |------------|
    | 1125458    |
    |------------|

在这些查询之外，你可以利用 Drill 的 [SQL 函数]({{ site.baseurl }}/docs/sql-reference) 进行更深入的分析。
如果你不习惯手写查询，你可以利用商务智能分析工具如 Tableau/MicroStrategy 来查询原始 文件/Hive/HBase 数据，或者通过[ODBC/JDBC
drivers]({{ site.baseurl }}/docs/odbc-jdbc-interfaces)直接创建 Drill 视图。

Apache Drill 的目标在于通过 SQL 技术自由和灵活的探索数据。社区正在围绕嵌套数据努力提供更多令人兴奋的特性，
并在接下来的版本中支持更改数据的 schema。

FLATTEN 函数可以动态的合理化半结构数据，可以帮助你应用于更复杂的 SQL 函数。参考下列示例：

#### 得到每个商户的平面化商品种类列表

    0: jdbc:drill:zk=local> select name, flatten(categories) as category 
    from dfs.`/<path-to-yelp-dataset>/yelp/yelp_academic_dataset_business.json`  limit 20;
    |-----------------------------|---------------------------------|
    | name                        | category                        |
    |-----------------------------|---------------------------------|
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
    |-----------------------------|---------------------------------|

#### 商户评论中提到最多的商品种类

    0: jdbc:drill:zk=local> select celltbl.catl, count(celltbl.catl) categorycnt 
    from (select flatten(categories) catl from dfs.`/yelp_academic_dataset_business.json` ) celltbl 
    group by celltbl.catl 
    order by count(celltbl.catl) desc limit 10 ;
    |------------------|-------------|
    | catl             | categorycnt |
    |------------------|-------------|
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
    |------------------|-------------|

与 Drill 社区保持密切联系来获得更多的特性以及了解即将到来的活动。

想更多了解 Drill，请参考如下资源：

  * 下载 Drill: ({{ site.baseurl }}/download/)
  * [选择 Drill 的十个原因]({{site.baseurl}}/docs/why-drill)
  * [简单的10分钟教程]({{ site.baseurl }}/docs/drill-in-10-minutes>)
  * [更多教程]({{ site.baseurl }}/docs/tutorials-introduction/)

