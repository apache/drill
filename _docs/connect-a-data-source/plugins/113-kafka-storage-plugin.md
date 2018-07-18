---
title: "Kafka Storage Plugin"
date: 2018-07-18 22:29:15 UTC
parent: "Connect a Data Source"
---

As of Drill 1.12, Drill provides a storage plugin for Kafka. The Kafka storage plugin enables you to run SQL queries on Apache Kafka and perform interactive analysis on the data. When you install Drill, a preconfigured Kafka storage plugin is available on the Storage page in the Drill Web Console. Once you enable and configure the storage plugin, you can query Kafka from Drill. 

The following sections provide information about the Kafka storage plugin, how to enable and configure the Kafka storage plugin in Drill, options that you can set at the system or session level, and example queries on a Kafka data source. You can refer to the [README](https://github.com/apache/drill/tree/master/contrib/storage-kafka#drill-kafka-plugin) and [Apache Kafka](https://kafka.apache.org/) for additional information.  

## Kafka Support

Currently, the Kafka storage plugin supports: 

- Kafka-0.10 and above
- Reading Kafka messages of type JSON only
- The following message reader to read the Kafka messages:  

| **MessageReader**     | **Description**           | **Key DeSerializer**                                            | **Value DeSerializer**                                             |
|-------------------|-----------------------|-------------------------------------------------------------|----------------------------------------------------------------|
| JsonMessageReader | To read Json messages | org.apache.kafka.common.serialization.ByteArrayDeserializer | org.apache.kafka.common.serialization.ByteArrayDeserializer    | 

## About the Kafka Storage Plugin  

In Drill, each Kafka topic is mapped to an SQL table. When run a query on a table, the query scans all the messages from the earliest offset to the latest offset of that topic at that point in time. The Kafka storage plugin automatically discovers all the topics (tables), so you can perform analysis without executing DDL statements.  

The Kafka storage plugin also fetches the following metadata for each message:  

- kafkaTopic
- kafkaPartitionId
- kafkaMsgOffset
- kafkaMsgTimestamp
- kafkaMsgKey, unless it is not null  

The following examples show Kafka topics and message offsets:  

       $bin/kafka-topics --list --zookeeper localhost:2181
       clicks
       clickstream
       clickstream-json-demo

       $ bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic clickstream-json-demo --from-beginning | more
       {"userID":"055e9af4-8c3c-4834-8482-8e05367a7bef","sessionID":"7badf08e-1e1d-4aeb-b853-7df2df4431ac","pageName":"shoes","refferalUrl":"yelp","ipAddress":"20.44.183.126","userAgent":"Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1","client_ts":1509926023099}
       {"userID":"a29454b3-642d-481e-9dd8-0e0d7ef32ef5","sessionID":"b4a89204-b98c-4b4b-a1a9-f28f22d5ead3","pageName":"books","refferalUrl":"yelp","ipAddress":"252.252.113.190","userAgent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36 OPR/38.0.2220.41","client_ts":1509926023100}
       {"userID":"8c53b1c6-da47-4b5a-989d-61b5594f3a1d","sessionID":"baae3a1d-25b2-4955-8d07-20191f29ab32","pageName":"login","refferalUrl":"yelp","ipAddress":"110.170.214.255","userAgent":"Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0","client_ts":1509926023100}

       $ bin/kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic clickstream-json-demo --time -2
       clickstream-json-demo:2:2765000
       clickstream-json-demo:1:2765000
       clickstream-json-demo:3:2765000
       clickstream-json-demo:0:2765000

       $ bin/kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic clickstream-json-demo --time -1
       clickstream-json-demo:2:2765245
       clickstream-json-demo:1:2765245
       clickstream-json-demo:3:2765245
       clickstream-json-demo:0:2765245  

## Filter Pushdown Support  

Pushing down filters to the Kafka data source reduces the number of messages that Drill scans and significantly decrease query time. Prior to Drill 1.14, Drill scanned all of the data in a topic before applying filters. Starting in Drill 1.14, Drill transforms the filter conditions on metadata fields to limit the range of offsets scanned from the topic. 

The Drill kafka storage plugin supports filter pushdown for query conditions on the following Kafka metadata fields in messages:  

- **kafkaPartitionId**  
Conditions on the kafkaPartitionId metadata field limit the number of partitions that Drill scans, which is useful for data exploration.
Drill can push down filters when a query contains the following conditions on the kafkaPartitionId metadata field:  
=, >, >=, <, <=
  
- **kafkaMsgOffset**  
Drill can push down filters when a query contains the  following conditions on the kafkaMsgOffset metadata field:    
=, >, >=, <, <=  
  
- **kafkaMsgTimestamp**  
The kafkaMsgTimestamp field maps to the timestamp stored for each Kafka message. Drill can push down filters when a query contains the  following conditions on the kafkaMsgTimestamp metadata field:  
=, >, >=  
   
Kafka exposes the following [Consumer API](https://kafka.apache.org/11/javadoc/org/apache/kafka/clients/consumer/MockConsumer.html) to obtain the earliest offset for a given timestamp value:  

       public java.util.Map<TopicPartition,OffsetAndTimestamp> offsetsForTimes(java.util.Map<TopicPartition,java.lang.Long> timestampsToSearch)  
  

This API is used to determine the startOffset for each partition in a topic. Note that the timestamps may not appear in increasing order when reading from a Kafka topic if you have defined the timestamp for a message. However, the API returns the first offset (from the beginning of a topic partition) where the timestamp is greater or equal to the timestamp requested. Therefore, Drill does not support pushdown on < or <= because an earlier timestamp may exist beyond endOffsetcomputed.     

## Enabling and Configuring the Kafka Storage Plugin  

Enable the Kafka storage plugin on the Storage page of the Drill Web Console and then edit the configuration as needed. 

The Kafka storage plugin configuration contains the `kafkaConsumerProps` property which  supports typical Kafka consumer properties, as described in [Kafka Consumer Configs](https://kafka.apache.org/documentation/#consumerconfigs).  

To enable the Kafka storage plugin, enter the following URL in the address bar of your browser to access the Storage page in the Drill Web Console:  

       http://<drill-node-ip-address>:8047/storage/  

In the Disabled Storage Plugins section, click **Enable** next to Kafka. Kafka moves to the Enabled Storage Plugins section. Click **Update** to see the default configuration. Modify the configuration, as needed, click **Update** again to save the changes. Click **Back** to return to the Storage page.  

The following configuration is an example Kafka storage plugin configuration:  

       {
         "type": "kafka",
         "kafkaConsumerProps": {
           "key.deserializer":   "org.apache.kafka.common.serialization.ByteArrayDeserializer",
           "auto.offset.reset": "earliest",
           "bootstrap.servers": "localhost:9092",
           "group.id": "drill-query-consumer-1",
           "enable.auto.commit": "true",
           "value.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
           "session.timeout.ms": "30000"
         },
         "enabled": true
       }  

##System|Session Options  

You can modify the following options in Drill at the system or session level using the [ALTER SYSTEM]({{site.baseurl}}/docs/alter-system/)|[SESSION SET]({{site.baseurl}}/docs/set/) commands:  

| Option                             | Description                                                                                                                                                                                                                                                                                                                               | Example                                                                                                             |
|------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| store.kafka.record.reader          | Message   Reader implementation to use while reading messages from Kafka. Default value   is set to org.apache.drill.exec.store.kafka.decoders.JsonMessageReader. You   can implement a custom record reader by extending   org.apache.drill.exec.store.kafka.decoders.MessageReader and setting   store.kafka.record.reader accordinlgy. | ALTER   SESSION SET `store.kafka.record.reader` =   'org.apache.drill.exec.store.kafka.decoders.JsonMessageReader'; |
| store.kafka.poll.timeout           | Polling timeout used by Kafka client while   fetching messages from Kafka cluster. Default value is 200 milliseconds.                                                                                                                                                                                                                     | ALTER SESSION SET `store.kafka.poll.timeout` =   200;                                                               |
| exec.enable_union_type             | Enable support for JSON union type. Default is   false.                                                                                                                                                                                                                                                                                   | ALTER SESSION SET `exec.enable_union_type` =   true;                                                                |
| store.kafka.all_text_mode          | Reads all data from JSON files as VARCHAR.   Default is false.                                                                                                                                                                                                                                                                            | ALTER SESSION SET `store.kafka.all_text_mode` =   true;                                                             |
| store.kafka.read_numbers_as_double | Reads numbers from JSON files with or without a   decimal point as DOUBLE. Default is false.                                                                                                                                                                                                                                              | ALTER SESSION SET   `store.kafka.read_numbers_as_double` = true;                                                    |  

See [Drill JSON Model](https://drill.apache.org/docs/json-data-model/) and [Drill system options configuration](https://drill.apache.org/docs/configuration-options-introduction/)s for more information.  

##Examples of Drill Queries on Kafka  

The following examples show queries on Kafka and the use of the ALTER SESSION SET command to change Kafka related options at the session level:

       $ bin/sqlline -u jdbc:drill:zk=localhost:2181
       Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0
       apache drill 1.12.0-SNAPSHOT
       "json ain't no thang"  


       0: jdbc:drill:zk=localhost:2181> use kafka;
       +-------+------------------------------------+
       |  ok   |              summary               |
       +-------+------------------------------------+
       | true  | Default schema changed to [kafka]  |
       +-------+------------------------------------+
       1 row selected (0.564 seconds)  

       0: jdbc:drill:zk=localhost:2181> show tables;
       +---------------+------------------------------+
       | TABLE_SCHEMA  |          TABLE_NAME          |
       +---------------+------------------------------+
       | kafka         | clickstream-json-demo        |
       | kafka         | clickstream                  |
       | kafka         | clicks                       |
       +---------------+------------------------------+
       17 rows selected (1.908 seconds)

       0: jdbc:drill:zk=localhost:2181> ALTER SESSION SET `store.kafka.poll.timeout` = 200;
       +-------+------------------------------------+
       |  ok   |              summary               |
       +-------+------------------------------------+
       | true  | store.kafka.poll.timeout updated.  |
       +-------+------------------------------------+
       1 row selected (0.102 seconds)
       0: jdbc:drill:zk=localhost:2181> ALTER SESSION SET `store.kafka.record.reader` = 'org.apache.drill.exec.store.kafka.decoders.JsonMessageReader';
       +-------+-------------------------------------+
       |  ok   |               summary               |
       +-------+-------------------------------------+
       | true  | store.kafka.record.reader updated.  |
       +-------+-------------------------------------+
       1 row selected (0.082 seconds)

       0: jdbc:drill:zk=localhost:2181> select * from kafka.`clickstream-json-demo` limit 2;
       +---------------------------------------+---------------------------------------+-------------+--------------+------------------+-----------------------------------------------------------------------------------+----------------+------------------------+-------------------+-----------------+--------------------+
       |                userID                 |               sessionID               |  pageName   | refferalUrl  |    ipAddress     |                                     userAgent                                     |   client_ts    |       kafkaTopic       | kafkaPartitionId  | kafkaMsgOffset  | kafkaMsgTimestamp  |
       +---------------------------------------+---------------------------------------+-------------+--------------+------------------+-----------------------------------------------------------------------------------+----------------+------------------------+-------------------+-----------------+--------------------+
       | 6b55a8fa-d0fd-41f0-94e3-7f6b551cdede  | e3bd34a8-b546-4cd5-a0c6-5438589839fc  | categories  | bing         | 198.105.119.221  | Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0  | 1509926023098  | clickstream-json-demo  | 2                 | 2765000         | 1509926023098      |
       | 74cffc37-2df0-4db4-aff9-ed0027a12d03  | 339e3821-5254-4d79-bbae-69bc12808eca  | furniture   | bing         | 161.169.50.60    | Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0     | 1509926023099  | clickstream-json-demo  | 2                 | 2765001         | 1509926023099      |
       +---------------------------------------+---------------------------------------+-------------+--------------+------------------+-----------------------------------------------------------------------------------+----------------+------------------------+-------------------+-----------------+--------------------+
       2 rows selected (1.18 seconds)

       0: jdbc:drill:zk=localhost:2181> select count(*) from kafka.`clickstream-json-demo`;
       +---------+
       | EXPR$0  |
       +---------+
       | 980     |
       +---------+
       1 row selected (0.732 seconds)

       0: jdbc:drill:zk=localhost:2181> select kafkaPartitionId, MIN(kafkaMsgOffset) as minOffset, MAX(kafkaMsgOffset) as maxOffset from kafka.`clickstream-json-demo` group by kafkaPartitionId;
       +-------------------+------------+------------+
       | kafkaPartitionId  | minOffset  | maxOffset  |
       +-------------------+------------+------------+
       | 2                 | 2765000    | 2765244    |
       | 1                 | 2765000    | 2765244    |
       | 3                 | 2765000    | 2765244    |
       | 0                 | 2765000    | 2765244    |
       +-------------------+------------+------------+
       4 rows selected (3.081 seconds)

       0: jdbc:drill:zk=localhost:2181> select kafkaPartitionId, from_unixtime(MIN(kafkaMsgTimestamp)/1000) as minKafkaTS, from_unixtime(MAX(kafkaMsgTimestamp)/1000) as maxKafkaTs from kafka.`clickstream-json-demo` group by kafkaPartitionId;
       +-------------------+----------------------+----------------------+
       | kafkaPartitionId  |      minKafkaTS      |      maxKafkaTs      |
       +-------------------+----------------------+----------------------+
       | 2                 | 2017-11-05 15:53:43  | 2017-11-05 15:53:43  |
       | 1                 | 2017-11-05 15:53:43  | 2017-11-05 15:53:43  |
       | 3                 | 2017-11-05 15:53:43  | 2017-11-05 15:53:43  |
       | 0                 | 2017-11-05 15:53:43  | 2017-11-05 15:53:43  |
       +-------------------+----------------------+----------------------+
       4 rows selected (2.758 seconds)

       0: jdbc:drill:zk=localhost:2181> select distinct(refferalUrl) from kafka.`clickstream-json-demo`;
       +--------------+
       | refferalUrl  |
       +--------------+
       | bing         |
       | yahoo        |
       | yelp         |
       | google       |
       +--------------+
       4 rows selected (2.944 seconds)

       0: jdbc:drill:zk=localhost:2181> select pageName, count(*) from kafka.`clickstream-json-demo` group by pageName;
       +--------------+---------+
       |   pageName   | EXPR$1  |
       +--------------+---------+
       | categories   | 89      |
       | furniture    | 89      |
       | mobiles      | 89      |
       | clothing     | 89      |
       | sports       | 89      |
       | offers       | 89      |
       | shoes        | 89      |
       | books        | 89      |
       | login        | 90      |
       | electronics  | 89      |
       | toys         | 89      |
       +--------------+---------+
       11 rows selected (2.493 seconds)





 


