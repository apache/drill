---
title: "Hive Metadata Caching"
date: 2018-12-08
parent: "Performance Tuning"
---

Drill caches Hive metadata in a Hive metastore client cache that resides in Drill instead of accessing the Hive metastore directly. During a query, Drill can access metadata faster from the cache than from the Hive metastore. By default, the Hive metastore client cache has a TTL (time to live) of 60 seconds. The TTL is how long cache entries exist before the cache reloads metadata from the Hive metastore. Drill expires an entry in the cache 60 seconds after the following events:  

*  creation of the entry
*  a read or write operation on the entry
*  the most recent replacement of the entry value  

You can modify the TTL depending on how frequently the Hive metadata is updated. If the Hive metadata is updated frequently, decrease the cache TTL value. If Hive metadata is updated infrequently, increase the cache TTL value.

For example, when you run a Drill query on a Hive table, Drill refreshes the cache 60 seconds after the read on the table. If the table is updated in Hive within that 60 second window and you issue another query on the table, Drill may not be aware of the changes until the cache expires. In such a scenario where Hive metadata is changing so quickly, you could reduce the cache TTL to 2 seconds so that Drill refreshes the cache more frequently.  

## Configuring the Cache  

As of Drill 1.5, you can modify the Hive storage plugin to change the rate at which the cache is reloaded. You can also modify whether the cache reloads after reads and writes or just writes.  

{% include startnote.html %}The configuration applies specifically to the storage plugin that you modify. If you have multiple Hive storage plugins configured in Drill, the configuration does not apply globally. You can configure a different caching policy for each Hive metastore server.{% include endnote.html %}  

To configure the Hive metastore client cache in Drill, complete the following steps:  

1. Start the [Drill Web UI]({{site.baseurl}}/docs/starting-the-web-console/).
2. Select the **Storage** tab.
3. Click **Update** next to the “hive” storage plugin.
4. Add the following parameters:  

              "hive.metastore.cache-ttl-seconds": "<value>",
              "hive.metastore.cache-expire-after": "<value>"  
The `cache-ttl-seconds` value can be any non-negative value, including 0, which turns caching off. The `cache-expire-after` value can be “`access`” or “`write`”. Access indicates expiry after a read or write operation, and write indicates expiry after a write operation only.
5. **Enable** the storage plugin to save the changes.  

Example:  

       {
             "type": "hive",
             "enabled": true,
             "configProps": {
               "hive.metastore.uris": "",
               "javax.jdo.option.ConnectionURL": "jdbc:derby:;databaseName=../sample-data/drill_hive_db;create=true",
               "hive.metastore.warehouse.dir": "/tmp/drill_hive_wh",
               "fs.default.name": "file:///",
               "hive.metastore.sasl.enabled": "false"
        	   "hive.metastore.cache-ttl-seconds": "2",
               "hive.metastore.cache-expire-after": "access"
        
         	  }
       	}
