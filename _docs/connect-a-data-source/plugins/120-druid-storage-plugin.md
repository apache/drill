---
title: "Druid Storage Plugin"
slug: "Druid Storage Plugin"
parent: "Connect a Data Source"
---

**Introduced in release:** 1.18

The Druid storage plugin allows you to perform SQL queries against [Apache Druid](https://Druid.apache.org/) data sources.

### Tested Druid versions

[0.16.0-incubating](https://github.com/apache/incubator-Druid/releases/tag/Druid-0.16.0-incubating)

## Configuration

The plugin can be registered in Apache Drill using the drill web interface by navigating to the ```storage``` page.  

### Configuration Options

|---------------------|-----------------------|-------------------------------------------|
| Option              | Default               | Description                               |
|---------------------|-----------------------|-------------------------------------------|
| type                | (none)                | Set to "Druid" to make use of this plugin |
| brokerAddress       | http://localhost:8082 | Web address of the Druid broker           |
| coordinatorAddress  | http://localhost:8081 | Web address of the Druid coodinator       |
| averageRowSizeBytes | 100                   |                                           |
| enabled             | false                 | Set to true to enable this storage plugin |
|---------------------|-----------------------|-------------------------------------------|

### Example Configuration

    {
      "type" : "Druid",
      "brokerAddress" : "http://localhost:8082",
      "coordinatorAddress": "http://localhost:8081",
      "averageRowSizeBytes": 100,
      "enabled" : false
    }

## Usage Notes

### Druid API

Druid supports multiple native queries to address sundry use-cases.  To fetch raw Druid rows, Druid's API support two forms of query, [Select](https://Druid.apache.org/docs/latest/querying/select-query.html) (no relation to SQL) and [Scan](https://Druid.apache.org/docs/latest/querying/scan-query.html).  Currently, this plugin uses the Select query API to fetch raw rows from Druid as json.

### Filter Push-Down

Filters are pushed down to native Druid filter structure, converting SQL where clauses to the respective Druid [Filters](https://Druid.apache.org/docs/latest/querying/filters.html).

