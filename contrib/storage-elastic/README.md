# ElasticSearch Storage Plugin 

This plugin enables you to query ElasticSearch from Apache Drill.  

Tested with ElasticSearch versions:
* 5.6

## Configuration

The following configuration options are available:

* `hostsAndPorts`: This contains a list of hosts and ports for your ES cluster. This variable should contain a list of a [PROTOCOL]://[HOST]:[PORT] separated by ','. For example
: `'http://localhost:9200,http://localhost:9201'`
* `credentials`: Format should be "[USERNAME]:[PASSWORD]". For example: 'me:myPassword'. In case of null or empty String, no Authorization will be used'

```json
{
  "storage":{
    "elasticsearch" : {
      "type": "elasticsearch",
      "credentials": "user:password",
      "enabled": false,
      "hostsAndPorts": "http://localhost:9200"
    }
  }
}
```

## Querying ElasticSearch from Drill
Since Elasticsearch is a document database, each record is referred to as a document and these documents are stored in indexes. For Drill, to access docuemnts, you could 

```sql
SELECT *
FROM es.<index>.<type>
```

