# Mongo storage plugin credentials

Apache Drill provides the ability to connect and submit queries to
the MongoDB cluster with enabled authentication.

## Configuring storage plugin

Drill provides numerous ways for providing credentials
that will be used for connecting to MongoDB.

### Providing username and password with the connection string

The simplest way for providing credentials is by specifying them within the mongo connection string:

```json
{
  "type": "mongo",
  "connection": "mongodb://user1:user1Pass@mongoHost:27017/",
  "batchSize": 100,
  "enabled": true
}
```

where
 - `user1` - name of the user
 - `user1Pass` - user password
 - `mongoHost` mongo host

This way of providing username and password takes precedence over all other methods.

### Providing username and password with Credentials Provider

Mongo storage plugin is integrated with Credentials provider, so it is possible to specify credentials using it.

Credentials provider creates connection string with provided username and password, 
similar to the previous section, so Drill will use this connection string when connecting to MongoDB.

Here is the example of using Plain Credentials Provider with Mongo storage plugin, but it is possible to use any
other Credentials Provider implementation (including custom ones) in a similar manner:

```json
{
  "type": "mongo",
  "connection": "mongodb://mongoHost:27017/",
  "batchSize": 100,
  "credentialsProvider": {
    "credentialsProviderType": "PlainCredentialsProvider",
    "credentials": {
      "username": "user1",
      "password": "user1Pass"
    }
  },
  "enabled": true
}
```

Please refer to [Plugin credentials provider](https://github.com/apache/drill/blob/master/docs/dev/PluginCredentialsProvider.md#developer-notes)
for more details related to Credentials Provider.
