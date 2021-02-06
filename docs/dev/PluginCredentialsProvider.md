# Plugin credentials provider

Drill provides a variety of ways for specifying credentials for storage plugins.
Though all storage plugin credentials may be stored in Zookeeper, it may be unsafe to specify them directly in the plugin configs.

Here is the example for specifying storage plugin credentials directly:
```json
{
  "type": "jdbc",
  "driver": "xxx.Driver",
  "url": "jdbc:xxx:xxx",
  "username": "xxx",
  "password": "xxx"
}
```

Drill provides `credentialsProvider` property for specifying desired credential provider type and its configs
instead of holding raw credentials in storage plugin configs.

## Using credentials from Hadoop Configuration

One of the implementations for credentials provider is `HadoopCredentialsProvider` that allows using Hadoop 
Configuration property values as credentials.
To use it, `credentialsProviderType` property should be set to `HadoopCredentialsProvider`:
```json
{
  "type": "jdbc",
  "driver": "xxx.Driver",
  "url": "jdbc:xxx:xxx",
  "credentialsProvider": {
    "credentialsProviderType": "HadoopCredentialsProvider",
    "propertyNames": {
      "username": "hadoop.user.property",
      "password": "hadoop.password.property"
    }
  }
}
```

`propertyNames` map contains keys that specify which credential will be obtained from the Hadoop Configuration 
property with the name of the `propertyNames` value.

For example, user may create in drill config directory `core-site.xml` config file with the following content:
```xml
<configuration>

    <property>
        <name>hadoop.user.property</name>
        <value>user1</value>
    </property>

    <property>
        <name>hadoop.password.property</name>
        <value>user1Pass</value>
    </property>

</configuration>
```

In this case, the storage `jdbc` plugin will use `user1` value as the `username` and `user1Pass` value as its `password`.

## Using credentials from Environment Variables

`EnvCredentialsProvider` credentials provider implementation allows using Environment Variable values as plugin credentials.
This way for specifying credentials is consistent with the Kubernetes world, where different user secrets and configmaps may be exposed as environment variables to be used by a container.

To use it, `credentialsProviderType` property should be set to `EnvCredentialsProvider`:
```json
{
  "type": "jdbc",
  "driver": "xxx.Driver",
  "url": "jdbc:xxx:xxx",
  "credentialsProvider": {
    "credentialsProviderType": "EnvCredentialsProvider",
    "envVariableNames": {
      "username": "USER_NAME",
      "password": "USER_PASSWORD"
    }
  }
}
```

`propertyNames` map contains keys that specify which credential will be obtained from the Environment Variable
value with the name of the `propertyNames` value.

For example, user may export the following variables:
```shell
export USER_NAME='user1'
export USER_PASSWORD='user1Pass'
```

In this case, the storage `jdbc` plugin will use `user1` value as the `username` and `user1Pass` value as its `password`.

## Using credentials managed by Vault

`VaultCredentialsProvider` credentials provider implementation allows using Vault secrets as plugin credentials.

Before using this credential provider, the following Drill properties should be configured in `drill-override.conf`:
```
"drill.exec.storage.vault.address" - address of the Vault server
"drill.exec.storage.vault.token" - token used to access Vault
```

Once it is set, we can configure storage plugin to use this way of obtaining credentials:
```json
{
  "type": "jdbc",
  "driver": "xxx.Driver",
  "url": "jdbc:xxx:xxx",
  "credentialsProvider": {
    "credentialsProviderType": "VaultCredentialsProvider",
    "secretPath": "secret/jdbc",
    "propertyNames": {
      "username": "usernameSecret",
      "password": "passwordSecret"
    }
  }
}
```

`secretPath` property specifies the Vault key value from which to read
`propertyNames` map contains keys that specify which credential will be obtained from the Vault secret with the secret name of the `propertyNames` value.

For example, user may store the following secrets in the Vault:
```shell
vault kv put secret/jdbc usernameSecret=muser passwordSecret=mpassword
```

In this case, the storage `jdbc` plugin will use `user1` value as the `username` and `user1Pass` value as its `password`.

## Specifying credentials inlined using credentials provider

To be consistent with credentials providers implementations, Drill provides a new way of specifying credentials directly in storage plugin config:
```json
{
  "type": "jdbc",
  "driver": "xxx.Driver",
  "url": "jdbc:xxx:xxx",
  "credentialsProvider": {
    "credentialsProviderType": "PlainCredentialsProvider",
    "credentials": {
      "username": "user1",
      "password": "user1Pass"
    }
  }
}
```

This way of specifying credentials directly should be used instead of the old one since it groups credentials and
makes it easier to replace `PlainCredentialsProvider` with a more secured alternative.

# Developer notes

`CredentialsProvider` is a base interface for all credential provider implementations.
Users may create and use their own credential provider implementations without changing Drill code.
To achieve that, just add dependency on the `drill-logical` jar (where `CredentialsProvider` interface is placed),
create own implementation of this interface, and create `drill-module.conf` file that will add implementation class 
to Drill classpath scanning, for example if the class will have the following full name: `foo.bar.package.ExampleCredentialsProvider`,
`drill-module.conf` content should be the following:
```
drill.classpath.scanning: {
  packages += "foo.bar.package"
}
```
