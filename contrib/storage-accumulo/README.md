# Apache Accumulo Storage Plugin for Apache Drill

This storage plugin enables Apache Drill to query Apache Accumulo tables using SQL.

## Features

- **Full SQL Support**: Query Accumulo tables using standard SQL syntax
- **Schema Discovery**: Automatic schema inference from Accumulo data
- **Pushdown Optimization**: Efficient query execution with multiple pushdown strategies:
  - **Filter Pushdown**: Row key range filters translated to Accumulo Range scans
  - **Projection Pushdown**: Column family/qualifier projections pushed to Accumulo Scanner
  - **Limit Pushdown**: LIMIT clauses pushed to reduce data scanning
  - **Sort Pushdown**: ORDER BY row_key uses Accumulo's natural ordering
- **Authentication**: Support for password and Kerberos authentication
- **User Impersonation**: Per-user query execution with delegation tokens

## Requirements

- Apache Drill 1.23.0 or later
- Apache Accumulo 2.1.x (tested with 2.1.4 LTS)
- Java 11 or later

## Installation

The Accumulo storage plugin is included in the Drill distribution. No additional installation is required.

## Configuration

### Password Authentication (Basic)

Configure the plugin through the Drill Web UI (http://localhost:8047/storage) or via REST API:

```json
{
  "type": "accumulo",
  "zookeeperQuorum": "localhost:2181",
  "instanceName": "accumulo",
  "username": "root",
  "password": "secret",
  "enabled": true
}
```

### Kerberos Authentication

For enterprise environments with Kerberos, configure the plugin with Kerberos authentication:

#### Shared User Mode (Service Principal Only)

All queries run as the service principal. Simple setup, but no per-user authorization.

```json
{
  "type": "accumulo",
  "zookeeperQuorum": "zk1:2181,zk2:2181,zk3:2181",
  "instanceName": "accumulo",
  "authenticationType": "KERBEROS",
  "principal": "drill/drillserver.example.com@EXAMPLE.COM",
  "keytabPath": "/etc/security/keytabs/drill.keytab",
  "saslQop": "auth",
  "authMode": "SHARED_USER",
  "enabled": true
}
```

#### User Translation Mode (Per-User Credentials)

Each Drill user has their own Accumulo credentials stored in the credentials provider. The plugin looks up credentials based on the query user.

```json
{
  "type": "accumulo",
  "zookeeperQuorum": "zk1:2181,zk2:2181,zk3:2181",
  "instanceName": "accumulo",
  "authMode": "USER_TRANSLATION",
  "credentialsProvider": {
    "credentialsProviderType": "PlainCredentialsProvider",
    "credentials": {},
    "userCredentials": {
      "alice": {"username": "accumulo_alice", "password": "alice_pass"},
      "bob": {"username": "accumulo_bob", "password": "bob_pass"}
    }
  },
  "enabled": true
}
```

#### User Impersonation Mode (Delegation Tokens)

The service authenticates with Kerberos, then impersonates the Drill query user via delegation tokens. This enables per-user authorization and audit trails.

```json
{
  "type": "accumulo",
  "zookeeperQuorum": "zk1:2181,zk2:2181,zk3:2181",
  "instanceName": "accumulo",
  "authenticationType": "KERBEROS",
  "principal": "drill/drillserver.example.com@EXAMPLE.COM",
  "keytabPath": "/etc/security/keytabs/drill.keytab",
  "saslQop": "auth-conf",
  "accumuloServicePrimary": "accumulo",
  "useDelegationTokens": true,
  "authMode": "USER_IMPERSONATION",
  "enabled": true
}
```

### Configuration Properties

#### Connection Properties

| Property | Description | Default |
|----------|-------------|---------|
| `zookeeperQuorum` | Comma-separated list of ZooKeeper servers (host:port) | Required |
| `instanceName` | Accumulo instance name | Required |

#### Password Authentication Properties

| Property | Description | Default |
|----------|-------------|---------|
| `username` | Accumulo username | Required for PASSWORD auth |
| `password` | Accumulo password | Required for PASSWORD auth |
| `credentialsProvider` | Alternative credential provider | null |

#### Kerberos Authentication Properties

| Property | Description | Default |
|----------|-------------|---------|
| `authenticationType` | Authentication type: `PASSWORD` or `KERBEROS` | `PASSWORD` |
| `principal` | Kerberos principal (e.g., `drill/host@REALM`) | Required for KERBEROS |
| `keytabPath` | Path to the Kerberos keytab file | Required for KERBEROS |
| `saslQop` | SASL Quality of Protection: `auth`, `auth-int`, `auth-conf` | `auth` |
| `accumuloServicePrimary` | Accumulo service principal primary name | `accumulo` |

#### User Impersonation Properties

| Property | Description | Default |
|----------|-------------|---------|
| `authMode` | Authorization mode: `SHARED_USER` or `USER_IMPERSONATION` | `SHARED_USER` |
| `useDelegationTokens` | Enable delegation tokens for distributed execution | `false` |

#### Optional Properties

| Property | Description | Default |
|----------|-------------|---------|
| `schemaMetadataTable` | Table for schema metadata | `_drill_schema` |
| `clientTimeout` | Client operation timeout (ms) | `30000` |
| `batchScannerThreads` | Number of batch scanner threads | `10` |

### SASL Quality of Protection (QoP)

| QoP Value | Description |
|-----------|-------------|
| `auth` | Authentication only (default) |
| `auth-int` | Authentication + integrity protection |
| `auth-conf` | Authentication + integrity + confidentiality (encryption) |

For production environments, `auth-conf` is recommended for full encryption of data in transit.

## Usage

### Basic Queries

```sql
-- Select all columns from a table
SELECT * FROM accumulo.`my_table`;

-- Select specific columns. Reaching into a column family requires a table alias
-- (`t` below), the same as for the HBase plugin.
SELECT row_key, t.cf.column1, t.cf.column2 FROM accumulo.`my_table` t;

-- Select entire column family
SELECT personal FROM accumulo.`users`;
```

Values, including the row key, come back as `VARBINARY`. Decode them with
`CONVERT_FROM` to compare or display them as text:

```sql
SELECT CONVERT_FROM(row_key, 'UTF8') AS row_key,
       CONVERT_FROM(t.cf.name, 'UTF8') AS name
FROM accumulo.`my_table` t;
```

### Filter Queries

```sql
-- Row key equality
SELECT * FROM accumulo.`my_table` WHERE row_key = 'row_001';

-- Row key range
SELECT * FROM accumulo.`my_table`
WHERE row_key >= 'row_100' AND row_key < 'row_200';
```

### Limit Queries

```sql
-- Limit results (pushed down to Accumulo)
SELECT * FROM accumulo.`my_table` LIMIT 100;
```

### Sort Queries

```sql
-- Order by row key (uses Accumulo's natural ordering)
SELECT * FROM accumulo.`my_table` ORDER BY row_key ASC;
```

### Combined Queries

```sql
-- Filter, project, sort, and limit
SELECT row_key, t.cf.name, t.cf.value
FROM accumulo.`my_table` t
WHERE row_key >= 'row_100'
ORDER BY row_key ASC
LIMIT 50;
```

## Data Model

### Row Key

The Accumulo row key is exposed as a special column named `row_key` of type `VARBINARY`.

### Column Families

Each Accumulo column family is exposed as a Drill MAP type. Column qualifiers within a family become fields in the map:

```
Accumulo: row_001 -> personal:first_name = "John", personal:last_name = "Doe"
Drill:    row_key = 'row_001', personal = {first_name: "John", last_name: "Doe"}
```

Accumulo, unlike HBase, keeps no catalog of its column families, so when a table has
no entry in the schema metadata table the plugin infers them by reading the first
1000 entries of the table at plan time. A family that appears only later in the table
will not be visible to the planner; define an explicit schema for tables where that
matters.

### Data Types

All values are stored as `VARBINARY` by default. Use Drill's CAST functions for type conversion:

```sql
SELECT
  row_key,
  CAST(CONVERT_FROM(t.cf.age, 'UTF8') AS INT) as age,
  CAST(CONVERT_FROM(t.cf.salary, 'UTF8') AS DOUBLE) as salary
FROM accumulo.`employees` t;
```

## Schema Metadata (Optional)

For better schema support, you can store table schemas in a metadata table. Create a table `_drill_schema` (configurable) with the following structure:

- Row key: table name
- Column family: `schema`
- Qualifiers: column definitions in format `column_family:qualifier:type`

Example:
```
_drill_schema -> employees -> schema:cf.name = "VARCHAR"
                            -> schema:cf.age = "INT"
                            -> schema:cf.salary = "DOUBLE"
```

## Kerberos Setup Guide

### Prerequisites

1. **Kerberos KDC**: A running Kerberos KDC with principals configured
2. **Accumulo with Kerberos**: Accumulo configured for Kerberos authentication
3. **Service Principal**: Create a principal for Drill (e.g., `drill/hostname@REALM`)
4. **Keytab**: Export the keytab for the Drill principal

### Steps

1. **Create Drill Service Principal**
   ```bash
   kadmin -q "addprinc -randkey drill/drillserver.example.com@EXAMPLE.COM"
   kadmin -q "xst -k /etc/security/keytabs/drill.keytab drill/drillserver.example.com@EXAMPLE.COM"
   ```

2. **Configure Drill Impersonation** (if using USER_IMPERSONATION mode)

   Enable impersonation in `drill-override.conf`:
   ```
   drill.exec.impersonation.enabled: true
   ```

3. **Configure Accumulo for Delegation Tokens** (if using USER_IMPERSONATION mode)

   Ensure Accumulo is configured to support delegation tokens. This typically requires:
   - `general.kerberos.keytab` and `general.kerberos.principal` set in `accumulo.properties`
   - Accumulo master configured for token generation

4. **Configure the Storage Plugin**

   Use the Drill Web UI or REST API to configure the plugin with your Kerberos settings.

### Troubleshooting Kerberos

- **Check keytab validity**: `klist -kt /path/to/drill.keytab`
- **Test kinit**: `kinit -kt /path/to/drill.keytab drill/hostname@REALM`
- **Verify Accumulo connection**: Use Accumulo shell with Kerberos to verify connectivity
- **Check Drill logs**: Enable debug logging for `org.apache.drill.exec.store.accumulo`

## Performance Considerations

1. **Row Key Filters**: Always filter on `row_key` when possible - these filters are pushed down to Accumulo as Range scans.

2. **Projection**: Select only the columns you need - column projections are pushed to Accumulo's `fetchColumn()` API.

3. **Limit**: Use LIMIT when you only need a subset of rows - limits are pushed down to stop scanning early.

4. **Batch Size**: The default batch size of 4000 rows works well for most queries. Adjust `batchScannerThreads` for parallel scanning.

5. **User Impersonation**: When using delegation tokens, tokens are cached per-user with a 1-hour TTL to minimize overhead.

## Troubleshooting

### Connection Issues

If you see connection errors, verify:
1. ZooKeeper is running and accessible
2. Accumulo instance name is correct
3. Credentials are valid
4. Network connectivity to ZooKeeper and Accumulo tablet servers

### Kerberos Authentication Issues

1. Verify keytab file exists and is readable
2. Check principal format matches the keytab
3. Ensure KDC is accessible from Drill nodes
4. Verify Accumulo SASL settings match Drill configuration

### Query Performance

For slow queries:
1. Check if row key filters can be added
2. Use EXPLAIN to verify pushdowns are working
3. Consider reducing batch size for memory-constrained environments

## Development

### Building

```bash
mvn clean install -pl contrib/storage-accumulo -DskipTests
```

### Running Tests

```bash
# Unit tests
mvn test -pl contrib/storage-accumulo

# Integration tests (requires MiniAccumuloCluster)
mvn test -Dtest=AccumuloIntegrationTestsSuite -pl contrib/storage-accumulo

# Kerberos tests (requires Kerberos-enabled cluster)
mvn test -pl contrib/storage-accumulo \
  -Ddrill.accumulo.kerberos.enabled=true \
  -Ddrill.accumulo.principal=drill/host@REALM \
  -Ddrill.accumulo.keytab=/path/to/keytab
```

## Authentication Modes Summary

| Mode | Auth Type | Description | Use Case |
|------|-----------|-------------|----------|
| PASSWORD + SHARED_USER | Password | Username/password for all queries | Development, simple deployments |
| PASSWORD + USER_TRANSLATION | Password | Per-user Accumulo credentials from CredentialsProvider | Multi-user with separate Accumulo accounts |
| KERBEROS + SHARED_USER | Kerberos | Service principal for all queries | Enterprise, single service account |
| KERBEROS + USER_IMPERSONATION | Kerberos + Delegation Tokens | Service authenticates, then impersonates Drill user | Enterprise, per-user audit/authorization |

## Future Enhancements

Planned features for future releases:
- Custom Accumulo iterators for server-side processing
- Visibility/Authorization support
- Write support (INSERT/UPDATE/DELETE)
- Statistics-based cost estimation

## License

Apache License 2.0
