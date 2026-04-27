<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Apache Drill Microsoft Sentinel Storage Plugin

A read-only Apache Drill storage plugin that enables native querying of Microsoft Sentinel data with comprehensive query pushdown support. This plugin translates SQL queries into KQL (Kusto Query Language) and executes them against the Azure Log Analytics Query API, supporting filter, project, limit, sort, and aggregate pushdowns.

## Features

- **Native Microsoft Sentinel Integration**: Query Sentinel tables directly from Drill without data export
- **Comprehensive Query Pushdown**: Supports filter, project, limit, sort, and aggregate operations pushed down to KQL
- **KQL Translation**: Automatic conversion of SQL WHERE, SELECT, ORDER BY, LIMIT, and GROUP BY clauses to KQL pipeline syntax
- **OAuth2 Authentication**: Uses Azure AD client_credentials grant for secure authentication
- **Columnar Data Handling**: Efficiently transposes columnar JSON responses from Log Analytics API into Drill row format
- **Type Mapping**: Full support for KQL types (string, int, long, real, bool, datetime, dynamic)
- **Pagination Support**: Handles large result sets through @odata.nextLink pagination
- **Read-Only**: Designed for analytics and querying; no write operations supported

## Prerequisites

### System Requirements
- Apache Drill 1.23.0 or later
- Java 11 or higher
- Network access to `api.loganalytics.io` and `login.microsoftonline.com`

### Azure Requirements
- Microsoft Sentinel workspace configured in Azure Log Analytics
- Azure AD tenant with an application registration
- Log Analytics workspace ID (GUID format)
- Azure AD tenant ID
- Application registration with:
  - Client ID
  - Client Secret
  - API permission: `https://api.loganalytics.io/user_impersonation` (or use default scope `https://api.loganalytics.io/.default`)

### Installation

1. **Build the Plugin**
   ```bash
   mvn clean package -pl contrib/storage-sentinel -DskipTests
   ```

2. **Deploy to Drill**
   Copy the JAR to your Drill installation:
   ```bash
   cp contrib/storage-sentinel/target/drill-storage-sentinel-1.23.0-SNAPSHOT.jar \
      $DRILL_HOME/jars/3rdparty/
   ```

3. **Restart Drill**
   ```bash
   $DRILL_HOME/bin/drillbit.sh restart
   ```

## Configuration

The plugin is configured through Drill's storage plugin interface. Use the Drill Web UI or REST API to create a new storage plugin with type `sentinel`.

### Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `workspaceId` | String | Yes | - | Azure Log Analytics workspace ID (GUID) |
| `tenantId` | String | Yes | - | Azure AD tenant ID |
| `clientId` | String | Yes | - | Azure AD application client ID |
| `clientSecret` | String | Yes | - | Azure AD application client secret |
| `defaultTimespan` | String | No | `P1D` | ISO 8601 duration for query time range (e.g., `P7D` for 7 days, `PT1H` for 1 hour) |
| `maxRows` | Integer | No | `10000` | Safety limit; automatically appended as `\| take N` to all queries |
| `tables` | List<String> | No | Built-in list | Explicit table name list; if empty, uses default Sentinel tables |

### Web UI Configuration

1. Navigate to **Storage Plugins** in the Drill Web UI
2. Click **Create** and enter:
   ```json
   {
     "type": "sentinel",
     "workspaceId": "12345678-abcd-efgh-ijkl-mnopqrstuvwx",
     "tenantId": "abcdef01-2345-6789-abcd-ef0123456789",
     "clientId": "12345678-1234-1234-1234-123456789012",
     "clientSecret": "your-secret-here~abcdefghijklmn-._",
     "defaultTimespan": "P7D",
     "maxRows": 50000,
     "tables": ["SecurityAlert", "SecurityEvent", "CommonSecurityLog"]
   }
   ```
3. Click **Create Plugin**

### REST API Configuration

```bash
curl -X POST http://localhost:8047/storage/sentinel \
  -H "Content-Type: application/json" \
  -d '{
    "type": "sentinel",
    "workspaceId": "12345678-abcd-efgh-ijkl-mnopqrstuvwx",
    "tenantId": "abcdef01-2345-6789-abcd-ef0123456789",
    "clientId": "12345678-1234-1234-1234-123456789012",
    "clientSecret": "your-secret-here~abcdefghijklmn-._",
    "defaultTimespan": "P7D",
    "maxRows": 50000
  }'
```

## Usage

### Listing Tables

View all available Sentinel tables:
```sql
SHOW TABLES IN sentinel;
```

Default available tables (when no explicit list is configured):
- SecurityAlert
- SecurityEvent
- CommonSecurityLog
- AuditLogs
- SigninLogs
- AADNonInteractiveUserSignInLogs
- AADServicePrincipalSignInLogs
- Heartbeat
- Syslog
- Event
- W3CIISLog
- WindowsFirewall
- NetworkMonitoring
- DNSEvents
- OfficeActivity

### Basic Queries

**Simple SELECT:**
```sql
SELECT * FROM sentinel.SecurityAlert LIMIT 100;
```

**Column projection:**
```sql
SELECT AlertName, Severity, TimeGenerated 
FROM sentinel.SecurityAlert;
```

**Filtering:**
```sql
SELECT * FROM sentinel.SecurityAlert
WHERE Severity = 'High' AND Status = 'New'
LIMIT 50;
```

### Query Pushdown Examples

All examples below demonstrate queries that are fully pushed down to KQL, eliminating the need for Drill to process the data.

**WHERE clause pushdown:**
```sql
SELECT * FROM sentinel.SecurityAlert
WHERE Severity = 'High';
-- Becomes: SecurityAlert | where Severity == "High"
```

**Multiple conditions with AND/OR:**
```sql
SELECT * FROM sentinel.SecurityAlert
WHERE (Severity = 'High' OR Severity = 'Critical')
  AND Status != 'Dismissed';
-- Becomes: SecurityAlert | where (Severity == "High" or Severity == "Critical") and (Status != "Dismissed")
```

**IS NULL / IS NOT NULL:**
```sql
SELECT * FROM sentinel.SecurityAlert
WHERE AlertName IS NOT NULL AND SourceIP IS NULL;
-- Becomes: SecurityAlert | where isnotnull(AlertName) and isnull(SourceIP)
```

**LIKE pattern matching:**
```sql
SELECT * FROM sentinel.SecurityAlert
WHERE AlertName LIKE 'Malware%';
-- Becomes: SecurityAlert | where AlertName startswith "Malware"
```

**Column projection:**
```sql
SELECT AlertName, Severity, TimeGenerated, SourceIP
FROM sentinel.SecurityAlert;
-- Becomes: SecurityAlert | project AlertName, Severity, TimeGenerated, SourceIP
```

**Sorting:**
```sql
SELECT AlertName, Severity
FROM sentinel.SecurityAlert
ORDER BY TimeGenerated DESC, Severity ASC;
-- Becomes: SecurityAlert | sort by TimeGenerated desc, Severity asc
```

**LIMIT/TAKE:**
```sql
SELECT TOP 500 AlertName, Severity
FROM sentinel.SecurityAlert;
-- Becomes: SecurityAlert | take 500
```

**GROUP BY with aggregates:**
```sql
SELECT AlertName, COUNT(*) as AlertCount, 
       SUM(ConfidenceLevel) as TotalConfidence,
       MIN(ConfidenceLevel) as MinConfidence,
       MAX(ConfidenceLevel) as MaxConfidence,
       AVG(ConfidenceLevel) as AvgConfidence
FROM sentinel.SecurityAlert
GROUP BY AlertName;
-- Becomes: SecurityAlert | summarize count(), sum(ConfidenceLevel), min(ConfidenceLevel), max(ConfidenceLevel), avg(ConfidenceLevel) by AlertName
```

**Complex query with all pushdowns:**
```sql
SELECT Severity, COUNT(*) as AlertCount
FROM sentinel.SecurityAlert
WHERE TimeGenerated > CAST('2026-04-20' AS DATE)
  AND Status = 'New'
  AND Severity IN ('High', 'Critical')
GROUP BY Severity
ORDER BY AlertCount DESC
LIMIT 100;
-- Becomes: SecurityAlert | where (Severity == "High" or Severity == "Critical") and Status == "New" | summarize count() by Severity | sort by count_ desc | take 100
```

## Query Pushdown Support

The plugin supports comprehensive pushdown of relational operations. When operations cannot be pushed down, Drill performs them locally. The following table shows what is supported:

| SQL Operation | KQL Equivalent | Pushdown Support | Notes |
|---|---|---|---|
| WHERE col = val | `\| where col == "val"` | ✅ Yes | Comparison operators: =, <>, <, <=, >, >= |
| WHERE col IN (...) | `\| where col == "a" or col == "b"` | ✅ Yes | Translated to OR chain |
| WHERE col LIKE 'prefix%' | `\| where col startswith "prefix"` | ✅ Yes | Prefix matching only |
| WHERE col LIKE '%val%' | `\| where col contains "val"` | ✅ Yes | Substring matching |
| WHERE col IS NULL | `\| where isnull(col)` | ✅ Yes | Null checking |
| WHERE col AND/OR conditions | `\| where (col1 == "a") and (col2 == "b")` | ✅ Yes | Logical operators with parentheses |
| SELECT cols | `\| project col1, col2` | ✅ Yes | Column projection |
| SELECT DISTINCT | Not supported | ❌ No | Handled by Drill |
| LIMIT N | `\| take N` | ✅ Yes | Applied after all operations |
| ORDER BY col [ASC\|DESC] | `\| sort by col [asc\|desc]` | ✅ Yes | Single and multiple columns |
| GROUP BY cols | `\| summarize ... by col` | ✅ Yes | Without HAVING clause |
| COUNT(*) | `count()` | ✅ Yes | All aggregate variants supported |
| SUM(col) | `sum(col)` | ✅ Yes | - |
| AVG(col) | `avg(col)` | ✅ Yes | - |
| MIN(col) | `min(col)` | ✅ Yes | - |
| MAX(col) | `max(col)` | ✅ Yes | - |
| JOIN | Not supported | ❌ No | Semantics differ; filter and union instead |
| UNION | Not supported | ❌ No | Use separate queries |
| HAVING clause | Not supported | ❌ No | Use subqueries with WHERE instead |
| Window functions | Not supported | ❌ No | Not available in KQL |

## Type Mapping

The plugin maps KQL data types to Drill types as follows:

| KQL Type | Drill Type | Notes |
|----------|-----------|-------|
| `string` | VARCHAR | Default string type |
| `int` | INTEGER | 32-bit signed integer |
| `long` | BIGINT | 64-bit signed integer |
| `real` | FLOAT8 | Double-precision floating point |
| `decimal` | FLOAT8 | Converted to double for compatibility |
| `bool` | BIT | Boolean (true/false) |
| `datetime` | TIMESTAMP | ISO 8601 format with timezone |
| `timespan` | VARCHAR | Time duration as string |
| `dynamic` | VARCHAR | JSON objects as string (not parsed) |
| `guid` | VARCHAR | UUID as string |
| `null` | NULL | Null value |

## Architecture Overview

### Component Hierarchy

```
SentinelStoragePlugin
├── SentinelStoragePluginConfig (configuration & credentials)
├── SentinelTokenManager (OAuth2 authentication)
├── SentinelSchemaFactory
│   └── SentinelSchema
│       └── DynamicDrillTable (per table)
├── SentinelGroupScan (query optimization & planning)
│   └── SentinelSubScan (physical plan)
│       └── SentinelScanBatchCreator
│           └── SentinelBatchReader (execution)
└── SentinelPluginImplementor (query pushdown planning)
    └── RexToKqlConverter (expression translation)
```

### Query Execution Flow

1. **Planning Phase**
   - SQL query parsed by Calcite optimizer
   - `SentinelPluginImplementor` evaluates if operations can be pushed down
   - Relational algebra tree (Calcite RexNode) converted to KQL via `RexToKqlConverter`
   - Accumulated KQL stored in `SentinelScanSpec`

2. **Physical Planning**
   - `SentinelGroupScan` creates execution plan with `SentinelSubScan`
   - Cost estimates help Drill optimizer decide between pushdown and local execution

3. **Execution**
   - `SentinelScanBatchCreator` instantiates `SentinelBatchReader`
   - `SentinelBatchReader` obtains bearer token from `SentinelTokenManager`
   - HTTP POST to Log Analytics API with complete KQL query
   - Columnar JSON response parsed into row batches
   - Column writers convert KQL types to Drill types
   - Results streamed back to Drill for aggregation or local processing

### Authentication Flow

1. Plugin initialized with tenant ID, client ID, and client secret
2. On first query, `SentinelTokenManager` requests token:
   - POST to `https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token`
   - Request body: `grant_type=client_credentials&scope=https://api.loganalytics.io/.default`
   - Response includes `access_token` and `expires_in` (seconds)
3. Token cached and reused for subsequent queries
4. 60 seconds before expiry, token automatically refreshed
5. Refresh failures trigger exception; query fails with diagnostic message

## Testing

Comprehensive unit tests are included with the plugin. Run tests with:

```bash
mvn -pl contrib/storage-sentinel test
```

### Test Coverage

- **TestSentinelTokenManager** (13 tests): OAuth2 authentication, token caching, bearer token formatting
- **TestSentinelQueryBuilder** (19 tests): KQL query generation for all pushdown operations and expression types
- **TestSentinelBatchReader** (7 tests): JSON response parsing, type conversion, pagination, null handling
- **TestSentinelPushDowns** (9 tests): Integration tests for all pushdown capabilities
- **Test Fixtures** (4 JSON files): Mock responses for SecurityAlert, empty results, pagination, and aggregates

All 48 tests pass with no external dependencies (uses MockWebServer for HTTP mocking).

## Security Considerations

### Credential Management

- **Client Secret Storage**: Store in Drill's encrypted credential store, not in configuration files
- **Token Caching**: Tokens held in memory only; not persisted to disk
- **Bearer Token**: Transmitted over HTTPS only to Log Analytics API
- **Audit Logging**: All queries appear in Azure Log Analytics audit logs under the service principal

### Access Control

- **Workspace-Level**: Plugin configured with single workspace ID; cannot access other workspaces
- **Service Principal**: Permissions determined by Azure RBAC on the workspace
- **Table-Level**: Restrict tables via `tables` configuration parameter if needed
- **Query Auditing**: Enable Log Analytics audit logs to track all queries executed by the service principal

### Best Practices

1. Use separate service principals for different environments (dev, staging, prod)
2. Rotate client secrets regularly (e.g., every 90 days)
3. Grant service principal minimum required Log Analytics Reader role
4. Monitor authentication failures in Azure AD sign-in logs
5. Use network security groups to restrict access to Log Analytics API endpoints
6. Encrypt Drill configuration files at rest
7. Enable HTTP/TLS inspection to prevent token interception

## Performance Tuning

### Configuration Optimization

```json
{
  "workspaceId": "...",
  "tenantId": "...",
  "clientId": "...",
  "clientSecret": "...",
  "defaultTimespan": "P1D",
  "maxRows": 100000
}
```

- **defaultTimespan**: Narrow to specific time windows in WHERE clause for faster queries
- **maxRows**: Increase if you need larger result sets, but watch memory usage

### Query Optimization

1. **Always include time filters**: Log Analytics indexes on TimeGenerated
   ```sql
   WHERE TimeGenerated > CAST('2026-04-20' AS DATE)
   ```

2. **Push filters early**: More selective filters earlier in pipeline
   ```sql
   -- Good: filters early
   WHERE Severity = 'High' AND SourceIP = '1.2.3.4'
   
   -- Less efficient: joins filter only after aggregation
   SELECT SourceIP, COUNT(*) FROM SecurityAlert GROUP BY SourceIP
   HAVING SourceIP = '1.2.3.4'
   ```

3. **Use projections**: Select only needed columns
   ```sql
   SELECT AlertName, Severity  -- Pushes column filter to Log Analytics
   FROM sentinel.SecurityAlert;
   ```

4. **Aggregate at source**: GROUP BY is pushed to KQL
   ```sql
   -- Pushed to Log Analytics
   SELECT Severity, COUNT(*) FROM SecurityAlert GROUP BY Severity;
   ```

5. **Use LIMIT/TAKE**: Reduces data transferred
   ```sql
   SELECT TOP 1000 * FROM SecurityAlert;  -- Only returns 1000 rows
   ```

## Troubleshooting

### Common Issues

#### "Failed to obtain Azure AD token: HTTP 401"
**Cause**: Invalid client credentials  
**Solution**:
1. Verify `clientId` and `clientSecret` in Azure AD app registration
2. Confirm the service principal hasn't expired or been deleted
3. Check tenant ID is correct

#### "Failed to obtain Azure AD token: HTTP 403"
**Cause**: Service principal lacks Log Analytics permissions  
**Solution**:
1. Verify service principal has "Log Analytics Reader" or higher role on the workspace
2. Go to Workspace → Access Control (IAM) in Azure portal
3. Add service principal with appropriate role assignment

#### "Query failed: HTTP 400"
**Cause**: Invalid KQL syntax generated from SQL query  
**Solution**:
1. Check EXPLAIN PLAN output for generated KQL
2. Verify all column names are valid in the target table
3. Check for unsupported operations (e.g., LIKE without %, unsupported functions)

#### "Timeout waiting for query results"
**Cause**: Query executing too long in Log Analytics  
**Solution**:
1. Add more restrictive WHERE clauses (especially time filters)
2. Narrow `defaultTimespan` to shorter duration
3. Check for expensive operations (large joins, complex aggregations)
4. Review Log Analytics query performance: Log Analytics → Logs → Performance

#### "Table not found: SecurityAlert"
**Cause**: Table name is case-sensitive; workspace doesn't have the table  
**Solution**:
1. Run `SHOW TABLES IN sentinel;` to see available tables
2. Verify table exists in Sentinel workspace
3. Check table name capitalization exactly

#### "Column not found: AlertName"
**Cause**: Column doesn't exist in the table or has different name  
**Solution**:
1. Query the table with `SELECT * LIMIT 1` to inspect columns
2. Use exact case-sensitive column names
3. Reference Sentinel table schema documentation

#### "HTTP 429 - Too Many Requests"
**Cause**: Rate limiting from Log Analytics API  
**Solution**:
1. Add delays between queries
2. Batch multiple operations into single query where possible
3. Reduce concurrent query execution
4. Contact Azure support if limits are too restrictive

### Logging and Debugging

Enable debug logging to troubleshoot issues:

```
# In logback.xml or log4j2.xml
<logger name="org.apache.drill.exec.store.sentinel" level="DEBUG"/>
```

Debug logging includes:
- HTTP request/response details (without credentials)
- Token refresh events
- KQL query construction
- Column type mapping
- Pagination details

Check Drill logs in `$DRILL_HOME/logs/` for detailed diagnostics.

## Limitations and Future Work

### Current Limitations

1. **Read-Only**: No INSERT, UPDATE, DELETE, or CREATE TABLE support
2. **No Joins**: Cross-table joins not supported (different semantics than SQL)
3. **No Complex Aggregates**: HAVING clause, window functions not supported
4. **Single Workspace**: One plugin instance = one workspace
5. **No Data Modification**: Cannot write results back to Sentinel
6. **Type Limitations**: `dynamic` type (JSON) returned as VARCHAR string

### Potential Future Enhancements

- [ ] Support for HAVING clause (filtered aggregates)
- [ ] DISTINCT keyword support
- [ ] UNION/UNION ALL support
- [ ] Materialized view caching
- [ ] Multi-workspace configuration
- [ ] Custom function support for KQL
- [ ] Automatic cost-based pushdown decisions
- [ ] Real-time data streaming (if Log Analytics supports it)

## Contributing

To contribute improvements to the plugin:

1. Fork the Apache Drill repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make changes and add tests
4. Run full test suite: `mvn -pl contrib/storage-sentinel test`
5. Submit pull request with clear description of changes

## License

Apache License 2.0 (same as Apache Drill)

## Support and Resources

- **Apache Drill Documentation**: https://drill.apache.org/docs/
- **Microsoft Sentinel Documentation**: https://learn.microsoft.com/en-us/azure/sentinel/
- **KQL Reference**: https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/
- **Log Analytics Query Examples**: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/queries

## FAQ

**Q: Can I query multiple Sentinel workspaces?**  
A: No, each plugin instance connects to one workspace. Create multiple plugin instances for multiple workspaces.

**Q: Does the plugin support real-time queries?**  
A: Queries execute against the latest data in Log Analytics, but are not continuous streaming. For real-time monitoring, use Sentinel's built-in rules and automation.

**Q: How much data can I query at once?**  
A: Limited by Log Analytics quotas and your `maxRows` configuration. Typical queries return 10K-100K rows; larger queries may timeout.

**Q: Can I modify Sentinel data through Drill?**  
A: No, this is a read-only plugin. Use Log Analytics API or Azure portal for data modifications.

**Q: How do I optimize queries for performance?**  
A: See "Performance Tuning" section. Key: filter by TimeGenerated, project needed columns, push aggregates to Log Analytics.

**Q: What happens if the Log Analytics API is unavailable?**  
A: Queries fail with HTTP error. Drill will not retry automatically; use your orchestration layer to retry.

**Q: How are large result sets handled?**  
A: The plugin uses pagination through @odata.nextLink and streams results through Drill batches. Memory usage should be reasonable even for million-row results.
