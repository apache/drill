# Apache Accumulo Storage Plugin for Drill - Design Document

**Status**: Design Review
**Target Implementation**: Option A (High Abstraction) with extensibility for Option B
**Target Accumulo Version**: 2.1.x LTS
**Target Drill Version**: Latest (master branch)

---

## 1. Executive Summary

This document describes the architecture for a production-grade Accumulo storage plugin for Apache Drill with:

- **Option A (Primary)**: High-level abstraction treating Accumulo as a SQL-queryable table store
- **Maximum pushdowns**: Filter, column projection, limit pushdown
- **Thorough testing**: Unit tests + integration tests using MiniAccumuloCluster
- **Future extensibility**: Clear design points for Option B (low abstraction / advanced iterators)

---

## 2. Architecture Overview

### 2.1 System Design Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         Drill Query Engine                       │
│                      (Calcite Planner, etc.)                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
         ┌───────────────┴────────────────┐
         │  AccumuloStoragePlugin         │
         │  (Lifecycle, Schema Mgmt)      │
         └───────────────┬────────────────┘
                         │
         ┌───────────────┴───────────────────┐
         │  Optimizer Rules Layer             │
         ├─────────────────────────────────┤
         │ • FilterPushDownRule              │
         │ • ProjectionPushDownRule          │
         │ • LimitPushDownRule               │
         └──────────────┬────────────────────┘
                        │
         ┌──────────────┴───────────────────┐
         │  Physical Plan Layer              │
         ├──────────────────────────────────┤
         │ • AccumuloGroupScan               │
         │ • AccumuloSubScan (per tablet)    │
         │ • AccumuloRecordReader            │
         └──────────────┬────────────────────┘
                        │
         ┌──────────────┴───────────────────┐
         │  Accumulo Client Layer            │
         ├──────────────────────────────────┤
         │ • AccumuloClient (singleton)      │
         │ • Scanner/BatchScanner creation   │
         │ • Iterator construction (internal)│
         └──────────────┬────────────────────┘
                        │
         ┌──────────────┴───────────────────┐
         │  Accumulo Cluster                 │
         │  (ZooKeeper, TabletServers, etc.) │
         └───────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│               SCHEMA MANAGEMENT SUBSYSTEM (Option A)             │
├─────────────────────────────────────────────────────────────────┤
│  • AccumuloSchemaFactory: Table/schema discovery                 │
│  • AccumuloSchemaProvider: (Interface for future Option B)       │
│  • DefaultSchemaProvider: Implements metadata table approach     │
│  • ScanSamplingProvider: Fallback schema inference (future)      │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Architecture

### 3.1 Core Components (Required for Both Options A and B)

#### **AccumuloStoragePluginConfig**
```
Responsibilities:
├── Store connection parameters
│   ├── ZooKeeper quorum
│   ├── Accumulo instance name
│   ├── Username/password
│   └── Optional: Custom iterator library paths
├── Serialize/deserialize via Jackson
├── Provide equals/hashCode for caching
└── Extension point: Custom iterator configuration (Option B)
```

#### **AccumuloStoragePlugin**
```
Responsibilities:
├── Lifecycle management
│   ├── Initialize AccumuloClient (singleton/pooled)
│   ├── Close client on shutdown
│   └── Thread-safe connection reuse
├── Schema registration
│   ├── Register Calcite schema with table discovery
│   ├── Use AccumuloSchemaProvider (pluggable)
│   └── Support multiple schema discovery strategies
├── Optimizer rule registration
│   ├── Return filter pushdown rule
│   ├── Return projection pushdown rule
│   ├── Return limit pushdown rule
│   └── Return sort rules (if applicable)
└── Extension point: Register custom iterator rules (Option B)
```

#### **AccumuloScanSpec** (Logical Scan Selection)
```
Responsibilities:
├── Store scan parameters
│   ├── Table name
│   ├── Row key range (start, stop, inclusive flags)
│   ├── Pushed-down filter (as serializable expression)
│   ├── Projected column families/qualifiers
│   └── Limit value (if pushed down)
├── Serialize via Jackson for distributed execution
└── Extension point: Custom iterator spec (Option B)
```

#### **AccumuloGroupScan** (Logical→Physical Planning)
```
Responsibilities:
├── Fragment scan across tablets
│   ├── Discover tablet ranges from Accumulo metadata
│   ├── Map tablets to endpoints (TabletServer affinity)
│   └── Create SubScan for each tablet
├── Modifiable by optimizer rules
│   ├── Support cloning for filter/projection modifications
│   ├── Track what's been pushed down (flags for idempotency)
│   └── Fall back to client-side filtering if needed
├── Provide scan statistics
│   ├── Row count estimates
│   └── Memory/data size estimates
└── Extension point: Custom iterator serialization (Option B)
```

#### **AccumuloSubScan** (Physical Scan on Single Tablet)
```
Responsibilities:
├── Represent scan on one tablet
├── Carry scan parameters for that partition
├── Be non-executable (converted to RecordReader)
└── Support physical visitor pattern for Drill execution
```

#### **AccumuloRecordReader** (Data Streaming)
```
Responsibilities:
├── Read data from Accumulo
│   ├── Create Scanner with range/filters/projections
│   ├── Iterate through results
│   └── Convert to Drill ValueVectors
├── Handle schema discovery
│   ├── Infer types from first batch (Option A)
│   └── Apply configured schema (if provided)
├── Manage memory and batching
│   ├── Target record count per batch
│   ├── Respect max memory per batch
│   └── Handle column mapping (Accumulo row → Drill columns)
└── Extension point: Custom iterator integration (Option B)
```

### 3.2 Schema Management Subsystem (Pluggable)

#### **AccumuloSchemaProvider** (Interface)
```java
interface AccumuloSchemaProvider {
  TableSchema getTableSchema(AccumuloClient client, String tableName);
  Set<String> discoverTableNames(AccumuloClient client);
}
```

#### **DefaultMetadataTableProvider** (Option A - Recommended)
```
Responsibilities:
├── Maintain schema metadata in Accumulo table
│   ├── Table name: "_schema" (configurable)
│   ├── Row key format: {table_name}
│   ├── Column families: metadata
│   └── Columns: family:qualifiers, type_mapping, etc.
├── Discover and cache table schemas
├── Support schema updates via special API
└── Graceful fallback to scan sampling if metadata not found
```

Format example:
```
Row: "users_table"
  metadata:columns → "name,age,email"
  metadata:families → "cf1:name,cf1:age,cf1:email"
  metadata:types → "STRING,INT,STRING"
```

#### **ScanSamplingProvider** (Fallback)
```
Responsibilities:
├── Sample first N rows from table
├── Infer column structure from sampled data
├── Infer type hints (basic - STRING for all)
└── Use only if metadata table unavailable
```

---

## 4. Pushdown Strategy (Option A - High Abstraction)

### 4.1 Filter Pushdown

**Architecture:**
```
SQL Filter (e.g., "WHERE age > 30 AND city = 'NYC'")
    ↓
Calcite RexNode (optimizer expression)
    ↓
Drill LogicalExpression (via DrillOptiq.toDrill)
    ↓
FilterToPushdownConverter (Accumulo-specific)
    ├── Converts to Accumulo Scanner filters (if possible)
    └── OR marks for client-side filtering (partial pushdown)
    ↓
AccumuloScanSpec (includes serialized filter or NONE)
    ↓
AccumuloRecordReader (applies filters)
```

**Supported Predicates (Option A):**
- Comparison operators: =, !=, <, >, <=, >=
- Logical operators: AND, OR (with limits - see below)
- Null checks: IS NULL, IS NOT NULL
- String operations: LIKE (basic patterns)
- Numeric ranges: BETWEEN

**Limitations & Strategy:**
- Complex predicates (nested OR/AND beyond 2 levels, complex LIKE patterns) → client-side
- Non-scalar expressions (function calls, computed columns) → client-side
- **Partial pushdown**: If only part of filter can be pushed, keep FilterPrel in plan

**Implementation Class: `AccumuloFilterPushdownRule`**
```
Pattern match: Filter → Scan
├── Extract filter conditions
├── Try converting each condition to Scanner filter range
├── Track which conditions were pushed
├── If all pushed: Remove Filter prel
└── If partial: Keep Filter prel for client-side processing
```

### 4.2 Column Projection Pushdown

**Architecture:**
```
Projected Columns (e.g., "SELECT name, age FROM users")
    ↓
SchemaPath list in GroupScan
    ↓
Map to Accumulo column families/qualifiers
    ↓
ProjectionToCFQualifierConverter
    ├── Parse Drill column names
    ├── Map to Accumulo CF:CQ pairs
    └── Create column projection spec
    ↓
Scanner.fetchColumnFamilies()/fetchColumns()
    ↓
TabletServer applies system iterator
    ↓
Unnecessary columns never transferred over network
```

**Implementation Class: `AccumuloProjectionPushdownRule`**
```
Pattern match: Project → Scan
├── Extract projected columns from ProjectPrel
├── Map to Accumulo families/qualifiers
├── Update AccumuloGroupScan with column spec
├── Remove ProjectPrel (handled by scanner)
```

### 4.3 Limit Pushdown

**Strategy (Partial):**
- Accumulo iterators don't natively support LIMIT
- **Implementation**:
  - Store limit in AccumuloScanSpec
  - RecordReader stops after limit rows received
  - Saves compute, not network bandwidth (network bound for limit)
  - Still beneficial: stops TabletServer iteration early

**Implementation Class: `AccumuloLimitPushdownRule`**
```
Pattern match: Limit → Scan
├── Extract limit value
├── Add to AccumuloScanSpec
├── Remove Limit prel (handled by RecordReader)
```

### 4.4 Sort/Order Pushdown

**Architecture:**
```
ORDER BY clause (e.g., "ORDER BY row_key, name")
    ↓
Calcite SortRel (sort operator)
    ↓
SortToScannerConverter
├── Extract sort columns
├── Check if sort can be satisfied by Accumulo's natural order
│   └── Natural order: Row Key (primary), then CF, then CQ
├── If matches: Mark GroupScan to use Scanner (sorted, single-threaded)
└── If doesn't match: Keep SortRel (client-side sort)
    ↓
AccumuloGroupScan with sort mode flag
    ├── sort_mode = SCANNER (single-threaded, sorted)
    └── sort_mode = BATCH_SCANNER (multi-threaded, unsorted - default)
    ↓
AccumuloRecordReader uses appropriate scanner type
```

**Trade-offs & Cost Analysis:**

| Aspect | Unsorted (BatchScanner) | Sorted (Scanner) |
|--------|------------------------|------------------|
| **Parallelism** | Multi-threaded tablets | Single-threaded tablet scan |
| **Network Throughput** | High (parallel) | Lower (sequential) |
| **Sorted Result** | NO | YES (by row key) |
| **Memory** | Constant (streaming) | Constant (streaming) |
| **Best For** | Large scans without sort | Small result sets or required sort |

**Supported Sort Patterns (Option A):**
1. **No ORDER BY** → Use BatchScanner (default, maximum parallelism)
2. **ORDER BY row_key** → Use Scanner (leverages Accumulo sort)
3. **ORDER BY row_key, column_family** → Use Scanner (Accumulo's secondary sort)
4. **ORDER BY row_key ASC** → Use Scanner
5. **Complex sort** (e.g., by non-key columns) → Keep SortRel (client-side)

**Implementation Class: `AccumuloSortPushdownRule`**
```
Pattern match: Sort → Scan
├── Extract sort columns and directions
├── Check if sort matches Accumulo's natural order
│   ├── All sort keys must be row_key (primary)
│   ├── All must be ASC (Accumulo sorts ascending)
│   └── No computed columns or expressions
├── If matches:
│   ├── Set GroupScan.useSortedScanner = true
│   ├── Remove SortRel from plan
│   └── Track in AccumuloGroupScan for cost estimation
├── If doesn't match:
│   └── Keep SortRel (optimizer will do client-side sort)
└── Cost estimate: Parallelism loss vs. sort savings
```

**Cost Estimation Logic:**
```
If using Scanner (sorted, single-threaded):
  cost = row_count / tablet_count  // No parallelism benefit

If using BatchScanner (unsorted, multi-threaded):
  cost = row_count / tablet_count / parallel_factor

Sort cost (if client-side):
  cost += row_count * log(row_count)

Optimizer chooses Scanner only if:
  scanner_cost < batchscanner_cost + sort_cost
```

**Configuration Flag in AccumuloGroupScan:**
```java
private boolean sortedScannerRequired = false;  // Set by optimizer rule
private List<String> sortColumns;               // For cost estimation

@Override
public ScanStats getScanStats() {
  // Adjust cost based on scanner type
  double baseCost = calculateBaseCost();
  if (sortedScannerRequired) {
    baseCost *= 1.5;  // Penalty for losing parallelism
  }
  return new ScanStats(...);
}
```

**Limitations of Sort Pushdown:**
1. Only sort by row_key supported (not by column values)
2. Ascending only (Accumulo's natural order)
3. No multi-column sorts (would need complex key ordering)
4. Batch results from different tablets may still need client-side sorting if using BatchScanner

### 4.5 Potential Future Pushdowns

- **Aggregation pushdown**: Requires custom iterators (Option B)
  - COUNT, SUM, etc. via server-side combiners
  - Out of scope for Option A
  - Will be enabled by Option B iterator framework

---

## 5. Schema Discovery & Type Mapping (Option A)

### 5.1 Table Structure in Drill

**Assumption**: Each Accumulo table maps to a Drill table with this logical structure:

```
Accumulo Row (key)
├── Row ID → Drill column: "row_key" (BYTES)
└── For each configured column family:
    ├── Column family data → Drill column (structure TBD)
    ├── Could be: Separate columns per qualifier (flat)
    └── Could be: MAP<VARCHAR, VARCHAR> (nested)
```

**Design Decision for Option A:**
```
Flat mapping (recommended for OLAP):
  Accumulo row with CF "user_data" and qualifiers "name", "age"

  Maps to Drill columns:
  ├── row_key (BYTES) - actual Accumulo row key
  ├── user_data_name (VARCHAR) - CF:qualifier → column
  └── user_data_age (INTEGER) - CF:qualifier → column
```

### 5.2 Schema Discovery Workflow

```
Drill startup or table reference
    ↓
AccumuloSchemaFactory.registerSchemas()
    ↓
Iterate over configured tables / discover via Accumulo
    ↓
For each table: getTableSchema()
    ├── Check metadata table (DefaultMetadataTableProvider)
    ├── If found: Parse schema definition
    ├── If not: Fall back to scan sampling
    └── Cache schema
    ↓
DrillAccumuloTable created with schema
    ↓
getRowType() → RelDataType for Calcite
```

### 5.3 Metadata Table Format (Option A - Recommended)

**System Metadata Table**: `_drill_schema` (or configured name)

**Row Key**: Actual Accumulo table name (e.g., "users_table")

**Schema Structure**:
```
Column Family: "metadata"

Row: "users_table"
├── metadata:qualified_name → "users_table" (redundant, for clarity)
├── metadata:row_key_type → "BYTES"
├── metadata:column_definitions → JSON array
│   [
│     {"family":"cf1", "qualifier":"name", "column_name":"name", "type":"VARCHAR"},
│     {"family":"cf1", "qualifier":"age", "column_name":"age", "type":"INT"},
│     {"family":"cf2", "qualifier":"email", "column_name":"email", "type":"VARCHAR"}
│   ]
└── metadata:updated → timestamp
```

**Alternative (simpler)**: Plain text format
```
metadata:columns_csv → "cf1:name:VARCHAR,cf1:age:INT,cf2:email:VARCHAR"
```

---

## 6. Configuration & Connection Management

### 6.1 Plugin Configuration File

**Location**: `conf.d/accumulo.conf` or via REST API

```json
{
  "type": "accumulo",
  "enabled": true,
  "config": {
    "zooKeeper.quorum": "localhost:2181",
    "instanceName": "accumulo",
    "userName": "root",
    "password": "password",
    "clientTimeout": "30s",
    "schemaProvider": "default",
    "metadataTable": "_drill_schema",
    "maxConnectionPoolSize": 10
  }
}
```

### 6.2 Connection Management

```
Plugin initialization
    ↓
Create AccumuloClient singleton via builder
    ├── client = Accumulo.newClient()
    │            .to(instanceName, zooKeepers)
    │            .as(userName, password)
    │            .build()
    └── Store in plugin instance (thread-safe, reusable)
    ↓
Maintain reference count / lifecycle
    ├── Close on plugin shutdown
    └── Reuse for all scans
    ↓
RecordReaders share same client
    ├── Create Scanner/BatchScanner from client
    └── Automatic resource cleanup (try-with-resources)
```

---

## 7. Testing Strategy

### 7.1 Test Infrastructure

**Primary**: MiniAccumuloCluster (embedded Accumulo for unit/integration tests)

```java
@BeforeClass
public static void setupCluster() {
  File tmpDir = new File(System.getProperty("java.io.tmpdir"), "accumulo-test");
  miniCluster = new MiniAccumuloCluster(tmpDir, "password");
  miniCluster.start();

  client = miniCluster.getAccumuloClient("root", new PasswordToken("password"));
  // Create test tables, populate data
}

@AfterClass
public static void shutdownCluster() throws Exception {
  client.close();
  miniCluster.stop();
}
```

**Advantages**:
- Full Accumulo functionality (iterators, tablets, etc.)
- No external infrastructure needed
- Reproduces production scenarios

**Disadvantages**:
- Slower startup/shutdown than unit tests
- ZooKeeper process overhead
- Best for integration tests, not unit-level

### 7.2 Test Categories & Coverage

#### **Unit Tests** (Fast, mocked dependencies)
- `AccumuloStoragePluginConfigTest`
  - Jackson serialization/deserialization
  - Config validation
  - Equality/hashcode

- `AccumuloScanSpecTest`
  - Spec serialization (distributed execution)
  - Filter/projection spec encoding

- `FilterPushdownConverterTest`
  - RexNode → Accumulo filter conversion
  - Partial pushdown scenarios
  - Unsupported filter detection

- `SchemaProviderTest`
  - Metadata table parsing
  - Schema cache behavior
  - Fallback to sampling

#### **Integration Tests** (MiniAccumuloCluster-based)

- `BaseAccumuloTest`
  - Setup/teardown MiniAccumuloCluster
  - Utility methods for test data creation

- `AccumuloPluginInitializationTest`
  - Plugin initialization
  - Schema discovery
  - Table listing

- `AccumuloQueryTest` (Extends BaseTestQuery - full Drill integration)
  - Basic SELECT queries
  - Filter pushdown verification (query plan inspection)
  - Projection pushdown verification
  - Limit pushdown verification
  - Sort pushdown verification (Scanner vs BatchScanner selection)
  - Complex queries (joins, aggregates - without pushdown)
  - Error handling (missing tables, invalid credentials)

- `AccumuloRecordReaderTest`
  - Data type mapping
  - Batch size handling
  - Column projection
  - Schema inference (fallback mode)

- `AccumuloGroupScanTest`
  - Tablet fragmentation
  - Endpoint affinity mapping
  - Scan statistics accuracy

#### **Test Data Scenarios**

```
Setup:
├── Empty tables (edge case)
├── Single-row tables
├── Large tables (1M+ rows)
├── Tables with multiple column families
├── Tables with null values
├── Tables with different data types
│   ├── Strings
│   ├── Numbers (INT, BIGINT)
│   ├── Floats/doubles
│   └── Bytes/blobs
└── Tables with special characters in keys/values

Queries:
├── SELECT * → all columns
├── SELECT specific columns → projection
├── WHERE conditions → filters
├── WHERE + ORDER BY
├── LIMIT
├── Combinations: project + filter + limit
└── Aggregates (COUNT, SUM) → no pushdown, client-side only
```

### 7.3 Test Coverage Goals

- **Unit tests**: 80%+ code coverage (configs, utilities, converters)
- **Integration tests**: Critical paths (query execution, data retrieval, pushdown verification)
- **Edge cases**: Empty results, null values, type mismatches, connection failures

---

## 8. Future Extension: Option B (Advanced/Power User Mode)

### 8.1 Design for Option B Extensibility

**Option B Goals:**
- Expose Accumulo iterators to Drill users
- Allow custom iterator specification
- Support advanced server-side aggregation/computation

**Design Points for Future Option B:**

#### **1. Iterator Configuration in Accumulo Config**
```json
{
  "type": "accumulo",
  "iterators": {
    "custom": {
      "jar_path": "/path/to/custom-iterators.jar",
      "class_prefix": "com.example.accumulo.iterators"
    }
  }
}
```

#### **2. Extended AccumuloScanSpec for Iterators**
```java
// Current (Option A):
public class AccumuloScanSpec {
  private HBaseScanSpec scanSpec;
  private Filter filter;
  // ...
}

// Future (Option B):
public class AccumuloScanSpec {
  // ... Option A fields ...

  // Option B extension:
  @JsonProperty("custom_iterators")
  private List<IteratorConfig> customIterators; // NEW

  @JsonProperty("iterator_options")
  private Map<String, String> iteratorOptions; // NEW
}

class IteratorConfig {
  String name;
  String className;
  int priority;
  Map<String, String> options;
}
```

#### **3. Option B Optimizer Rule: CustomIteratorPushdownRule**
```
Pattern: Aggregate → Scan
├── Detect if aggregation can map to custom iterator
├── Check if iterator class available
├── Create IteratorConfig
├── Add to AccumuloScanSpec.customIterators
└── Remove Aggregate prel (handled server-side)
```

#### **4. Option B Configuration in SQL (Future)**
```sql
-- Hypothetical future syntax:
SELECT * FROM accumulo.users_table
WITH (iterator_name = 'custom_agg', iterator_class = '...')
WHERE age > 30
```

#### **5. Table Schema Hints for Option B**
```
Metadata table addition:
  metadata:custom_iterators → JSON list of available iterators for this table

Used for:
├── Query plan optimization (which iterators can be used)
└── User hints (which iterators are recommended)
```

#### **6. AccumuloRecordReader Option B Extension**
```java
// Current (Option A):
public class AccumuloRecordReader {
  private Scanner scanner;
  // Uses default iterators
}

// Future (Option B):
public class AccumuloRecordReader {
  private Scanner scanner;

  // NEW:
  private void addCustomIterators(List<IteratorConfig> iterators) {
    for (IteratorConfig cfg : iterators) {
      IteratorSetting settings = new IteratorSetting(
          cfg.priority, cfg.name, cfg.className);
      cfg.options.forEach(settings::addOption);
      scanner.addScanIterator(settings);
    }
  }
}
```

### 8.2 Migration Path from Option A to Option B

1. **Phase 1 (Current - Option A)**: High abstraction, filter/projection/limit pushdowns
2. **Phase 2 (Future - Option B prep)**: Support custom iterator configuration in plugin config
3. **Phase 3 (Future - Option B full)**: Implement iterator optimizer rules
4. **Phase 4 (Future - Option B advanced)**: Support SQL hints for iterator selection

**Backward Compatibility**: Option A queries remain valid in Option B; new iterator features are opt-in.

---

## 9. Implementation Roadmap

### Phase 1: Core Infrastructure
- [ ] Pom.xml and module setup
- [ ] AccumuloStoragePluginConfig
- [ ] AccumuloStoragePlugin
- [ ] Basic connection management
- [ ] Tests for config/connection

### Phase 2: Schema & Table Discovery
- [ ] AccumuloSchemaProvider interface
- [ ] DefaultMetadataTableProvider implementation
- [ ] AccumuloSchemaFactory
- [ ] DrillAccumuloTable
- [ ] Tests for schema discovery

### Phase 3: Basic Scan Capability
- [ ] AccumuloScanSpec
- [ ] AccumuloGroupScan (tablet fragmentation)
- [ ] AccumuloSubScan
- [ ] AccumuloRecordReader (basic data reading)
- [ ] Tests: RecordReader, basic queries

### Phase 4: Filter Pushdown
- [ ] FilterToPushdownConverter (RexNode → filter spec)
- [ ] AccumuloFilterPushdownRule
- [ ] Update AccumuloGroupScan for filter tracking
- [ ] Tests: Filter pushdown verification in query plans

### Phase 5: Projection Pushdown
- [ ] ProjectionToCFQualifierConverter
- [ ] AccumuloProjectionPushdownRule
- [ ] Update RecordReader for column projection
- [ ] Tests: Projection pushdown verification

### Phase 6: Limit & Sort Pushdown
- [ ] LimitPushdownRule
- [ ] RecordReader limit enforcement
- [ ] AccumuloSortPushdownRule (Scanner vs BatchScanner selection)
- [ ] Cost estimation for sort vs parallelism trade-off
- [ ] Update AccumuloGroupScan for sort mode
- [ ] Tests: Limit pushdown verification
- [ ] Tests: Sort pushdown verification (Scanner vs BatchScanner in plan)

### Phase 7: Scan Statistics & Optimization
- [ ] Implement ScanStats calculation
- [ ] Tablet fragmentation cost estimation
- [ ] Row count estimates
- [ ] Cost-based optimizer integration

### Phase 8: Integration Testing & Polish
- [ ] Comprehensive integration tests (queries, edge cases)
- [ ] Error handling and recovery
- [ ] Documentation and examples
- [ ] Performance tuning

### Phase 9: Option B Design Prep (Future)
- [ ] Document iterator configuration in config schema
- [ ] Add Option B extension fields to AccumuloScanSpec (with ignore markers)
- [ ] Leave hook points in RecordReader for custom iterators
- [ ] No functional changes, just structure

---

## 10. Key Design Principles

### 10.1 SOLID Principles Applied

**S - Single Responsibility**
- AccumuloStoragePlugin: Lifecycle only
- AccumuloGroupScan: Planning only
- AccumuloRecordReader: Data reading only
- Each converter class: Single conversion type

**O - Open/Closed**
- AccumuloSchemaProvider interface allows new schema discovery strategies
- FilterPushdownConverter extensible for new filter types
- RecordReader designed for custom iterator injection (Option B)

**L - Liskov Substitution**
- All providers implement AccumuloSchemaProvider contract
- All rules follow StoragePluginOptimizerRule pattern
- Drill interfaces implemented correctly

**I - Interface Segregation**
- AccumuloSchemaProvider: Only schema methods
- Separate interfaces for converters (filter, projection)
- Plugin config separated from runtime state

**D - Dependency Injection**
- RecordReader receives dependencies (client, spec, columns)
- Rules receive context and relationships
- No global state except plugin singleton

### 10.2 Robustness Principles

- **Partial pushdown**: Always safer than all-or-nothing
- **Graceful degradation**: Fall back to client-side processing
- **Error handling**: Clear exceptions, proper resource cleanup
- **Caching**: Schema cache with refresh mechanism
- **Idempotency**: Optimizer rules safe to run multiple times

### 10.3 Performance Principles

- **Avoid over-fragmentation**: Merge small tablets if beneficial
- **Endpoint affinity**: Prioritize data locality
- **Batch sizing**: Respect Drill's memory budgets
- **Network efficiency**: Project columns, push filters to server
- **Connection reuse**: Single client instance, pooled scanners

---

## 11. Configuration Reference (Option A)

### bootstrap-storage-plugins.json
```json
{
  "storage": {
    "accumulo": {
      "type": "accumulo",
      "enabled": false,
      "config": {
        "zooKeeper": {
          "quorum": "localhost:2181"
        },
        "instance": {
          "name": "accumulo"
        },
        "auth": {
          "principal": "root",
          "token_type": "password",
          "token": "password"
        },
        "schema": {
          "provider": "metadata_table",
          "metadata_table": "_drill_schema"
        }
      }
    }
  }
}
```

### Example table schema in metadata table
```
Table: _drill_schema
Row: "users"
  metadata:columns_json →
    [
      {"family":"cf1", "qualifier":"name", "column":"name", "type":"VARCHAR", "nullable":false},
      {"family":"cf1", "qualifier":"age", "column":"age", "type":"INT", "nullable":true},
      {"family":"cf1", "qualifier":"email", "column":"email", "type":"VARCHAR", "nullable":true}
    ]
```

---

## 12. Dependencies

### Maven Coordinates

```xml
<!-- Accumulo -->
<dependency>
  <groupId>org.apache.accumulo</groupId>
  <artifactId>accumulo-core</artifactId>
  <version>2.1.4</version>
</dependency>

<!-- Accumulo client (bundled, but explicit for clarity) -->
<dependency>
  <groupId>org.apache.accumulo</groupId>
  <artifactId>accumulo-core</artifactId>
  <version>2.1.4</version>
</dependency>

<!-- Testing -->
<dependency>
  <groupId>org.apache.accumulo</groupId>
  <artifactId>accumulo-minicluster</artifactId>
  <version>2.1.4</version>
  <scope>test</scope>
</dependency>

<!-- Drill core (inherited from parent, but list here for clarity) -->
<dependency>
  <groupId>org.apache.drill.exec</groupId>
  <artifactId>drill-java-exec</artifactId>
  <scope>provided</scope>
</dependency>

<!-- Existing Drill test utilities -->
<dependency>
  <groupId>org.apache.drill</groupId>
  <artifactId>drill-common</artifactId>
  <scope>provided</scope>
</dependency>
```

---

## 13. Known Limitations & Trade-offs

### Option A Limitations
1. **No custom iterator access**: Advanced Accumulo features not exposed
2. **Type inference**: Relies on metadata or sampling; limited automatic type detection
3. **Schema management**: Manual metadata maintenance required
4. **Sort optimization**: Complex due to Accumulo's scan model trade-offs

### Accumulo Limitations (Not Plugin-specific)
1. **No built-in aggregation operators**: COUNT, SUM require custom iterators or client-side computation
2. **Sort incompatible with parallelism**: Can't use BatchScanner if sort order needed
3. **Schema-less nature**: Type consistency not enforced by Accumulo

### Future Mitigations
- Option B for advanced users needing iterators
- Improved schema management tools/UI
- Performance benchmarks for sort trade-offs

---

## 14. Success Criteria

### Option A Completion
- [ ] All core components implemented and tested
- [ ] Filter pushdown working (verified in query plans)
- [ ] Projection pushdown working
- [ ] Limit pushdown working
- [ ] Sort pushdown working (Scanner vs BatchScanner selection)
- [ ] 80%+ unit test coverage
- [ ] Comprehensive integration tests passing
- [ ] Production-ready error handling
- [ ] Documentation complete
- [ ] Example queries/use cases documented

### Quality Metrics
- No critical bugs in testing
- Query performance within expected range (compared to HBase plugin)
- Scan statistics accurate (±20% for row count estimates)
- All pushdowns verified by query plan inspection
- Sort optimization correctly chooses between Scanner (sorted) and BatchScanner (parallel)

---

## 15. References & Resources

- **Accumulo Documentation**: https://accumulo.apache.org/docs/2.x/
- **Accumulo Client API**: https://accumulo.apache.org/docs/2.x/apidocs/
- **Drill Plugin Architecture**: [HBase/Kudu plugins in codebase]
- **Drill Schema/Type System**: Calcite integration points
- **Iterator Development**: https://accumulo.apache.org/docs/2.x/development/iterators

---

**Document Status**: Ready for Review
**Next Step**: Design review and approval, then proceed to Phase 1 implementation
