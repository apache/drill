# Daffodil Schema Management in Apache Drill

This package implements dynamic Daffodil schema management for Apache Drill, allowing users to register and manage Daffodil (DFDL) schema files at runtime using SQL commands.

## Overview

The Daffodil schema management system allows users to:
- Register Daffodil schema JARs dynamically without restarting Drill
- Distribute schema JARs across all Drillbits in a cluster
- Unregister schemas when no longer needed
- Version control schema registrations

## Architecture

### Key Components

1. **RemoteDaffodilSchemaRegistry**: Main registry managing schema JARs across the cluster
   - Persistent storage for registered schemas
   - Transient locks for concurrent access control
   - Version-controlled updates with retry logic

2. **DaffodilSchemaProvider**: Lifecycle manager for schema registry
   - Initialized during Drillbit startup
   - Provides access to RemoteDaffodilSchemaRegistry

3. **CreateDaffodilSchemaHandler**: Handles schema registration SQL commands
4. **DropDaffodilSchemaHandler**: Handles schema unregistration SQL commands

### File System Layout

The system uses three directories for managing schema JARs:

- **Staging**: `/path/to/base/staging/`
  - Users copy schema JAR files here before registration
  - Temporary location before validation and registration

- **Registry**: `/path/to/base/registry/`
  - Permanent storage for registered schema JARs
  - Accessible by all Drillbits in the cluster

- **Tmp**: `/path/to/base/tmp/`
  - Temporary backup during registration process
  - Used for rollback on registration failure

## Configuration

Configure Daffodil schema directories in `drill-override.conf`:

```hocon
drill.exec.daffodil {
  directory {
    # Base directory for all Daffodil schema directories
    base: "/opt/drill/daffodil"

    # Optional: specific filesystem (defaults to default FS)
    # fs: "hdfs://namenode:8020"

    # Optional: root directory (defaults to user home)
    # root: "/user/drill"

    # Staging directory for uploading schema JARs
    staging: ${drill.exec.daffodil.directory.base}"/staging"

    # Registry directory for registered schemas
    registry: ${drill.exec.daffodil.directory.base}"/registry"

    # Temporary directory for backup during registration
    tmp: ${drill.exec.daffodil.directory.base}"/tmp"
  }
}
```

## Usage

### Registering a Daffodil Schema

1. **Copy the schema JAR to the staging directory:**

```bash
# Copy your Daffodil schema JAR to the configured staging directory
cp my-schema.jar /opt/drill/daffodil/staging/
```

2. **Register the schema using SQL:**

```sql
CREATE DAFFODIL SCHEMA USING JAR 'my-schema.jar';
```

**Success Response:**
```
+------+----------------------------------------------------------+
| ok   | summary                                                  |
+------+----------------------------------------------------------+
| true | Daffodil schema jar my-schema.jar has been registered    |
|      | successfully.                                            |
+------+----------------------------------------------------------+
```

### Unregistering a Daffodil Schema

```sql
DROP DAFFODIL SCHEMA USING JAR 'my-schema.jar';
```

**Success Response:**
```
+------+------------------------------------------------------------+
| ok   | summary                                                    |
+------+------------------------------------------------------------+
| true | Daffodil schema jar my-schema.jar has been unregistered    |
|      | successfully.                                              |
+------+------------------------------------------------------------+
```

## Error Handling

### Common Errors

**JAR not found in staging:**
```
File /opt/drill/daffodil/staging/my-schema.jar does not exist on file system
```
**Solution:** Ensure the JAR file is copied to the staging directory

**Duplicate schema registration:**
```
Jar with my-schema.jar name has been already registered
```
**Solution:** Use DROP to unregister the existing schema first, or use a different JAR name

**Schema not registered:**
```
Jar my-schema.jar is not registered in remote registry
```
**Solution:** Verify the schema was previously registered using CREATE DAFFODIL SCHEMA

**Concurrent access:**
```
Jar with my-schema.jar name is used. Action: REGISTRATION
```
**Solution:** Wait for the current operation to complete

## Registration Process Flow

1. **Validation**: Checks JAR exists in staging area
2. **Locking**: Acquires lock to prevent concurrent operations
3. **Backup**: Copies JAR to temporary area
4. **Duplicate Check**: Validates no duplicate in remote registry
5. **Registration**: Copies JAR to registry area
6. **Update**: Updates persistent registry with version control
7. **Cleanup**: Removes JAR from staging area
8. **Unlock**: Releases lock

## Unregistration Process Flow

1. **Locking**: Acquires lock for unregistration
2. **Lookup**: Finds JAR in remote registry
3. **Update**: Removes from persistent registry
4. **Deletion**: Deletes JAR from registry area
5. **Unlock**: Releases lock

## Best Practices

1. **Naming Convention**: Use descriptive, versioned names for schema JARs
   - Good: `customer-schema-v1.0.jar`
   - Avoid: `schema.jar`

2. **Testing**: Test schemas in a development environment before production

3. **Backup**: Keep backup copies of schema JARs outside the Drill directories

4. **Cleanup**: Remove unused schemas with DROP DAFFODIL SCHEMA

5. **Concurrent Access**: Avoid registering/unregistering the same schema simultaneously from multiple clients

## Development and Testing

### Running Tests

```bash
# Run all Daffodil schema tests
mvn test -pl exec/java-exec -Dtest=TestDaffodilSchemaHandlers

# Run RemoteDaffodilSchemaRegistry tests
mvn test -pl exec/java-exec -Dtest=TestRemoteDaffodilSchemaRegistry
```

### Test Coverage

- Basic syntax validation
- JAR registration and unregistration
- Duplicate detection
- Concurrent access handling
- Error scenarios (missing JAR, not registered, etc.)
- File system operations

## Related Documentation

- [Apache Daffodil](https://daffodil.apache.org/)
- [DFDL Specification](https://www.ogf.org/ogf/doku.php/standards/dfdl/dfdl)
- [Drill Dynamic UDFs](https://drill.apache.org/docs/dynamic-udfs/)
