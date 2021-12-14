# Aliases

Apache Drill provides functionality for creating persistent aliases for its tables and storage names.
These aliases can be used in queries instead of specifying the original name.

This feature is enabled by default and can be disabled using `exec.enable_aliases` **system** option.

Aliases can be either public or user-specific.

## Public aliases
Public aliases are available for all users, but only admins can create, modify or delete them.

The following command may be used to create public table alias:
```sql
CREATE PUBLIC ALIAS tpch_lineitem FOR TABLE cp.`tpch/lineitem.parquet`
```

Public alias for storage can be created with the following command:
```sql
CREATE PUBLIC ALIAS classpath FOR STORAGE cp
```

Existing public table aliases can be replaced with the following command:
```sql
CREATE OR REPLACE PUBLIC ALIAS tpch_lineitem FOR TABLE cp.`tpch/nation.parquet`
```

It is possible to omit `TABLE` keyword:
```sql
CREATE OR REPLACE PUBLIC ALIAS tpch_lineitem FOR cp.`tpch/nation.parquet`
```

The following command can be used to drop public table alias:
```sql
DROP PUBLIC ALIAS tpch_lineitem FOR TABLE
```

Query to drop all public table aliases is the following:
```sql
DROP ALL PUBLIC ALIASES FOR TABLE
```

## User aliases
User aliases are specific for the concrete users, so different users can have aliases with the same name
but different values.
User aliases have a greater priority than public aliases, but if no user alias is found, a public one will be used.

The following command may be used to create user table alias:
```sql
CREATE ALIAS tpch_lineitem FOR TABLE cp.`tpch/lineitem.parquet`
```

User alias for storage can be created with the following command:
```sql
CREATE ALIAS classpath FOR STORAGE cp
```

Drill admins can also create aliases for other users with the following command:
```sql
CREATE ALIAS tpch_lineitem FOR TABLE cp.`tpch/lineitem.parquet` AS USER 'user1'
```
_Non-admin users also can use syntax above, but only with their username._

Commands for replacing or deleting user aliases similar to the public one:
```sql
CREATE OR REPLACE ALIAS tpch_lineitem FOR TABLE cp.`tpch/nation.parquet`;
CREATE OR REPLACE ALIAS tpch_lineitem FOR TABLE cp.`tpch/nation.parquet` AS USER 'user1';

DROP ALIAS tpch_lineitem FOR TABLE;
DROP ALIAS tpch_lineitem FOR TABLE AS USER 'user1';

DROP ALL ALIASES FOR TABLE;
DROP ALL ALIASES FOR TABLE AS USER 'user1';
```

## System tables for storage and table aliases
Table aliases can be queried using `sys.table_aliases` system table:
```sql
SELECT * FROM sys.table_aliases
```
| alias<VARCHAR(OPTIONAL)> | name<VARCHAR(OPTIONAL)>      | user<VARCHAR(OPTIONAL)> | isPublic<BIT(REQUIRED)> |
|:-------------------------|:-----------------------------|:------------------------|:------------------------|
| `t1`                     | `cp`.`tpch/lineitem.parquet` | null                    | true                    |
| `t2`                     | `cp`.`tpch/lineitem.parquet` | null                    | true                    |
| `t3`                     | `cp`.`tpch/lineitem.parquet` | testUser2               | false                   |
| `t3`                     | `cp`.`tpch/nation.parquet`   | testUser1               | false                   |

Storage aliases can be obtained from `sys.storage_aliases` system table:
```sql
SELECT * FROM sys.storage_aliases
```
| alias<VARCHAR(OPTIONAL)> | name<VARCHAR(OPTIONAL)> | user<VARCHAR(OPTIONAL)> | isPublic<BIT(REQUIRED)> |
|:-------------------------|:------------------------|:------------------------|:------------------------|
| `t1`                     | `dfs`                   | null                    | true                    |
| `t2`                     | `cp`                    | null                    | true                    |
| `t3`                     | `dfs`                   | testUser2               | false                   |
| `t3`                     | `cp`                    | testUser1               | false                   |

## Developer notes
Table and storage aliases are stored in the pre-configured persistent store using `storage_aliases` and 
`table_aliases` accordingly.
