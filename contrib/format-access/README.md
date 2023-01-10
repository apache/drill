# Drill MS Access Format Plugin
This plugin enables Drill to read Microsoft Access database files. This plugin can read Access Files from all versions later than Access 1997.

## Configuration
Simply add the following to any Drill file system configuration.  Typically, MS Access files will use the extension `accdb` or `mdb`.  Drill comes pre-configured to recognize these extensions as MS Access.

```json
"msaccess": {
  "type": "msaccess",
  "extensions": ["mdb", "accdb"]
}
```

## Schemas
Drill will discover the schema automatically from the Access file.  The plugin does support schema provisioning, but that should not be used.

## Querying a Table
Access files will contain multiple tables.  To access a specific table, use the `table()` function in the `FROM` clause, and specify the table name using the `tableName` parameter, as shown below.

```sql
SELECT * 
FROM table(dfs.`file_name.accdb` (type=> 'msaccess', tableName => 'Table1'))
```
