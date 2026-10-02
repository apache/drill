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

### Linked Tables
An Access file can define tables that live in another database file, referenced by an absolute local path or a UNC
share embedded in the file itself.  Drill refuses to follow those references by default: reading an untrusted file
would otherwise make the Drillbit open a path of the file author's choosing, as the Drillbit service account.

If you trust the files being queried and need linked tables to work, set `allowLinkedDatabases` to `true`:

```json
"msaccess": {
  "type": "msaccess",
  "extensions": ["mdb", "accdb"],
  "allowLinkedDatabases": true
}
```

It can also be enabled per query:

```sql
SELECT *
FROM table(dfs.`file_name.accdb` (type=> 'msaccess', tableName => 'Linked', allowLinkedDatabases => true))
```

## Schemas
Drill will discover the schema automatically from the Access file.  The plugin does support schema provisioning for consistency, but is not recommended.

## Querying a Table
Access files will contain multiple tables.  To access a specific table, use the `table()` function in the `FROM` clause, and specify the table name using the `tableName` parameter, as shown below.

```sql
SELECT * 
FROM table(dfs.`file_name.accdb` (type=> 'msaccess', tableName => 'Table1'))
```

## Metadata Queries
Since an Access file may contain multiple tables, there needs to be a way to determine what tables are present in the Access file.  In Drill, simply querying a file, without specifying a `tableName` will result in a metadata query, rather than getting the actual data back.  

For example:

```sql
SELECT * FROM dfs.test.`access/data/V2019/extDateTestV2019.accdb`;
+--------+-------------------------+-------------------------+-----------+-----------+----------------------------------------------------------------------+
| table  |      created_date       |      updated_date       | row_count | col_count |                               columns                                |
+--------+-------------------------+-------------------------+-----------+-----------+----------------------------------------------------------------------+
| Table1 | 2021-06-03 20:09:56.993 | 2021-06-03 20:09:56.993 | 9         | 6         | ["ID","Field1","DateExt","DateNormal","DateExtStr","DateNormalCalc"] |
+--------+-------------------------+-------------------------+-----------+-----------+----------------------------------------------------------------------+

```


## Password-Protected Files
The password protection in Access is just a software level protection and really does not offer any security.  Drill can query password protected files without any password.