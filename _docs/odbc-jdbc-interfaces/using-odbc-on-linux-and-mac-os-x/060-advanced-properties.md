---
title: "Advanced Properties"
parent: "Using ODBC on Linux and Mac OS X"
---
When you use advanced properties on Linux and Mac OS X, you must separate them using a semi-colon
(;).

For example, the following Advanced Properties string excludes the schemas
named `test` and `abc`; sets the timeout to 30 seconds; and sets the time zone
to Coordinated Universal:

`Time:HandshakeTimeout=30;QueryTimeout=30;
TimestampTZDisplayTimezone=utc;ExcludedSchemas=test,abc`

The following table lists and describes the advanced properties that you can
set when using the MapR Drill ODBC Driver on Linux and Mac OS X.

| Property Name              | Default Value           | Description                                                                                                                                                                                                |
|----------------------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| HandshakeTimeout           | 5                       | An integer value representing the number of seconds that the driver waits before aborting an attempt to connect to a data source. When set to a value of 0, the driver does not abort connection attempts. |
| QueryTimeout               | 180                     | An integer value representing the number of seconds for the driver to wait before automatically stopping a query. When set to a value of 0, the driver does not stop queries automatically.                |
| TimestampTZDisplayTimezone | local                   | Two values are possible:local—Timestamps are dependent on the time zone of the user.utc—Timestamps appear in Coordinated Universal Time (UTC).                                                             |
| ExcludedSchemas            | sys, INFORMATION_SCHEMA | The value of ExcludedSchemas is a list of schemas that do not appear in client applications such as Drill Explorer, Tableau, and Excel. Separate schemas in the list using a comma (,).                    |
| CastAnyToVarchar           | true                    | Casts the “ANY” and “(VARCHAR(1), ANY) MAP” data types returned from SQL column calls into type “VARCHAR”.                                                                                                 |

