---
title: "Advanced Properties"
parent: "Using ODBC on Linux and Mac OS X"
---
When you use advanced properties, you must separate them using a semi-colon
(;).

For example, the following Advanced Properties string excludes the schemas
named `test` and `abc`; sets the timeout to 30 seconds; and sets the time zone
to Coordinated Universal:

`Time:HandshakeTimeout=30;QueryTimeout=30;
TimestampTZDisplayTimezone=utc;ExcludedSchemas=test,abc`

The following table lists and describes the advanced properties that you can
set when using the MapR Drill ODBC Driver on Linux and Mac OS X.

<table ><tbody><tr><td valign="top"><strong>Property Name</strong></td><td valign="top"><strong>Default Value</strong></td><td valign="top"><strong>Description</strong></td></tr><tr><td valign="top">HandshakeTimeout</td><td valign="top">5</td><td valign="top">An integer value representing the number of seconds that the driver waits before aborting an attempt to connect to a data source. When set to a value of 0, the driver does not abort connection attempts.</td></tr><tr><td valign="top">QueryTimeout</td><td valign="top">180</td><td valign="top">An integer value representing the number of seconds for the driver to wait before automatically stopping a query. When set to a value of 0, the driver does not stop queries automatically.</td></tr><tr><td valign="top">TimestampTZDisplayTimezone</td><td valign="top">local</td><td valign="top">Two values are possible:local—Timestamps are dependent on the time zone of the user.utc—Timestamps appear in Coordinated Universal Time (UTC).</td></tr><tr><td valign="top">ExcludedSchemas</td><td valign="top">sys, INFORMATION_SCHEMA</td><td valign="top">The value of ExcludedSchemas is a list of schemas that do not appear in client applications such as Drill Explorer, Tableau, and Excel. Separate schemas in the list using a comma (,).</td></tr><tr><td colspan="1" valign="top"><span style="color: rgb(0,0,0);">CastAnyToVarchar</span></td><td colspan="1" valign="top">true</td><td colspan="1" valign="top"><span style="color: rgb(0,0,0);">Casts the “ANY” and “(VARCHAR(1), ANY) MAP” data types returned from SQL column calls into type “VARCHAR”.</span></td></tr></tbody></table>

