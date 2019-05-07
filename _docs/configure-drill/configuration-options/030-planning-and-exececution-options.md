---
title: "Planning and Execution Options"
date: 2019-05-07
parent: "Configuration Options"
---
You can set Drill query planning and execution options per cluster, at the
system or session level. Options set at the session level only apply to
queries that you run during the current Drill connection. Options set at the
system level affect the entire system and persist between restarts. Session
level settings override system level settings for the duration of the session.  

Planning options are prepended by planner, for example `planner.enable_hashjoin`. Execution options are prepended by drill.exe, for example `drill.exec.functions.cast_empty_string_to_null`.

## Setting Planning and Execution Options  
You can set planning and execution options for Drill from the Drill shell (SQLLine) or the Drill Web UI. Options set in the Drill Web UI are set at the system level. You can override system level options set in the Drill Web UI by setting the options at the session level from the Drill shell.   

### Setting Options from the Drill Shell 

Use the [ALTER SYSTEM]({{site.baseurl}}/docs/alter-system/) or [SET]({{site.baseurl}}/docs/set/) commands to set planning and execution options at the system or session level. Typically,
you set the options at the session level unless you want the setting to
persist across all sessions. 

You can run the following query to see a list of options:  
	
    SELECT * FROM sys.options;    

The query returns a table that lists options with descriptions and other details. As of Drill 1.15, there are 179 options:  

	SELECT COUNT() AS num_of_sysopts FROM sys.options;
	+-----------------+
	| num_of_sysopts  |
	+-----------------+
	| 179             |
	+-----------------+  

See [Querying the Options Table]({{site.baseurl}}/docs/querying-system-tables/#querying-the-options-table).   


### Setting Options from the Drill Web UI  

When Drill is running, you can access the Drill Web UI at `http://<drill-hostname>:8047/`. The Drill Web UI has an Options button located in the upper right corner that you can click to display an Options page. 

The Options page lists all the Drill options that you can set at the system level. To override an option for the duration of a session, set the option from the Drill shell using the SET command.  

Starting in Drill 1.15, the Options page includes the following enhancements:  
  
**Search Field**  
The search field enables you to quickly search across all available Drill configuration options to quickly find options you are interested in. For example, you can search on the keyword "memory" for a list of all options related to configuring Drill memory. Starting in Drill 1.16, the Search field contains an x that clears the search field when clicked. 

**Quick Filters**  
Next to the search field are quick filter buttons that filter options by important keywords in one click. For example, the "planner" quick filter button quickly lists all options related to the query planner. The query planning options are prepended by the keyword "planner." Quick filters return options that include the keyword in the option name and/or description. 

**Default Button**  
Each option has a Default button that indicates whether the default value for the option was changed. If the Default button is not active, the option is currently set to the default value. If the default button is active, the value of the option was changed from the default. Clicking an active Default button resets the value of the option back to the original (default) value. 

**Web Display Options**  
Prior to Drill 1.15, timestamp, date, and time values did not display correctly in query results when running queries from the Query page in the Drill Web UI; however, you can set the output format for these values through the following three options:

- `web.display_format.date`
- `web.display_format.time`
- `web.display_format.timestamp`  

**Example: Setting Web Display Options**  

The following examples demonstrate how the web display options change the format for date, time, and timestamp values:  

**Date**  
Issuing `select date '2008-2-23' from (values(1))` returns the following results, by default, in the Web UI:  

	2008-02-23  

Issuing the query after setting the `web.display_format.date` option to the format `EEE, MMM d, yyyy` returns the following results in the Web UI:  
 
	Sat, Feb 23, 2008  

**Time**  
Issuing `select time '12:23:34' from (values(1))` returns the following results, by default, in the Web UI:  

	12:23:34  

Issuing the query after setting the `web.display_format.time` option to the format `HH:mm:ss.SS` returns the following results in the Web UI:  
 
	12:23:34.00 

**Timestamp**  
Issuing ` select timestamp '2008-2-23 12:23:34' from (values(1))` returns the following results, by default, in the Web UI:  

	2008-02-23T12:23:34  

Issuing the query after setting the `web.display_format.timestamp` option to the format `yyyy-MM-dd HH:mm:ss.SS` returns the following results in the Web UI:  
 
	2008-02-23 12:23:34.00  

##Setting an Auto Limit on the Number of Rows Returned for Result Sets  
The `exec.query.max_rows` option, introduced in Drill 1.16, automatically limits the number of rows that Drill returns from a result set. When you configure a limit, the limit is automatically applied by Drill when you run a query. You do not need to include the LIMIT clause. Configuring a limit on results reduces the amount of processing and streaming work for the web server when using the Drill WEb UI. 

The default setting for `exec.query.max_rows` is 0, which indicates no limit; queries will return all results. An administrator can modify the exec.query.max_rows setting at the system level (setting persists across restarts) from the Options page in the Drill Web UI or using the ALTER SYSTEM command. Alternatively, the option can be set at the session level (valid for a user session only) using the SET command or from SQLLine.  

**Session Level**  
To set the `exec.query.max_rows` option per user session, use the SET command or the SQLLine line command, as shown:  
	
	SET `exec.query.max_rows` = <value>;
	!set rowlimit <value>  

**System Level**  
To set the `exec.query.max_rows` option such that it persists across users, change the setting in the Drill Web UI (`http://<drill-hostname>:8047/options`) or use the ALTER SYSTEM SET command, as shown:  

	ALTER SYSTEM SET `exec.query.max_rows` = <value>;  

***  
**NOTE**  
The `exec.query.max_rows` option overrides row limit settings configured in the Drill Web UI and `drill-override.conf` file, which are described below.   
***   

### Drill Web UI Row Limit Settings  
In addition to the `exec.query.max_rows` option, the Query page in the Drill Web UI provides a field where you can enter a limit on the number of rows returned from a result set when you run a query.  
  
**Note:** You can also configure the row limit for a REST client through the [autoLimit parameter in the REST API]({{site.baseurl}}/docs/rest-api-introduction/#post-query-json) or a JDBC client through the `setMaxRows` method in the [Java API]({{site.baseurl}}/docs/using-the-jdbc-driver/#using-the-drill-driver-class-name).   
  
![](https://i.imgur.com/7nF0RBq.png)    
When you submit the query, results display across one or multiple pages. You can set the number of results that display per page. When results span multiple pages, scroll through the pages using the links in the bottom right corner of the screen.
![](https://i.imgur.com/eYn22NL.png)   
Clicking the Query Profile tab at the top of the results page displays the query profile with a warning stating that the results were automatically limited.
![](https://i.imgur.com/L4Yx666.png)  
Note that the `exec.query.max_rows` setting overrides the setting in the Web UI. For example, if the `exec.query.max_rows` option is set to 10 and you limit results to 20 on the Query page in the Web UI, Drill only returns 10 rows from the result set.  

###Drill-override.conf Row Limit Settings  
The `drill-override.conf` file contains the same row limit setting as the Drill Web UI; however, the setting in the `drill-override.conf` file overrides the limit set in the Drill Web UI. Note that the `exec.query.max_rows` option overrides the setting in `drill-override.conf` and the Drill Web UI. You must restart the Drillbit after you modify `drill-override.conf`. 

Add the following configuration to `drill-override.conf`, changing the row limit setting and rows per page setting, as needed:  

	drill.exec.http: {
	    web.client.resultset: {
	        autolimit {
	            enable: true,
	            rows: 1000
	        },
	        defaultRowsPerPage: 10
	    }
	}




 







 



