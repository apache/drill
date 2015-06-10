---
title: "Browsing Data and Creating Views"
parent: "Using Drill Explorer"
---
After connecting Drill Explorer to data, the Browse and SQL tab appear on the right side of the console. On the Browse tab, you view any metadata that might exist for a schema that you access with Drill. On the SQL tab, you preview the results
of custom queries and save the results as a view.

## Browsing Data

You can browse files and directories if you have permission to read them. The following example shows how browse Drill sample data.

1. Start Drill if necessary.  
2. Check that the Schema property in the `.odbc.ini` is blank, which puts the default `dfs` schema into effect.  
3. Start Drill Explorer and connect to the sample DSN.  
4. In the **Schemas** section of Drill Explorer, on the **Browse** tab, navigate to the ` Drill installation directory, to the `sample-data` directory. Click `nation.parquet`.  
   ![nation parquet]({{ site.baseurl }}/docs/img/explorer-nation-data.png) 


## Creating a View

1. Start Drill if necessary.  
2. Check that the Schema property in the .odbc.ini is blank, which puts the default `dfs`schema into effect.  
3. Start Drill Explorer and connect to the sample DSN.  
4. In the **Schemas** section on the **Browse** tab, navigate to the the data source that you want to create a view for.  
   After you select a data souce, the metadata and data displays on the Browse tab and the SQL that is used to access the data displays on the SQL tab.  
5. Click the **SQL** tab.  
6. In the **View Definition SQL** field, enter the SQL query that you want to create a view for.  
7. Click **Preview**.   
   If the results are not as expected, you can edit the SQL query and click
Preview again.  
8. Click **Create As**.  
   The _Create As_ dialog displays.  
9. In the **Schema** field, select the schema where you want to save the view.  
   You can save views only to file-based schemas.  
10. In the **View Name** field, enter a descriptive name for the view.  
    Do not include spaces in the view name.  
11. Click **Save**.   
    The status and any error message associated with the view creation displays in
the Create As dialog. When a view saves successfully, the Save button changes
to a Close button.  
12. Click **Close**.


