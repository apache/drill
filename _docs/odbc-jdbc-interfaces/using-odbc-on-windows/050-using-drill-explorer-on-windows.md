---
title: "Using Drill Explorer on Windows"
parent: "Using ODBC on Windows"
---
Drill Explorer is a simple user interface that is embedded within the ODBC
DSN. Drill Explorer enables users to understand the metadata and data before
visualizing the data in a BI tool. Use Drill Explorer to browse Drill data
sources, preview the results of a SQL query, and create a view that you can
query.

The Browse tab of Drill Explorer allows you to view metadata for each schema
that you can access with Drill. The SQL tab allows you to preview the results
of custom queries and save the results as a view.

**To Browse Data:**

  1. To launch the ODBC Administrator, click
     **Start > All Programs > MapR Drill ODBC Driver 1.0 (32|64-bit) > (32|64-bit) ODBC Administrator**.
  2. Click the **User DSN** tab or the **System DSN** tab and then select the DSN that corresponds to the Drill data source that you want to explore.
  3. Click **Configure**.  
     The _MapR Drill ODBC Driver DSN Setup_ dialog appears.
  4. Click **Drill Explorer**.
  5. In the **Schemas** section on the **Browse** tab, navigate to the the data source that you want to explore.

**To Create a View**:

  1. To launch the ODBC Administrator, click **Start > All Programs > MapR Drill ODBC Driver 1.0 (32|64-bit) > (32|64-bit) ODBC Administrator**.
  2. Click the **User DSN** tab or the **System DSN** tab and then select the DSN that corresponds to the Drill data source that you want to explore.
  3. Click **Configure**.  
     The _MapR Drill ODBC Driver DSN Setup_ dialog appears.
  4. Click **Drill Explorer**.
  5. In the **Schemas** section on the **Browse** tab, navigate to the the data source that you want to create a view for.  
     After you select a data souce, the metadata and data displays on the Browse tab and the SQL that is used to access the data displays on the SQL tab.
  6. Click the **SQL** tab.
  7. In the **View Definition SQL** field, enter the SQL query that you want to create a view for.
  8. Click **Preview**.   
      If the results are not as expected, you can edit the SQL query and click
Preview again.
  9. Click **Create As**.  
     The _Create As_ dialog displays.
  10. In the **Schema** field, select the schema where you want to save the view.
      You can save views only to file-based schemas.
  11. In the **View Name** field, enter a descriptive name for the view.
      Do not include spaces in the view name.
  12. Click **Save**.   
      The status and any error message associated with the view creation displays in
the Create As dialog. When a view saves successfully, the Save button changes
to a Close button.
  13. Click **Close**.

