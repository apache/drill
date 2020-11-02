---
title: "Excel Format Plugin"
date: 2020-11-02
parent: "Data Sources and File Formats"
---

**Introduced in release:** 1.17.

This plugin enables Drill to read Microsoft Excel files.  This format is best used with Excel files that do not have extensive formatting, however it will work with formatted files, by allowing you to define a region within the file which contains the data to be read by Drill.

The plugin will automatically evaluate cells which contain formulae. 

<!-- TODO: are all versions of Excel files supported? -->

## Configuring the Excel Format Plugin

This plugin has several configuration variables which must be set in order to read Excel files effectively. Since Excel files often contain other elements besides data, you can use the configuration variables to define a region within your spreadsheet in which Drill should extract data. This is potentially useful if your spreadsheet contains a lot of formatting or other complications. 

### Configuration Options

The most basic configuration is simply to add the following to a file based storage plugin.

    "excel": {
      "type": "excel"
    }

Additional configuration options are tabled below.

|-------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Option      | Default       | Description                                                                                                                                                                                                                                            |
|-------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| headerRow   | 0             | Set to -1 if there are no column headers. If the data does not have a header row, Drill will assign column names of `field_n` for each column.                                                                                                         |
| lastRow     | 1048576       | This defines the last row of data. It is only necessary to set this if you want Drill to stop reading at a specific location.                                                                                                                          |
| sheetName   | (first sheet) | This is the name of the sheet which Drill will query. Drill will throw an exception if the sheet is not found.                                                                                                                                         |
| firstColumn | (none)        | In order to define a region within a spreadsheet, this is the left-most column index. This is indexed from one. If set to 0, Drill will start at the left most column.                                                                                 |
| lastColumn  | (none)        | To define a region within a spreadsheet, this is the right-most column index. This is indexed from one. If set to 0 Drill will read all available columns. This is not right-inclusive, so if you ask for columns 2-5 you will get columns 2, 3 and 4. |
| allTextMode | false         | When set to true, Drill will not attempt to infer column data types and will read everything as VARCHAR.                                                                                                                                               |
|-------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|

## Usage Notes

You can specify the configuration at runtime via the `table()` function or in the storage plugin configuration. For instance, if you just want to query an Excel file, you could
 execute the query as follows:

    SELECT <fields> 
    FROM dfs.`somefile.xlsx`

To query a different sheet other than the default, use the `table()` function as shown below:

    SELECT <fields> 
    FROM table( dfs.`test_data.xlsx` (type => 'excel', sheetName => 'secondSheet'))

To join data together from different sheets in one file, use the query below:

    SELECT <fields> 
    FROM table( dfs.`test_data.xlsx` (type => 'excel', sheetName => 'secondSheet')) AS t1
    INNER JOIN table( dfs.`test_data.xlsx` (type => 'excel', sheetName => 'thirdSheet')) AS t2 
    ON t1.id = t2.id


### Implicit Columns

Drill includes several columns of file metadata in the query results. These fields are **not** included in star queries and thus must be explicitly listed in a query. 

<!-- TODO: convert to a table including data types and descriptions -->

The fields are:

    _category
    _content_status
    _content_type;
    _creator
    _description
    _identifier
    _keywords
    _last_modified_by_user
    _revision
    _subject
    _title
    _created
    _last_printed
    _modified


### Known Limitations

At present, Drill requires that all columns be of the same data type. If they are not, Drill will throw an exception upon trying to read a column of mixed data type. If you are trying to query data with heterogenoeus columns, it will be necessary to set `allTextMode` to true. 

An additional limitation is that Drill infers the column data type from the first row of data. If a column is null in the first row, Drill will default to a datatype of VARCHAR. However if in fact the column is NUMERIC this will cause errors. 
 
 Drill ignores blank rows.
