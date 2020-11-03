---
title: "SPSS Format Plugin"
slug: "SPSS Format Plugin"
parent: "Data Sources and File Formats"
---

**Introduced in release:** 1.18.

Drill provides a format plugin for reading and querying data files from Statistical Package for the Social Sciences (SPSS).


## Configuring the SPSS Format Plugin  

To configure Drill to read SPSS files, simply add the following code to the formats section of your 
file-based storage plugin.  This should happen automatically for the default
 `cp`, `dfs`, and `S3` storage plugins.
 
Other than the file extensions, there are no variables to configure.
 
```json
"spss": {         
  "type": "spss",
  "extensions": ["sav"]
 }
```

## Data Model

SPSS only supports two data types: Numeric and String.  Drill maps these to its DOUBLE and VARCHAR types respectively.  However, for some numeric columns, SPSS maps these numbers to text, similar to an `enum` field in Java.
 
For instance, a field called `Survey` might have labels as shown below.

|-------|-----------|
| Value | Text      |
|-------|-----------|
| 1     | Yes       |
| 2     | No        |
| 99    | No answer |
|-------|-----------|


In this situation Drill will create _two_ columns, one called `Survey` which takes on numeric values in { 1, 2, 99 } and another called `Survey_value` which contains the corresponding text.  The next table gives an example of data returned by Drill when the `Survey` column is queried.

|--------|--------------|
| Survey | Survey_value |
|--------|--------------|
| 1      | Yes          |
| 1      | Yes          |
| 1      | Yes          |
| 2      | No           |
| 1      | Yes          |
| 2      | No           |
| 99     | No answer    |
|--------|--------------|
 

<!-- TODO: add an example -->	
