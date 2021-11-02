# Format Plugin for PDF Table Reader
One of the most annoying tasks is when you are working on a data science project and you get data that is in a PDF file. This plugin endeavours to enable you to query data in
 PDF tables using Drill's SQL interface.  

## Data Model
Since PDF files generally are not intended to be queried or read by machines, mapping the data to tables and rows is not a perfect process.  The PDF reader does support 
provided schema. 

### Merging Pages
The PDF reader reads tables from PDF files on each page.  If your PDF file has tables that span multiple pages, you can set the `combinePages` parameter to `true` and Drill 
will merge all the tables in the PDF file.  You can also do this at query time with the `table()` function.

## Configuration
To configure the PDF reader, simply add the information below to the `formats` section of a file base storage plugin. 

```json
"pdf": {
  "type": "pdf",
  "extensions": [
    "pdf"
  ],
  "extractHeaders": true,
  "combinePages": false
}
```
The avaialable options are:
* `extractHeaders`: Extracts the first row of any tables as the header row.  If set to false, Drill will assign column names of `field_0`, `field_1` to each column.
* `combinePages`: Merges multipage tables together.
* `defaultTableIndex`:  Allows you to query different tables within the PDF file. Index starts at `0`. 


## Accessing Document Metadata Fields
PDF files have a considerable amount of metadata which can be useful for analysis.  Drill will extract the following fields from every PDF file.  Note that these fields are not
 projected in star queries and must be selected explicitly.  The document's creator populates these fields and some or all may be empty. With the exception of `_page_count
 ` which is an `INT` and the two date fields, all the other fields are `VARCHAR` fields.
 
 The fields are:
 * `_page_count`
 * `_author`
 * `_title`
 * `_keywords`
 * `_creator`
 * `_producer`
 * `_creation_date`
 * `_modification_date`
 * `_trapped`
 * `_table_count`
 
 The query below will access a document's metadata:
 
 ```sql
SELECT _page_count, _title, _author, _subject, 
_keywords, _creator, _producer, _creation_date, 
_modification_date, _trapped 
FROM dfs.`pdf/20.pdf`
```
The query below demonstrates how to define a schema at query time:

```sql
SELECT * FROM table(cp.`pdf/schools.pdf` (type => 'pdf', combinePages => true, 
schema => 'inline=(`Last Name` VARCHAR, `First Name Address` VARCHAR, 
`field_0` VARCHAR, `City` VARCHAR, `State` VARCHAR, `Zip` VARCHAR, 
`field_1` VARCHAR, `Occupation Employer` VARCHAR, 
`Date` VARCHAR, `field_2` DATE properties {`drill.format` = `M/d/yyyy`}, 
`Amount` DOUBLE)')) 
LIMIT 5
```
