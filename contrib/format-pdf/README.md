# Format Plugin for PDF Table Reader
One of the most annoying tasks is when you are working on a data science project and you get data that is in a PDF file. This plugin endeavours to enable you to query data in
 PDF tables using Drill's SQL interface.  

## Data Model
Since PDF files generally are not intended to be queried or read by machines, mapping the data to tables and rows is not a perfect process.  The PDF reader does support 
provided schema. 

### Merging Pages
The PDF reader reads tables from PDF files on each page.  If your PDF file has tables that span multiple pages, you can set the `combinePages` parameter to `true` and Drill 
will merge all the tables in the PDF file.  You can also do this at query time with the `table()` function.

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
