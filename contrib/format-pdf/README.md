# Format Plugin for PDF Table Reader
One of the most annoying tasks is when you are working on a data science project and you get data that is in a PDF file. This plugin endeavours to enable you to query data in
 PDF tables using Drill's SQL interface.  

## Data Model
Since PDF files were not intended to be queried or read by machines, mapping the data to tables and rows is not a perfect process.

## Implicit Fields
PDF files have a considerable amount of metadata which can be useful for analysis.  Drill will extract the following fields from every PDF file.  Note that these fields are not
 projected in star queries and must be selected explicitly.
 
 
TODO 
* Wrap pages together (add option to do so) 
