# Format Plugin for SAS Files
This format plugin enables Drill to read SAS files. (sas7bdat)

## Data Types
The SAS format supports the `VARCHAR`, `LONG`, `DOUBLE` and `DATE` formats.

## Schema Provisioning
Drill will infer the schema of your data.  

## Configuration Options 
This function has no configuration options other than the file extension.

```json
  "sas": {
  "type": "sas",
  "extensions": [
    "sas7bdat"
  ]
}
```
This plugin is enabled by default. 
