# CSV options and configuration

CSV parser of HTTP Storage plugin can be configured using `csvOptions`.

```json
{
  "csvOptions": {
    "delimiter": ",",
    "quote": "\"",
    "quoteEscape": "\"",
    "lineSeparator": "\n",
    "headerExtractionEnabled": null,
    "numberOfRowsToSkip": 0,
    "numberOfRecordsToRead": -1,
    "lineSeparatorDetectionEnabled": true,
    "maxColumns": 512,
    "maxCharsPerColumn": 4096,
    "skipEmptyLines": true,
    "ignoreLeadingWhitespaces": true,
    "ignoreTrailingWhitespaces": true,
    "nullValue": null
  }
}
```

## Configuration options

- **delimiter**: The character used to separate individual values in a CSV record.
  Default: `,`

- **quote**: The character used to enclose fields that may contain special characters (like the
  delimiter or line separator).
  Default: `"`

- **quoteEscape**: The character used to escape a quote inside a field enclosed by quotes.
  Default: `"`

- **lineSeparator**: The string that represents a line break in the CSV file.
  Default: `\n`

- **headerExtractionEnabled**: Determines if the first row of the CSV contains the headers (field
  names). If set to `true`, the parser will use the first row as headers.
  Default: `null`

- **numberOfRowsToSkip**: Number of rows to skip before starting to read records. Useful for
  skipping initial lines that are not records or headers.
  Default: `0`

- **numberOfRecordsToRead**: Specifies the maximum number of records to read from the input. A
  negative value (e.g., `-1`) means there's no limit.
  Default: `-1`

- **lineSeparatorDetectionEnabled**: When set to `true`, the parser will automatically detect and
  use the line separator present in the input. This is useful when you don't know the line separator
  in advance.
  Default: `true`

- **maxColumns**: The maximum number of columns a record can have. Any record with more columns than
  this will cause an exception.
  Default: `512`

- **maxCharsPerColumn**: The maximum number of characters a single field can have. Any field with
  more characters than this will cause an exception.
  Default: `4096`

- **skipEmptyLines**: When set to `true`, the parser will skip any lines that are empty or only
  contain whitespace.
  Default: `true`

- **ignoreLeadingWhitespaces**: When set to `true`, the parser will ignore any whitespaces at the
  start of a field.
  Default: `true`

- **ignoreTrailingWhitespaces**: When set to `true`, the parser will ignore any whitespaces at the
  end of a field.
  Default: `true`

- **nullValue**: Specifies a string that should be interpreted as a `null` value when reading. If a
  field matches this string, it will be returned as `null`.
  Default: `null`

## Example

### Parse tsv

To parse `.tsv` files you can use a following `csvOptions` config:

```json
{
  "csvOptions": {
    "delimiter": "\t"
  }
}
```

Then we can create a following connector plugin which queries a `.tsv` file from GitHub, let's call
it `github`:

```json
{
  "type": "http",
  "connections": {
    "test-data": {
      "url": "https://raw.githubusercontent.com/semantic-web-company/wic-tsv/master/data/de/Test/test_examples.txt",
      "requireTail": false,
      "method": "GET",
      "authType": "none",
      "inputType": "csv",
      "xmlDataLevel": 1,
      "postParameterLocation": "QUERY_STRING",
      "csvOptions": {
        "delimiter": "\t",
        "quote": "\"",
        "quoteEscape": "\"",
        "lineSeparator": "\n",
        "numberOfRecordsToRead": -1,
        "lineSeparatorDetectionEnabled": true,
        "maxColumns": 512,
        "maxCharsPerColumn": 4096,
        "skipEmptyLines": true,
        "ignoreLeadingWhitespaces": true,
        "ignoreTrailingWhitespaces": true
      },
      "verifySSLCert": true
    }
  },
  "timeout": 5,
  "retryDelay": 1000,
  "proxyType": "direct",
  "authMode": "SHARED_USER",
  "enabled": true
}
```

And we can query it using a following query:

```sql
SELECT * from github.`test-data`
```
