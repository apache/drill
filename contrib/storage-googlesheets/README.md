# Google Sheets Connector for Apache Drill
This connector enables you to query and write to Google Sheets.

### Usage Notes:
This feature should be considered experimental as Google's API for Sheets is quite complex and amazingly
poorly documented.

## Setup Step 1:  Obtain Credential Information from Google
Ok... this is a pain.  Google Sheets uses OAuth2.0 (may it be quickly deprecated) for authorization. In order to query Google Sheets, you will first need to obtain three artifacts:

* Your `clientID`:  This is an identifier which uniquely identifies your application to Google
* Your `client_secret`: You can think of this as your password for your application to access Google Sheets
* Your redirect URL:  This is the URL to which Google will send the various access tokens and which you will need later.  For a local installation of Drill, it will be:
  `http://localhost:8047/credentials/<plugin name>/update_oauth2_authtoken`.

1. To obtain the `clientID` and `clientSecret` you will need to obtain the Google keys, open the [Google Sheets API](https://console.cloud.google.com/apis/library/sheets.googleapis.com) and click on the `Enable` button.
2. Once you've enabled the API, you will be taken to the API Manager.  Either select an existing project or create a new one.
3. Next, navigate to the `Credentials` in the left panel.
4. Click on `+Create Credentials` at the top of the page.  Select `OAuth client ID` and select `Web Application` or `Desktop` as the type.  Follow the instructions and download
   the JSON file that Google provides.

Drill does not use the JSON file, but you will be cutting and pasting values from the JSON file into the Drill configuration.

## Setup Step 2:  Configure Drill
Create a storage plugin following the normal procedure for doing so.  You can use the example below as a template.  Cut and paste the `clientID` and `client_secret` from the
JSON file into your Drill configuration as shown below.  Once you've done that, save the configuration.

```json
{
  "type": "googlesheets",
  "allTextMode": true,
  "extractHeaders": true,
  "oAuthConfig": {
    "callbackURL": "http://localhost:8047/credentials/googlesheets/update_oauth2_authtoken",
    "authorizationURL": "https://accounts.google.com/o/oauth2/auth",
    "authorizationParams": {
      "response_type": "code",
      "scope": "https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/drive.metadata.readonly"
    }
  },
  "credentialsProvider": {
    "credentialsProviderType": "PlainCredentialsProvider",
    "credentials": {
      "clientID": "<YOUR CLIENT ID>",
      "clientSecret": "<YOUR CLIENT SECRET>",
      "tokenURI": "https://oauth2.googleapis.com/token"
    },
    "userCredentials": {}
  },
  "enabled": true,
  "authMode": "SHARED_USER"
}
```

With the exception of the clientID, client_secret and redirects, you should not have to modify any of the other parameters in the configuration.

### Other Configuration Parameters

There are two configuration parameters which you may want to adjust:
* `allTextMode`:  This parameter when `true` disables Drill's data type inferencing for your files.  If your data has inconsistent data types, set this to `true`.  Default is
  `true`.
* `extractHeaders`:  When `true`, Drill will treat the first row of your data as headers.  When `false` Drill will assign column names like `field_n` for each column.

### Authenticating with Google
Once you have configured Drill to query Google Sheets, there is one final step before you can access data.  You must authenticate the application (Drill) with Google Sheets.  After you have saved your Google Sheets configuration, navigate back to the configuration screen for your plugin and click on `Authorize`. A new window should appear which will prompt you to authenticate with Google services.  Once you have done that, you should be able to query Google Sheets!  See, that wasn't so hard!

### Authentication Modes:
The Google Sheets plugin supports the `SHARED_USER` and `USER_TRANSLATION` authentication modes. `SHARED_USER` is as the name implies, one user for everyone. `USER_TRANSLATION`
uses different credentials for each individual user.  In this case, the credentials are the OAuth2.0 access tokens.

At the time of writing, we have not yet documented `USER_TRANSLATION` fully, however we will update this readme once that is complete.

## Querying Data
Once you have configured Drill to connect to Google Sheets, querying is very straightforward.

### Obtaining the SpreadsheetID
The URL below is a public spreadsheet hosted on Google Sheets:
[https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/](https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/)

In this URL, the portion `1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms` is the spreadsheetID. Thus,
if you wanted to query this sheet in Drill, after configuring Drill, you could do so with the following
query:

```sql
SELECT *
FROM googlesheets.`1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms`.`Class Data`
```

The format for the `FROM` clause for Google Sheets is:
```sql
FROM <plugin name>.<sheet ID>.<tab name>
```
Note that you must specify the tab name to successfully query Google Sheets.

### Accessing Tabs by Index
If you don't know the names of the available tabs in your GoogleSheets document, you can query the sheets by index using the `tab[n]` format.  Indexing starts at zero and every Sheets document must have at least one sheet.  Note that this must be enclosed in backticks.

```sql
SELECT *
FROM googlesheets.<sheet id>.`tab[0]`
```


### Metadata
You can obtain a list of available sheets by querying the `INFORMATION_SCHEMA` as shown below.  Assuming that you have a connection to Google Sheets called `googlesheets`:

```sql
SELECT *
FROM `INFORMATION_SCHEMA`.`SCHEMATA`
WHERE SCHEMA_NAME LIKE 'googlesheets%'
```

Due to rate limits from Google, the tabs are not reported to the `INFORMATION_SCHEMA`.  However, it is possible to obtain a list of all available tabs with the following query:

```sql
SELECT _sheets
FROM googlesheets.`<token>`.`<sheet>`
LIMIT 1
```

You can also access the file name with the `_title` field.  Note that the file name is NOT
unique and should not be used for querying data.

### Using Aliases
Since the sheet IDs from Google are not human readable, one way to make your life easier is to use Drill's aliasing features to provide a better name for the actual sheet name.

### Data Types
Drill's Google Sheets reader will attempt to infer the data types of the incoming data.  As with other connectors, this is an imperfect process since Google Sheets does not
supply a schema or other information to allow Drill to identify the data types of a column.  At present, here is how Drill will map your data:
* Numbers:  All numeric columns will be mapped to `DOUBLE` data types
* Boolean:  Columns containing `true/false` will be mapped to the `BOOLEAN` type
* Time, Date, Timestamp:  Temporal fields will be mapped to the correct type.
* Text:  Anything else will be projected as `VARCHAR`

If the data type inference is not working for you, you can set the `allTextMode` to `true` and Drill will read everything as a `VARCHAR`.

#### Schema Provisioning
As with other plugins, you can provide a schema inline as shown in the example query below.

```sql
SELECT *
FROM table(`googlesheets`.`<your google sheet>`.`MixedSheet`
    (schema => 'inline=(`Col1` VARCHAR, `Col2` INTEGER, `Col3` VARCHAR)'))
LIMIT 5
```


### Column Headers
When Drill reads Google Sheets, it is assumed that the first row contains column headers.
If this is incorrect you can set the `extractHeaders` parameter to `false`and Drill will name each field `field_n` where `n` is the column index.

# Writing Data To Google Sheets
When Drill is connected to Google Sheets, you can also write data to Google Sheets. The basic procedure is
the same as with any other data source.  Simply write a `CREATE TABLE AS` (CTAS) query and your data will be
written to Google Sheets.

One challenge is that once you have created the new sheet, you will have to manually retrieve the spreadsheet ID
from Google Sheets in order to query your new data.

### Dropping Tables
At the time of implementation, it is only possible to delete tables from within a Google Sheets document. You may encounter errors if you try to delete tables from documents
that only have one table in them.  The format for deleting a table is:

```sql
DROP TABLE googlesheets.<sheet id>.<tab name>
```

# Possible Future Work

### Auto-Aliasing
As of Drill 1.20, Drill allows you to create user and public aliases for tables and storage plugins. Since Google Sheets
requires you to use a non-human readable ID to identify the Sheet.  One possible idea to make the Drill connection to Google Sheets
much more usable would be to automatically create an alias (either public) automatically mapping the unreadable sheetID to the document title.
This could be accomplished after the first query or after a CTAS query.

### Google Drive Integration
Integrating with Google Drive may allow additional functionality such as getting the actual document name, deleting documents and a few other basic functions. However, the
Google Drive permissions require additional validation from Google.

### Additional Pushdowns
The current implementation supports pushdowns for projection and limit.
The Google Sheets API is quite complex and incredibly poorly documented. In this author's opinion, it is quite possibly one of the worst APIs he has ever seen.
In any event, it may be possible to add filter, sort and perhaps other pushdowns.
The current implementation keeps the logic to push filters down to the batch reader, but does not act on these filters.
If someone figures out how to add the filter pushdowns and wishes to do so, the query planning logic is all there.
