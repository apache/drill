# Google Sheets Connector for Apache Drill
This connector enables you to query and write to Google Sheets.  

### Usage Notes:
This feature should be considered experimental as Google's API for Sheets is quite complex and amazingly 
poorly documented.

## Setup Step 1:  Obtain Credential Information from Google
Ok... this is a pain.  GoogleSheets uses OAuth2.0 (may it be quickly deprecated) for authorization. In order to query GoogleSheets, you will first need to obtain three artifacts:

* Your `clientID`:  This is an identifier which uniquely identifies your application to Google
* Your `client_secret`: You can think of this as your password for your application to access GoogleSheets
* Your redirect URL:  This is the URL which Google will send the various access tokens which you will need later.  For a local installation of Drill, it will be: 
  `http://localhost:8047/credentials/<plugin name>/update_oauth2_authtoken`.

1. To obtain the `clientID` and `client_secret` you will need to obtain the Google keys, open the [Google Sheets API](https://console.cloud.google.com/apis/library/sheets.googleapis.com) and click on the `Enable` button. 
2. Once you've enabled teh API, you will be taken to the API Manager.  Either select a pre-existing project or create a new one.
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
      "scope": "https://www.googleapis.com/auth/spreadsheets"
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
Once you have configured Drill to query GoogleSheets, there is one final step before you can access data.  You must authenticate the application (Drill) with GoogleSheets.  After you have saved your GoogleSheets configuration, navigate back to the configuration screen for your plugin and click on `Authorize`. A new window should appear which will prompt you to authenticate with Google services.  Once you have done that, you should be able to query GoogleSheets!  See, that wasn't so hard!

### Authentication Modes:
The GoogleSheets plugin supports the `SHARED_USER` and `USER_TRANSLATION` authentication modes. `SHARED_USER` is as the name implies, one user for everyone. `USER_TRANSLATION` 
uses different credentials for each individual user.  In this case, the credentials are the OAuth2.0 access tokens.  

At the time of writing, we have not yet documented `USER_TRANSLATION` fully, however we will update this readme once that is complete.

## Querying Data
Once you have configured Drill to query 

### Obtaining the SpreadsheetID
The URL below is a public spreadsheet hosted on GoogleSheets:
[https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/](https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/)

In this URL, the portion `1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms` is the spreadsheetID. Thus, 
if you wanted to query this sheet in Drill, after configuring Drill, you could do so with the following
query:

```sql
SELECT * 
FROM googlesheets.`1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms`.`Class Data`
```

The format for the `FROM` clause for GoogleSheets is:
```sql
FROM <plugin name>.<sheet ID>.<tab name>
```
Note that you must specify the tab name to successfully query GoogleSheets.

### Using Aliases
Since the sheet IDs from Google are not human readable, one way to make your life easier is to use Drill's aliasing features to provide a better name for the actual sheet name. 

### Data Types
Drill's Google Sheets reader will attempt to infer the data types of the incoming data.  As with other connectors, this is an imperfect process since GoogleSheets does not 
supply a schema or other information to allow Drill to identify the data types of a column.  At present, here is how Drill will map your data:
* Numbers:  All numeric columns will be mapped to `DOUBLE` data types
* Boolean:  Columns containing `true/false` will be mapped to the `BOOLEAN` type
* Time, Date, Timestamp:  Temporal fields will be mapped to the correct type.  You can disable able temporal fields by setting the config option `XXX` to `false`.
* Text:  Anything else will be projected as `VARCHAR`

If the data type inference is not working for you, you can set the `allTextMode` to `true` and Drill will read everything as a `VARCHAR`.

#### Schema Provisioning


### Column Headers
When Drill reads GoogleSheets, it is assumed that the first row contains column headers.  
If this is incorrect you can set the `extractHeaders` parameter to `false`and Drill will name each field `field_n` where `n` is the column index. 

# Writing Data To GoogleSheets
When Drill is connected to GoogleSheets, you can also write data to GoogleSheets. The basic procedure is 
the same as with any other data source.  Simply write a `CREATE TABLE AS` (CTAS) query and your data will be
written to GoogleSheets.

One challenge is that once you have created the new sheet, you will have to manually retrieve the spreadsheet ID 
from GoogleSheets in order to query your new data.

### Dropping Tables
At the time of implementation, it is only possible to delete tables from within a GoogleSheets document. You may encounter errors if you try to delete tables from documents 
that only have one table in them.  The format for deleting a table is:

```sql
DROP TABLE googlesheets.<sheet id>.<tab name>
```

# Possible Future Work

### Auto-Aliasing
As of Drill 1.20, Drill allows you to create user and public aliases for tables and storage plugins. Since GoogleSheets
requires you to use a non-human readable ID to identify the Sheet.  One possible idea to make the Drill connection to GoogleSheets 
much more usable would be to automatically create an alias (either public) automatically mapping the unreadable sheetID to the document title.
This could be accomplished after the first query or after a CTAS query.

### Google Drive Integration
Integrating with Google Drive may allow additional functionality such as getting the actual document name, deleting documents and a few other basic functions. However, the 
Google Drive permissions require additional validation from Google. 

### Additional Pushdowns
The current implementation supports pushdowns for projection and limit.  
The GoogleSheets API is quite complex and incredibly poorly documented. In this author's opinion, it is quite possibly one of the worst APIs he has ever seen.
In any event, it may be possible to add filter, sort and perhaps other pushdowns.  
The current implementation keeps the logic to push filters down to the batch reader, but does not act on these filters.  
If someone figures out how to add the filter pushdowns and wishes to do so, the query planning logic is all there.