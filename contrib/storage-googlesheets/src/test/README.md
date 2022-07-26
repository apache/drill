# Testing Procedures for Google Sheets Plugin
The GS plugin is a little tricky to test because it makes extensive use of the Google APIs. The plugin is designed to make extensive use of static functions in the `GoogleSheetsUtils` class which can be tested without a live connection to Google Sheets.  

This plugin makes extensive use of the `GoogleSheetUtils` class to test the various functions and steps.  The functions which do not require a live connection to GoogleSheets are mostly covered by the unit tests in `TestGoogleSheetUtils` and `TestRangeBuilder`.  

# Testing Actual Queries

## Step One:  Obtaining Credentials
To run the end to end tests in `TestGoogleSheetsQueries` and `TestGoogleSheetsWriter` you first have to provide credentials in the form of the client id, client secret, access token, and refresh token.  The `client_id` and `client_secret` tokens can be obtained by following the instructions in the main `README` in the root directory. 

To obtain the access and refresh tokens, build Drill, add a GoogleSheets plugin and use the `clientID` and `clientSecret` to authorize your Drill.  Then look in your Drill folder for the files where Drill stores OAuth tokens and you will find your access and refresh tokens.

## Step Two:  Saving Your Tokens
Now that you have the actual tokens, the next step is to actually save them in a file called `oauth_tokens.json` in the `/test/resources/token` directory.  Create the file and copy the JSON below, filling in your tokens. 

```json
{
  "client_id": "<your client id>",
  "client_secret": "<your client secret>",
  "access_token":"<your access token>",
  "refresh_token":"<your refresh token>",
  "sheet_id": "<your sheet id>"
}

```

## Step 3:  Populating Data
The final step is to create the actual test GoogleSheet.  In the `test/resources` document, there is a file called `Drill Test Data.xlsx`.  Upload this file to GoogleSheets.  Once you have done so, simply add your sheet ID to the tokens.json file that you created earlier.

Once this is done, you should be able to run the tests in `TestGoogleSheetsWriter` and `TestGoogleSheetsQueries`. 

