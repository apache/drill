#Dropbox and Drill
As of Drill 1.20.0 it is possible to connect Drill to a Dropbox account and query files stored there.  Clearly, the performance will be much better if the files are stored 
locally, however, if your data is located in dropbox Drill makes it easy to explore that data.

## Creating an API Token
The first step to enabling Drill to query Dropbox is creating an API token.
1. Navigate to https://www.dropbox.com/developers/apps/create
2. Choose `Scoped Access` under Choose an API. 
3. Depending on the access limitations you are looking for select either full or limited to a particular folder.
4. In the permissions tab, make sure all the permissions associated with reading data are enabled.  

Once you've done that, and hit submit, you'll see a section in your newly created Dropbox App called `Generated Access Token`.  Copy the value here and that is what you will 
use in your Drill configuration.

## Configuring Drill
Once you've created a Dropbox access token, you are now ready to configure Drill to query Dropbox.  To create a dropbox connection, in Drill's UI, navigate to the Storage tab, 
click on `Create New Storage Plugin` and add the items below:

```json
"type": "file",
  "connection": "dropbox:///",
  "config": {
    "dropboxAccessToken": "<your access token here>"
  },
  "workspaces": {
    "root": {
      "location": "/",
      "writable": false,
      "defaultInputFormat": null,
      "allowAccessOutsideWorkspace": false
    }
  }
}
```
Paste your access token in the appropriate field and at that point you should be able to query Dropbox.  Drill treats Dropbox as any other file system, so all the instructions 
here (https://drill.apache.org/docs/file-system-storage-plugin/) and here (https://drill.apache.org/docs/workspaces/)
about configuring a workspace, and adding format plugins are exactly the same as any other on Drill.

### Securing Dropbox Credentials
As with any other storage plugin, you have a few options as to how to store the credentials. See [Drill Credentials Provider](./PluginCredentialsProvider.md) for more 
information about how you can store your credentials securely in Drill. 

## Running the Unit Tests
Unfortunately, in order to run the unit tests, it is necessary to have an external API token.  Therefore, the unit tests have to be run manually.  To run the unit tests:

1.  Get your Dropbox API key as explained above and paste it above into the `ACCESS_TOKEN` variable.
2.  In your dropbox account, create a folder called 'csv' and upload the file `hdf-test.csvh` into that folder
3.  In your dropbox account, upload the file `http-pcap.json` to the root directory of your dropbox account
4.  In the `testListFiles` test, you will have to update the modified dates
5.  Run tests.

### Test Files
Test files can be found in the `java-exec/src/test/resources/dropboxTestFiles`
folder.  Simply copy these files in the structure there into your dropbox account.

## Limitations
1. It is not possible to save files to Dropbox from Drill, thus CTAS queries will fail.
2. Dropbox does not expose directory metadata, so it is not possible to obtain the directory size, modification date or access dates. 
3. Dropbox does not maintain the last access date as distinct from the modification date of files. 
