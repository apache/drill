# Box and Drill
As of Drill 2.0 it is possible to connect Drill to a Box account and query files stored there.  Clearly, the performance will be much better if the files are stored  locally, however, if your data is located in box, Drill makes it easy to explore that data.

## Setting up your Box Account
Box uses OAuth 2.0 for authorization and authentication. In order to connect Drill to Box.com, you will first need to obtain API Keys from Box.

You can follow the instructions here: https://developer.box.com/guides/authentication/oauth2/oauth2-setup/ to obtain the necessary tokens.

Once you have obtained the client tokens, the next step is to configure Drill to connect to Box.

## Configuring Drill to Connect to Box
Connecting Drill with Box is basically the same as any other file system.  The default configuration below doesn't list the formats, but you can use this as as a template.

The only fields that will need to be populated are the `clientID`, `clientSecret` and `callbackURL`.

You may also specify the connection and read timeouts as shown below.  The times are in milliseconds and will default to 5 seconds.


```json
{
  "type": "file",
  "connection": "box:///",
  "config": {
    "boxConnectionTimeout": 5000,
    "boxReadTimeout": 5000
  },
  "workspaces": {
    "root": {
      "location": "/",
      "writable": false,
      "defaultInputFormat": null,
      "allowAccessOutsideWorkspace": false
    }
  },
  "formats": {
    ...
  },
  "oAuthConfig": {
    "callbackURL": "http://localhost:8047/credentials/<your plugin name>/update_oauth2_authtoken",
    "authorizationURL": "https://account.box.com/api/oauth2/authorize",
    "authorizationParams": {
      "response_type": "code"
    }
  },
  "authMode": "SHARED_USER",
  "credentialsProvider": {
    "credentialsProviderType": "PlainCredentialsProvider",
    "credentials": {
      "clientID": "<Your client ID here>",
      "clientSecret": "<Your client secret here>",
      "tokenURI": "https://api.box.com/oauth2/token"
    },
    "userCredentials": {}
  },
  "enabled": true
}

```

## User Impersonation / User Translation Support
When using OAuth 2.0 Box supports user translation.  Simply set the `authMode` to `USER_TRANSLATION`.

## Testing
Box's OAuth tokens are very short-lived and make testing much more difficult. The unit tests therefore use a Box developer token.  These tokens are only valid for one hour.  They should only be used for testing.  You can obtain a developer token in the same page as the `clientID` and `clientSecret`.

If you wish to run unit tests using a developer token, use the following configuration:

```json
"type": "file",
  "connection": "box:///",
  "config": {
    "boxAccessToken": "<your access token here>"
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

