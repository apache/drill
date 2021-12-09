# OAuth2.0 Authentication in Drill
Many APIs use OAuth2.0 as a means of authentication. Drill can connect to APIs that use OAuth2 for authentication but OAuth2 is significantly more complex than simple 
username/password authentication.

The good news, and bottom line here is that you can configure Drill to handle all this automagically, but you do have to understand how to configure it so that it works. First, 
let's get a high level understanding of how OAuth works.

### Understanding the OAuth2 Process
There are many tutorials as to how OAuth works which we will not repeat here.  There are some slight variations but this is a good enough high level overview so you will understand the process works. 
Thus, we will summarize the process as three steps:

#### Step 1:  Obtain an Authorization Code
For the first step, a user will have to log into the API's front end, and authorize the application to access the API.  The API will issue you a `clientID` and a 
`client_secret`.  We will use these tokens in later stages.  

You will also need to provide the API with a `callbackURL`.  This URL is how the API sends your application the `authorizationCode` which we will use in step 2.  
Once you have the `clientID` and the `callbackURL`, your application will make a `GET` request to the API to obtain the `authorizationCode`. 

#### Step 2:  Swap the Authorization Code for an Access Token
At this point, we need to obtain the `accessToken`.  We do so by sending a `POST` request to the API with the `clientID`, the `clientSecret` and the `authorizationCode` we 
obtained in step 1.  Note that the `authorizationCode` is a short lived token, but the `accessToken` lasts for a longer period.  When the access token expires, you may need to 
either re-authorize the application or use a refresh token to obtain a new one.

#### Step 3:  Call the Protected Resource with the Access Token
Once you have the `accessToken` you are ready to make authenticated API calls. All you have to do here is add the `accessToken` to the API header and you can make API calls 
just like any other. 

## Configuring Drill to Use OAuth2.0 for API Calls
The OAuth2.0 configuration for HTTP/REST plugins is done at the plugin level and thus the access token is used for all endpoints in that connection.  This made the most sense 
because typically, APIs that use OAuth will have many endpoints.

### Configuration Options
For OAuth to work, there are a lot of configuration options, but not all are required.  Let's start with the bare minimum:

* `clientID`:  The clientID is a token which you obtain from the API source when you register.
* `clientSecret`:  The client secret is also a token that you obtain from the API source when you register.
* `baseURL`:  If your API has a root URL for all the OAuth calls, provide that here.
* `callbackURL`:  This is the URL which you provide to the API to which the API will send the access token.  It will be something like `<YOUR DRILL HOST>/storage/<plugin 
  name>/update_oath2_authtoken`.  Note that the API source must be able to access this URL.
* `accessTokenPath`:  The path to be added to the `baseURL` to obtain the access token.
* `authorizationPath`:  The path to be added (if any) to the `baseURL` to obtain the authorization code.

These are the minimum config options you will need to connect to an OAuth enabled API.  However, there are other parameters which you can add to get your API to work with Drill.
They are:
* `authorizationParams`:  Some APIs requires you to add additional parameters to the authorization request.  This is a key/value map of parameters.  Note, you should not have 
  to include `Content-Type` or `Accept` headers.
* `authorizationURL`: In the event that your API uses a completely different URL than your access token URL, set the authorization URL with this parameter.  This overrides the 
  `authorizationPath` if it is defined.
* `scope`:  The scope of your access token.  This will be provided in your API documentation.



```json
"oauthConfig": {
  "clientID": "<Your client ID>",
  "clientSecret": "<Your client secret>",
  "callbackURL": 
  "baseURL"
}

```
