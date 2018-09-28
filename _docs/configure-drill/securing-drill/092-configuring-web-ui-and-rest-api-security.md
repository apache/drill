---
title: "Configuring Web Console and REST API Security"
date: 2018-09-28 23:05:25 UTC
parent: "Securing Drill"
---
Drill 1.5 extends [Drill user security]({{site.baseurl}}/docs/configuring-user-security/) to the Web Console and underlying REST API. As administrator, you can control the extent of access to the Web Console and REST API client applications. For example,
you can limit the access of certain users to Web Console functionality, such as viewing the in-progress or completed queries of other users. You can limit users from viewing other users' query profiles, who can cancel queries of other users, and other functionality.

With Web Console security in place, users who do not have administrator privileges need to use the SHOW SCHEMAS command instead of the Web Console for storage plugin configuration information.

## HTTPS Support
Drill 1.2 uses code-level support for transport layer security (TLS) to secure the Web Console and REST API. By default, the Web Console and REST API support the HTTP protocol. You set the following start-up option to TRUE to enable HTTPS support:

`drill.exec.http.ssl_enabled`

By default this start-up option is set to FALSE.

Drill generates a self-signed certificate that works with SSL for HTTPS access to the Web Console. Because Drill uses a self-signed certificate, you see a warning in the browser when you go to `https://<node IP address>:8047`. The Chrome browser, for example, requires you to click `Advanced`, and then `Proceed to <address> (unsafe)`. If you have a signed certificate by an authority, you can set up a custom SSL to avoid this warning. You can set up SSL to specify the keystore or truststore, or both, for your organization, as described in the next section.

## Setting Up a Custom SSL Configuration

As cluster administrator, you can set the following SSL configuration parameters in the `conf/drill-override.conf` file, as described in the [Java product documentation](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jsse/JSSERefGuide.html#Customization):

* javax.net.ssl.keyStore  
  Path to the application's certificate and private key in the Java keystore file.  
* javax.net.ssl.keyStorePassword  
  Password for accessing the private key from the keystore file.  
* javax.net.ssl.trustStore  
  Path to the trusted CA certificates in a keystore file.  
* javax.net.ssl.trustStorePassword  
  Password for accessing the trusted keystore file.   

See [SSL Certificates in a Drill Cluster]({{site.baseurl}}/docs/configuring-ssl-tls-for-encryption/#ssl-certificates-in-a-drill-cluster) for more information. 
 
## Prerequisites for Web Console and REST API Security

You need to perform the following configuration tasks using Web Console and REST API security.  

* Configure [user security]({{site.baseurl}}/docs/configuring-user-security/)  
* Set up Web Console administrators  
  Optionally, you can set up Web Console administrator-user groups to facilitate management of multiple Web Console administrators.

## Setting up Web Console Administrators and Administrator-User Groups

Configure the following system options using the [ALTER SYSTEM]({{site.baseurl}}/docs/alter-system/) command:

* security.admin.users  
  Set the value of this option to a comma-separated list of user names who you want to give administrator privileges, such as changing system options.  
* security.admin.user_groups  
  Set the value of this option to a comma-separated list of administrator groups.

Any user who is a member of any group listed in security.admin.user.groups is a Drill cluster administrator. Any user for whom you have configured Drill user authentication, but not set up as a Drill cluster administrator, has only user privileges to access the Web Console and REST API client applications.

## Web Console and REST API Privileges

The following table and subsections describe the privilege levels for accessing the REST API methods and corresponding Web Console functions:

* Administrator (ADMIN)  
* User (USER)  
* Administrator and User (ALL) 

| Resource Method          | Path                         | Request Type | Output Type      | Functionality                                                                                                                                                                                                                                               | Privilege Level                                                                                               |
|--------------------------|------------------------------|--------------|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| getStats                 | /                            | GET          | text/html        | Returns Drillbit stats in a table in HTML format.                                                                                                                                                                                                           | ALL                                                                                                           |
| getStatsJSON             | /stats.json                  | GET          | application/json | Returns Drillbit stats such as ports and max direct memory in json format.                                                                                                                                                                                  | ALL                                                                                                           |
| getStatus                | /status                      | GET          | text/html        | Returns Running!                                                                                                                                                                                                                                            | ALL                                                                                                           |
| getSystemOptionsJSON     | /options.json                | GET          | application/json | Returns a list of options. Each option consists of name-value-type-kind (for example: (boot system datatype).                                                                                                                                               | ALL                                                                                                           |
| getSystemOptions         | /options                     | GET          | text/html        | Returns an HTML table where each row is a form containing the option details that allows option values to be modified.                                                                                                                                      | ALL                                                                                                           |
| updateSystemOption       | /option/{optionName}         | POST         | text/html        | Updates the options and calls getSystemOptions. So again an option list is displayed.                                                                                                                                                                       | ADMIN                                                                                                         |
| getStoragePluginsJSON    | /storage.json                | GET          | application/json | Returns a list of storage plugin wrappers each containing name-config (instance of StoragePluginConfig) and enabled.                                                                                                                                        | ADMIN                                                                                                         |
| getStoragePlugins        | /storage                     | GET          | text/html        | Returns an HTML page with two sections: The first section contains a table of rows that are forms containing the plugin button for the update page and a button to disable the plugin. The second section is the same except the button enables the plugin. | ADMIN                                                                                                         |
| getStoragePluginJSON     | /storage/{name}.json         | GET          | application/json | Returns a plugin config wrapper for the requested web page.                                                                                                                                                                                                 | ADMIN                                                                                                         |
| getStoragePlugin         | /storage/{name}              | GET          | text/html        | Returns an HTML page that has an editable text box for configuration changes and buttons for creating/updating/deleting. Each button makes calls that regenerate the page.                                                                                  | ADMIN                                                                                                         |
| enablePlugin             | /storage/{name}/enable/{val} | GET          | application/json | Updates the storage plugin configuration status. Returns success or failure.                                                                                                                                                                                | ADMIN                                                                                                         |
| deletePluginJSON         | /storage/{name}.json         | DELETE       | application/json | Deletes the storage plugin. Returns success or failure.                                                                                                                                                                                                     | ADMIN                                                                                                         |
| deletePlugin             | /storage/{name}/delete       | GET          | application/json | Same as deletePluginJSON but a GET instead of a DELETE request.                                                                                                                                                                                             | ADMIN                                                                                                         |
| createOrUpdatePluginJSON | /storage/{name}.json         | POST         | application/json | Creates or updates the storage plugin configuration. Returns success or failure. Expects JSON input.                                                                                                                                                        | ADMIN                                                                                                         |
| createOrUpdatePlugin     | /storage/{name}              | POST         | application/json | Same as createOrUpdatePluginJSON expects JSON or FORM input.                                                                                                                                                                                                | ADMIN                                                                                                         |
| getProfilesJSON          | /profiles.json               | GET          | application/json | Returns currently running and completed profiles from PStore. For each profile a queryId, startTime, foremanAddress, query, user, and state is returned. Each list (running and completed) is organized in reverse chronological order.                     | [ADMIN, USER]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/#get-/profiles.json)             |
| getProfiles              | /profiles                    | GET          | text/html        | Generates an HTML page from the data returned by getProfilesJSON with a hyperlink to a detailed query page,                                                                                                                                                 | [ADMIN, USER]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/#get-/profiles)                  |
| getProfileJSON           | /profiles/{queryid}.json     | GET          | application/json | Returns the entire profile in JSON.                                                                                                                                                                                                                         | [ADMIN, USER]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/#get-/profiles/{queryid}.json)   |
| getProfile               | /profiles/{queryid}          | GET          | text/html        | Returns a complicated profile page.                                                                                                                                                                                                                         | [ADMIN, USER]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/#get-/profiles/{queryid})        |
| cancelQuery              | /profiles/cancel/{queryid}   | GET          | text/html        | Cancels the given query and sends a message.                                                                                                                                                                                                                | [ADMIN, USER]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/#get-/profiles/cancel/{queryid}) |
| getQuery                 | /query                       | GET          | text/html        | Gets the query input page.                                                                                                                                                                                                                                  | ALL                                                                                                           |
| submitQueryJSON          | /query.json                  | POST         | application/json | Submits a query and waits until it is completed and then returns the results as one big JSON object.                                                                                                                                                        | ALL                                                                                                           |
| submitQuery              | /query                       | POST         | text/html        | Returns results from submitQueryJSON populated in a HTML table.                                                                                                                                                                                             | ALL                                                                                                           |
| getMetrics               | /metrics                     | GET          | text/html        | Returns a page that fetches metric info from resource, status, and metrics.                                                                                                                                                                                 | ALL                                                                                                           |
| getThreads               | /threads                     | GET          | text/html        | Returns a page that fetches metric information from resource, status, and threads.                                                                                                                                                                          | ALL                                                                                                           |

### GET /profiles.json

* ADMIN - gets all profiles on the system.  
* USER - only the profiles of the queries the user has launched.

### GET /profiles

* ADMIN - gets all profiles on the system.  
* USER - only the profiles of the queries the user has launched.

### GET /profiles/{queryid}.json

* ADMIN - return the profile.  
* USER - if the query is launched the by the requesting user return it. Otherwise, return an error saying no such profile exists.

### GET /profiles/{queryid}

* ADMIN - return the profile.   
* USER - if the query is launched the by the requesting user return it. Otherwise, return an error saying no such profile exists

### GET /profiles/cancel/{queryid}

* ADMIN - can cancel the query.  
* USER - cancel the query only if the query is launched by the user requesting the cancellation. 
