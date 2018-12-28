---
title: "Starting the Web UI"
date: 2018-12-28
parent: Install Drill
---

The Drill Web UI is one of several [client interfaces](/docs/architecture-introduction/#drill-clients) that you can use to access Drill. You can perform the following activities from the Drill Web UI:  

- [Run queries]({{site.baseurl}}/docs/query-data-introduction/) 
- [Monitor and cancel queries]({{site.baseurl}}/docs/monitoring-and-canceling-queries-in-the-drill-web-ui/)
- [View query profiles]({{site.baseurl}}/docs/query-profiles/)
- [Update and configure storage plugins]({{site.baseurl}}/docs/storage-plugin-registration/)
- View [logs]({{site.baseurl}}/docs/log-and-debug-introduction/) and [metrics]({{site.baseurl}}/docs/monitoring-metrics/)
- [Set configuration options]({{site.baseurl}}/docs/planning-and-execution-options/#setting-options-from-the-drill-web-ui)  

Starting in Drill 1.15, you can use a Meta+Enter key combination instead of clicking the Submit button to submit queries through the query editor (Query page) in  Drill Web UI. On Mac keyboards, the combination is Ctrl+Meta or Ctrl+Enter. On Windows and Linux, the combination is Ctrl+Enter; however, on Linux, if you mapped the meta key to another physical key on the keyboard, use that key + Enter. 
 
## Accessing the Web UI
To access the Drill Web UI, enter the URL appropriate for your Drill configuration. The following list describes the URLs for various Drill configurations: 

* `http://<IP address or host name>:8047`  
  Use this URL when [HTTPS support]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/#https-support) is disabled (the default).
* `https://<IP address or host name>:8047`  
  Use this URL when HTTPS support is enabled.  
* `http://localhost:8047`   
  Use  this URL when running Drill in embedded mode (./drill-embedded).

## Web UI Security

If [user authentication]({{site.baseurl}}/docs/configuring-user-authentication/) is not enabled, all the Web UI controls appear to users as well as administrators:  

![Web UI]({{ site.baseurl }}/docs/img/web-ui.png)  

**Note:** As of Drill 1.12, users must enter a username to issue queries through the Query page in the Drill Web UI if user impersonation is enabled and authentication is disabled. To re-run a query from the Profiles page, users must also submit a username. See [REST API]({{site.baseurl}}/docs/submitting-queries-from-the-rest-api-when-impersonation-is-enabled-and-authentication-is-disabled/) for more information.  

**Note:** As of Drill 1.13, an administrator can configure FORM and/or SPNEGO authentication mechanisms. The Drill Web UI provides two possible log in options for a user depending on the configuration. If a user selects FORM, s/he must enter their username and password to access restricted pages in the Drill Web UI. The user is authenticated through PAM. If the user selects SPNEGO, the user is automatically logged in if they are an authenticated Kerberos user. If accessing a protected page directly, the user is redirected to the authentication log in page.

If [user authentication]({{site.baseurl}}/docs/configuring-user-authentication/) is enabled, Drill prompts you for a user name/password:

![Web UI Login]({{ site.baseurl }}/docs/img/web-ui-login.png)

If an [administrator]({{ site.baseurl }}/docs/configuring-user-authentication/#administrator-privileges) logs in, all the Web UI controls appear: Query, Profiles, Storage, Metrics, Threads, and Options. The Profiles page for administrators contains the profiles of all queries executed on a cluster. Only administrators can see and use the Storage tab to view, update, or add a new [storage plugin configuration]({{site.baseurl}}/docs/plugin-configuration-basics/). Only administrators can see and use the Threads tab, which provides information about threads running in Drill.

![Web UI Admin View]({{ site.baseurl }}/docs/img/web-ui-admin-view.png)

If a user, who is not an administrator, logs in, the Web UI controls are limited to Query, Metrics, and Profiles. The Profiles tab for a non-administrator user contains the profiles of all queries the user issued either through ODBC, JDBC, or the Web UI. 

![Web UI User View]({{ site.baseurl }}/docs/img/web-ui-user-view.png)


