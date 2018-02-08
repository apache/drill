---
title: "Starting the Web Console"
date: 2018-02-08 00:38:57 UTC
parent: Install Drill
---

The Drill Web Console is one of several [client interfaces](/docs/architecture-introduction/#drill-clients) you can use to access Drill. 

To open the Drill Web Console, launch a web browser, and go to one of the following URLs depending on the configuration of HTTPS support:

* `http://<IP address or host name>:8047`  
  Use this URL when [HTTPS support]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/#https-support) is disabled (the default).
* `https://<IP address or host name>:8047`  
  Use this URL when HTTPS support is enabled.  
* `http://localhost:8047`   
Use  this URL when running ./drill-embedded.

## Drill 1.2 and Later

If [user authentication]({{site.baseurl}}/docs/configuring-user-authentication/) is not enabled, all the Web Console controls appear to users as well as administrators:  

![Web Console]({{ site.baseurl }}/docs/img/web-ui.png)  

**Note:** As of Drill 1.12, users must enter a username to issue queries through the Query page in the Drill Web Console if user impersonation is enabled and authentication is disabled. To re-run a query from the Profiles page, users must also submit a username. See [REST API]({{site.baseurl}}/docs/submitting-queries-from-the-rest-api-when-impersonation-is-enabled-and-authentication-is-disabled/) for more information.

If [user authentication]({{site.baseurl}}/docs/configuring-user-authentication/) is enabled, Drill prompts you for a user name/password:

![Web Console Login]({{ site.baseurl }}/docs/img/web-ui-login.png)

If an [administrator]({{ site.baseurl }}/docs/configuring-user-authentication/#administrator-privileges) logs in, all the Web Console controls appear: Query, Profiles, Storage, Metrics, Threads, and Options. The Profiles page for administrators contains the profiles of all queries executed on a cluster. Only administrators can see and use the Storage tab to view, update, or add a new [storage plugin configuration]({{site.baseurl}}/docs/plugin-configuration-basics/). Only administrators can see and use the Threads tab, which provides information about threads running in Drill.

![Web Console Admin View]({{ site.baseurl }}/docs/img/web-ui-admin-view.png)

If a user, who is not an administrator, logs in, the Web Console controls are limited to Query, Metrics, and Profiles. The Profiles tab for a non-administrator user contains the profiles of all queries the user issued either through ODBC, JDBC, or the Web Console. 

![Web Console User View]({{ site.baseurl }}/docs/img/web-ui-user-view.png)


