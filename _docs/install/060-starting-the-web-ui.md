---
title: Starting the Web Console
parent: Install Drill
---

The Drill Web Console is one of several [client interfaces](/docs/architecture-introduction/#drill-clients) you can use to access Drill. 

## Drill 1.1 and Earlier

In Drill 1.1 and earlier, to open the Drill Web Console, launch a web browser, and go to the following URL:

`http://<IP address or host name>:8047` 

where IP address is the host name or IP address of one of the installed Drillbits in a distributed system or `localhost` in an embedded system.

## Drill 1.2 and Later

In Drill 1.2 and later, to open the Drill Web Console, launch a web browser, and go to one of the following URLs depending on the configuration of HTTPS support:

* `http://<IP address or host name>:8047`  
  Use this URL when [HTTPS support]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/#https-support) is disabled (the default).
* `https://<IP address or host name>:8047`  
  Use this URL when HTTPS support is enabled.

If HTTPS support is enabled, you need [authorization]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/) to see and use the Storage tab of the Web Console. 

If [user authentication]({{site.baseurl}}/docs/configuring-user-authentication/) is not enabled, the Web Console controls appear: 

![Web Console]({{ site.baseurl }}/docs/img/web-ui.png)

Select the Storage tab to view, update, or add a new [storage plugin configuration]({{site.baseurl}}/docs/plugin-configuration-basics/).

If [user authentication]({{site.baseurl}}/docs/configuring-user-authentication/) is enabled, Drill prompts you for a user name and password:

![Web Console Login]({{ site.baseurl }}/docs/img/web-ui-login.png)

If an [administrator]({{ site.baseurl }}/docs/configuring-user-authentication/#administrator-privileges) logs in, all the Web Console controls appear: Query, Profiles, Storage, Metrics, Threads, and Options.

![Web Console Admin View]({{ site.baseurl }}/docs/img/web-ui-admin-view.png)

If a user, who is not an administrator, logs in, the Web Console controls are limited to Query, Metrics, and Profiles. The Profiles page for a non-administrator user contains the profiles of all queries the user issued either through ODBC, JDBC, or the Web Console. The Profiles pages for administrators contains the profiles of all queries executed on a cluster. Only administrators can see and use the Storage control for managing storage plugin configurations.

![Web Console User View]({{ site.baseurl }}/docs/img/web-ui-user-view.png)


