---
title: Starting the Web UI
parent: Install Drill
---

The Drill Web UI is one of several [client interfaces](/docs/architecture-introduction/#drill-clients) you can use to access Drill.  To access the Web UI in Drill 1.2 and later, go to `https://<IP address>:8047`, where IP address is the host name or IP address of one of the installed Drillbits in a distributed system. In Drill 1.1 and earlier, go to `http://<IP address>:8047` to access the Web UI.

If [user authentication]({{site.baseurl}}/docs/configuring-user-authentication/) is not enabled, the Web UI controls appear: 

![Web UI]({{ site.baseurl }}/docs/img/web-ui.png)

If user authentication is enabled, Drill 1.2 and later prompts you for a user name and password:

![Web UI Login]({{ site.baseurl }}/docs/img/web-ui-login.png)

If an administrator logs in, all the Web UI controls appear: Query, Profiles, Storage, Metrics, Threads, and Options.

![Web UI Admin View]({{ site.baseurl }}/docs/img/web-ui-admin-view.png)

If a user, who is not an administrator, logs in, the Web UI controls are limited to Query, Metrics, Threads controls, and possibly, Profiles. An administrator can give users permission to access the Profiles control. Only administrators can see and use the Storage control for managing storage plugin configurations.

![Web UI User View]({{ site.baseurl }}/docs/img/web-ui-user-view.png)


