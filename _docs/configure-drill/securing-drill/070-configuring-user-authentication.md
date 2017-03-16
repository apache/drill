---
title: "Configuring User Authentication"
date: 2017-03-16 01:22:50 UTC
parent: "Securing Drill"
---
Authentication is the process of establishing confidence of authenticity. A Drill client user is authenticated when a drillbit process running in a Drill cluster confirms the identity it is presented with.  Drill 1.10 supports several authentication mechanisms through which users can prove their identity before accessing cluster data: 

* **Kerberos** - New in Drill 1.10. See [Kerberos Authentication]({{site.baseurl}}/docs/configuring-kerberos-authentication/).
* **Plain** [also known as basic authentication (auth), which is username and password-based authentication, through the Linux Pluggable Authentication Module (PAM)] - See [Plain Authentication]({{site.baseurl}}/docs/configuring-plain-authentication/).
* **Custom authenticators** - See [Creating Custom Authenticators]({{site.baseurl}}/docs/creating-custom-authenticators).

These authentication options are available through JDBC and ODBC interfaces.

---
**Note**

If user impersonation is enabled, Drill executes the client requests as the authenticated user. Otherwise, Drill executes client requests as the user that started the drillbit process. You can enable both authentication and impersonation to improve Drill security. See [Configuring User Impersonation]({{site.baseurl}}/docs/configuring-user-impersonation/) for more information.

---






