---
title: "Configuring User Security"
date: 2017-05-17 01:38:49 UTC
parent: "Securing Drill"
---
### Authentication

Authentication is the process of establishing confidence of authenticity. A Drill client user is authenticated when a drillbit process running in a Drill cluster confirms the identity it is presented with.  Drill supports several authentication mechanisms through which users can prove their identity before accessing cluster data: 

* **Kerberos** - See [Configuring Kerberos Security]({{site.baseurl}}/docs/configuring-kerberos-security/).
* **Plain** [also known as basic authentication (auth), which is username and password-based authentication, through the Linux Pluggable Authentication Module (PAM)] - See [Configuring Plain Security]({{site.baseurl}}/docs/configuring-plain-security/).
* **Custom authenticators** - See [Creating Custom Authenticators]({{site.baseurl}}/docs/creating-custom-authenticators).

These authentication options are available through JDBC and ODBC interfaces.  

### Encryption

Drill 1.11 introduces client-to-drillbit encryption for over-the-wire security using the Kerberos to:

* Ensure data integrity and privacy 
* Prevent data tampering and snooping


See [Configuring Kerberos Security]({{site.baseurl}}/docs/configuring-kerberos-security/) for information about how to enable a drillbit for encryption.

{% include startnote.html %}The Plain mechanism does not support encryption.{% include endnote.html %} 
#### Cipher Strength

By default, the highest security level is negotiated during the SASL handshake for each client-to-drillbit connection so that the AES-256 ciper is used when supported by key distribution center (KDC) encryption types. See [Encryption Types](http://web.mit.edu/kerberos/krb5-1.13/doc/admin/enctypes.html](http://web.mit.edu/kerberos/krb5-1.13/doc/admin/enctypes.html "Encryption Types") in the MIT Kerberos documentation for details about specific combinations of cipher algorithms. 

#### Client Compatibility
The following table shows Drill client version compatibility with secure Drill clusters enabled with encryption. Drill 1.10 clients and lower do not support encryption and will not be allowed to connect to a drillbit with encryption enabled. 

![compatEncrypt]({{site.baseurl}}/docs/img/client-encrypt-compatibility.png)

See *Client Encryption* in [Configuring Kerberos Security]({{site.baseurl}}/docs/server-communication-paths/#configuring-kerberos-security#client-encryption) for the client connection string parameter, `sasl_encrypt` usage information.

### Impersonation

Enabling both user impersonation and authentication is recommended to restrict access to data and improve security. When user impersonation is enabled, Drill executes the client requests as the authenticated user. Otherwise, Drill executes client requests as the user that started the drillbit process. 

For more information, see [Configuring User Impersonation]({{site.baseurl}}/docs/configuring-user-impersonation/) and [Configuring Inbound Impersonation]({{site.baseurl}}/docs/configuring-inbound-impersonation/).






