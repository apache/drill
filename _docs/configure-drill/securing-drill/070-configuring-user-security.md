c---
title: "Configuring User Security"
date: 2018-04-04 00:23:28 UTC
parent: "Securing Drill"
---
## Authentication

Authentication is the process of establishing confidence of authenticity. A Drill client user is authenticated when a Drillbit confirms the identity it is presented with.  Drill supports several authentication mechanisms through which users can prove their identity before accessing cluster data: 

* **Kerberos** - Featuring Drill client to Drillbit encryption, as of Drill 1.11. See [Configuring Kerberos Security]({{site.baseurl}}/docs/configuring-kerberos-security/).  
* **SPNEGO** - Drill 1.13 and later supports the Simple and Protected GSS-API Negotiation mechanism (SPNEGO) to extend the Kerberos-based single sign-on authentication mechanism to HTTP. See [Configuring Drill to use SPNEGO for HTTP Authentication]({{site.baseurl}}/docs/configuring-drill-to-use-spnego-for-http-authentication/).
* **Plain** - Also known as basic authentication (auth), which is username and password-based authentication through the Linux Pluggable Authentication Module (PAM). See [Configuring Plain Security]({{site.baseurl}}/docs/configuring-plain-security/).
* **Custom authenticators** - See [Creating Custom Authenticators]({{site.baseurl}}/docs/creating-custom-authenticators).

These authentication options are available through JDBC and ODBC interfaces.  

## Encryption

Drill 1.11 introduces client-to-drillbit encryption for over-the-wire security using the Kerberos to:

* Ensure data integrity and privacy 
* Prevent data tampering and snooping


See [Configuring Kerberos Security]({{site.baseurl}}/docs/configuring-kerberos-security/) for information about how to enable a drillbit for encryption.

{% include startnote.html %}The Plain mechanism does not support encryption.{% include endnote.html %}  
 
**Cipher Strength**

By default, the highest security level is negotiated during the SASL handshake for each client-to-drillbit connection so that the AES-256 ciper is used when supported by key distribution center (KDC) encryption types. See [Encryption Types](http://web.mit.edu/kerberos/krb5-1.13/doc/admin/enctypes.html](http://web.mit.edu/kerberos/krb5-1.13/doc/admin/enctypes.html "Encryption Types") in the MIT Kerberos documentation for details about specific combinations of cipher algorithms. 

**Client Compatibility**  

The following table shows Drill client version compatibility with secure Drill clusters enabled with encryption. Drill 1.10 clients and lower do not support encryption and will not be allowed to connect to a drillbit with encryption enabled. 

![compatEncrypt]({{site.baseurl}}/docs/img/client-encrypt-compatibility.png)

See *Client Encryption* in [Configuring Kerberos Security]({{site.baseurl}}/docs/configuring-kerberos-authentication/#client-encryption) for the client connection string parameter, `sasl_encrypt` usage information.

## Impersonation

Enabling both user impersonation and authentication is recommended to restrict access to data and improve security. When user impersonation is enabled, Drill executes the client requests as the authenticated user. Otherwise, Drill executes client requests as the user that started the drillbit process. 

For more information, see [Configuring User Impersonation]({{site.baseurl}}/docs/configuring-user-impersonation/) and [Configuring Inbound Impersonation]({{site.baseurl}}/docs/configuring-inbound-impersonation/).






