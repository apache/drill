---
title: "Configuring Kerberos Authentication"
date: 2017-03-16 03:01:56 UTC
parent: "Securing Drill"
---
As of version 1.10, Drill supports Kerberos v5 network security authentication.  Kerberos allows trusted hosts to prove their identity over a network to an information system.  A Kerberos realm is unique authentication domain. A centralized key distribution center (KDC) coordinates authentication between a clients and servers. Clients and servers obtain and use tickets from the KDC using a special keytab file to communicate with the KDC and prove their identity to gain access to a drillbit.  Administrators must create principal (user or server) identities and passwords to ensure the secure exchange of mutual authentication information passed to and from the drillbit. 

To use Kerberos with Drill and establish connectivity, use the JDBC driver packaged with Drill 1.10.

---
**NOTE**

Proper setup, configuration, administration, and usage of a Kerberos environment is beyond the scope of this documentation.  See the [MIT Kerberos](http://web.mit.edu/kerberos/ "MIT Kerberos") documentation for information about Kerberos.

---

## Prerequisites

The required Kerberos plugin is part of the 1.10 Drill package. You must have a working Kerberos infrastructure, which Drill does not provide. You must be working in a Linux-based or Windows Active Directory (AD) Kerberos environment and have a Drill server configured for Kerberos. See also [Enabling Authentication]({{site.baseurl}}/docs/configuring-kerberos-authentication/#enabling-authentication)

## Client Authentication Process 

This section shows a high-level overview of the client authentication process in Kerberos. It is assumed that Kerberos credentials are present in the client.

1. The client sends a request for a ticket granting ticket that contains the user principal to the Kerberos KDC, a network service that supplies tickets and temporary session keys. 

1. The authentication server validates the principal’s identity and sends the client a ticket granting ticket and session key encrypted with a secret key. A session key is a temporary encryption key used for one login session.

1. Using the ticket granting ticket, the principal requests access to a drillbit service from the ticket granting server.

1. The ticket granting server checks for a valid ticket granting ticket and the principal identity. If the request is valid, the ticket granting server returns a ticket granting service ticket.

1. The client uses the service ticket to request access to the drillbit.

1. The drillbit service has access to the keytab, a file that contains a list of keys for principals.  The key allows the service to decrypt the client’s ticket granting service ticket, identify the principal, and grant access.


![]({{ site.baseurl }}/docs/img/kerberosauthprocess.png)  

## Server Authentication Process
For Kerberos server authentication information, see the [MIT Kerberos](http://web.mit.edu/kerberos/ "MIT Kerberos") administration documentation. 


## Enabling Authentication
During startup, a drillbit service must authenticate. At runtime, Drill uses the keytab file. Trust is based on the keytab file; its secrets are shared with the KDC. The drillbit service also uses this keytab credential to validate service tickets from clients. Based on this information, the drillbit determines whether the client’s identity can be verified to use its service. 

---
**NOTE**

Drill must  run as a user capable of impersonation. The Kerberos provider in the SASL framework maps from the Kerberos identity to an OS user name. Drill impersonates the OS username when running queries. 

---


![]({{ site.baseurl }}/docs/img/kerberclientserver.png)  
 

1. Create a Kerberos principal identity and a keytab file.  You can create one principal for each drillbit or one principal for all drillbits in a cluster. The drill.keytab file must be owned by and readable by the administrator user.  
 
   * For a single principal per node in cluster:
       

            # kadmin  
			: addprinc -randkey <username>/<FQDN>@<REALM>.COM  
			: ktadd -k /opt/mapr/conf/drill.keytab <username>/<FQDN>@<REALM>.COM  


   * For a single principal per cluster, use `<clustername>` instead of `<FQDN>`: 
       

            # kadmin  
			: addprinc -randkey <username>/<clustername>@<REALM>.COM  
			: ktadd -k /opt/mapr/conf/drill.keytab <username>/<FQDN>@<REALM>.COM
       

2. Add the Kerberos principal identity and keytab file to the `drill-override.conf` file.  
  
 * The instance name must be lowercase. Also, if \_HOST is set as the instance name in the principal, it is replaced with the fully qualified domain name of that host for the instance name. For example, if a drillbit running on `host01.aws.lab` uses `drill/_HOST@<EXAMPLE>.COM` as the principal, the canonicalized principal is `drill/host01.aws.lab@<EXAMPLE>.COM`. 
 
   
             drill.exec {  
   			    security: {  
 			      user.auth.enabled:true,  
 			      auth.mechanisms:[“KERBEROS”],  
 			      auth.principal:“drill/<clustername>@<REALM>.COM”,  
 			      auth.keytab:“/etc/drill/conf/drill.keytab”  
				}  
			}  
    
 * To configure multiple mechanisms, extend the mechanisms list and provide additional configuration parameters. For example, the following configuration enables Kerberos and Plain (username and password) mechanisms. See Installing and Configuring Plain Authentication for PAM configuration instructions. 
   
 
             drill.exec: {  
              	security: {  
              	   user.auth.enabled:true,  
              	   user.auth.impl:"pam",  
              	   user.auth.pam_profile:["sudo", "login"],  
              	   auth.mechanisms:["KERBEROS","PLAIN"],  
              	   auth.principal:"drill/<clustername>@<REALM>.COM",  
              	   auth.keytab:"/etc/drill/conf/drill.keytab"  
              		}  
              	}  
   
3. Restart the drillbit process on each Drill node.  
   
        <DRILLINSTALL_HOME>/bin/drillbit.sh restart 
 


## Using Connection URLs

In Drill 1.10, Kerberos authentication introduces new URL parameters. The simplest way to connect using Kerberos is to generate a TGT on the client side. In the JDBC connection string, only specify the service principal. For example:

     jdbc:drill:  
     drillbit=10.10.10.10;  
     principal=drill/<serverhostname>@<REALM>.COM  


