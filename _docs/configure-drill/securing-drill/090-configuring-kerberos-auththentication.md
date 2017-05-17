---
title: "Configuring Kerberos Authentication"
date: 2017-05-17 01:38:52 UTC
parent: "Securing Drill"
---
In release 1.10 Drill supports Kerberos v5 network security authentication.  To use Kerberos with Drill and establish connectivity, use the JDBC driver packaged with Drill 1.10.

Kerberos allows trusted hosts to prove their identity over a network to an information system.  A Kerberos *realm* is unique authentication domain. A centralized *key distribution center (KDC)* coordinates authentication between a clients and servers. Clients and servers obtain and use tickets from the KDC using a special *keytab* file to communicate with the KDC and prove their identity to gain access to a drillbit.  Administrators must create *principal* (user or server) identities and passwords to ensure the secure exchange of mutual authentication information passed to and from the drillbit.   

{% include startnote.html %}Proper setup, configuration, administration, and usage of a Kerberos environment is beyond the scope of this documentation.{% include endnote.html %}  

See the [MIT Kerberos](http://web.mit.edu/kerberos/ "MIT Kerberos") documentation for information about Kerberos.  


## Prerequisites

The required Kerberos (JDBC) plugin is part of the 1.10 Drill package. To use it, you must have a working Kerberos infrastructure, which Drill does not provide. You must be working in a Linux-based or Windows Active Directory (AD) Kerberos environment with secure clusters and have a Drill server configured for Kerberos. See [Enabling Authentication]({{site.baseurl}}/docs/configuring-kerberos-authentication/#enabling-authentication).

## Client Authentication Process 

This section provides a high-level overview of the Kerberos client authentication process. It is assumed that Kerberos credentials are present in the client.

![kerberos auth process]({{ site.baseurl }}/docs/img/kerberos-auth-process.png)

1. The client sends a request for a ticket granting ticket that contains the user principal to the Kerberos KDC, a network service that supplies tickets and temporary session keys. 

1. The authentication server validates the principal’s identity and sends the client a ticket granting ticket and session key encrypted with a secret key. A session key is a temporary encryption key used for one login session.

1. Using the ticket granting ticket, the principal requests access to a drillbit service from the ticket granting server.

1. The ticket granting server checks for a valid ticket granting ticket and the principal identity. If the request is valid, the ticket granting server returns a ticket granting service ticket.

1. The client uses the service ticket to request access to the drillbit.

1. The drillbit service has access to the keytab, a file that contains a list of keys for principals.  The key allows the service to decrypt the client’s ticket granting service ticket, identify the principal, and grant access.  

## Server Authentication Process
For Kerberos server authentication information, see the [MIT Kerberos](http://web.mit.edu/kerberos/ "MIT Kerberos") administration documentation. 


## Enabling Authentication
During startup, a drillbit service must authenticate. At runtime, Drill uses the keytab file. Trust is based on the keytab file; its secrets are shared with the KDC. The drillbit service also uses this keytab credential to validate service tickets from clients. Based on this information, the drillbit determines whether the client’s identity can be verified to use its service. 

![kerberos client server]({{ site.baseurl }}/docs/img/kerberos-client-server.png)
 

&nbsp;1. Create a Kerberos principal identity and a keytab file.  You can create one principal for each drillbit or one principal for all drillbits in a cluster. The `drill.keytab` file must be owned by and readable by the administrator user.  
 
   * For a single principal per node in cluster:
       

            # kadmin  
			: addprinc -randkey <username>/<FQDN>@<REALM>.COM  
			: ktadd -k /opt/mapr/conf/drill.keytab <username>/<FQDN>@<REALM>.COM  


   * For a single principal per cluster, use `<clustername>` instead of `<FQDN>`: 
       

            # kadmin  
			: addprinc -randkey <username>/<clustername>@<REALM>.COM  
			: ktadd -k /opt/mapr/conf/drill.keytab <username>/<FQDN>@<REALM>.COM

&nbsp;
2.  Add the Kerberos principal identity and keytab file to the `drill-override.conf` file.  

 * The instance name must be lowercase. Also, if \_HOST is set as the instance name in the principal, it is replaced with the fully qualified domain name of that host for the instance name. For example, if a drillbit running on `host01.aws.lab` uses `drill/_HOST@<EXAMPLE>.COM` as the principal, the canonicalized principal is `drill/host01.aws.lab@<EXAMPLE>.COM`.  

              drill.exec: {
                cluster-id: "drillbits1",
                zk.connect: "qa102-81.qa.lab:2181,qa102-82.qa.lab:2181,qa102-83.qa.lab:2181",
                impersonation: {
                  enabled: true,
                  max_chained_user_hops: 3
                },
                security: {  
                        user.auth.enabled:true,  
                        auth.mechanisms:[“KERBEROS”],  
                        auth.principal:“drill/<clustername>@<REALM>.COM”,  
                        auth.keytab:“/etc/drill/conf/drill.keytab”  
                }
                
              }

 * To configure multiple mechanisms, extend the mechanisms list and provide additional configuration parameters. For example, the following configuration enables Kerberos and Plain (username and password) mechanisms. See [Installing and Connfiguring Plain Authentication]({{site.baseurl}}/docs/configuring-plain-authentication/#installing-and-configuring-plain-authentication) for Plain PAM configuration instructions.  

              drill.exec: {
                cluster-id: "drillbits1",
                zk.connect: "qa102-81.qa.lab:2181,qa102-82.qa.lab:2181,qa102-83.qa.lab:2181",
                impersonation: {
                  enabled: true,
                  max_chained_user_hops: 3
                },
                security: {  
                        user.auth.enabled:true,  
                        auth.mechanisms:["KERBEROS","PLAIN"],  
                        auth.principal:“drill/<clustername>@<REALM>.COM”,  
                        auth.keytab:“/etc/drill/conf/drill.keytab”  
                      }  
                security.user.auth: {
                        enabled: true,
                        packages += "org.apache.drill.exec.rpc.user.security",
                        impl: "pam",
                        pam_profiles: ["sudo", "login"]
                       }   
                }


&nbsp;
3. Restart the drillbit process on each Drill node.  
   
        <DRILLINSTALL_HOME>/bin/drillbit.sh restart 
 

&nbsp;
4. Configure the mapping from a Kerberos principal to a user account used by Drill. 

- Drill uses a Hadoop Kerberos name and rules to transform the Kerberos principal provided by client to the one it will use internally as the client’s identity. By default, this mapping rule extracts the first part from the provided principal. For example, if the principal format is `<Name1>/<Name2>@realm`, the default rule will extract only `Name1` from the principal and `Name1` as the client’s identity on server side.

- Administrators can configure custom rules by setting the `drill.exec.security.auth.auth_to_local` property in `drill-override.conf` file. 

See [Mapping from Kerberos Principal to OS user account](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SecureMode.html#Mapping_from_Kerberos_principal_to_OS_user_account "Mapping from Kerberos Principal") in the [Hadoop in Secure Mode](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SecureMode.html "Secure Mode Hadoop") documentation for details about how the rule works.

## Using Connection URLs

New client side connection URL parameters are introduced for Kerberos authentication in Drill 1.10. You can use these parameters in multiple combinations to authenticate a client with Drill. 

### Client Credentials

A client can provide its credentials in two ways:

- With a ticket granting ticket (TGT) generated on client side. The TGT must be present on client node; Drill does not generate the TGT. 

- With a keytab file and the client principal provided in the user property of the connection URL.

### Configuration Options
The following table lists configuration options for connection URLs. See the Connection URL Examples section for sample URLs.

| Connection Parameter | Description                                                                                                                                                                                                                                                                                                                                                                                                                         | Mandatory/Optional | Default Value                                                                                                                                                                                             |
|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| auth                 | Authentication mechanism. The value is deduced if not specified. Kerberos if principal is provided. Plain  if a user and password is provided. A Drill client can also explicitly specify a particular authentication mechanism to use using this parameter. For example, <auth=kerberos> for Kerberos along with service_name, service_host or principal and <auth=plain> for the Plain authentication with username and password. | Optional           | The preference order is Kerberos and Plain.                                                                                                                                                               |
| principal            | Drillbit service principal. The format of the principal is primary/instance@realm. For Kerberos, the Drill service principal is derived if the value is not provided using this configuration. service_name (primary) and service_host (instance) are used to generate a valid principal. Since the ticket or keytab contains the realm information, the realm is optional.                                                         | Optional           |                                                                                                                                                                                                           |
| keytab               | For Kerberos, if the client chooses to authenticate using a keytab rather than a ticket, set the keytab parameter to the location of the keytab file. The client principal must be provided through the user parameter. A Kerberos ticket is used as the default credential (It is assumed to be present on client-side. The Drill client does not generate the required credentials.)                                              | Optional           |                                                                                                                                                                                                           |
| service_name         | Primary name of the drillbit service principal.                                                                                                                                                                                                                                                                                                                                                                                     | Optional           | drill                                                                                                                                                                                                     |
| service_host         | Instance name of the drillbit service principal.                                                                                                                                                                                                                                                                                                                                                                                    | Optional           | Since this value is usually the hostname of the node where a drillbit is running, the default value is the drillbit hostname is  provided either through ZooKeeper or through a direct connection string. |
| realm                | Kerberos realm name for the drillbit service principal. The ticket or keytab contains the realm information.                                                                                                                                                                                                                                                                                                                        | Optional           |                                                                                                                                                                                                           |


### Connection URL Examples

The following five examples show the JDBC connection URL that the embedded JDBC client uses for Kerberos authentication. The first section, Example of a Simple Connection URL, includes a simple connection string and the second section, Examples of Connection URLs Used with Previously Generated TGTs, includes examples to use with previously generated TGTs.

- Example 1:  TGT for Client Credentials  
- Example 2:  Drillbit Provided by Direct Connection String and Configured with a Unique Service Principal  
- Example 3:  Drillbit Selected by ZooKeeper and Configured with a Unique Service Principal  
- Example 4:  Drillbit Selected by Zookeeper and Configured with a Common Service Principal  
- Example 5:  Keytab for Client Credentials

#### Example of a Simple Connection URL

##### Example 1: TGT for Client Credentials
The simplest way to connect using Kerberos is to generate a TGT on the client side. Only specify the service principal in the JDBC connection string for the drillbit the user wants to connect to.


    jdbc:drill:drillbit=10.10.10.10;principal=<principal for host 10.10.10.10>

In this example, the Drill client uses the:

- Default `service_name`, which is **`drill`**.
- `service_host` from the drillbit name provided in the connection URL, which is **`10.10.10.10`**.

The service principal format is `<primary>/<instance>@<realm from TGT>`. The service principal is **`principal for host 10.10.10.10`**.

#### Examples of Connection URLs Used with Previously Generated TGTs
If you do not provide a service principal in the connection string when using Kerberos authentication, then use the `service_name` or `service_host` parameters. Since these parameters are optional, their default values will be used internally (if not provided) to create a valid principal.

Examples 2 through 4 show a valid connection string for Kerberos authentication if a client has previously generated a TGT.  Realm information will be extracted from the TGT if it is not provided.  

{% include startnote.html %}For end-to-end authentication to function, it is assumed that the proper principal for the drillbit service is configured in the KDC.{% include endnote.html %}


##### Example 2: Drillbit Provided by Direct Connection String and Configured with a Unique Service Principal

This type of connection string is used when:

- Each drillbit in the cluster is configured with its own service principal.
- The instance component is the host address of the drillbit.

    `jdbc:drill:drillbit=host1;auth=kerberos`

In this example, the Drill client uses the:

- Default `service_name`, which is **`drill`**.
- `service_host`, which is the drillbit name provided in the connection URL (**`host1`**).

The internally created service principal will be **`drill/host1@<realm from TGT>`**.

##### Example 3: Drillbit Selected by ZooKeeper and Configured with Unique Service Principal

This type of connection string is used when the drillbit is chosen by ZooKeeper instead of directly from the connection string.

    jdbc:drill:zk=host01.aws.lab:5181;auth=kerberos;service_name=myDrill

In this example, the Drill client uses the:

- Provided `service_name`, which is **`myDrill`** as the primary name of the principal.
- `service_host` as the address of the drillbit, which is chosen from the list of active drillbits that ZooKeeper provides (**`host01.aws.lab:5181`**).

The internally created service principal will be **`myDrill/<host address from zk>@<realm from TGT>`**.

##### Example 4: Drillbit Selected by Zookeeper and Configured with a Common Service Principal

This type of connection string is used when all drillbits in a cluster use the same principal.

    jdbc:drill:zk=host01.aws.lab:5181;auth=kerberos;service_name=myDrill;service_host=myDrillCluster

In this example, the Drill client uses the:

- Provided `service_name`, which is **`myDrill`**.
- `service_host`, which is **`myDrillCluster`**.

The internally created service principal, which will be **`myDrill/myDrillCluster@<realm from TGT>`**.

##### Example 5: Keytab for Client Credentials

If a client chooses to provide its credentials in a keytab instead of a TGT, it must also provide a principal in the user parameter.  In this case, realm information will be extracted from the `/etc/krb5.conf` file on the node if it is not provided in the connection URL. All other parameters can be used as shown in the preceding examples (1-4). This connection string is for the case when all drillbits in a cluster use the same principal.

    jdbc:drill:zk=host01.aws.lab:5181;auth=kerberos;service_name=myDrill;service_host=myDrillCluster;keytab=<path to keytab file>;user=<client principal>

In this example, the Drill client:

- Will authenticate itself with the:
	- Keytab (**`path to keytab file`**) and 
	- Principal provided in the user parameter (**`client principal`**)
- Uses the: 
	- Provided `service_name`, which is **`myDrill`**.
	- `service_host`, which is **`myDrillCluster`**.

The internally created service principal will be **`myDrill/myDrillCluster@<realm from krb5.conf>`**.

##### 