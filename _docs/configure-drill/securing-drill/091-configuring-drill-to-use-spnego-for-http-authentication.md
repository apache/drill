---
title: "Configuring Drill to use SPNEGO for HTTP Authentication"
date: 2018-04-05 01:05:13 UTC
parent: "Securing Drill"
---  

Drill 1.13 and later supports the Simple and Protected GSS-API Negotiation mechanism (SPNEGO) to extend the Kerberos-based single sign-on authentication mechanism to HTTP. An administrator can configure both FORM (username and password) and SPNEGO authentication together, which provides the ability for clients with different security preferences to connect to the same Drill cluster. 
 
When a client (a web browser or a web client tool, such as curl) requests access to a secured page from the web server (Drillbit), the SPNEGO mechanism uses tokens to perform a handshake that authenticates the client browser and the web server. 

The following browsers were tested with Drill configured to use SPNEGO authentication:



- Firefox  
- Chrome  
- Safari  
- Web client tool, such as curl  

An IT administrator configures the web server (Drillbit) to use SPNEGO for authentication. Depending on the system, either the administrator or the user configures the client (web browser or web client tool) to use SPNEGO for authentication. Configuration instructions are presented later in this document.   

##Prerequisites  

SPNEGO authentication for Drill requires the following:  


- Drill 1.13 installed on each node.  
- To use SPNEGO, you must have a working Kerberos infrastructure, which Drill does not provide.  
- You must be working in a Linux-based or Windows Active Directory (AD) Kerberos environment with secure clusters and have a Drill server configured for Kerberos.   
- Kerberos principal and keytab on each Drillbit (web server) that will use SPNEGO for authentication.  
- Kerberos Ticket Granting Ticket on the client machine for the user accessing the Drillbit (web server).  
- Drill web server configured for SPNEGO.  

##Configure SPNEGO on the Web Server and Web Client  

The following sections provide the steps that an administrator can follow to configure SPNEGO on the web server (Drillbit). An administrator or a user can follow the steps for configuring the web browser or client tool, such as curl.  

###Configuring SPNEGO on the Drillbit (Web Server)  
To configure SPNEGO on the web server, complete the following steps:  
1-Generate a Kerberos principal on each web server that will receive inbound SPNEGO traffic. Each principal must have a corresponding keytab. The principal must have the following form:  

       “HTTP/<client-known-server-hostname@realm>”
       
       Example: “HTTP/example.QA.LAB@QA.LAB” 
       //In this example, the client known server hostname is example.QA.LAB.  

2-Update the `<DRILL_HOME>/conf/drill-override.conf` file on each Drillbit with the following server-side SPNEGO configurations:  



- To enable SPNEGO, add the following configuration to `drill-override.conf`:  

              drill.exec.http: {
                      spnego.auth.principal:"HTTP/hostname@realm",
                      spnego.auth.keytab:"path/to/keytab",
                      auth.mechanisms: [“SPNEGO”]    
                }   
              
              //The default authentication mechanism is “FORM”.   
 
- To enable SPNEGO and FORM authentication, add the following configuration to `drill-override.conf`:  

              impersonation: {
                       enabled: true,
                       max_chained_user_hops: 3
                     },
                     security.user.auth: {
                             enabled: true,
                             packages += "org.apache.drill.exec.rpc.user.security",
                             impl: "pam4j",
                             pam_profiles: [ "sudo", "login" ]
                      }
                    drill.exec.http: {
                             spnego.auth.principal:"HTTP/hostname@realm",
                             spnego.auth.keytab:"path/to/keytab",
                             auth.mechanisms: [“SPNEGO”, “FORM”]
                    }
              }  

3-(Optional) To configure the mapping from a Kerberos principal to a user account used by Drill, update the `drill.exec.security.auth.auth_to_local` property in the `drill-override.conf` file with custom rules, as described in [Mapping from Kerberos Principal to OS user account](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SecureMode.html#Mapping_from_Kerberos_principal_to_OS_user_account "Mapping from Kerberos Principal").  

**Note:** Drill uses a Hadoop Kerberos name and rules to transform the client Kerberos principal to the principal Drill uses internally as the client’s identity. By default, this mapping rule extracts the first portion from the provided principal. For example, if the principal format is <Name1>/<Name2>@realm, the default rule extracts only Name1 from the principal and stores Name1 as the client’s identity on server side. Drill uses the short name, for example Name1, as the user account known to Drill. This user account name is used to determine if the authenticated user has administrative privileges.
   

##Configuring SPNEGO on the Client  

An administrator or user can configure SPNEGO on the client (web browser or client tools, such as curl). To configure SPNEGO on the client, a Kerberos Ticket Granting Ticket must exist for the user accessing the web server. The Kerberos Ticket Granting Ticket generated on the client side is used by the web client to get a service ticket from the KDC. This service ticket is used to generate a SPNEGO token, which is presented to the web server for authentication.

The client should use the same web server hostname (as configured in the server-side principal) to access the Drill Web Console. If the server hostname differs, SPNEGO authentication will fail. For example, if the server principal is `"HTTP/example.QA.LAB@QA.LAB”`, the client should use `http://example.QA.LAB:8047` as the Drill Web Console URL.

The following sections provide instructions for configuring the supported client-side browsers: 

**Note:** SPNEGO is not tested on Windows browsers in Drill 1.13.  

###Firefox
To configure Firefox to use a negotiation dialog, such as SPNEGO to authenticate, complete the following steps:  

1-Go to About > Config, and accept the warnings.  
2-Navigate to the network settings.  
3-Set network.negotiate-auth.delegation-uris to “http://,https://”.  
4-Set network.negotiate-auth.trusted-uris to “http://,https://”.  

###Chrome
For MacOS or Linux, add the `--auth-server-whitelist` parameter to the `google-chrome` command. For example, to run Chrome from a Linux prompt, run the `google-chrome` command, as follows:

       google-chrome --auth-server-whitelist = "hostname/domain"  
       Example: google-chrome --auth-server-whitelist = "example.QA.LAB"  

###Safari
No configuration is required for Safari. Safari automatically authenticates using SPNEGO when requested by the server.  

###REST API
You can use CURL commands to authenticate using SPNEGO and access secure web resources over REST.
 
Issue the following `curl` command to log in using SPNEGO, and save the authenticated session cookie to a file, such as `cookie.txt`, as shown:
 
       curl -v --negotiate -c cookie.txt -u : http://<hostname>:8047/spnegoLogin
 
Use the authenticated session cookie stored in the file, for example `cookie.txt`, to access the Drill Web Console pages, as shown in the following example:
 
       curl -v --negotiate -b cookie.txt -u : http://<hostname>:8047/query       
       Example: curl -v --negotiate -b cookie.txt -u : http://example.QA.LAB:8047/query  

##Logging in to the Drill Web Console
With the addition of SPNEGO authentication in Drill 1.13, an administrator can configure FORM and/or SPNEGO authentication mechanisms. The Drill Web Console provides two possible log in options for a user depending on the configuration. 

If a user selects FORM, he/she must enter their username and password to access restricted pages in the Drill Web Console. The user is authenticated through PAM. 

If the user selects SPNEGO, the user is automatically logged in if they are an authenticated Kerberos user. 

If accessing a protected page directly, the user is redirected to the authentication log in page. If the client fails to authenticate using SPNEGO, an error page displays with an option to use FORM authentication, assuming FORM authentication is configured on the server side.


                           	
 



 









