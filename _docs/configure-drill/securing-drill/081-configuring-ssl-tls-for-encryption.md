---
title: "Configuring SSL/TLS for Encryption"
date: 2018-07-07 01:29:20 UTC
parent: "Securing Drill"
---  

You can enable SSL in a Drill cluster. SSL (Secure Sockets Layer), more recently called TLS, is a security mechanism that encrypts data passed between the Drill client and Drillbit (server). SSL also provides one-way authentication through which the Drill client verifies the identity of the Drillbit.

Authentication occurs during the SSL handshake when the Drillbit (server) presents its certificate to the client, and the client checks if the certificate exists in its truststore or if the certificate is signed by a trusted CA (Certificate Authority) that exists in its truststore.

The following diagram depicts the communication between the Drill client and the Drillbit (server):  

![](https://i.imgur.com/UbM6u6r.png) 

The SASL feature in Drill provides authentication and an option to encrypt data, however the encryption feature is not available when using Plain authentication. If you need to use Plain authentication (certain BI tools only use Plain authentication), you can enable SSL to encrypt data. You can have both SASL and SSL enabled when using Plain authentication only. For any other scenario, using SSL and SASL encryption together is strongly discouraged.

The following diagram depicts the SSL communication paths between the Drill client and Drillbit (server), including the scenario where Plain authentication is used:  

![](https://i.imgur.com/NxgANHb.png)  

**Note:** The REST API supports HTTPS. SSL is not supported for communication between Drillbits.  

The following sections provide information about how to use certificates in a Drill cluster, enabling and configuring SSL, connection parameters, and common SSL issues.  

## SSL Certificates in a Drill Cluster  

The Drill server requires an SSL certificate. The certificate can be self-signed or signed by a CA (Certificate Authority).

Before you can enable SSL in a Drill cluster, you must either get a certificate or generate a certificate and then import the certificate into the Java keystore. You can do this using the Java keytool utility. See [To Use keytool to Create a Server Certificate](https://docs.oracle.com/cd/E19798-01/821-1841/gjrgy/) for instructions.

If you have a custom certificate, you can import it using the method described in [this keytool document](https://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html). 

After you generate or import a server certificate, add the path (and password) to the keystore in the SSL configuration for Drill. See the [SSL configuration options section]({{site.baseurl}}/docs/configuring-ssl-tls-for-encryption/#configuring-ssl/tls) below for information on how to update the SSL configuration.

[Restart Drill]({{site.baseurl}}/docs/starting-drill-in-distributed-mode/) after you modify the configuration options.  

## Enabling and Configuring SSL/TLS  

Enable SSL in `<DRILL_INSTALL_HOME>/conf/drill-override.conf`. You can use several configuration options to customize SSL.

You must [restart the Drillbit process]({{site.baseurl}}/docs/starting-drill-in-distributed-mode/) on each Drill node after you modify the configuration options.  

The following sections provide information and instructions for enabling and configuring SSL/TLS:

### Enabling SSL/TLS  

When SSL is enabled, all Drill clients, such as JDBC and ODBC, must connect to Drill servers using SSL. Enable SSL in the Drill startup configuration file, `<DRILL_HOME>/conf/drill-override.conf`.

To enable SSL for Drill, set the `drill.exec.security.user.encryption.ssl.enabled` option in `drill-override.conf` to `"true"`.  

### Configuring SSL/TLS  

You can customize SSL on a Drillbit through the SSL configuration options. You can set the options from the command-line (using Java system properties), in the `drill-override.conf` file, or in the property file to which the Hadoop parameter `hadoop.ssl.server.conf` points (recommended).  

**Note:** Specifying values in `drill-override.conf` can expose the security parameters to end users. Administrators should set these values in the Hadoop security file and restrict permissions on that file.  

If a parameter is specified in multiple places, the value in the Hadoop configuration takes precedence over the Drill configuration, which takes precedence over the system property.

The Hadoop configuration is specified in the file pointed to by the `hadoop.ssl.server.conf` parameter in the Hadoop `core-site.xml` file. Typically, this parameter points to `$HADOOP_CONF/ssl-server.xml`, which contains the property names to configure SSL. Both the `core-site.xml` file and the `ssl-server.xml` files must exist in Drill’s classpath. Drill’s SSL configuration picks up the Hadoop SSL configuration.  

**Note:** Since the Drillbit implementation is based on JSSE, several standard parameters that apply to JSSE will also apply to the Drillbit; however, you typically do not need to configure JSSE parameters.  

The following table lists the SSL configuration options with their descriptions and default values:  

| Drill Property Name                             | Hadoop Property Name            | System Property Name             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Allowed Values                 | Drill Default         |
|-------------------------------------------------|---------------------------------|----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|-----------------------|
| drill.exec.security.user.encryption.ssl.enabled |                                 |                                  | Enable   or disable TLS for Drill client - Drill Server communication. You must set   this option in drill-override.conf.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | true,false                     | FALSE                 |
| drill.exec.ssl.protocol                         |                                 |                                  | The   version of the TLS protocol to use                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | TLS,   TLSV1, TLSv1.1, TLSv1.2 | TLSv1.2 (recommended) |
| drill.exec.ssl.keyStoreType                     | ssl.server.keystore.type        | javax.net.ssl.keyStoreType       | Format   of the keystore file                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | jks,   jceks, pkcs12           | JKS                   |
| drill.exec.ssl.keyStorePath                     | ssl.server.keystore.location    | javax.net.ssl.keyStore           | Location   of the Java keystore file containing the Drillbit’s own certificate and   private key. On Windows, the specified pathname must use forward slashes, /,   in place of backslashes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |                                |                       |
| drill.exec.ssl.keyStorePassword                 | ssl.server.keystore.password    | javax.net.ssl.keyStorePassword   | Password   to access the private key from the keystore file. This password is used   twice: To unlock the keystore file (store password), and to decrypt the   private key stored in the keystore (key password) unless a key password is   specified separately.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |                                |                       |
| drill.exec.ssl.keyPassword                      | ssl.server.keystore.keypassword |                                  | Password   to access the private key from the keystore file. May be different from the   keystore password.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |                                |                       |
| drill.exec.ssl.trustStoreType                   | ssl.server.truststore.type      | javax.net.ssl.trustStoreType     | Format   of the truststore file                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | jks,   jceks, pkcs12           | JKS                   |
| drill.exec.ssl.trustStorePath                   | ssl.server.truststore.location  | javax.net.ssl.trustStore         | Location   of the Java keystore file containing the collection of CA certificates   trusted by the Drill client. On Windows, the specified pathname must use   forward slashes, /, in place of backslashes.              Note: If the trustStorePath is not provided, Drill ignores the   trustStorePassword parameter and gets the default Java truststore instead,   which causes issues if the Java truststore has a non-default password. The   Java APIs to load the default keystore assume the default password. The only   way to use the default keystore with a non-default password is to specify   both the path and the password to the keystore. To work around this issue,   pass the default Java truststore to the trustStorePath parameter.    |                                |                       |
| drill.exec.ssl.trustStorePassword               | ssl.server.truststore.password  | javax.net.ssl.trustStorePassword | Password   to access the private key from the keystore file specified as the truststore.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |                                |                       |
| drill.exec.ssl.provider                         |                                 |                                  | Changes   the underlying implementation to the chosen value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | OPENSSL/JDK                    | default: JDK          |
| drill.exec.ssl.useHadoopConfig                  |                                 |                                  | Use the setting in the hadoop configuration   file.          The hadoop configuration is specified in the file pointed to by the   hadoop.ssl.server.conf parameter in the core-site.xml file. Typically, this   parameter points to $HADOOP_CONF/ssl-server.xml which contains the property   names to configure TLS.                                                                                                                                                                                                                                                                                                                                                                                                                                           | true/false                     | default:true          |   


## JDBC Connection Parameters  

Use the SSL JDBC connection parameters and fully qualified host name to configure the jdbc connection string in SQLLine and connect to Drill.

The following table lists the parameters that you can include in the jdbc connection string using SQLLine.  

**Note:** Examples are provided after the table. For additional instructions, see the [Drill JDBC Driver](https://maprdocs.mapr.com/60/attachments/JDBC_ODBC_drivers/DrillJDBCInstallandConfigurationGuide.pdf) documentation.  

| Parameter                      | Value                          | Required                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|--------------------------------|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| enableTLS                      | true/false                     | [Optional]   If true, TLS is enabled. If not set or set to false, TLS is not enabled.                                                                                                                                                                                                                                                                                                                                                                                                  |
| trustStoreType                 | string                         | [Optional]     Default: JKS     The trustStore type.     Allowed values are :     JKS     PKCS12     If the useSystemTrustStore option is set to true (on Windows only), the   allowed values are:     Windows-MY     Windows-ROOT          Import the certificate into the "Trusted Root Certificate Authorities”   and set trustStoreType=Windows-ROOT. Also import the certificate into   "Trusted Root Certificate Authorities" or "Personal" and   set trustStoreType=Windows-MY. |
| trustStorePath                 | string                         | [Optional]   Path to the truststore. If not provided the default Java truststore will be   used. If this is not provided the trustStorePassword parameter will be   ignored.          Note that the order for looking for the default trustStore then java-home/lib/security/jssecacerts                                                                                                                                                                                               |
| trustStorePassword             | string                         | [Optional]   Password to the truststore.                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| disableHostVerification        | true/false                     | [Optional]   If true, we will not verify that the host in the certificate is the host we   are connecting to.      False by default.     (Hostname verification follows the specification in RFC2818)                                                                                                                                                                                                                                                                                  |
| disableCertificateVerification | true/false                     | [Optional]   If true we will not validate the certificate against the truststore.      False by default.                                                                                                                                                                                                                                                                                                                                                                               |
| TLSProtocol                    | TLS, TLSV1, TLSv1.1,   TLSv1.2 | [Optional]     Default: TLSv1.2 (recommended)                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| TLSHandshakeTimeout            | Time in milliseconds           | [Optional]     Default: 10 seconds     In some cases, the TLS handshake may fail and leave the client hanging.   This option sets the time for the client to timeout.                                                                                                                                                                                                                                                                                                                  |
| TLSProvider                    | JDK/OPENSSL                    | [Optional]     Default: JDK     Changes the underlying implementation to the chosen value.                                                                                                                                                                                                                                                                                                                                                                                             |
| useSystemTrustStore            | true/false                     | [Optional, Windows only]     Default: false      If provided, the client will read certificates from the Windows truststore.   In this case, trustStorePath and trustStorePassword, if specified, will be   ignored.      The user should set the default provider in   $JRE_HOME/lib/security/java.security to SunMSCAPI.     The trustStoreType should be set to either Windows-MY or Windows-ROOT.                                                                                  |  

### Examples  

The following examples show you how to connect to Drill through SQLLine with the jdbc connection string when SSL is not enabled and when SSL is enabled with and without a truststore.

**No SSL/TLS**  

       ./sqlline -u "jdbc:drill:schema=dfs.work;drillbit=localhost:31010;enableTLS=false"  

**SSL/TLS Enabled - No truststore**  

The default JSSE truststore will be tried with default password; the provided password will be ignored. If the default truststore password has been changed, this gives an error. To use the default truststore with a different password, pass the path to the default truststore with the password.  

       ./sqlline -u "jdbc:drill:schema=dfs.work;drillbit=localhost:31010;enableTLS=true;trustStorePassword=drill123"   
 
**SSL/TLS enabled - With truststore**  

       ./sqlline -u "jdbc:drill:schema=dfs.work;drillbit=localhost:31010;enableTLS=true;trustStorePath=~/ssl/truststore.ks;trustStorePassword=drill123"  

## ODBC Connection Parameters  
  
Use the SSL ODBC connection parameters to configure a connection to Drill through an ODBC tool.

The following table lists the ODBC connection parameters.  

**Note:** The Drill ODBC driver does not support password protected PEM/CRT files or multiple CRT certificates in a single PEM/CRT file. For additional instructions, see the [Drill ODBC Driver documentation](https://maprdocs.mapr.com/60/attachments/JDBC_ODBC_drivers/DrillODBCInstallandConfigurationGuide.pdf). 

| Name                           | Value                                                                                                                                                                                                                                                                     | Required | Description                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|--------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SSL                            | Clear (0)                                                                                                                                                                                                                                                                 | No       | This option specifies whether the cliet uses an SSL encrypted connection to communicate with Drill.Enabled(1):The client communicates with Drill using SSL.Disabled(0):SS Lis disabled.SSL is configured independently of authentication.When authentication and SSL are both enabled, the driver performs the specified authentication method over an SSL connection.                                                                                 |
| TLSProtocol                    | Empty, which defaults totlsv12.                                                                                                                                                                                                                                           | No       | This property specifies the TLS protocol version used.Accepted values are:tlsv1tlsv11tlsv12                                                                                                                                                                                                                                                                                                                                                            |
| TrustedCerts                   | The cacerts.pem file in the \lib subfolder within the Driver's installation directory. The exact file path varies depending on the version of the driver that is installed.For example, the path for the Windows driver is different from the path for the Mac OS driver. | No       | The full path of the PEM file containing Trusted CA certificates, for verifying the server. If this option is not set, then the driver defaults to using the trusted CA certificates PEM file installed by the driver.                                                                                                                                                                                                                                 |
| UseSystemTrustStore            | Clear (0)                                                                                                                                                                                                                                                                 | No       | This option specifies whether to use a CA certificate from the system truststore, or from a specified PEM file.Enabled (1): The driver verifies the connection using a certificate in the system truststoreDisabled (0): The driver verifies the connection using a specified PEM file.Note: This option is only available on Windows. If using this option, import the certificate into the "Trusted Root Certificate Authorities” certificate store. |
| DisableCertificateVerification | 0                                                                                                                                                                                                                                                                         | No       | This property specifies that the driver verifies the host certificate against the truststore. Accepted values are:0: The driver verifies the certificate against the truststore.1: The driver does not verify the certificate against the truststore.                                                                                                                                                                                                  |
| DisableHostVerification        | 0                                                                                                                                                                                                                                                                         | No       | This property specifies if the driver verifies that the host in the certificate is the host being connected to. Accepted values are:0: The driver verifies the certificate against the host being connected to.1: The driver does not verify the certificate against the host.                                                                                                                                                                         |  

## Avoiding Common SSL Issues  

The following sections provide insight to some common error messages that you may encounter with SSL.  

### ERROR: No Cipher suites in common.  

This is a general purpose error message that may have many reasons. The most common reason is that in order to use certain cipher suites, JSSE needs to use the private key stored in the Keystore. If this key is not accessible, JSSE filters out all cipher suites that need a private key. This effectively prunes out all available cipher suites so that no cipher suites match between the client and the server.

The private key from the keystore may be inaccessible for the following reasons:  

- Missing Keystore file  
- Invalid Keystore password  
- Empty key password or a key password that is different from the keystore password  

JSSE does not allow a key password that is null or an empty string even though it is possible to create a keystore with such a key password. Also, JSSE does not provide a system property to specify the key password. Drill provides a way to set the key password, but if you are using only system properties to configure JSSE, Drill will use the *keystore* password. If the keystore password is not the same as the key password, the key will again be inaccessible.  

- Corrupt keystore  

You can validate the keystore using keytool.  

### ERROR: SSL is enabled, but cannot be initialized due to the ‘Cannot recover key’ exception.  

The key is protected with a password and the provided password is not correct.  

### ERROR: Client connection timeout.  

A client connection can timeout because of networking issues or if there is a mismatch between the TLS/SSL configuration on the client and server.

Before trying to debug the TLS/SSL configuration, check if the server is reachable from the client.

If there is a mismatch between the TLS/SSL configuration, the TLS/SSL handshake between the client and server will fail. The server will silently drop the connection and the client will eventually time out. The handshake may fail due to many reasons, including:  

1-The server is configured to enableTLS and the client is not (and vice versa).  

a-If the client is not configured to use TLS and the server is, the error message will be similar to the following:  

       Error: Failure in connecting to Drill: org.apache.drill.exec.rpc.RpcException: HANDSHAKE_COMMUNICATION : Channel closed /10.10.10.11:49907 <--> hostname/10.10.10.11:31010. (state=,code=0)
       java.sql.SQLNonTransientConnectionException: Failure in connecting to Drill: org.apache.drill.exec.rpc.RpcException: HANDSHAKE_COMMUNICATION : Channel closed /10.10.10.11:49907 <-->hostname/10.10.10.11:31010.  

b-If the server is not configured to use TLS and the client tries to connect using TLS, the error message will be similar to the following:  

       Error: Failure in connecting to Drill: org.apache.drill.exec.rpc.NonTransientRpcException: Connecting to the server timed out. This is sometimes due to a mismatch in the SSL configuration between client and server. [ Exception: Timeout waiting for task.] (state=,code=0)

2-The server presents a certificate to the client containing a hostname that is not valid. When the client connects to a server, the hostname the client used to connect to the server must match the name of the host the certificate was assigned to. Certificates can contain wildcards for the hostname, so if you’re connecting to a Drill cluster via ZooKeeper, it would be best to have a certificate that contains wildcards that cover all the hosts on which Drill might be running. It is also important to ensure that the DNS and the hostnames of the machines in the cluster are set up consistently so that the Drillbits are registered with ZooKeeper using the same name as the name assigned in the certificate. The error message in this case is the same as the previous case:  

       Error: Failure in connecting to Drill: org.apache.drill.exec.rpc.NonTransientRpcException: Connecting to the server timed out. This is sometimes due to a mismatch in the SSL configuration between client and server. [ Exception: Timeout waiting for task.] (state=,code=0)  

Hostname verification can be turned off if there is no way to change the host configuration or the certificate. This is generally not recommended.



