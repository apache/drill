---
title: "Configuring ODBC on Windows"
parent: "Configuring ODBC"
---

Complete one of the following steps to create an ODBC connection on Windows to Drill data
sources:

  * Create a Data Source Name
  * Create an ODBC Connection String

**Prerequisite:** An Apache Drill installation must be [configured]({{ site.baseurl }}/docs/connect-a-data-source-introduction/) to access the data sources that you want to connect to.  For information about how to install Apache Drill, see [Install Drill]({{ site.baseurl }}/docs/install-drill).

## Create a Data Source Name (DSN)

Create a DSN that an application can use to connect to Drill data sources. If
you want to create a DSN for a 32-bit application, you must use the 32-bit
version of the ODBC Administrator to create the DSN.

1. To launch the ODBC Administrator, click **Start > All Programs > MapR Drill ODBC Driver 1.0 (32|64-bit) > (32|64-bit) ODBC Administrator**.  
   
    To launch the 32-bit version of the ODBC driver on a 64-bit machine, run:
`C:\WINDOWS\SysWOW64\odbcad32.exe`.  
    The ODBC Data Source Administrator window appears.  
2. Create a system or user DSN on the **System DSN** or **User DSN** tab, respectively. A system DSN is available for all users who log in to the machine. A user DSN is available to the user who creates the DSN.  
3. Click **Add**.  
4. Select **MapR Drill ODBC Driver** and click **Finish**.  
   The _MapR Drill ODBC Driver DSN Setup_ window appears.
5. In the **Data Source Name** field, enter a name for the DSN.  
6. Optionally, enter a description of the DSN in the Description field.
7. In the Connection Type section, select one of the following connection types:  
   * ZooKeeper Quorum  
   * Direct to Drillbit  
8. If you select **ZooKeeper Quorum**, provide values for the following properties:  
   * Quorum  
     A comma separated list of ZooKeeper nodes in the following format:  
     `<host name/ip address> : <port number>, <host name/ip address> : <port number>, . . .`  
   * Cluster ID  
     Name of the drillbit cluster, drillbits1 by default. Check the `drill-override.conf` file for any cluster name changes.  

    If you select Direct to Drillbit, provide the IP address or host name of the Drill server and the Drill listen port number, 31010 by default. Check the `drill-override.conf` file for any port changes.  
9. In **Default Schema**, select the [default schema]({{site.baseurl}}/docs/configuring-connections-on-windows/#schema) to connect to.  
10. Optionally, perform the following operations:  
    * Update the [advanced properties]({{site.baseurl}}/docs/configuring-connections-on-windows/#advanced-properties) configuration.  
    * Configure [logging options]({{site.baseurl}}/docs/configuring-connections-on-windows/#logging-options) to log types of events.  
11. Click **OK** to save the DSN.

### Next Step

[Connect to Drill Data Sources from a BI Tool]({{ site.baseurl }}/docs/connecting-to-odbc-data-sources).

