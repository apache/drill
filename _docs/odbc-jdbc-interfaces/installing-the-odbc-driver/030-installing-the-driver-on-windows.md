---
title: "Installing the Driver on Windows"
date: 2017-05-31 00:03:41 UTC
parent: "Installing the ODBC Driver"
---
The MapR Drill ODBC Driver installer is available for 32- and 64-bit
applications on Windows®. On 64-bit Windows operating systems, you can execute both 32- and 64-bit applications. However, 64-bit applications must use 64-bit drivers, and 32-bit applications must use 32-bit drivers. Make sure that you use the driver version that matches the bitness of the client application machine. 
					
  * MapR Drill 1.3 32-bit.msi for 32-bit applications
  * MapR Drill 1.3 64-bit.msi for 64-bit applications


Install the MapR Drill ODBC Driver on a system that meets the system requirements. 

##  System Requirements

Each computer where you install the driver must meet the following system
requirements:

  * One of the following operating systems are supported:
    * Windows Vista, 7, 8, or 10
    * Windows Server 2008 or later
  * 75 MB of available disk space
  * .NET Framework 4.5, installed and enabled by default on Windows 8 and later
  * Visual C++ Redistributable for Visual Studio 2013 installed (with the same bitness as the driver that you are installing)
 
  * The client must be able to resolve the actual host name of the Drill node or nodes from the IP address. Verify that a DNS entry was created on the client machine for the Drill node or nodes.   
If not, create an entry in `\Windows\system32\drivers\etc\hosts` for each node in the following format:  

    `<drill-machine-IP> <drill-machine-hostname>`  
    Example: `127.0.0.1 localhost`

 {% include startnote.html %}Currently Drill does not support a 32-bit Windows machine; however, the 32- or 64-bit MapR Drill ODBC Driver is supported on a 64-bit machine.{% include endnote.html %}

To install the driver, you need Administrator privileges on the computer.

----------

## Step 1: Download the MapR Drill ODBC Driver

Download the installer that corresponds to the bitness of the client application from which you want to create an ODBC connection. Version 1.3.8 is the current version.

* [MapR Drill ODBC Driver (32-bit)](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/)  
* [MapR Drill ODBC Driver (64-bit)](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/)

----------

## Step 2: Install the MapR Drill ODBC Driver

1. Double-click the installer from the location where you downloaded it.
2. Click **Next.**
3. Select the check box to accept the terms of the License Agreement and click **Next**.
4. Verify or change the install location. Then, click **Next**.
5. Click **Install**.
6. When the installation completes, click **Finish**.

----------

## Step 3: Verify the installation

To verify the installation on Windows 10, perform the following steps:

1. Go to **Settings** and enter **odbc**. Select ***Setup ODBC data sources <version>***.

2. The ODBC Data Source Administrator <version> dialog appears. Click the **System DSN** to view 
   the MapR Drill data source.  

   ![odbcuser]({{site.baseurl}}/docs/img/odbc-user-dsn.png)    

3. Click the **Drivers** tab and verify that the MapR Drill ODBC Driver appears in the list of drivers that are installed on the computer.  

   ![odbcdriver]({{site.baseurl}}/docs/img/odbc-drivers.png)

Configure and start Drill before [testing]({{site.baseurl}}/docs/testing-the-odbc-connection/) the ODBC Data Source Administrator.

### Next Step 
[Configuring ODBC on Windows]({{ site.baseurl }}/docs/configuring-odbc-on-windows).
