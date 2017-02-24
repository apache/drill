---
title: "Installing the Driver on Windows"
date: 2017-02-24 20:28:56 UTC
parent: "Installing the ODBC Driver"
---
The MapR Drill ODBC Driver installer is available for 32- and 64-bit
applications on Windows. Both versions of the driver can be installed on a 64-bit
machine. 

Install the MapR Drill ODBC Driver on a system that meets the system requirements. 

##  System Requirements

Each computer where you install the driver must meet the following system
requirements:

  * The 64-bit editions of the following operating systems are supported:
    * Windows® 8 and 8.1
    * Windows® 7 Professional
    * Windows® Server 2008, 2013 R2
  * .NET Framework 4.5, installed and enabled by default on Windows 8 and later
  * 60 MB of available disk space
  * The client must be able to resolve the actual host name of the Drill node or nodes from the IP address. Verify that a DNS entry was created on the client machine for the Drill node or nodes.   
If not, create an entry in `\Windows\system32\drivers\etc\hosts` for each node in the following format:  

    `<drill-machine-IP> <drill-machine-hostname>`  
    Example: `127.0.0.1 localhost`

 {% include startnote.html %}Currently Drill does not support a 32-bit Windows machine; however, the 32- or 64-bit MapR Drill ODBC Driver is supported on a 64-bit machine.{% include endnote.html %}

To install the driver, you need Administrator privileges on the computer.

----------

## Step 1: Download the MapR Drill ODBC Driver

Download the installer that corresponds to the bitness of the client application from which you want to create an ODBC connection:

* [MapR Drill ODBC Driver (32-bit)](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/MapRDrill_odbc_v1.3.0.1009/MapR_Drill_1.3_32-bit.msi)  
* [MapR Drill ODBC Driver (64-bit)](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/MapRDrill_odbc_v1.3.0.1009/MapR_Drill_1.3_64-bit.msi)

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

To verify the installation, perform the following steps:

1. Click **Start**, and locate the ODBC Administrator app that you just installed.  
   Installing the ODBC Administrator installs Drill Explorer and the Tableau TDC file. For example, on Windows 8.1 in Apps, several apps appear under MaprDrill ODBC Driver 1.0:
   ![]({{ site.baseurl }}/docs/img/odbc-mapr-drill-apps.png)

2. Click the ODBC Administrator app icon.
   The ODBC Data Source Administrator dialog appears.
   ![]({{ site.baseurl }}/docs/img/odbc-user-dsn.png)
3. Click the **Drivers** tab and verify that the MapR Drill ODBC Driver appears in the list of drivers that are installed on the computer.
   ![]({{ site.baseurl }}/docs/img/odbc-drivers.png)

You need to configure and start Drill before [testing]({{site.baseurl}}/docs/testing-the-odbc-connection/) the ODBC Data Source Administrator.


### Next Step 
[Configuring ODBC on Windows]({{ site.baseurl }}/docs/configuring-odbc-on-windows).
