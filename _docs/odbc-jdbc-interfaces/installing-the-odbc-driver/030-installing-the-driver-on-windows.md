---
title: "Installing the Driver on Windows"
parent: "Installing the ODBC Driver"
---
The MapR Drill ODBC Driver installer is available for 32-bit and 64-bit
applications on Windows. Both versions of the driver can be installed on a 64-bit
machine. 

To ensure success, install the MapR Drill ODBC Driver on a system that meets the [system requirements]({{site.baseurl}}/docs/installing-the-driver-on-windows/). Complete the following steps described in detail in this document:

* [Step 1: Download the MapR Drill ODBC Driver]({{site.baseurl}}/docs/installing-the-driver-on-windows/#step-1:-download-the-mapr-drill-odbc-driver)
* [Step 2: Install the MapR Drill ODBC Driver]({{site.baseurl}}/docs/installing-the-driver-on-windows/#step-2:-install-the-mapr-drill-odbc-driver)
* [Step 3: Verify the installation]({{site.baseurl}}/docs/installing-the-driver-on-windows/#step-3:-verify-the-installation)

##  System Requirements

Each computer where you install the driver must meet the following system
requirements:

  * The 32- and 64-bit editions of the following operating systems are recommended:
    * Windows® 8 and 8.1
    * Windows® 7 Professional
    * Windows® Server 2008, 2013 R2
  * .NET Framework 4.5, installed and enabled by default on Windows 8 and later
  * 60 MB of available disk space
  * The client must be able to resolve the actual hostname of the Drill node(s) with the IP(s). Verify that a DNS entry was created on the client machine for the Drill node(s). If not, create an entry for the Drill node(s) in the HOSTS file:
    
    `<drill-machine-IP> <drill-machine-hostname>`  
    Example: `127.0.1.1 apachedemo`

To install the driver, you need Administrator privileges on the computer.

----------

## Step 1: Download the MapR Drill ODBC Driver

Download the installer that corresponds to the bitness of the client application from which you want to create an ODBC connection:

* [MapR Drill ODBC Driver (32-bit)](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/MapRDrill_odbc_v1.0.0.1001/MapRDrillODBC32.msi)  
* [MapR Drill ODBC Driver (64-bit)](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/MapRDrill_odbc_v1.0.0.1001/MapRDrillODBC64.msi)

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
   Installing the ODBC Administrator installs Drill Explorer and the Tableau TDC file. For example, on Windows 8.1 in Apps, the several apps appear under MaprDrill ODBC Driver 1.0:
   ![]({{ site.baseurl }}/docs/img/odbc-mapr-drill-apps.png)

2. Click the ODBC Administrator app icon.
   The ODBC Data Source Administrator dialog appears.
   ![]({{ site.baseurl }}/docs/img/odbc-user-dsn.png)
3. Click the **Drivers** tab and verify that the MapR Drill ODBC Driver appears in the list of drivers that are installed on the computer.
   ![]({{ site.baseurl }}/docs/img/odbc-drivers.png)

You need to start Drill before [testing]({{site.baseurl}}/docs/testing-the-odbc-connection/) the ODBC Data Source Administrator.

## The Tableau Data-connection Customization (TDC) File

The MapR Drill ODBC Driver includes a file named `MapRDrillODBC.TDC`. The TDC file includes customizations that improve ODBC configuration and performance
when using Tableau.

### Next Step 
[Configuring ODBC on Windows]({{ site.baseurl }}/docs/configuring-odbc-on-windows).
