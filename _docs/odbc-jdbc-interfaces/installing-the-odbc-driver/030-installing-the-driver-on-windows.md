---
title: "Installing the Driver on Windows"
date: 2017-08-18 17:48:08 UTC
parent: "Installing the ODBC Driver"
---
The Drill ODBC Driver installer is available for 32- and 64-bit
applications on Windows. On 64-bit Windows operating systems, you can execute both 32- and 64-bit applications. However, 64-bit applications must use 64-bit drivers, and 32-bit applications must use 32-bit drivers. Make sure that you use the driver version that matches the bitness of the client application machine. 
					
  * Drill 1.3 32-bit.msi for 32-bit applications
  * Drill 1.3 64-bit.msi for 64-bit applications  

  
{% include startnote.html %}Currently Drill does not support a 32-bit Windows machine. However, the 32- or 64-bit Drill ODBC Driver is supported on a 64-bit machine.{% include endnote.html %}


To install the Drill ODBC Driver on a system that meets the system requirements, complete the following steps:  

   *  [Step 1: Download the Drill ODBC Driver]({{site.baseurl}}/docs/installing-the-driver-on-windows/#step-1:-download-the-drill-odbc-driver)
   *  [Step 2: Install the Drill ODBC Driver]({{site.baseurl}}/docs/installing-the-driver-on-windows/#step-2:-install-the-drill-odbc-driver) 
   *  [Step 3: Verify the Installation]({{site.baseurl}}/docs/installing-the-driver-on-windows/#step-3:-verify-the-installation) 

##  System Requirements

Each computer where you install the driver must meet the following system
requirements:

  * One of the following operating systems are supported:
    * Windows Vista, 7, 8, or 10
    * Windows Server 2008 or later
  * 75 MB of available disk space
  * .NET Framework 4.5, installed and enabled by default on Windows 8 and later
  * [Visual C++ Redistributable for Visual Studio 2013](https://www.microsoft.com/en-us/download/details.aspx?id=40784) installed (with the same bitness as the driver that you are installing)
 
  * The client must be able to resolve the actual host name of the Drill node or nodes from the IP address. Verify that a DNS entry was created on the client machine for the Drill node or nodes. If not, create an entry in `\Windows\system32\drivers\etc\hosts` for each node in the following format:  

    `<drill-machine-IP> <drill-machine-hostname>`

	Example: 

	`127.0.0.1 localhost`

 {% include startnote.html %}Currently Drill does not support a 32-bit Windows machine; however, the 32- or 64-bit Drill ODBC Driver is supported on a 64-bit machine.{% include endnote.html %}

To install the driver, you need Administrator privileges on the computer.

----------

## Step 1: Download the Drill ODBC Driver

Download the installer that corresponds to the bitness of the client application from which you want to create an ODBC connection. The current version is 1.3.8.

* [Drill ODBC Driver (32-bit)](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/)  
* [Drill ODBC Driver (64-bit)](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/)

----------

## Step 2: Install the Drill ODBC Driver

1. Double-click the installer from the location where you downloaded it.
2. Click **Next.**
3. Select the check box to accept the terms of the License Agreement and click **Next**.
4. Verify or change the install location. Then, click **Next**.
5. Click **Install**.
6. When the installation completes, click **Finish**.

----------

## Step 3: Verify the Installation

To verify the installation on Windows 10, perform the following steps:

1\. Go to **Settings** and enter **odbc**.  


2\. Select **Set up ODBC data sources `<version>`**.  The **ODBC Data Source Administrator `<version>`** dialog appears. Click the **System DSN** to view the Drill data source. 

![](http://i.imgur.com/IEN5iek.png) 

3\. Click the **Drivers** tab and verify that the **Drill ODBC Driver** appears in the list of drivers that are installed on the computer.  

![](http://i.imgur.com/xM2QXcB.png)  

4\.Configure and start Drill before [testing]({{site.baseurl}}/docs/testing-the-odbc-connection/) the ODBC Data Source Administrator.

### Next Step 
[Configuring ODBC on Windows]({{ site.baseurl }}/docs/configuring-odbc-on-windows).
