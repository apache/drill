---
title: "Installing the Driver on Mac OS X"
date: 2016-12-24 00:01:40 UTC
parent: "Installing the ODBC Driver"
---
Install the MapR Drill ODBC Driver on the machine from which you connect to
the Drill service.

Install the MapR Drill ODBC Driver on a system that meets the [system requirements]({{site.baseurl}}/docs/installing-the-driver-on-mac-os-x/#system-requirements). Complete the following steps, described in detail in this document:

  * [Step 1: Download the MapR Drill ODBC Driver]({{site.baseurl}}/docs/installing-the-driver-on-mac-os-x/#step-1-download-the-mapr-drill-odbc-driver)  
  * [Step 2: Install the MapR Drill ODBC Driver]({{site.baseurl}}/docs/installing-the-driver-on-mac-os-x/#step-2-install-the-mapr-drill-odbc-driver) 
  * [Step 3: Check the MapR Drill ODBC Driver Version]({{site.baseurl}}/docs/installing-the-driver-on-mac-os-x/#step-3-check-the-mapr-drill-odbc-driver-version)

## System Requirements

  * Mac OS X version 10.6.8 or later  
  * 100 MB of available disk space  
  * iODBC 3.52.7 or later  
    The iodbc-config file in the `/usr/local/iODBC/bin` includes the version.  
  * The client must be able to resolve the actual host name of the Drill node or nodes from the IP address. Verify that a DNS entry was created on the client machine for the Drill node or nodes.   
If not, create an entry in `/etc/hosts` for each node in the following format:  

    `<drill-machine-IP> <drill-machine-hostname>`  
    Example: `127.0.0.1 localhost`

To install the driver, you need Administrator privileges on the computer.

----------

## Step 1: Download the MapR Drill ODBC Driver

Click the following link to download the driver:  

[MapR Drill ODBC Driver for Mac](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/MapRDrill_odbc_v1.3.0.1009/MapR_Drill_1.3.dmg)

----------

## Step 2: Install the MapR Drill ODBC Driver

To install the driver, complete the following steps:

  1. Double-click `MapRDrill1.3.dmg` to mount the disk image.
  2. Double-click `MapRDrillODBC.pkg` to run the Installer.
  3. Follow the instructions in the Installer to complete the installation process.
  4. When the installation completes, click **Close.**

{% include startnote.html %}MapR Drill ODBC Driver files install in the following locations:{% include endnote.html %}

  * `/opt/mapr/drillodbc/ErrorMessages` – Error messages files directory
  * `/opt/mapr/drillodbc/Setup` – Sample configuration files directory
  * `/opt/mapr/drillodbc/lib/universal` – Binaries directory

## Step 3: Check the MapR Drill ODBC Driver version

To check the version of the driver you installed, use the following command on the terminal command line:

    $ pkgutil --info mapr.drillodbc
    package-id: mapr.drillodbc
    version: 1.3.0 (may also display as 3.52.12)
    volume: /
    location: 
    install-time: 1433465518

### Next Step

[Configuring ODBC on Mac OS X]({{ site.baseurl }}/docs/configuring-odbc-on-mac-os-x/).
