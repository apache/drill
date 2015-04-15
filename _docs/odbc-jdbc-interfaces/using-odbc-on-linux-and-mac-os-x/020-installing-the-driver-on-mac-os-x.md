---
title: "Installing the Driver on Mac OS X"
parent: "Using ODBC on Linux and Mac OS X"
---
Install the MapR Drill ODBC Driver on the machine from which you connect to
the Drill service.

To install the MapR Drill ODBC Driver, complete the following steps:

  * Step 1: Downloading the MapR Drill ODBC Driver 
  * Step 2: Installing the MapR Drill ODBC Driver 
  * Step 3: Updating the DYLD_LIBRARY_PATH Environment Variable

After you complete the installation steps, complete the steps listed in
[Configuring ODBC Connections for Linux and Mac OS X](/docs/configuring-connections-on-linux-and-mac-os-x)
.

Verify that your system meets the following prerequisites before you start.

**System Requirements**

  * Mac OS X version 10.6.8 or later
  * 100 MB of available disk space
  * iODBC 3.52.7 or above
  * The client must be able to resolve the actual hostname of the Drill node(s) with the IP(s). Verify that a DNS entry was created on the client machine for the Drill node(s).   
If not, create the following entry in `/etc/hosts` for the Drill node(s):  
`<drill-machine-IP> <drill-machine-hostname>`  
Example: `127.0.1.1 apachedemo`

To install the driver, you need Administrator privileges on the computer.

## Step 1: Downloading the MapR Drill ODBC Driver

Click the following link to download the driver:

  * [MapR Drill ODBC Driver for Mac](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/MapRDrill_odbc_v0.08.1.0618/MapRDrillODBC_DriverOnly.dmg)

## Step 2: Installing the MapR Drill ODBC Driver

To install the driver, complete the following steps:

  1. Double-click `MapRDrillODBC.dmg` to mount the disk image.
  2. Double-click `MapRDrillODBC.pkg` to run the Installer.
  3. Follow the instructions in the Installer to complete the installation process.
  4. When the installation completes, click **Close.**

**Note:** MapR Drill ODBC Driver files install in the following locations:

  * `/opt/mapr/drillodbc/ErrorMessages` – Error messages files directory
  * `/opt/mapr/drillodbc/Setup` – Sample configuration files directory
  * `/opt/mapr/drillodbc/lib/universal` – Binaries directory

## Step 3: Updating the DYLD_LIBRARY_PATH Environment Variable

The DYLD_LIBRARY_PATH environment variable must include paths to the following
libraries:

  * Installed ODBC driver manager libraries
  * Installed MapR Drill ODBC Driver for Drill shared libraries

For example, if the ODBC driver manager libraries are installed in
`/usr/local/lib`, then set `DYLD_LIBRARY_PATH` to the following:

`export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:/usr/local/lib:/opt/mapr/drillodb
c/lib/universal`

#### Next Step

Complete the steps listed in [Configuring ODBC Connections for Linux and Mac
OS X](/docs/configuring-connections-on-linux-and-mac-os-x).
