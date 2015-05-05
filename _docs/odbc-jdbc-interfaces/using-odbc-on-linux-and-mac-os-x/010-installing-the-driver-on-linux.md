---
title: "Installing the Driver on Linux"
parent: "Using ODBC on Linux and Mac OS X"
---
Install the MapR Drill ODBC Driver on the machine from which you connect to
the Drill service. You can install the 32- or 64-bit driver on Linux. Install
the version of the driver that matches the architecture of the client
application that you use to access Drill. The 64-bit editions of Linux support
32- and 64-bit applications.

To install the MapR Drill ODBC Driver, complete the following steps:

  * Step 1: Downloading the MapR Drill ODBC Driver 
  * Step 2: Installing the MapR Drill ODBC Driver
  * Step 3: Setting the LD_LIBRARY_PATH Environment Variable

After you complete the installation steps, complete the steps listed in
[Configuring ODBC Connections on Linux and Mac OS X]({{ site.baseurl }}/docs/configuring-connections-on-linux-and-mac-os-x).

Verify that your system meets the system requirements before you start.

**System Requirements**

  * One of the following distributions (32- and 64-bit editions are supported):
    * Red Hat® Enterprise Linux® (RHEL) 5.0/6.0
    * CentOS 5.0/6.0
    * SUSE Linux Enterprise Server (SLES) 11
  * 90 MB of available disk space.
  * An installed ODBC driver manager:
    * iODBC 3.52.7 or above  
      OR 
    * unixODBC 2.2.12 or above
  * The client must be able to resolve the actual hostname of the Drill node(s) with the IP(s). Verify that a DNS entry was created on the client machine for the Drill node(s).   
If not, create the following entry in `/etc/hosts` for the Drill node(s):  

    `<drill-machine-IP> <drill-machine-hostname>`  
    Example: `127.0.1.1 apachedemo`

To install the driver, you need Administrator privileges on the computer.

## Step 1: Downloading the MapR Drill ODBC Driver

Click on a link below to download the driver:

  * [MapR Drill ODBC Driver (32-bit)](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/MapRDrill_odbc_v0.09.0.0620/MapRDrillODBC-32bit-0.09.0.i686.rpm)
  * [MapR Drill ODBC Driver (64-bit)](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/MapRDrill_odbc_v0.09.0.0620/MapRDrillODBC-0.09.0.x86_64.rpm)

## Step 2: Installing the MapR Drill ODBC Driver

To install the driver, complete the following steps:

  1. Login as the root user.
  2. Navigate to the folder that contains the driver RPM packages to install.
  3. Enter the following command where _RPMFileName_ is the file name of the RPM package containing the version of the driver that you want to install: 

     **RedHat/CentOS**
     
     `yum localinstall --nogpgcheck RPMFileName`

     **SUSE**
     
     `zypper install RPMFileName`

{% include startnote.html %}The MapR Drill ODBC Driver dependencies need to be resolved.{% include endnote.html %}

The MapR Drill ODBC Driver depends on the following resources:

  * `cyrus-sasl-2.1.22-7` or above
  * `cyrus-sasl-gssapi-2.1.22-7` or above
  * `cyrus-sasl-plain-2.1.22-7` or above

If the package manager in your Linux distribution cannot resolve the
dependencies automatically when installing the driver, download and manually
install the packages.

The following table provides a list of the MapR Drill ODBC Driver file
locations and descriptions:

File| Description  
---|---  
`/opt/mapr/drillodbc/ErrorMessages `| Error messages files directory.  
`/opt/mapr/drillodbc/Setup`| Sample configuration files directory.  
`/opt/mapr/drillodbc/lib/32 `| 32-bit shared libraries directory.  
`/opt/mapr/drillodbc/lib/64`| 64-bit shared libraries directory.  
  
## Step 3: Setting the LD_LIBRARY_PATH Environment Variable

The `LD_LIBRARY_PATH` environment variable must include the paths to the
following:

  * Installed ODBC driver manager libraries
  * Installed MapR ODBC Driver for Apache Drill shared libraries

You can have both 32- and 64-bit versions of the driver installed at the same time on the same computer. 
{% include startimportant.html %}Do not include the paths to both 32- and 64-bit shared libraries in LD_LIBRARY PATH at the same time.{% include endimportant.html %}
Only include the path to the shared libraries corresponding to the driver matching the bitness of the client application used.

For example, if you are using a 64-bit client application and ODBC driver
manager libraries are installed in` /usr/local/lib`, then set
`LD_LIBRARY_PATH` as follows:  
`export LD_LIBRARY_PATH=/usr/local/lib:/opt/simba/drillodbc/lib/64`  
Refer to your Linux shell documentation for details on how to set environment
variables permanently.

#### Next Step

Complete the steps listed in [Configuring ODBC Connections for Linux and Mac
OS X]({{ site.baseurl }}/docs/configuring-connections-on-linux-and-mac-os-x).

