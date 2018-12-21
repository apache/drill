---
title: "Installing the Driver on Linux"
date: 2018-12-21
parent: "Installing the ODBC Driver"
---
Install the Drill ODBC Driver on the machine from which you connect to
the Drill service. You can install the 32- or 64-bit driver on Linux. Install
the version of the driver that matches the architecture of the client
application that you use to access Drill. The 64-bit editions of Linux support
32- and 64-bit applications.

Install the Drill ODBC Driver on a system that meets the [system requirements]({{site.baseurl}}/docs/installing-the-driver-on-linux/#system-requirements), and then complete the following steps described in detail in this document:

  * [Step 1: Download the Drill ODBC Driver]({{site.baseurl}}/docs/installing-the-driver-on-linux/#step-1:-download-the-drill-odbc-driver)
  * [Step 2: Install the Drill ODBC Driver]({{site.baseurl}}/docs/installing-the-driver-on-linux/#step-2:-install-the-drill-odbc-driver)
  * [Step 3: Check the Drill ODBC Driver Version]({{site.baseurl}}/docs/installing-the-driver-on-linux/#step-3:-check-the-drill-odbc-driver-version)

Verify that your system meets the system requirements before you start.


## System Requirements

  * One of the following distributions (32- and 64-bit editions are supported):
    * Red Hat® Enterprise Linux® (RHEL) 5, 6, or 7
    * CentOS 5, 6, or 7
    * SUSE Linux Enterprise Server (SLES) 11 or 12     
 * 90 MB of available disk space.
 * An installed ODBC driver manager such as, iODBC 3.52.7 (or above) or unixODBC 2.2.14 (or above). On Linux, iODBC 3.52.7 is available as a tarball. After unpacking the tarball, see the README for instructions about building the driver manager.  
 * The client must be able to resolve the actual host name of the Drill node or nodes from the IP address. Verify that a DNS entry was created on the client machine for the Drill node or nodes. If not, create an entry in `/etc/hosts` for each node in the following format:  

    	<drill-machine-IP> <drill-machine-hostname>  
    	
	Example: 

		127.0.0.1 localhost

To install the driver, you need Administrator privileges on the computer. 

## Step 1: Download the Drill ODBC Driver
Download the latest driver from the [download site](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/). 

## Step 2: Install the Drill ODBC Driver

To install the driver, complete the following steps:

  1. Login as the root user.
  
  2. Navigate to the directory that contains the driver RPM packages to install.
  
  3. Enter the following command (where `<RPMFileName>` is the file name of the RPM package containing the version of the driver that you want to install):  
  
     * RedHat/CentOS  
     
		 `yum localinstall --nogpgcheck <RPMFileName>`

     * SUSE  
     
      	`zypper install RPMFileName`


The Drill ODBC Driver typically resolves dependencies automatically. If not, error messages during the installation indicate that the package manager in your Linux distribution cannot resolve the
dependencies automatically. In this case, manually install the packages.

The following table provides a list of the Drill ODBC Driver file
locations and descriptions:

File| Description  
---|---  
`/opt/mapr/drill/ErrorMessages `| Error messages files directory.  
`/opt/mapr/drill/Setup`| Sample configuration files directory.  
`/opt/mapr/drill/lib/32 `| 32-bit shared libraries directory (will be created if you install the 32-bit driver).  
`/opt/mapr/drill/lib/64`| 64-bit shared libraries directory (will be created if you install the 64-bit driver)..  
  
## Step 3: Check the Drill ODBC Driver Version

To check the version of the driver you installed, use the following case-sensitive command on the terminal command line:

`yum list | grep MapRDrill`

or

`rpm -qa | grep MapRDrill`


### Next Step

[Configuring ODBC on Linux]({{ site.baseurl }}/docs/configuring-odbc-on-linux/).