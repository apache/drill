---
title: "Configuring ODBC on Linux/Mac OS X"
parent: "Configuring ODBC"
---
ODBC driver managers use configuration files to define and configure ODBC data
sources and drivers. To configure an ODBC connection for Linux or Mac OS X, complete the following
steps:

* Step 1: Set Environment Variables (Linux only)
* Step 2: Define the ODBC Data Sources in odbc.ini
* Step 3: (Optional) Define the ODBC Driver in odbcinst.ini
* Step 4: Configure the MapR Drill ODBC Driver

## Sample Configuration Files

Before you connect to Drill through an ODBC client tool
on Linux or Mac OS X, copy the following configuration files in `/opt/mapr/drillobdc/Setup` to your home directory unless the files already exist in your home directory:

* `mapr.drillodbc.ini`
* `odbc.ini`
* `odbcinst.ini`

In your home directory, use sudo to rename the files as hidden files:

* .mapr.drillodbc.ini
* .odbc.ini
* .odbcinst.ini

The installer for Mac OS X creates a sample user DSN in odbc.ini in either of the following locations:

* ~/Library/ODBC/odbc.ini
* ~/.odbc.ini

Depending on the driver manager you use, the user DSN in one of these files will be effective.

----------

## Step 1: Set Environment Variables 

### Linux

Set the following environment variables to point to the `.odbc.ini`
and `.mapr.drillodbc.ini` configuration files, respectively:

  * `ODBCINI` (point to `.odbc.ini`)
  * `MAPRDRILLINI` (point to `.mapr.drillodbc.ini`)

The `LD_LIBRARY_PATH` environment variable must include the paths to the
following:

  * Installed ODBC driver manager libraries
  * Installed MapR ODBC Driver for Apache Drill shared libraries

You can have both 32- and 64-bit versions of the driver installed at the same time on the same computer. 
{% include startimportant.html %}Do not include the paths to both 32- and 64-bit shared libraries in LD_LIBRARY PATH at the same time.{% include endimportant.html %}
Only include the path to the shared libraries corresponding to the driver matching the bitness of the client application used.

For example, if you are using a 64-bit client application and ODBC driver
manager libraries are installed in `/usr/local/lib`, then set
`LD_LIBRARY_PATH` as follows:  

`export LD_LIBRARY_PATH=/usr/local/lib:/opt/simba/drillodbc/lib/64`  

### Mac OS X

Create or modify the `/etc/launchd.conf` file to set environment variables. Set the SIMBAINI variable to point to the `.mapr.drillodbc.ini` file, the ODBCSYSINI varialbe to the `.odbcinst.ini` file, the ODBCINI variable to the `.odbc.ini` file, and the DYLD_LIBRARY_PATH to the location of the dynamic linker (DYLD) libraries and to the MapR Drill ODBC Driver. If you installed the iODBC driver manager using the DMG, the DYLD libraries are installed in `/usr/local/iODBC/lib`. The launchd.conf file should look something like this:

    setenv SIMBAINI /Users/joeuser/.mapr.drillodbc.ini
    setenv ODBCSYSINI /Users/joeuser/.odbcinst.ini
    setenv ODBCINI /Users/joeuser/.odbc.ini
    launchctl setenv DYLD_LIBRARY_PATH /usr/local/iODBC/lib:/opt/mapr/drillodbc/lib/universal

Restart the Mac OS X or run `launchctl load /etc/launchd.conf`.
----------

## Step 2: Define the ODBC Data Sources in .odbc.ini

Define the ODBC data sources in the `~/.odbc.ini` configuration file for your environment. The following sample shows a possible configuration for using Drill in embedded mode. 

**Example**
          
    [ODBC]
    # Specify any global ODBC configuration here such as ODBC tracing.
  
    [ODBC Data Sources]
    Sample MapR Drill DSN=MapR Drill ODBC Driver
  
    [Sample MapR Drill DSN]
    # Description: DSN Description.
    # This key is not necessary and is only to give a description of the data source.
    Description=Sample MapR Drill ODBC Driver DSN
    # Driver: The location where the ODBC driver is installed to.
    Driver=/opt/mapr/drillodbc/lib/universal/libmaprdrillodbc.dylib
  
    # Values for ConnectionType, AdvancedProperties, Catalog, Schema should be set here.
    # If ConnectionType is Direct, include Host and Port. If ConnectionType is ZooKeeper, include ZKQuorum and ZKClusterID
    # They can also be specified in the connection string.
    ConnectionType=Direct
    HOST=localhost
    PORT=31010
    ZKQuorum=
    ZKClusterID=
    AdvancedProperties={HandshakeTimeout=5;QueryTimeout=180;TimestampTZDisplayTimeout=utc;ExcludedSchemas=sys,INFORMATION_SCHEMA}
    Catalog=DRILL
    Schema=

If ConnectionType=Zookeeper, get the ZKQuorum and ZKClusterID values from the `drill-override.conf` file, and define the ZKQuorum and ZKClusterID properties. For example:

* `ZKQuorum=localhost:2181`  
* `ZKClusterID=drillbits1`

If ConnectionType=Direct, define HOST and PORT properties. For example:

* `HOST=localhost`  
* `PORT=31010`

[Driver
Configuration
Options]({{ site.baseurl }}/docs/odbc-configuration-reference/#configuration-options) describes configuration options available for controlling the
behavior of DSNs using the MapR Drill ODBC Driver.

----------

## Step 3: (Optional) Define the ODBC Driver in .odbcinst.ini

The `.odbcinst.ini` is an optional configuration file that defines the ODBC
Drivers. This configuration file is optional because you can specify drivers
directly in the` .odbc.ini` configuration file. The following sample shows a possible configuration.
  
**Example**

    [ODBC Drivers]
    MapR Drill ODBC Driver=Installed
   
    [MapR Drill ODBC Driver]
    Description=MapR Drill ODBC Driver
    Driver=/opt/mapr/drillodbc/lib/universal/libmaprdrillodbc.dylib

----------

## Step 4: Configure the MapR Drill ODBC Driver

Configure the MapR Drill ODBC Driver for your environment by modifying the `.mapr.drillodbc.ini` configuration
file. This configures the driver to work with your ODBC driver manager. The following sample shows a possible configuration, which you can use as is if you installed the default iODBC driver manager.

**Example**

    [Driver]
    ## - Note that this default DriverManagerEncoding of UTF-32 is for iODBC.
    DriverManagerEncoding=UTF-32
    ErrorMessagesPath=/opt/mapr/drillodbc/ErrorMessages

    LogLevel=0
    LogPath=
    SwapFilePath=/tmp

    # iODBC
    ODBCInstLib=libiodbcinst.dylib

### Configuring .mapr.drillodbc.ini

To configure the MapR Drill ODBC Driver in the `mapr.drillodbc.ini` configuration file, complete the following steps:

  1. Open the `mapr.drillodbc.ini` configuration file in a text editor.
  2. Edit the DriverManagerEncoding setting if necessary. The value is typically UTF-16 or UTF-32, but depends on the driver manager used. iODBC uses UTF-32 and unixODBC uses UTF-16. Review your ODBC Driver Manager documentation for the correct setting.
  3. Edit the `ODBCInstLib` setting. The value is the name of the `ODBCInst` shared library for the ODBC driver manager that you use. The configuration file defaults to the shared library for `iODBC`. In Linux, the shared library name for iODBC is `libiodbcinst.so`. In Mac OS X, the shared library name for `iODBC` is `libiodbcinst.dylib`.
     {% include startnote.html %}Review your ODBC Driver Manager documentation for the correct
setting.{% include endnote.html %} 
     Specify an absolute or relative filename for the library. If you use
the relative file name, include the path to the library in the library path
environment variable. In Linux, the library path environment variable is named
`LD_LIBRARY_PATH`. In Mac OS X, the library path environment variable is
named `DYLD_LIBRARY_PATH`.
  4. Save the `mapr.drillodbc.ini` configuration file.

### Next Step

Refer to [Testing the ODBC Connection on Linux and Mac OS X]({{ site.baseurl }}/docs/testing-the-odbc-connection).

