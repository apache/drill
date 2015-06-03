---
title: "Configuring Connections on Linux and Mac OS X"
parent: "Using ODBC on Linux and Mac OS X"
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

If the configuration files already exist in your home directory, you can use the sample configuration files as a guide for modifying the existing configuration files as described in Steps 2-4.

----------

## Step 1: Set Environment Variables (Linux only)

Set the following environment variables to point to the` odbc.ini`
and `mapr.drillodbc.ini `configuration files, respectively:

  * `ODBCINI` (point to `odbc.ini`)
  * `MAPRDRILLINI` (point to `mapr.drillodbc.ini`)

For example, if you are using the 32-bit driver and the files are in the
default install directory, set the environment variables as follows:

{% include startnote.html %}You do not need to set these variables for the Mac OS X version of the driver.{% include endnote.html %}

----------

## Step 2: Define the ODBC Data Sources in .odbc.ini

Define the ODBC data sources in the `odbc.ini` configuration file for your environment. The following sample shows a possible configuration for using Drill in embedded mode. Get the ZKQuorum and ZKClusterID values from the `drill-override.conf` file.

**Example**
          
    [ODBC]
    # Specify any global ODBC configuration here such as ODBC tracing.
  
    [ODBC Data Sources]
    My MapR Drill DSN=MapR Drill ODBC Driver
  
    [Sample MapR Drill DSN]
    # Description: DSN Description.
    # This key is not necessary and is only to give a description of the data source.
    Description=My MapR Drill ODBC Driver DSN
    # Driver: The location where the ODBC driver is installed to.
    Driver=/opt/mapr/drillodbc/lib/universal/libmaprdrillodbc.dylib
  
    # Values for ConnectionType, AdvancedProperties, Catalog, Schema should be set here.
    # If ConnectionType is Direct, include Host and Port. If ConnectionType is ZooKeeper, include ZKQuorum and ZKClusterID
    # They can also be specified in the connection string.
    ConnectionType=Zookeeper
    HOST=localhost
    PORT=31010
    ZKQuorum=localhost:2181
    ZKClusterID=drillbits1
    AdvancedProperties={HandshakeTimeout=5;QueryTimeout=180;TimestampTZDisplayTimeout=utc;ExcludedSchemas=sys,INFORMATION_SCHEMA}
    Catalog=DRILL
    Schema=

[Driver
Configuration
Options]({{ site.baseurl }}/docs/driver-configuration-options) describes configuration options available for controlling the
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
file. This configures the driver to work with your ODBC driver manager. The following sample shows a possible configuration.

**Example**

    [Driver]
    ## - Note that this default DriverManagerEncoding of UTF-32 is for iODBC.
    DriverManagerEncoding=UTF-32
    ErrorMessagesPath=/opt/mapr/drillodbc/ErrorMessages

    LogLevel=0
    LogPath=
    SwapFilePath=/tmp

    #   iODBC
    ODBCInstLib=libiodbcinst.dylib

### Configuring .mapr.drillodbc.ini

To configure the MapR Drill ODBC Driver in the `mapr.drillodbc.ini` configuration file, complete the following steps:

  1. Open the `mapr.drillodbc.ini` configuration file in a text editor.
  2. Edit the DriverManagerEncoding setting if necessary. The value is typically UTF-16 or UTF-32, but depends on the driver manger used. iODBC uses UTF-32 and unixODBC uses UTF-16. Review your ODBC Driver Manager documentation for the correct setting.
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

