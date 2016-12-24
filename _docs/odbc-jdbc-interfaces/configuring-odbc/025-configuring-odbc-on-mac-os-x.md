---
title: "Configuring ODBC on Mac OS X"
date: 2016-12-24 00:01:39 UTC
parent: "Configuring ODBC"
---
ODBC driver managers use configuration files to define and configure ODBC data
sources and drivers. To configure an ODBC connection for Linux or Mac OS X, complete the following
steps:

* [Step 1: Set Environment Variables]({{site.baseurl}}/docs/configuring-odbc-on-mac-os-x/#step-1:-set-environment-variables)
* [Step 2: Define the ODBC Data Sources in odbc.ini]({{site.baseurl}}/docs/configuring-odbc-on-mac-os-x/#step-2:-define-the-odbc-data-sources-in-.odbc.ini)
* [Step 3: Configure the MapR Drill ODBC Driver]({{site.baseurl}}/docs/configuring-odbc-on-mac-os-x/#step-4:-configure-the-mapr-drill-odbc-driver)

## Sample Configuration Files

Before you connect to Drill through an ODBC client tool
on Mac OS X, copy the following configuration files in `/opt/mapr/drillodbc/Setup` to your home directory unless the files already exist in your home directory:

* `mapr.drillodbc.ini`
* `odbc.ini`
* `odbcinst.ini`

In your home directory, rename the files as hidden files. Use sudo if necessary:

* .mapr.drillodbc.ini
* .odbc.ini

The installer for Mac OS X creates a sample user DSN in odbc.ini in either of the following locations:

* ~/Library/ODBC/odbc.ini
* ~/.odbc.ini

Depending on the driver manager you use, the user DSN in one of these files will be effective.

{% include startnote.html %}The System and User DSN use different ini files in different locations on OS X.{% include endnote.html %}

----------

## Step 1: Set Environment Variables 

Create or modify the `/etc/launchd.conf` file to set environment variables. Set the SIMBAINI variable to point to the `.mapr.drillodbc.ini` file, the ODBCINI variable to the `.odbc.ini` file, and the DYLD_LIBRARY_PATH to the location of the dynamic linker (DYLD) libraries and to the MapR Drill ODBC Driver. 

If you installed the iODBC driver manager using the DMG, the DYLD libraries are installed in `/usr/local/iODBC/lib`. The launchd.conf file should look something like this:

    setenv SIMBAINI /Users/joeuser/.mapr.drillodbc.ini
    setenv ODBCINI /Users/joeuser/.odbc.ini
    setenv DYLD_LIBRARY_PATH /usr/local/iODBC/lib:/opt/mapr/drillodbc/lib/universal

Restart the Mac OS X or run `launchctl load /etc/launchd.conf`.

----------

## Step 2: Define the ODBC Data Sources in .odbc.ini

Define the ODBC data sources in the `~/.odbc.ini` configuration file for your environment. 

You set the following properties for using Drill in embedded mode:

    ConnectionType=Direct
    HOST=localhost
    PORT=31010
    ZKQuorum=
    ZKClusterID=

You set the following properties for using Drill in distributed mode:

    ConnectionType=ZooKeeper
    HOST=
    PORT=
    ZKQuorum=<host name>:<port>,<host name>:<port> . . . <host name>:<port>
    ZKClusterID=<cluster name in `drill-override.conf`>

The following sample shows a possible configuration for using Drill in embedded mode. 
          
    [ODBC]
    # Specify any global ODBC configuration here such as ODBC tracing.
  
    [ODBC Data Sources]
    Sample MapR Drill DSN=MapR Drill ODBC Driver
  
    [Sample MapR Drill DSN]
    # Description: DSN Description.
    # This key is not necessary and only describes the data source.
    Description=Sample MapR Drill ODBC Driver DSN
    # Driver: The location where the ODBC driver is installed.
    Driver=/opt/mapr/drillodbc/lib/universal/libmaprdrillodbc.dylib
  
    # The DriverUnicodeEncoding setting is only used for SimbaDM
    # When set to 1, SimbaDM runs in UTF-16 mode.
    # When set to 2, SimbaDM runs in UTF-8 mode.
    #DriverUnicodeEncoding=2

    # Values for ConnectionType, AdvancedProperties, Catalog, Schema should be set here.

    # If ConnectionType is Direct, include Host and Port. If ConnectionType is ZooKeeper, include ZKQuorum and ZKClusterID
    # They can also be specified on the connection string.
    # AuthenticationType: No authentication; Basic Authentication
    ConnectionType=Direct
    HOST=localhost
    PORT=31010
    ZKQuorum=[Zookeeper Quorum]
    ZKClusterID=[Cluster ID]
    AuthenticationType=No Authentication
    UID=[USERNAME]
    PWD=[PASSWORD]
    AdvancedProperties=CastAnyToVarchar=true;HandshakeTimeout=5;QueryTimeout=180;TimestampTZDisplayTimezone=utc;ExcludedSchemas=sys,INFORMATION_SCHEMA;NumberOfPrefetchBuffers=5;
    Catalog=DRILL
    Schema=


### Authentication Properties
To password protect the DSN, uncomment the AuthenticationType, select Basic Authentication for the AuthenticationType, and configure UID and PWD properties.

### Direct and ZooKeeper Quorum Properties
To use Drill in distributed mode, set ConnectionType to Zookeeper, get the ZKQuorum and ZKClusterID values from the `drill-override.conf` file, and define the ZKQuorum and ZKClusterID properties. Format ZKQuorum as a comma separated list of ZooKeeper nodes in the following format:  
`<host name/ip address> : <port number>, <host name/ip address> : <port number>, . . .` 

For example:

* `ZKQuorum=centos23:5181,centos28:5181,centos29:5181`  
* `ZKClusterID=docs41cluster-drillbits`

To use Drill in local mode, do not define the ZKQuorum and ZKClusterID properties. Start Drill using the drill-localhost command, set ConnectionType to Direct, and define HOST and PORT properties. For example:

* `HOST=centos32.lab:5181`  
* `PORT=31010`

[Driver Configuration Options]({{ site.baseurl }}/docs/odbc-configuration-reference/#configuration-options) describes configuration options available for controlling the
behavior of DSNs using the MapR Drill ODBC Driver.

----------

## Step 3: Configure the MapR Drill ODBC Driver

Configure the MapR Drill ODBC Driver for your environment by modifying the `.mapr.drillodbc.ini` configuration
file. This configures the driver to work with your ODBC driver manager. The following sample shows a possible configuration, which you can use as is if you installed the default iODBC driver manager.

**Example**

    [Driver]
    ## - Note that this default DriverManagerEncoding of UTF-32 is for iODBC.
    DisableAsync=0
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
  2. Edit the DisableAsync setting if you want to enable a synchronous ODBC connection for performance reasons. Change the default 0 to 1 to disable the asynchronous and enable the synchronous connection.  
     A change in state occurs during driver initialization and is propagated to all driver DSNs.  
  3. Edit the DriverManagerEncoding setting if necessary. The value is typically UTF-16 or UTF-32, but depends on the driver manager used. iODBC uses UTF-32 and unixODBC uses UTF-16. Review your ODBC Driver Manager documentation for the correct setting.  
  4. Edit the `ODBCInstLib` setting. The value is the name of the `ODBCInst` shared library for the ODBC driver manager that you use. The configuration file defaults to the shared library for `iODBC`. The shared library name for `iODBC` is `libiodbcinst.dylib`.  
      
     Specify an absolute or relative filename for the library. If you use
the relative file name, include the path to the library in the library path
environment variable. The library path environment variable is
named `DYLD_LIBRARY_PATH`.  
  5. Save the `mapr.drillodbc.ini` configuration file.

### Next Step

Refer to [Testing the ODBC Connection]({{ site.baseurl }}/docs/testing-the-odbc-connection).

