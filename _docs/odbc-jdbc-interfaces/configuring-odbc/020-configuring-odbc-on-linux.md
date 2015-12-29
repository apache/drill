---
title: "Configuring ODBC on Linux"
date:  
parent: "Configuring ODBC"
---
ODBC driver managers use configuration files to define and configure ODBC data
sources and drivers. To configure an ODBC connection for Linux, complete the following
steps:

* [Step 1: Set Environment Variables]({{site.baseurl}}/docs/configuring-odbc-on-linux/#step-1:-set-environment-variables)
* [Step 2: Define the ODBC Data Sources in odbc.ini]({{site.baseurl}}/docs/configuring-odbc-on-linux/#step-2:-define-the-odbc-data-sources-in-.odbc.ini)
* [Step 3: (Optional) Define the ODBC Driver in odbcinst.ini]({{site.baseurl}}/docs/configuring-odbc-on-linux/#step-3:-(optional)-define-the-odbc-driver-in-.odbcinst.ini)
* [Step 4: Configure the MapR Drill ODBC Driver]({{site.baseurl}}/docs/configuring-odbc-on-linux/#configuring-.mapr.drillodbc.ini)

## Sample Configuration Files

Before you connect to Drill through an ODBC client tool
on Linux, copy the following configuration files in `/opt/mapr/drillobdc/Setup` to your home directory unless the files already exist in your home directory:

* `mapr.drillodbc.ini`
* `odbc.ini`
* `odbcinst.ini`

In your home directory, rename the files as hidden files. Use sudo if necessary:

* .mapr.drillodbc.ini
* .odbc.ini
* .odbcinst.ini

----------

## Step 1: Set Environment Variables 

1. Set the ODBCINI environment variable to point to the `.odbc.ini` in your home directory. For example:  
   `export ODBCINI=~/.odbc.ini`
2. Set the MAPRDRILLINI environment variable to point to `.mapr.drillodbc.ini` in your home directory. For example:  
   `export MAPRDRILLINI=~/.mapr.drillodbc.ini`
3. Set the LD_LIBRARY_PATH environment variable  to point to your ODBC driver manager libraries and the MapR ODBC Driver for Apache Drill shared libraries. For example:  
   `export LD_LIBRARY_PATH=/usr/local/lib:/opt/mapr/drillodbc/lib/64`

You can have both 32- and 64-bit versions of the driver installed at the same time on the same computer. 
{% include startimportant.html %}Do not include the paths to both 32- and 64-bit shared libraries in LD_LIBRARY PATH at the same time.{% include endimportant.html %}
Only include the path to the shared libraries corresponding to the driver matching the bitness of the client application you use to access Drill.

----------

## Step 2: Define the ODBC Data Sources in .odbc.ini

Define the ODBC data sources in the `~/.odbc.ini` configuration file for your environment. To use Drill in embedded mode, set the following properties:

    ConnectionType=Direct
    HOST=localhost
    PORT=31010
    ZKQuorum=
    ZKClusterID=

To use Drill in distributed mode, set the following properties, described in detail in section ["Direct and ZooKeeper Quorum Properties"]({{site.baseurl}}/docs/configuring-odbc-on-linux/#direct-and-zookeeper-quorum-properties):

    ConnectionType=ZooKeeper
    HOST=
    PORT=
    ZKQuorum=<host name>:<port>,<host name>:<port> . . . <host name>:<port>
    ZKClusterID=<cluster name in `drill-override.conf`>

The following Linux sample shows a possible configuration for using Drill in distributed mode. The configuration assumes you started Drill using the `drill-conf` command. The example modifies the default Linux-installed `.odbc.ini` for a 64-bit system by commenting out 32-bit properties, adding 64-bit properties, and removes the extraneous [Sample MapR Drill DSN 64] from `.odbc.ini`.

    [ODBC]
    Trace=no

    [ODBC Data Sources]
    #Sample MapR Drill DSN 32=MapR Drill ODBC Driver 32-bit
    Sample MapR Drill DSN 64=MapR Drill ODBC Driver 64-bit

    [Sample MapR Drill DSN 64]
    #[Sample MapR Drill DSN 32]
    # This key is not necessary and only describes the data source.
    #Description=MapR Drill ODBC Driver (32-bit) DSN
    Description=MapR Drill ODBC Driver (64-bit) DSN


    # Driver: The location where the ODBC driver is installed to.
    #Driver=/opt/mapr/drillodbc/lib/32/libmaprdrillodbc32.so
    Driver=/opt/mapr/drillodbc/lib/64/libmaprdrillodbc64.so

    # The DriverUnicodeEncoding setting is only used for SimbaDM
    # When set to 1, SimbaDM runs in UTF-16 mode.
    # When set to 2, SimbaDM runs in UTF-8 mode.
    #DriverUnicodeEncoding=2

    # Values for ConnectionType, AdvancedProperties, Catalog, Schema should be set here.
    # If ConnectionType is Direct, include Host and Port. If ConnectionType is ZooKeeper, include ZKQuorum and ZKClusterID
    # They can also be specified on the connection string.
    # AuthenticationType: No authentication; Basic Authentication
    ConnectionType=ZooKeeper
    HOST=
    PORT=
    ZKQuorum=centos23:5181,centos28:5181,centos29:5181
    ZKClusterID=docs41cluster-drillbits
    AuthenticationType=No Authentication
    UID=[USERNAME]
    PWD=[PASSWORD]
    AdvancedProperties=CastAnyToVarchar=true;HandshakeTimeout=5;QueryTimeout=180;TimestampTZDisplayTimezone=utc;ExcludedSchemas=sys,INFORMATION_SCHEMA;NumberOfPrefetchBuffers=5;
    Catalog=DRILL
    Schema=

    # The DriverUnicodeEncoding setting is only used for SimbaDM

### Authentication Properties
To password protect the DSN, uncomment the AuthenticationType, select Basic Authentication for the AuthenticationType, and configure UID and PWD properties.

### Direct and ZooKeeper Quorum Properties
To use Drill in distributed mode, set ConnectionType to Zookeeper, get the ZKQuorum and ZKClusterID values from the `drill-override.conf` file, and define the ZKQuorum and ZKClusterID properties. The `drill-override.conf` is in the `/drill/drill-<version>/conf` directory. Format ZKQuorum as a comma separated list of ZooKeeper nodes in the following format:  
`<host name/ip address> : <port number>, <host name/ip address> : <port number>, . . .` 

For example:

* `ZKQuorum=centos23:5181,centos28:5181,centos29:5181`  
* `ZKClusterID=docs41cluster-drillbits`

To use Drill in embedded mode, do not define the ZKQuorum and ZKClusterID properties. Start Drill using the drill-localhost command, set ConnectionType to Direct, and define HOST and PORT properties. For example:

* `HOST=centos32.lab:5181`  
* `PORT=31010`

[Driver Configuration Options]({{ site.baseurl }}/docs/odbc-configuration-reference/#configuration-options) describes configuration options available for controlling the
behavior of DSNs using the MapR Drill ODBC Driver.

----------

## Step 3: (Optional) Define the ODBC Driver in .odbcinst.ini

The `.odbcinst.ini` is an optional configuration file that defines the ODBC
Drivers. This configuration file is optional because you can specify drivers
directly in the` .odbc.ini` configuration file. The `.odbinst.ini` file contains the following sample configurations.
  
**Example**

    [ODBC Drivers]
    [ODBC Drivers]
    MapR Drill ODBC Driver 32-bit=Installed
    MapR Drill ODBC Driver 64-bit=Installed

    [MapR Drill ODBC Driver 32-bit]
    Description=MapR Drill ODBC Driver(32-bit)
    Driver=/opt/mapr/drillodbc/lib/32/libmaprdrillodbc32.so

    [MapR Drill ODBC Driver 64-bit]
    Description=MapR Drill ODBC Driver(64-bit)
    Driver=/opt/mapr/drillodbc/lib/64/libmaprdrillodbc64.so 

----------

## Step 4: Configure the MapR Drill ODBC Driver

Configure the MapR Drill ODBC Driver for your environment by modifying the `.mapr.drillodbc.ini` configuration
file. This configures the driver to work with your ODBC driver manager. The following sample shows a possible configuration, which you can use as is if you installed the default iODBC driver manager.

**Example**

    . . .
    [Driver]
    DisableAsync=0
    DriverManagerEncoding=UTF-32
    ErrorMessagesPath=/opt/mapr/drillodbc/ErrorMessages
    LogLevel=0
    LogPath=[LogPath]
    SwapFilePath=/tmp

    ## - Uncomment the ODBCInstLib corresponding to the Driver Manager being used.
    ## - Note that the path to your ODBC Driver Manager must be specified in LD_LIBRARY_PATH.

    # Generic ODBCInstLib
    #   iODBC
    ODBCInstLib=libiodbcinst.so
    . . .

### Configuring .mapr.drillodbc.ini

To configure the MapR Drill ODBC Driver in the `mapr.drillodbc.ini` configuration file, complete the following steps:

  1. Open the `mapr.drillodbc.ini` configuration file in a text editor.  
  2. Edit the DisableAsync setting if you want to enable a synchronous ODBC connection for performance reasons. Change the default 0 to 1 to disable the asynchronous and enable the synchronous connection.  
     A change in state occurs during driver initialization and is propagated to all driver DSNs.  
  3. Edit the DriverManagerEncoding setting if necessary. The value is typically UTF-16 or UTF-32, but depends on the driver manager used. iODBC uses UTF-32 and unixODBC uses UTF-16. Review your ODBC Driver Manager documentation for the correct setting.  
  4. Edit the `ODBCInstLib` setting. The value is the name of the `ODBCInst` shared library for the ODBC driver manager that you use. The configuration file defaults to the shared library for `iODBC`. In Linux, the shared library name for iODBC is `libiodbcinst.so`.  
     
     Specify an absolute or relative filename for the library. If you use
the relative file name, include the path to the library in the library path
environment variable. The library path environment variable is named
`LD_LIBRARY_PATH`.  
  5. Save the `mapr.drillodbc.ini` configuration file.

### Next Step

Refer to [Testing the ODBC Connection]({{ site.baseurl }}/docs/testing-the-odbc-connection).

