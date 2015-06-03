---
title: "Configuring Connections on Linux and Mac OS X"
parent: "Using ODBC on Linux and Mac OS X"
---
ODBC driver managers use configuration files to define and configure ODBC data
sources and drivers. Before you connect to Drill through an ODBC client tool
on Linux or Mac OS X, you must update the following configuration files:

  * `odbc.ini`
  * `odbcinst.ini`
  * `mapr.drillodbc.ini`

You can locate the configuration in `/opt/mapr/drillobdc/Setup`.

{% include startnote.html %}The installer for the Mac OS X version of the driver creates a sample User DSN that some driver managers use in ~/Library/ODBC/odbc.ini and ~/.odbc.ini.{% include endnote.html %}

To configure an ODBC connection for Linux or Mac OS X, complete the following
steps:

  * Step 1: Set Environment Variables (Linux only)
  * Step 2: Define the ODBC Data Sources in odbc.ini
  * Step 3: (Optional) Define the ODBC Driver in odbcinst.ini
  * Step 4: Configure the MapR Drill ODBC Driver

## Sample Configuration Files

The driver installation contains sample configuration files in the ~/`Setup`
directory that you can use as a reference.

If the configuration files do not already exist in your home directory, copy
the sample configuration files to that directory and rename them as hidden files:

* .mapr.drillodbc.ini
* .odbc.ini
* .odbcinst.ini

If the configuration files already exist in your home directory, you can use the sample configuration files as a guide for modifying the existing configuration files.

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

## Step 2: Define the ODBC Data Sources in `odbc.ini`

Define the ODBC data sources in the `odbc.ini` configuration file. This file
is divided into sections.

The following table provides information about each of the sections:

Section| Description  
---|---  
[ODBC]| (Optional) Controls the global ODBC configuration.  
[ODBC Data Sources]| (Required) Lists DSNs and associates them with a driver.  
User Defined|(Required) A section that you create. It must have the same name as the data source specified in the `[ODBC Data Sources]` section.  This is required to configure the data source.  
  
You can see the required sections in the following example `odbc.ini` file:

**Example**
          
    [ODBC]
    # Specify any global ODBC configuration here such as ODBC tracing.
  
    [ODBC Data Sources]
    Sample MapR Drill DSN=MapR Drill ODBC Driver
  
    [Sample MapR Drill DSN]
    # Description: DSN Description.
    # This key is not necessary and is only to give a description of the data source.
    Description=MapR Drill ODBC Driver DSN
    # Driver: The location where the ODBC driver is installed to.
    Driver=/opt/mapr/drillodbc/lib/universal/libmaprdrillodbc.dylib
  
    # Values for ConnectionType, AdvancedProperties, Catalog, Schema should be set here.
    # If ConnectionType is Direct, include Host and Port. If ConnectionType is ZooKeeper, include ZKQuorum and ZKClusterID
    # They can also be specified in the connection string.
    ConnectionType=Zookeeper
    HOST=[HOST]
    PORT=[PORT]
    ZKQuorum=
    ZKClusterID=
    AdvancedProperties={HandshakeTimeout=5;QueryTimeout=180;TimestampTZDisplayTimeout=utc;ExcludedSchemas=sys,INFORMATION_SCHEMA}
    Catalog=DRILL
    Schema=

### Configuring odbc.ini

To create a data source in the `odbc.ini` configuration file, complete the
following steps:

  1. Open the `.odbc.ini` configuration file in a text editor.
  2. Add a new entry to the `[ODBC Data Sources]` section, and type the data source name (DSN) and the driver name.
  3. To set configuration options, add a new section with a name that matches the data source name (DSN) that you specified in step 2. Specify configuration options as key-value pairs.
  4. Save the `.odbc.ini` configuration file.

For details on the configuration options available for controlling the
behavior of DSNs using Simba ODBC Driver for Apache Drill, see [Driver
Configuration
Options]({{ site.baseurl }}/docs/driver-configuration-options).

----------

## Step 3: (Optional) Define the ODBC Driver in `odbcinst.ini`

The `odbcinst.ini` is an optional configuration file that defines the ODBC
Drivers. This configuration file is optional because you can specify drivers
directly in the` odbc.ini` configuration file.

The `odbcinst.ini` file is divided into sections.

The following table provides information about each of the sections:

Section| Description  
---|---  
[ODBC Drivers]| This section lists the names of all the installed ODBC drivers.  
User Defined|  A section having the same name as the driver name specified in the [ODBC Drivers] section lists driver attributes and values.
  
You can see the sections in the following example `odbcinst.ini` file:

**Example**

    [ODBC Drivers]
    MapR Drill ODBC Driver=Installed
   
    [MapR Drill ODBC Driver]
    Description=MapR Drill ODBC Driver
    Driver=/opt/mapr/drillodbc/lib/universal/libmaprdrillodbc.dylib

### Configuring odbcinst.ini

To define a driver in the `odbcinst.ini` configuration file, complete the
following steps:

  1. Open the `odbcinst.ini` configuration file in a text editor.
  2. Add a new entry to the [ODBC Drivers] section. Type the driver name, and then type `=Installed`. Assign the driver name as the value of the Driver attribute in the data source definition instead of the driver shared library name.
  3. In `odbcinst.ini,` add a new section with a name that matches the driver name you typed in step 2, and add configuration options to the section based on the sample `odbcinst.ini` file provided with MapR Drill ODBC Driver in the Setup directory. Specify configuration options as key-value pairs.
  4. Save the `.odbcinst.ini` configuration file.

----------

## Step 4: Configure the MapR Drill ODBC Driver

Configure the MapR Drill ODBC Driver in the `mapr.drillodbc.ini` configuration
file. This configures the driver so it works with your ODBC driver manager.

### Configuring mapr.drillodbc.ini

To configure the MapR Drill ODBC Driver in the `mapr.drillodbc.ini` configuration file, complete the following steps:

  1. Open the `mapr.drillodbc.ini` configuration file in a text editor.
  2. Edit the DriverManagerEncoding setting. The value is typically UTF-16 or UTF-32, but depends on the driver manger used. iODBC uses UTF-32 and unixODBC uses UTF-16. Review your ODBC Driver Manager documentation for the correct setting.
  3. Edit the `ODBCInstLib` setting. The value is the name of the `ODBCInst` shared library for the ODBC driver manager that you use. The configuration file defaults to the shared library for`iODBC`. In Linux, the shared library name for iODBC is `libiodbcinst.so`. In Mac OS X, the shared library name for `iODBC` is `libiodbcinst.dylib`.
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

