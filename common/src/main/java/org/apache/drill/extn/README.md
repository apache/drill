# Drill Service Provider Interface (SPI)

The Drill SPI allows extensions (AKA plugins, providers) to extend Drill.
This package provides the core extension mechanism and is a minor refinement
of the standard Java
[`ServiceLoader`](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html)
mechanism.

## Layout

A _service provider_ provides one or more _services_. The provider implements a
provider interface, defined in the Drill SPI package. That interface provides factory
methods to implement services.

The provider developer creates a jar file with the properties described below. The jar
file is then placed in Drill's extension directory which is either:

* `$DRILL_HOME/ext` if using the "classic" approach, or
* `$DRILL_SITE/ext` if usign a separate "site" directory.

### Simple Jars

The simplest service is one with no dependencies other than the JDK and Drill.
These reside in a single jar file placed in the `ext` directory. A UDF is a typical
example:

```
$DRILL_SITE
|- ext
   |- coolUdfs.jar
   |- fancyUdfs.jar
```

The name of the jar can be anything, though Drill recommends Maven-style naming:

```
collUdfs-1.3.0.jar
```

### Extension Packages

More complex extensions (such as storage pugins) have dependencies in the form
of additional jars. In this case, the provider developer provides a directory
which contains any number of jars and other files:

```
$DRILL_SITE
|- ext
   |- coolUdfs-1.3.0.jar
   |- fancyUdfs-2.1.0.jar
   |- megaDbPlugin-0.7.0
      |- metadbplugin-0.7.0.jar
      |- dependency1.jar
      |- dependency2.jar
      |- README.md
```

The directory should also have Maven-format name, though this is not required.
The directory contains the provider jar which *must* have the same name as the
extension directory (this is how Drill knows which jar is the main one.)

## Provider Configuration File

Each extension jar must contain a _provider-configuration file_ as defined
by the Java `ServiceLoader`. The file has the name of the Drill-defined
provider interface, and contains a single line which is the fully-qualified
name of the provider class which implements the interface.

```
coolUdfs-1.3.0.jar
|- META-INF
|   |- services
|   |  |- org.apache.drill.spi
|- com/foo/drill/udfs
   |- SpiImpl.class
   |- class files
```

The contents of `org.apache.drill.spi` would be:

```
com.foo.drill.uds.SpiImpl
```

### Drill Clusters

The provider must be installed on each node in the Drill cluster. To avoid
race conditions, Drill ignores any providers added after Drill starts. To
install a new provider, first copy it to all Drillbits, then restart the
cluster so that all Drillbits see the same set of providers.

## Bootstrap Process

Drill performs the following operations on bootstrap:

* Determine the extension directory name as explained above.
* Scan through each jar in the directory, and each subdirectory.
* If a subdirectory, look for the main jar with the same name as the directory.
* Look for the provider configuration file.
* Load the class named in the config file.
* Create an instance of the provider class.

If any of the above steps fail, a message is logged to the log file explaining
the issue. Drill also logs those providers which pass the gauntlet and are
successfully loaded.

## Drill Extension Class Loader

The eventual goal is to provide class loader isolation for extensions.

Extensions normally run in a class loader separate from that for Drill. The
extension has visibility only to the SPI interfaces, but not to Drill's implemetation.
As a result, the extension is isolated from changes to Drill's internals across
Drill versions. Also, the extension's dependencies to not conflict with Drill's
own dependencies. This is the "servlet" model from JEE as explained in the
[Tomcat documentation](https://tomcat.apache.org/tomcat-8.0-doc/class-loader-howto.html).

An extension may also elect to be loaded in the same class loader as Drill.
Use this for extensions built as part of Drill, or extensions that need to use
Drill internals which have not yet been abstracted out by the SPI interfaces.

*TODO*: For now, each extension has its own class loader so that
one extension does not conflict with another, but every plugin has visibility to
Drill's internal classes. Per normal Java behavior, Drill's class loader is searched
first to find a class, then the extension's class loader.
