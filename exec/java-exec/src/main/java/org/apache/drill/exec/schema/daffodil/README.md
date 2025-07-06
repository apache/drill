# Daffodil Schema Providers
The classes in this package implement a persistent store for Apache Daffodil schemata.  These files create a store which Drill will use to track schemata.  When the files are installed via SQL commands, the store will copy them from the staging directory and propagate them to every node in the Drill cluster.

