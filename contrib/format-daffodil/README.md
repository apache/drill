# Daffodil 'Format' Reader
This plugin enables Drill to read DFDL-described data from files by way of the Apache Daffodil DFDL implementation.

## Validation

Data read by Daffodil is always validated using Daffodil's Limited Validation mode.

TBD: do we need an option to control escalating validation errors to fatal? Currently this is not provided.

## Limitations:  TBD

At the moment, the DFDL schema is found on the local file system, which won't support Drill's distributed architecture.

There are restrictions on the DFDL schemas that this can handle.

In particular, all element children must have distinct element names, including across choice branches.
(This rules out a number of large DFDL schemas.)

TBD: Auto renaming as part of the Daffodil-to-Drill metadata mapping?

The data is parsed fully from its native form into a Drill data structure held in memory.
No attempt is made to avoid access to parts of the DFDL-described data that are not needed to answer the query.

If the data is not well-formed, an error occurs and the query fails.

If the data is invalid, and validity checking by Daffodil is enabled, then an error occurs and the query fails.

