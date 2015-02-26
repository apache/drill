---
title: "Getting Started Tutorial"
parent: "Query Data"
---

## Goal

This tutorial covers how to query a file and a directory on your local file
system. Files and directories are like standard SQL tables to Drill. If you
install Drill in [embedded
mode](/drill/docs/installing-drill-in-embedded-mode), the
installer registers and configures your file system as the `dfs` instance.
You can query these types of files using the default `dfs` storage plugin:

  * Plain text files, such as comma-separated values (CSV) or tab-separated values (TSV) files
  * JSON files
  * Parquet files

In this tutorial, you query plain text files using the `dfs` storage plugin. You also create a custom storage
plugin to simplify querying plain text files.

## Prerequisites

This tutorial assumes that you installed Drill in [embedded
mode](/drill/docs/installing-drill-in-embedded-mode). The first few lessons of the tutorial
use a Google file of Ngram data that you download from the internet. The
compressed Google Ngram files are 8 and 58MB. To expand the compressed files,
you need an additional 448MB of free disk space for this exercise.

To get started, use the SQLLine command to start the Drill command line
interface (CLI) on Linux, Mac OS X, or Windows.

### Start Drill (Linux or Mac OS X)

To [start Drill](/drill/docs/starting-stopping-drill) on Linux
or Mac OS X, use the SQLLine command.

  1. Open a terminal.
  2. Navigate to the Drill installation directory.
  
     Example: `$ cd ~/apache-drill-<version>`
  3. Issue the following command:
  
        $ bin/sqlline -u jdbc:drill:zk=local
     The Drill prompt appears: `0: jdbc:drill:zk=local`

### Start Drill (Windows)

To [start Drill](/drill/docs/starting-stopping-drill) on
Windows, use the SQLLine command.

  1. Open the `apache-drill-<version>` folder.
  2. Open the `bin` folder, and double-click on the `sqlline.bat` file. The Windows command prompt opens.
  3. At the `sqlline>` prompt, issue the following command, and then press **Enter**:
  
        !connect jdbc:drill:zk=local  
     The following prompt appears: `0: jdbc:drill:zk=local`

### Stop Drill

To stop Drill, issue the following command at the Drill prompt.

        0: jdbc:drill:zk=local> !quit

In some cases, such as stopping while a query is in progress, this command does not stop Drill. You need to kill the Drill process. For example, on Mac OS X and Linux, follow
these steps:

  1. Issue a CTRL Z to stop the query, then start Drill again. If the startup message indicates success, skip the rest of the steps. If not, proceed to step 2.
  2. Search for the Drill process ID.
  
        $ ps auwx | grep drill
  3. Kill the process using the process number in the grep output. For example:

        $ sudo kill -9 2674

