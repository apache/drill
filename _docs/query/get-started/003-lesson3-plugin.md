---
title: "Lesson 3: Create a Storage Plugin"
parent: "Getting Started Tutorial"
---
The Drill default storage plugins support common file formats. If you need
support for some other file format, create a custom storage plugin. You can also create a storage plugin to simplify querying file having long path names. A workspace name replaces the long path name.

This lesson covers how to create and use a storage plugin to simplify queries. First,
you create the storage plugin in the Drill Web UI. Next, you connect to the
file through the plugin to query a file, and then a directory, and finally you
query multiple files in a directory.

## Create a Storage Plugin

You can create a storage plugin using the Apache Drill Web UI.

  1. Create an `ngram` directory on your file system.
  2. Copy `googlebooks-eng-all-5gram-20120701-zo.tsv` to the `ngram` directory.
  3. Open the Drill Web UI by navigating to <http://localhost:8047/storage>.   
     To open the Drill Web UI, SQLLine must still be running.
  4. In New Storage Plugin, type `myplugin`.  
     ![new plugin]({{ site.baseurl }}/docs/img/ngram_plugin.png)    
  5. Click **Create**.  
     The Configuration screen appears.
  6. Replace null with the following storage plugin definition, except on the location line, use the path to your `ngram` directory instead of the drilluser's path and give your workspace an arbitrary name, for example, ngram:
  
        {
          "type": "file",
          "enabled": true,
          "connection": "file:///",
          "workspaces": {
            "ngram": {
              "location": "/Users/drilluser/ngram",
              "writable": false,
              "defaultInputFormat": null
           }
         },
         "formats": {
           "tsv": {
             "type": "text",
             "extensions": [
               "tsv"
             ],
             "delimiter": "\t"
            }
          }
        }

  7. Click **Create**.  
     The success message appears briefly.
  8. Click **Back**.  
     The new plugin appears in Enabled Storage Plugins.  
     ![new plugin]({{ site.baseurl }}/docs/img/ngram_plugin.png) 
  9. Go back to the SQLLine prompt in the CLI, and list the storage plugins. Press RETURN in the CLI to get a prompt if necessary.

Your custom plugin appears in the list and has two workspaces: the `ngram`
workspace that you defined and a default workspace.

## Connect to and Query a File

When querying the same data source repeatedly, avoiding long path names is
important. This exercise demonstrates how to simplify the query. Instead of
using the full path to the Ngram file, you use dot notation in the FROM
clause.

``<workspace name>.`<location>```

This syntax assumes you connected to a storage plugin that defines the
location of the data. To query the data source while you are _not_ connected to
that storage plugin, include the plugin name:

``<plugin name>.<workspace name>.`<location>```

This exercise shows how to query Ngram data when you are, and when you are
not, connected to `myplugin`.

  1. Connect to the ngram file through the custom storage plugin.  
     `USE myplugin;`
  2. Get data about "Zoological Journal of the Linnean" that appears more than 250 times a year in the books that Google scans. In the FROM clause, instead of using the full path to the file as you did in the last exercise, connect to the data using the storage plugin workspace name ngram.
  
         SELECT COLUMNS[0], 
                COLUMNS[1], 
                COLUMNS[2] 
         FROM ngram.`/googlebooks-eng-all-5gram-20120701-zo.tsv` 
         WHERE ((columns[0] = 'Zoological Journal of the Linnean') 
          AND (columns[2] > 250)) 
         LIMIT 10;

     The output consists of 5 rows of data.  
  3. Switch to the `dfs` storage plugin.
  
         0: jdbc:drill:zk=local> USE dfs;
         +------------+------------+
         |     ok     |  summary   |
         +------------+------------+
         | true       | Default schema changed to 'dfs' |
         +------------+------------+
         1 row selected (0.019 seconds)
  4. Query the TSV file again. Because you switched to `dfs`, Drill does not know the location of the file. To provide the information to Drill, preface the file name with the storage plugin and workspace names in the FROM clause.  
  
         SELECT COLUMNS[0], 
                COLUMNS[1], 
                COLUMNS[2] 
         FROM myplugin.ngram.`/googlebooks-eng-all-5gram-20120701-zo.tsv` 
         WHERE ((columns[0] = 'Zoological Journal of the Linnean') 
           AND (columns[2] > 250)) 
         LIMIT 10;

## Query Multiple Files in a Directory

In this exercise, first you create a subdirectory in the `ngram` directory.
Next, you download, unzip, and add an extension to a second Ngram file. You
move both Ngram TSV files to the subdirectory. Finally, using the custom
plugin workspace, you query both files. In the FROM clause, simply reference
the subdirectory.

  1. Download a second file of compressed Google Ngram data from this location: 
  
     http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20120701-ze.gz
  2. Unzip `googlebooks-eng-all-2gram-20120701-ze.gz` and move `googlebooks-eng-all-2gram-20120701-ze` to the `ngram/myfiles` subdirectory. 
  3. Change the name of `googlebooks-eng-all-2gram-20120701-ze` to add a `.tsv` extension.    
  4. Move the 5gram file you worked with earlier `googlebooks-eng-all-5gram-20120701-zo.tsv` from the `ngram` directory to the `ngram/myfiles` subdirectory.
  5. At the SQLLine prompt, use the `myplugin.ngrams` workspace. 
   
          USE myplugin.ngram;
  6. Query the myfiles directory for the "Zoological Journal of the Linnean" or "zero temperatures" in books published in 1998.
  
          SELECT * 
          FROM myfiles 
          WHERE (((COLUMNS[0] = 'Zoological Journal of the Linnean')
            OR (COLUMNS[0] = 'zero temperatures')) 
            AND (COLUMNS[1] = '1998'));
The output lists ngrams from both files.

          +------------+
          |  columns   |
          +------------+
          | ["Zoological Journal of the Linnean","1998","157","53"] |
          | ["zero temperatures","1998","628","487"] |
          +------------+
          2 rows selected (5.316 seconds)
   
