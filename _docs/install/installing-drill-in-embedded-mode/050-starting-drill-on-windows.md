---
title: "Starting Drill on Windows"
date: 2018-11-02
parent: "Installing Drill in Embedded Mode"
---
Start the Drill shell using the **sqlline command**. Complete the following steps to launch the Drill shell:

1. Open Command Prompt.  
2. Navigate to the Drill installation folder. 
3. Go to the `bin` directory. For example:  
   ``cd bin``
4. Type the following command on the command line:
   ``sqlline.bat -u "jdbc:drill:zk=local"``
   ![drill install dir]({{ site.baseurl }}/docs/img/sqlline1.png)

The [Drill prompt]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/#about-the-drill-prompt) appears. You can [submit queries]({{ site.baseurl }}/docs/drill-in-10-minutes/#query-sample-data) to Drill.

You can use the schema option in the **sqlline** command to specify a storage plugin. Specifying the storage plugin when you start up eliminates the need to specify the storage plugin in the query. For example, this command specifies the `dfs` storage plugin:

`C:\bin\sqlline sqlline.bat –u "jdbc:drill:zk=local;schema=dfs"`

If you start Drill on one network, and then want to use Drill on another network, such as your home network, restart Drill.

## Exiting the Drill Shell

To exit the Drill shell, issue the following command:

`!quit`

