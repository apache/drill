---
title: "Configuring User Impersonation"
date: 2018-04-05 01:05:13 UTC
parent: "Securing Drill"
---
Impersonation allows a service to act on behalf of a client while performing the action requested by the client. By default, user impersonation is disabled in Drill. You can configure user impersonation in the <DRILLINSTALL_HOME>/conf/drill-override.conf file.
 
When you enable impersonation, Drill executes client requests as the user logged in to the client. Drill passes the user credentials to the file system, and the file system checks to see if the user has permission to access the data. When you enable authentication, Drill uses the pluggable authentication module (PAM) to authenticate a user’s identity before the user can access the Drillbit process. See User Authentication.
 
If impersonation is not configured, Drill executes all of the client requests against the file system as the user that started the Drillbit service on the node. This is typically a privileged user. The file system verifies that the system user has permission to access the data.


## Example
When impersonation is disabled and user Bob issues a query through the SQLLine client, SQLLine passes the query to the connecting Drillbit. The Drillbit executes the query as the system user that started the Drill process on the node. For the purpose of this example, we will assume that the system user has full access to the file system. Drill executes the query and returns the results back to the client.
![](http://i.imgur.com/4XxQK2I.png)

When impersonation is enabled and user Bob issues a query through the SQLLine client, the Drillbit uses Bob's credentials to access data in the file system. The file system checks to see if Bob has permission to access the data. If so, Drill returns the query results to the client. If Bob does not have permission, Drill returns an error.
![](http://i.imgur.com/oigWqVg.png)

## Impersonation Support
The following table lists the clients, storage plugins, and types of queries that you can use with impersonation in Drill:  

| Type            | Supported                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | Not Supported |
|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| Clients         | SQLLine, ODBC, JDBC, REST API, Drill Web   Console                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |               |
| Storage Plugins | File System, Hive, MapR-DB                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | HBase         |
| Queries         | When you enable impersonation, the setting   applies to queries on data and metadata. For example, if you issue the SHOW   SCHEMAS command, Drill impersonates the user logged into the client to access   the requested metadata. If you issue a SELECT query on a workspace, Drill   impersonates the user logged in to the client to access the requested data.   Drill applies impersonation to queries issued using the following commands:   SHOW SCHEMAS, SHOW DATABASES, SHOW TABLES, CTAS, SELECT, CREATE VIEW, DROP   VIEW, SHOW FILES. To successfully run the CTAS and CREATE VIEW commands, a   user must have write permissions on the directory where the table or view   will exist. Running these commands creates artifacts on the file system. |               |

## Impersonation and Views
You can use views with impersonation to provide granular access to data and protect sensitive information. When you create a view, Drill stores the view definition in a file and suffixes the file with .drill.view. For example, if you create a view named myview, Drill creates a view file named myview.drill.view and saves it in the current workspace or the workspace specified, such as dfs.views.myview. See [CREATE VIEW]({{site.baseurl}}/docs/create-view) Command.

You can create a view and grant read permissions on the view to give other users access to the data that the view references. When a user queries the view, Drill impersonates the view owner to access the underlying data. If the user tries to access the data directory, Drill returns a permission denied error. A user with read access to a view can create new views from the originating view to further restrict access on data.

### View Permissions
A user must have write permission on a directory or workspace to create a view, as well as read access on the table(s) and/or view(s) that the view references. When a user creates a view, permission on the view is set to owner by default. Users can query an existing view or create new views from the view if they have read permissions on the view file and the directory or workspace where the view file is stored. 

When users query a view, Drill accesses the underlying data as the user that created the view. If a user does not have permission to access a view, the query fails and Drill returns an error. Only the view owner or a superuser can modify view permissions to change them from owner to group or world. 
 
The view owner or a superuser can modify permissions on the view file directly or they can set view permissions at the system or session level prior to creating any views. Any user that alters view permissions must have write access on the directory or workspace in which they are working. See Modifying Permissions on a View File and Modifying SYSTEM|SESSION Level View Permissions. 

### Modifying Permissions on a View File

Only a view owner or a super user can modify permissions on a view file to change them from owner to group or world readable. Before you grant permission to users to access a view, verify that they have access to the directory or workspace in which the view file is stored.

Use the `chmod` and `chown` commands with the appropriate octal code to change permissions on a view file:

    hadoop fs –chmod <octal code> <file_name>
    hadoop fs –chown <user>:<group> <file_name>
Example: `hadoop fs –chmod 750 employees.drill.view`

### Modifying SYSTEM|SESSION Level View Permissions

Use the `ALTER SESSION|SYSTEM` command with the `new_view_default_permissions` parameter and the appropriate octal code to set view permissions at the system or session level prior to creating a view.
 
    ALTER SESSION SET `new_view_default_permissions` = '<octal_code>';
    ALTER SYSTEM SET `new_view_default_permissions` = '<octal_code>';
 
Example: ``ALTER SESSION SET `new_view_default_permissions` = '777';``
 
After you set this parameter, Drill applies the same permissions on each view created during the session or across all sessions if set at the system level.

## Chained Impersonation
You can configure Drill to allow chained impersonation on views when you enable impersonation in the `drill-override.conf` file. Chained impersonation controls the number of identity transitions that Drill can make when a user queries a view. Each identity transition is equal to one hop.
 
An administrator can set the maximum number of hops on views to limit the number of times that Drill can impersonate a different user when other users query a view. The default maximum number of hops is set at 3. When the maximum number of hops is set to 0, Drill does not allow impersonation chaining, and a user can only read data for which they have direct permission to access. An administrator may set the chain length to 0 to protect highly sensitive data. 
 
The following example depicts a scenario where the maximum hop number is set to 3, and Drill must impersonate three users to access data when Chad queries a view that Jane created:

![]({{ site.baseurl }}/docs/img/drill_imp_simple.PNG)

In the previous example, Joe created V2 from the view that user Frank created. In the following example, Joe created V3 by joining a view that Frank created with a view that Bob created. 
 
![]({{ site.baseurl }}/docs/img/user_hops_joined_view.PNG)  

Although V3 was created by joining two different views, the number of hops remains at 3 because Drill does not read the views at the same time. Drill reads V2 first and then reads V1.  

In the next example, Bob queries V4 which was created by Frank. Frank's view was created from several underlying views. Charlie created V2 by joining Jane's V1 with Kris's V1.2. Kris's V1.2 was created from Amy's V1.1, increasing the complexity of the chaining. Assuming that the hop limit is set at 4, this scenario exceeds the limit.  

![]({{ site.baseurl }}/docs/img/user_hops_four.PNG)  

When Bob queries Franks’s view, Drill returns an error stating that the query cannot complete because the number of hops required to access the data exceeds the maximum hop setting of 4.

If users encounter this error, the administrator can increase the maximum hop setting to accommodate users running queries on views.

### Configuring Impersonation and Chaining
Chaining is a system-wide setting that applies to all views. Currently, Drill does not provide an option to  allow different chain lengths for different views.

Complete the following steps on each Drillbit node to enable user impersonation, and set the maximum number of chained user hops that Drill allows:

1. Navigate to `<drill_installation_directory>/conf/` and edit `drill-override.conf`.
2. Under `drill.exec`, add the following:

          drill.exec.impersonation: {
                enabled: true,
                 max_chained_user_hops: 3
          }

       Alternatively, you can nest impersonation within the `drill.exec` block, as shown in the following example: 

              drill.exec: {
                   cluster-id: "cluster_name",
                   zk.connect: "<hostname>:<port>,<hostname>:<port>,<hostname>:<port>",
                   sys.store.provider.zk.blobroot: "hdfs://",
                   impersonation: {
                     enabled: true,
                     max_chained_user_hops: 3
                   }
                 }

 
3. Verify that enabled is set to `‘true’`.
4. Set the maximum number of chained user hops that you want Drill to allow.
5. Restart the Drillbit process on each Drill node.

         <DRILLINSTALL_HOME>/bin/drillbit.sh restart

## Impersonation and Chaining Example
Frank is a senior HR manager at a company. Frank has access to all of the employee data because he is a member of the hr group. Frank created a table named “employees” in his home directory to store the employee data he uses. Only Frank has access to this table.
 
drwx------      frank:hr     /user/frank/employees
 
Each record in the employees table consists of the following information:
emp_id, emp_name, emp_ssn, emp_salary, emp_addr, emp_phone, emp_mgr
 
Frank needs to share a subset of this information with Joe who is an HR manager reporting to Frank. To share the employee data, Frank creates a view called emp_mgr_view that accesses a subset of the data. The emp_mgr_view filters out sensitive employee information, such as the employee social security numbers, and only shows data for the employees that report directly to Joe. Frank and Joe both belong to the mgr group. Managers have read permission on Frank’s directory.
 
rwxr-----     frank:mgr   /user/frank/emp_mgr_view.drill.view
 
The emp_mgr_view.drill.view file contains the following view definition:

(view definition: SELECT emp_id, emp_name, emp_salary, emp_addr, emp_phone FROM \`/user/frank/employee\` WHERE emp_mgr = 'Joe')
 
When Joe issues SELECT * FROM emp_mgr_view, Drill impersonates Frank when accessing the employee data, and the query returns the data that Joe has permission to see based on the view definition. The query results do not include any sensitive data because the view protects that information. If Joe tries to query the employees table directly, Drill returns an error or null values.
 
Because Joe has read permissions on the emp_mgr_view, he can create new views from it to give other users access to the employee data even though he does not own the employees table and cannot access the employees table directly.
 
Joe needs to share employee contact data with his direct reports, so he creates a special view called emp_team_view to share the employee contact information with his team. Joe creates the view and writes it to his home directory. Joe and his reports belong to a group named joeteam. The joeteam group has read permissions on Joe’s home directory so they can query the view and create new views from it.
 
rwxr-----     joe:joeteam   /user/joe/emp_team_view.drill.view
 
The emp_team_view.drill.view file contains the following view definition:
 
(view definition: SELECT emp_id, emp_name, emp_phone FROM \`/user/frank/emp_mgr_view.drill\`);
 
When anyone on Joe’s team issues SELECT * FROM emp_team_view, Drill impersonates Joe to access the emp_team_view and then impersonates Frank to access the emp_mgr_view and the employee data. Drill returns the data that Joe’s team has can see based on the view definition. If anyone on Joe’s team tries to query the emp_mgr_view or employees table directly, Drill returns an error or null values.
 
Because Joe’s team has read permissions on the emp_team_view, they can create new views from it and write the views to any directory for which they have write access. Creating views can continue until Drill reaches the maximum number of impersonation hops.
