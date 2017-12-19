---
title: "Using jpam as the PAM Authenticator"
date: 2017-05-17 01:38:50 UTC
parent: "Securing Drill"
---


To use jpam as the PAM authenticator with Drill, complete the following steps:

{% include startnote.html %}Do not point to an existing directory where other Hadoop components are installed. Other file system libraries can conflict with the Drill libraries and cause system errors.{% include endnote.html %}


1. Download the `tar.gz` file for the Linux platform:

	`http://sourceforge.net/projects/jpam/files/jpam/jpam-1.1`/

1. Untar the file, and copy the `libjpam.so` file into a directory that does not contain other Hadoop components. For example, `/opt/pam/`


1. Add the following line to `<DRILL_HOME>/conf/drill-env.sh`, including the directory where the `libjpam.so` file is located: 

        export DRILLBIT_JAVA_OPTS="-Djava.library.path=<directory>"

	**Example**

        export DRILLBIT_JAVA_OPTS="-Djava.library.path=/opt/pam/" 

1. Add the following configuration to the `drill.exec` block in `<DRILL_HOME>/conf/drill-override.conf`: 
		
              drill.exec: {
                cluster-id: "drillbits1",
                zk.connect: "qa102-81.qa.lab:5181,qa102-82.qa.lab:5181,qa102-83.qa.lab:5181",
                impersonation: {
                  enabled: true,
                  max_chained_user_hops: 3
                },
                security: {          
                        auth.mechanisms : ["PLAIN"],
                         },
                security.user.auth: {
                        enabled: true,
                        packages += "org.apache.drill.exec.rpc.user.security",
                        impl: "pam",
                        pam_profiles: [ "sudo", "login" ]
                 }
               }

1. (Optional) To add or remove different PAM profiles, add or delete the profile names in the “pam_profiles” array shown above. 

1. Restart the Drillbit process on each Drill node, as shown: 

    `<DRILLINSTALL_HOME>/bin/drillbit.sh restart`






