---
title: "Configuring cgroups to Control CPU Usage"
date: 2019-01-25
parent: "Configure Drill"
---   

Starting in Drill 1.13, you can configure a Linux cgroup to enforce CPU limits on the Drillbit service running on a node. Linux cgroups (control groups) enable you to limit system resources to defined user groups or processes. You can use the cgconfig service to configure a Drill cgroup to control CPU usage and then set the CPU limits for the Drill cgroup on each Drill node in the `/etc/cgconfig.conf` file.  

In Drill 1.13, you had to update the `cgroup.procs` file with the Drill process ID (PID) each time a Drillbit restarted in order to enforce the CPU limit for the Drillbit service.  As of Drill 1.14, Drill can directly manage the CPU resources through the Drill start-up script, `drill-env.sh`. You no longer have to manually add the PID to the `cgroup.procs` file each time a Drillbit restarts. This step occurs automatically upon restart. The start-up script checks for the specified cgroup, such as drillcpu, and then applies the cgroup to the launched Drillbit JVM. The Drillbit CPU resource usage is then managed under the cgroup, drillcpu. 

**Note:** cgroups V2 is recommended.  

The `drill-env.sh` script contains variables that you must enable for Drill to directly manage the CPU resources. Uncomment the following variables in drill-env.sh to enable the feature:  

| Variable                                                  | Description                                                                                                                                                                                                                       |   |
|-----------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|
| export DRILLBIT_CGROUP=${DRILLBIT_CGROUP:-"drillcpu"}     | Sets the cgroup to which the Drillbit belongs when running as a daemon using drillbit.sh start. Drill uses the cgroup for CPU enforcement only.                                                                                   |   |
| export SYS_CGROUP_DIR=${SYS_CGROUP_DIR:-"/sys/fs/cgroup"} | Drill assumes the default cgroup mount location set by systemd (the system and service manager for Linux operating systems). If your cgroup mount location is in a different location, change the setting to match your location. |   |
| export DRILL_PID_DIR=${DRILL_PID_DIR:-$DRILL_HOME}        | The location of the Drillbit PID file when Drill is running as a daemon using drillbit.sh start. By default, this location is set to $DRILL_HOME.                                                                                 |   |  


##CPU Limits  

You can set the CPU limit as a soft or hard limit, or both. You set the limits with parameters in the `/etc/cgconfig.conf` file. The hard limit takes precedence over the soft limit. When Drill hits the hard limit, in-progress queries may not complete. The following sections describe the parameters for soft and hard limits.  

**Soft Limit Parameter**  
You set the soft limit with the `cpu.shares` parameter. When you set a soft limit, Drill can exceed the CPU allocated if extra CPU is available for use on the system. Drill can continue to use CPU until there is contention with other processes over the CPU or Drill hits the hard limit.  

**Hard Limit Parameters**  
You set the hard limit with the `cpu.cfs_period_us` and `cpu.cfs_quota_us` parameters. The `cpu.cfs_period_us` and `cpu.cfs_quota_us` parameters set a hard limit on the amount of CPU time that the Drill process can use.  

- **`cpu.cfs_period_us`**   
The `cpu.cfs_quota_us` parameter specifies a segment of time (in microseconds represented by `us` for µs) for how often the access to CPU resources should be reallocated. For example, if tasks in a cgroup can access a single CPU for 0.2 seconds out of every 1 second, set cpu.cfs_quota_us to 200000 and cpu.cfs_period_us to 1000000. The upper limit of the `cpu.cfs_quota_us` parameter is 1 second and the lower limit is 1000 microseconds.    


- **`cpu.cfs_quota_us`**  
The `cpu.cfs_quota_us` parameter specifies the total amount of runtime (in microseconds represented by `us` for µs) for which all tasks in the Drill cgroup can run during one period (as defined by cpu.cfs_period_us). As soon as tasks in the Drill cgroup use the time specified by the quota, they are throttled for the remainder of the time specified by the period and not allowed to run until the next period. For example, if tasks in the Drill cgroup can access a single CPU for 0.2 seconds out of every 1 second, set `cpu.cfs_quota_us` to 200000 and `cpu.cfs_period_us` to 1000000. A value of -1 indicates that the group does not have any restrictions on CPU.  

##Before You Begin
Each Drill node must have the libcgroup package installed to configure CPU limits for a Drill cgroup. The libcgroup package installs the cgconfig service required to configure and manage the Drill cgroup.

You can install the libcgroup package using the `yum install` command, as shown:  

       yum install libcgroup  


##Configuring CPU Limits
Complete the following steps to set a hard and/or soft limit on Drill CPU usage for the Drill process running on the node:  

1-Start the cgconfig service:  

        service cgconfig start

2-Add a cgroup for Drill in the /etc/cgconfig.conf file:    

              group drillcpu {
                     cpu {
                            cpu.shares = 320;
                            cpu.cfs_quota_us = 400000;
                            cpu.cfs_period_us = 100000;
                            }
                     }  
**Note:** The cgroup name is specific to the Drill cgroup and does not correlate with any other configuration.   
  
In the configuration example, the `cpu.shares` parameter sets the soft limit. The other two parameters, `cpu.cfs_quota_us` and `cpu.cfs_period_us`, set the hard limit. If you prefer to set only one type of limit, remove the parameters that do not apply.  

To set a soft limit, allocate a specific number of CPU shares to the Drill cgroup in the configuration. Calculate the CPU shares as:  

       1024 (CPU allocated to Drill/Total available CPU)

In the example, CPU shares was calculated as:  

       1024 (10/32) = 320


To set a hard limit, add limits to the `cpu.cfs_quota_us` and `cpu.cfs_period_us` parameters. In the configuration example, the Drill process can fully utilize 4 CPU.  

**Note:** The hard limit parameter settings persist after each cgroup service restart. Alternatively, you can set the parameters at the session level using the following commands:  

       echo 400000 > /cgroup/cpu/drillcpu/cpu.cfs_quota_us
       echo 100000 > /cgroup/cpu/drillcpu/cpu.cfs_period_us

3-(Optional) If you want the cgconfig service to automatically restart upon system reboots, run the following command:  

       chkconfig cgconfig on  

**Note:** Only complete step 4 if you have Drill 1.13 running on the node, or you have Drill 1.14 running on the node and you have not enabled Drill to directly manage the CPU resources through the start-up script, `drill-env.sh`.   
  
4-Run the following command to add the Drill process ID (PID) to the /cgroup/cpu/drillcpu/cgroup.procs file, as shown:  

       echo 25809 > /cgroup/cpu/drillcpu/cgroup.procs

**Note:** You must perform this step each time a Drillbit restarts.  

For additional information, refer to the following documentation:  
- [https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html)  
- [resource_management_guide/sect-cpu-example_usage](resource_management_guide/sect-cpu-example_usage)  
- [https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html)
- [resource_management_guide/sec-cpu_and_memory-use_case](resource_management_guide/sec-cpu_and_memory-use_case)









