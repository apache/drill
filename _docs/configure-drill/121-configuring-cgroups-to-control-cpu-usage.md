---
title: "Configuring cgroups to Control CPU Usage"
date: 2019-02-06
parent: "Configure Drill"
---   

Starting in Drill 1.13, you can configure a Linux cgroup (control group) to enforce CPU limits on the Drillbit service running on a node. Linux cgroups enable you to limit system resources to defined user groups or processes. You can use the cgconfig service to configure a Drill cgroup to control CPU usage and then set the CPU limits for the Drill cgroup on each Drill node in the `/etc/cgconfig.conf` file.  

**Note:** cgroups V2 is recommended.  

##Before You Begin  

Each Drill node must have the `libcgroup` package installed to configure CPU limits for a Drill cgroup. The `libcgroup` package installs the cgconfig service required to configure and manage the Drill cgroup.

Install the `libcgroup` package using the `yum install` command, as shown:  

       yum install libcgroup  

##Enable Drill to Directly Manage CPU Resources   

Starting in Drill 1.14, Drill can directly manage CPU resources through the start-up script, `drill-env.sh`, which means that you no longer have to manually add the PID (Drill process ID) to the `cgroup.procs` file each time a Drillbit restarts. This step occurs automatically upon restart. The start-up script checks for the specified cgroup, such as drillcpu, and then applies the cgroup to the launched Drillbit JVM. The Drillbit CPU resource usage is then managed under the cgroup, drillcpu.  

For Drill to directly manage CPU resources, you must enable (uncomment) the following variables in the `drill-env.sh` script:  

| Variable                                                    | Description                                                                                                                                                                                                                             |
|-------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| export   DRILLBIT_CGROUP=${DRILLBIT_CGROUP:-"drillcpu"}     | Sets   the cgroup to which the Drillbit belongs when running as a daemon using   drillbit.sh start. Drill uses the cgroup for CPU enforcement only.                                                                                     |
| export   SYS_CGROUP_DIR=${SYS_CGROUP_DIR:-"/sys/fs/cgroup"} | Drill   assumes the default cgroup mount location set by systemd (the system and   service manager for Linux operating systems). If your cgroup mount location   is in a different location, change the setting to match your location. |
| export   DRILL_PID_DIR=${DRILL_PID_DIR:-$DRILL_HOME}        | The   location of the Drillbit PID file when Drill is running as a daemon using   drillbit.sh start. By default, this location is set to $DRILL_HOME.                                                                                   |  

**********  
**NOTE**  
If you have Drill 1.13 running on the node, or you have Drill 1.14 running on the node and you do not want to enable Drill to directly manage the CPU resources through `drill-env.sh`, you must manually update the `/cgroup/cpu/drillcpu/cgroup.procs` file with the PID (Drill process ID), as shown, each time a Drillbit restarts to enforce the CPU limit for the Drillbit service:  

	echo 25809 > /cgroup/cpu/drillcpu/cgroup.procs      
********  

##Set the CPU Limit for the Drillbit Service 

You can set the CPU limit as a soft or hard limit, or both. You set the limits with parameters in the `/etc/cgconfig.conf` file. The hard limit takes precedence over the soft limit. When Drill hits the hard limit, in-progress queries may not complete. Review the following sections that describe the soft and hard limit parameters and then configure CPU limits.  

**Soft Limit Parameter**  
You set the soft limit with the `cpu.shares` parameter. This parameter takes an integer value, which specifies a relative share of CPU time available to the tasks in a cgroup. For example, if there are two tasks and cpu.shares is set to 100, each task receives half of the CPU time. The value must be 2 or greater.When you set a soft limit, Drill can exceed the CPU allocated if extra CPU is available for use on the system. Drill can continue to use CPU until there is contention with other processes over the CPU or Drill hits the hard limit.   

**Hard Limit Parameters**  
You set the hard limit on the amount of CPU time that the Drill process can use through the `cpu.cfs_period_us` and `cpu.cfs_quota_us` parameters.  

- **`cpu.cfs_period_us`**   
Specifies a period in microseconds (represented by `us` for µs) to indicate how often a cgroup's access to CPU resources should be reallocated. For example, if you want tasks in a cgroup to have access to a single CPU for 0.2 seconds in a 1 second window, set `cpu.cfs_quota_us` to 200000 and `cpu.cfs_period_us` to 1000000. The upper limit of the `cpu.cfs_quota_us` parameter is 1 second and the lower limit is 1000 microseconds.    


- **`cpu.cfs_quota_us`**  
Specifies the total amount of runtime in microseconds (represented by `us` for µs), for which all tasks in the Drill cgroup can run during one period (as defined by `cpu.cfs_period_us`). When tasks in the Drill cgroup use up all the time specified by the quota, the tasks are throttled for the remainder of the time specified by the period and they cannot run until the next period. For example, if tasks in the Drill cgroup can access a single CPU for 0.2 seconds out of every 1 second, set `cpu.cfs_quota_us` to 200000 and `cpu.cfs_period_us` to 1000000. Setting the `cpu.cfs_quota_us` value to -1 indicates that the group does not have any restrictions on CPU. This is the default value for every cgroup, except for the root cgroup.   


###Configuring CPU Limits  
Complete the following steps to set a hard and/or soft CPU limit for the Drill process running on the node:  

1-Start the cgconfig service:  

        service cgconfig start

2-Add a cgroup for Drill in the `/etc/cgconfig.conf` file:    

              group drillcpu {
                     cpu {
                            cpu.shares = 320;
                            cpu.cfs_quota_us = 400000;
                            cpu.cfs_period_us = 100000;
                            }
                     }  
  
In the configuration example above, the `cpu.shares` parameter sets the soft limit. The other two parameters, `cpu.cfs_quota_us` and `cpu.cfs_period_us`, set the hard limit. If you prefer to set only one type of limit, remove the parameters that do not apply.  

When setting a soft limit, allocate a specific number of CPU shares to the Drill cgroup in the configuration. Calculate the CPU shares as:  

       1024 (CPU allocated to Drill/Total available CPU)

In the example, CPU shares is calculated as:  

       1024 (10/32) = 320


When setting a hard limit, add limits to the `cpu.cfs_quota_us` and `cpu.cfs_period_us` parameters. In the example, the Drill process can fully utilize 4 CPU.  

**Note:** The hard limit parameter settings persist after each cgroup service restart. Alternatively, you can set the parameters at the session level using the following commands:  

       echo 400000 > /cgroup/cpu/drillcpu/cpu.cfs_quota_us
       echo 100000 > /cgroup/cpu/drillcpu/cpu.cfs_period_us

3-(Optional) If you want the cgconfig service to automatically restart upon system reboots, run the following command:  

       chkconfig cgconfig on  

******************************************    

For additional information, refer to the [RedHat documentation](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/resource_management_guide/sec-cpu). 










