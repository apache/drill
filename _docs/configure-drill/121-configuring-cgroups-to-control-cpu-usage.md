---
title: "Configuring cgroups to Control CPU Usage"
date: 2018-03-22 22:14:41 UTC  
parent: "Configure Drill"
---   

Linux cgroups (control groups) enable you to limit system resources to defined user groups or processes. As of Drill 1.13, you can configure a cgroup for Drill to enforce CPU limits on the Drillbit service. You can set a CPU limit for the Drill cgroup on each Drill node in the /etc/cgconfig.conf file.

You can set the CPU limit as a soft or hard limit, or both. The hard limit takes precedence over the soft limit. When Drill hits the hard limit, in-progress queries may not complete.  

##CPU Limits  

You set the soft and hard limits with parameters in the /etc/cgconfig.conf file. The following sections describe the parameters for soft and hard limits.  

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
**Note:** The cgroup name is specific to the Drill cgroup and does not correlate with any other configuration. You can give this group any name you prefer. The name drillcpu is used as an example.  
  
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
  
4-Run the following command to add the Drill process ID (PID) to the /cgroup/cpu/drillcpu/cgroup.procs file, as shown:  

       echo 25809 > /cgroup/cpu/drillcpu/cgroup.procs

**Note:** You must perform this step each time a Drillbit restarts.  

For additional information, refer to the following documentation:  
- [https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html)  
- [resource_management_guide/sect-cpu-example_usage](resource_management_guide/sect-cpu-example_usage)  
- [https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html)
- [resource_management_guide/sec-cpu_and_memory-use_case](resource_management_guide/sec-cpu_and_memory-use_case)









