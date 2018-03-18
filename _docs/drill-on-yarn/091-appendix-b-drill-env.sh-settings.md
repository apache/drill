---
title: "Appendix B: drill-env.sh Settings"
date:  
parent: "Drill-on-YARN"
---  

When running Drill outside of YARN, you can set many start-up options in drill-env.sh. Most
users accept the defaults. However, some users require specialized settings. 

Under YARN, Drill still reads your $DRILL_SITE/drill-env.sh file to pick up configuration.
However, for most options, Drill-on-YARN provides configuration options in drill-on-yarn.conf to set options that were formerly set in drill-env.sh. 

The following table provides a mapping:  

![](https://i.imgur.com/WUvHM9M.png)  

*If you set these options in both places, the value in drill-env.sh takes precedence. Note that EXTN_CLASSPATH (and drill.yarn.drillbit.extn-class-path) are a newer, more general way to add extensions. Rather than setting specific Hadoop or HBase variables, you can combine any number of extensions into the single extension classpath.

