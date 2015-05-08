---
title: "Configuration Options Introduction"
parent: "Configuration Options"
---
Drill provides many configuration options that you can enable, disable, or
modify. Modifying certain configuration options can impact Drill’s
performance. Many of Drill's configuration options reside in the `drill-
env.sh` and `drill-override.conf` files. Drill stores these files in the
`/conf` directory. Drill sources` /etc/drill/conf` if it exists. Otherwise,
Drill sources the local `<drill_installation_directory>/conf` directory.

The sys.options table in Drill contains information about boot (start-up) and system options. The section, ["Start-up Options"]({{site.baseurl}}/docs/start-up-options), covers how to configure and view these options. The sys.options also contains many session or system level options, some of whicha are described in the section, ["Planning and Execution Options"]({{site.baseurl}}/docs/planning-and-execution-options). 


