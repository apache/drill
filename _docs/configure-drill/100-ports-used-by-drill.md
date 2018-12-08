---
title: "Ports Used by Drill"
date: 2018-12-08
parent: "Configure Drill"
---

The table below lists the default ports that Drill uses and provides descriptions for each, as well as the corresponding configuration options. You can modify the configuration options in `<drill_home>/conf/drill-override.conf` to change the ports that Drill uses. See [Start-Up Options]({{site.baseurl}}/docs/start-up-options/) for more information. 


| Default Port | Type | Configuration Option               | Description                                                                                                                                                                         |
|--------------|------|------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 8047         | TCP  | drill.exec.http.port               | Needed for the Drill Web UI.                                                                                                                                                   |
| 31010        | TCP  | drill.exec.rpc.user.server.port    | User port address. Used between nodes in a Drill cluster. Needed for an external client, such as Tableau, to connect into the cluster nodes. Also needed for the Drill Web UI. |
| 31011        | TCP  | drill.exec.rpc.bit.server.port     | Control port address. Used between nodes in a Drill cluster. Needed for multi-node installation of Apache Drill.                                                                    |
| 31012        | TCP  | drill.exec.rpc.bit.server.port + 1 | Data port address. Used between nodes in a Drill cluster. Needed for multi-node installation of Apache Drill.                                                                       |


<!---46655 UDP Used for JGroups and Infinispan. Needed for multi-node installation of Apache Drill.--->                                                                                                
