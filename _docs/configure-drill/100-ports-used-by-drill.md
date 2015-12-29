---
title: "Ports Used by Drill"
date:  
parent: "Configure Drill"
---
The following table provides a list of the ports that Drill uses, the port
type, and a description of how Drill uses the port:

| Port  | Type | Description                                                                                                                                                                    |
|-------|------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 8047  | TCP  | Needed for the Drill Web Console.                                                                                                                                                   |
| 31010 | TCP  | User port address. Used between nodes in a Drill cluster. Needed for an external client, such as Tableau, to connect into the cluster nodes. Also needed for the Drill Web Console. |
| 31011 | TCP  | Control port address. Used between nodes in a Drill cluster. Needed for multi-node installation of Apache Drill.                                                               |
| 31012 | TCP  | Data port address. Used between nodes in a Drill cluster. Needed for multi-node installation of Apache Drill.                                                                  |
| 46655 | UDP  | Used for JGroups and Infinispan. Needed for multi-node installation of Apache Drill.                                                                                           |

