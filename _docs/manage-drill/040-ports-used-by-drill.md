---
title: "Ports Used by Drill"
parent: "Manage Drill"
---
The following table provides a list of the ports that Drill uses, the port
type, and a description of how Drill uses the port:

<table ><tbody><tr><th >Port</th><th colspan="1" >Type</th><th >Description</th></tr><tr><td valign="top" >8047</td><td valign="top" colspan="1" >TCP</td><td valign="top" >Needed for <span style="color: rgb(34,34,34);">the Drill Web UI.</span><span style="color: rgb(34,34,34);"> </span></td></tr><tr><td valign="top" >31010</td><td valign="top" colspan="1" >TCP</td><td valign="top" >User port address. Used between nodes in a Drill cluster. <br />Needed for an external client, such as Tableau, to connect into the<br />cluster nodes. Also needed for the Drill Web UI.</td></tr><tr><td valign="top" >31011</td><td valign="top" colspan="1" >TCP</td><td valign="top" >Control port address. Used between nodes in a Drill cluster. <br />Needed for multi-node installation of Apache Drill.</td></tr><tr><td valign="top" colspan="1" >31012</td><td valign="top" colspan="1" >TCP</td><td valign="top" colspan="1" >Data port address. Used between nodes in a Drill cluster. <br />Needed for multi-node installation of Apache Drill.</td></tr><tr><td valign="top" colspan="1" >46655</td><td valign="top" colspan="1" >UDP</td><td valign="top" colspan="1" >Used for JGroups and Infinispan. Needed for multi-node installation of Apache Drill.</td></tr></tbody></table></div>

