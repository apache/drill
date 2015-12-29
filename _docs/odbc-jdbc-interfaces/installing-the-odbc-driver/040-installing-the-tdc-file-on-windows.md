---
title: "Installing the TDC File on Windows"
date:  
parent: "Installing the ODBC Driver"
---
The MapR Drill ODBC driver installer automatically installs the TDC file if the installer can find the Tableau installation. If you installed the MapR Drill ODBC driver first and then installed Tableau, the TDC file is not installed automatically, and you need to install the TDC file manually. 

**To install the MapRDrillODBC.TDC file manually:**

1. Click **Start**, and locate the Install Tableau TDC File app that the MapR Drill ODBC Driver installer installed.   
   For example, on Windows 8.1 in Apps, the Install Tableau TDC File appears under MaprDrill ODBC Driver:
   ![]({{ site.baseurl }}/docs/img/odbc-mapr-drill-apps.png)

2. Click **Install Tableau TDC File**. 
3. When the installation completes, press any key to continue.   
For example, you can press the SPACEBAR key.

If the installation of the TDC file fails, this is likely due to your Tableau repository being in location other than the default one.  In this case, manually copy the My Tableau Repository to C:\Users\<user>\Documents\My Tableau Repository. Repeat the procedure to install the MapRDrillODBC.TDC file manually.
