---
title: "Installing the Apache Drill Sandbox"
date: 2018-12-08
parent: "Learn Drill with the MapR Sandbox"
---
## Prerequisites

The MapR Sandbox with Apache Drill runs on VMware Player and VirtualBox, free
desktop applications that you can use to run a virtual machine on a Windows,
Mac, or Linux PC. Before you install the MapR Sandbox with Apache Drill,
verify that the host system meets the following prerequisites:

  * VMware Player or VirtualBox is installed.
  * At least 20 GB free hard disk space, at least 4 physical cores, and 8 GB of RAM is available. Performance increases with more RAM and free hard disk space.
  * Uses one of the following 64-bit x86 architectures:
    * A 1.3 GHz or faster AMD CPU with segment-limit support in long mode
    * A 1.3 GHz or faster Intel CPU with VT-x support
  * If you have an Intel CPU with VT-x support, verify that VT-x support is enabled in the host system BIOS. The BIOS settings that must be enabled for VT-x support vary depending on the system vendor. See the VMware knowledge base article at <http://kb.vmware.com/kb/1003944> for information about how to determine if VT-x support is enabled.

### VM Player Downloads

For Linux, Mac, or Windows, download the free [VMware Player](https://my.vmware.com/web/vmware/free#desktop_end_user_computing/vmware_player/6_0 "VMwarePlayer") or
[VirtualBox](https://www.virtualbox.org/wiki/Downloads). Optionally, you can
purchase [VMware Fusion](http://www.vmware.com/products/fusion/) for Mac.

### VM Player Installation

The following list provides links to the virtual machine installation
instructions:

  * To install the VMware Player, see the [VMware documentation](http://www.vmware.com/support/pubs/player_pubs.html). Use of VMware Player is subject to the VMware Player end user license terms. VMware does not provide support for VMware Player. For self-help resources, see the [VMware Player FAQ](http://www.vmware.com/products/player/faqs.html).
  * To install VirtualBox, see the [Oracle VM VirtualBox User Manual](http://dlc.sun.com.edgesuite.net/virtualbox/4.3.4/UserManual.pdf). By downloading VirtualBox, you agree to the terms and conditions of the respective license.

## Installing the MapR Sandbox with Apache Drill on VMware Player/VMware Fusion

Complete the following steps to install the MapR Sandbox with Apache Drill on
VMware Player or VMware Fusion:

1. Download the MapR Sandbox with Drill file to a directory on your machine:  
   <https://www.mapr.com/products/mapr-sandbox-hadoop/download-sandbox-drill>
2. Open the virtual machine player, and select the **Open a Virtual Machine** option.  
  
    **Tip for VMware Fusion**  

    If you are running VMware Fusion, select **Import**.  

    ![drill query flow]({{ site.baseurl }}/docs/img/vmWelcome.png)
3. Navigate to the directory where you downloaded the MapR Sandbox with Apache Drill file, and select `MapR-Sandbox-For-Apache-Drill-<version>-vmware.ova`.  
    The Import Virtual Machine dialog appears.
4. Click **Import**. The virtual machine player imports the sandbox.  
5. Select `MapR-Sandbox-For-Apache-Drill-<version>_VM`, and click **Play virtual machine**. It takes a few minutes for the MapR services to start. After the MapR services start and installation completes, a screen that displays the following message appears:  

              MapR-Sandbox-For-Apache-Drill-<version> installation finished successfully.
              Please go to http://127.0.0.1:8047 to begin your experience.
              Open a browser on your host machine and enter the URL in the browser's address field.
              You can access the host via SSH by ssh mapr@localhost -p 2222
              Log in to this virtual machine: Linux/Windows <Alt+F2>, Mac OS X <Options+F5>
**Note:** The URL provided corresponds to the Web UI in Apache Drill.  
     
6. Verify that a DNS entry was created on the host machine for the virtual machine. If not, create the entry.
    * For Linux and Mac, create the entry in `/etc/hosts`.  
    * For Windows, create the entry in the `%WINDIR%\system32\drivers\etc\hosts` file.    
     
           Example: `127.0.1.1 <vm_hostname>`

7. Navigate to [localhost:8047](http://localhost:8047) to experience the Drill Web UI, or log in to the sandbox through the command line.  

   * Login using ssh as described in ["Getting to Know the Sandbox"]({{site.baseurl}}/docs/getting-to-know-the-drill-sandbox). When prompted, enter `mapr` as the login name and password.  
   * Alternatively, access the command line on the VM: Press Alt+F2 on Windows or Option+F5 on Mac.  

### What's Next

After downloading and installing the sandbox, continue with the tutorial by
[Getting to Know the Drill
Sandbox]({{ site.baseurl }}/docs/getting-to-know-the-drill-sandbox/).

## Installing the MapR Sandbox with Apache Drill on VirtualBox

The MapR Sandbox for Apache Drill on VirtualBox comes with NAT port forwarding
enabled, which allows you to access the sandbox using localhost as hostname.

Complete the following steps to install the MapR Sandbox with Apache Drill on
VirtualBox:

1. Download the MapR Sandbox with Apache Drill file to a directory on your machine:   
<https://www.mapr.com/products/mapr-sandbox-hadoop/download-sandbox-drill>
2. Open the virtual machine player.
3. Select **File > Import Appliance**. The Import Virtual Appliance dialog appears.

     ![drill query flow]({{ site.baseurl }}/docs/img/1_vbImport.png)
4. Navigate to the directory where you downloaded the MapR Sandbox with Apache Drill, select `MapR-Sandbox-For-Apache-Drill-<version>.ova`, and click **Next**. The Appliance Settings window appears.

     ![drill query flow]({{ site.baseurl }}/docs/img/vbApplSettings.png)
5. Select the check box at the bottom of the screen: **Reinitialize the MAC address of all network cards**, then click **Import**. The Import Appliance imports the sandbox.
6. When the import completes, select **Settings**. The VirtualBox - Settings dialog appears.

     ![drill query flow]({{ site.baseurl }}/docs/img/3_vbNetwork.png)
7. Select **Network**.  

    Verify that Adapter 1 is attached to **NAT**. This option should work for most scenarios. However, if you are going to use a wired Ethernet connection, you can select **NAT Network**. If you use ODBC or JDBC on a remote host, select **Bridged Adapter**. 

     ![drill query flow]({{ site.baseurl }}/docs/img/4_vbMaprSetting.png)
8. Click **OK** to continue.

9. Click **Start**. It takes a few minutes for the MapR services to start. After the MapR services start and installation completes, a screen that displays the following message appears:  

              MapR-Sandbox-For-Apache-Drill-<version> installation finished successfully.
              Please go to http://127.0.0.1:8047 to begin your experience.
              Open a browser on your host machine and enter the URL in the browser's address field.
              You can access the host via SSH by ssh mapr@localhost -p 2222
              Log in to this virtual machine: Linux/Windows <Alt+F2>, Mac OS X <Options+F5>  

       **Note:** The URL provided corresponds to the Web UI in Apache Drill.        

10. Navigate to [localhost:8047](http://localhost:8047) to experience the Drill Web UI, or log into the sandbox through the command line.  
       * Login using ssh as described in ["Getting to Know the Sandbox"]({{site.baseurl}}/docs/getting-to-know-the-drill-sandbox). When prompted, enter `mapr` as the login name and password.  
       * Alternatively, access the command line on the VM: Press Alt+F2 on Windows or Option+F5 on Mac.   

### What's Next

After downloading and installing the sandbox, continue with the tutorial by
[Getting to Know the Drill Sandbox]({{ site.baseurl }}/docs/getting-to-know-the-drill-sandbox/).
