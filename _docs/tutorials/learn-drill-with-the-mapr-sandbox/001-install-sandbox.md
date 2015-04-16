---
title: "Installing the Apache Drill Sandbox"
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
3. Navigate to the directory where you downloaded the MapR Sandbox with Apache Drill file, and select `MapR-Sandbox-For-Apache-Drill-4.0.1_VM.ova`.

    ![drill query flow]({{ site.baseurl }}/docs/img/vmShare.png)

    The Import Virtual Machine dialog appears.
4. Click **Import**. The virtual machine player imports the sandbox.

    ![drill query flow]({{ site.baseurl }}/docs/img/vmLibrary.png)
5. Select `MapR-Sandbox-For-Apache-Drill-4.0.1_VM`, and click **Play virtual machine**. It takes a few minutes for the MapR services to start.  

     After the MapR services start and installation completes, the following screen
appears:

     ![drill query flow]({{ site.baseurl }}/docs/img/loginSandBox.png)

     Note the URL provided in the screen, which corresponds to the Web UI in Apache
Drill.
6. Verify that a DNS entry was created on the host machine for the virtual machine. If not, create the entry.
    * For Linux and Mac, create the entry in `/etc/hosts`.  
    * For Windows, create the entry in the `%WINDIR%\system32\drivers\etc\hosts` file.    
     
    For example: `127.0.1.1 <vm_hostname>`

7. You can navigate to the URL provided to experience Drill Web UI or you can login to the sandbox through the command line.  

    a. To navigate to the MapR Sandbox with Apache Drill, enter the provided URL in your browser's address bar.  
    b. To login to the virtual machine and access the command line, press Alt+F2 on Windows or Option+F5 on Mac. When prompted, enter `mapr` as the login name and password.

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

     ![drill query flow]({{ site.baseurl }}/docs/img/vbImport.png)
4. Navigate to the directory where you downloaded the MapR Sandbox with Apache Drill and click **Next**. The Appliance Settings window appears.

     ![drill query flow]({{ site.baseurl }}/docs/img/vbApplSettings.png)
5. Select the check box at the bottom of the screen: **Reinitialize the MAC address of all network cards**, then click **Import**. The Import Appliance imports the sandbox.
6. When the import completes, select **File > Preferences**. The VirtualBox - Settings dialog appears.

     ![drill query flow]({{ site.baseurl }}/docs/img/vbNetwork.png)
7. Select **Network**.  

    The correct setting depends on your network connectivity when you run the
Sandbox. In general, if you are going to use a wired Ethernet connection,
select **NAT Networks** and **vboxnet0**. If you are going to use a wireless
network, select **Host-only Networks** and the **VirtualBox Host-Only Ethernet
Adapter**. If no adapters appear, click the **green** + **button** to add the
VirtualBox adapter.

     ![drill query flow]({{ site.baseurl }}/docs/img/vbMaprSetting.png)
8. Click **OK** to continue.
9. Click Settings.

    ![settings icon]({{ site.baseurl }}/docs/img/settings.png)  
   The MapR-Sandbox-For-Apache-Drill-0.6.0-r2-4.0.1 - Settings dialog appears.
     
     ![drill query flow]({{ site.baseurl }}/docs/img/vbGenSettings.png)    
10. Click **OK** to continue.
11. Click **Start**. It takes a few minutes for the MapR services to start.   
 
      After the MapR services start and installation completes, the following screen appears:
      
       ![drill query flow]({{ site.baseurl }}/docs/img/vbloginSandBox.png)
12. The client must be able to resolve the actual hostname of the Drill node(s) with the IP(s). Verify that a DNS entry was created on the client machine for the Drill node(s).  
 
     If a DNS entry does not exist, create the entry for the Drill node(s).
     * For Windows, create the entry in the %WINDIR%\system32\drivers\etc\hosts file.
     * For Linux and Mac, create the entry in /etc/hosts.  
<drill-machine-IP> <drill-machine-hostname>  
  
     Example: `127.0.1.1 maprdemo`
13. You can navigate to the URL provided or to [localhost:8047](http://localhost:8047) to experience the Drill Web UI, or you can log into the sandbox through the command line.  

    a. To navigate to the MapR Sandbox with Apache Drill, enter the provided URL in your browser's address bar.  
    b. To log into the virtual machine and access the command line, enter Alt+F2 on Windows or Option+F5 on Mac. When prompted, enter `mapr` as the login name and password.

### What's Next

After downloading and installing the sandbox, continue with the tutorial by
[Getting to Know the Drill Sandbox]({{ site.baseurl }}/docs/getting-to-know-the-drill-sandbox/).
