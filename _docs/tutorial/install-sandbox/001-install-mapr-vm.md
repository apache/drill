---
title: "Installing the MapR Sandbox with Apache Drill on VMware Player/VMware Fusion"
parent: "Installing the Apache Drill Sandbox"
---
Complete the following steps to install the MapR Sandbox with Apache Drill on
VMware Player or VMware Fusion:

1. Download the MapR Sandbox with Drill file to a directory on your machine:  
   <https://www.mapr.com/products/mapr-sandbox-hadoop/download-sandbox-drill>
2. Open the virtual machine player, and select the **Open a Virtual Machine **option.  
  
    **Tip for VMware Fusion**  

    If you are running VMware Fusion, select** Import**.  

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

## What's Next

After downloading and installing the sandbox, continue with the tutorial by
[Getting to Know the Drill
Setup](/docs/getting-to-know-the-drill-sandbox).