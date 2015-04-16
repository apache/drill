---
title: "Installing Drill on Windows"
parent: "Installing Drill in Embedded Mode"
---
You can install Drill on Windows 7 or 8. To install Drill on Windows, you must
have JDK 7, and you must set the `JAVA_HOME` path in the Windows Environment
Variables. You must also have a utility, such as
[7-zip](http://www.7-zip.org/), installed on your machine. These instructions
assume that the [7-zip](http://www.7-zip.org/) decompression utility is
installed to extract the Drill archive file that you download.

#### Setting JAVA_HOME

Complete the following steps to set `JAVA_HOME`:

  1. Navigate to `Control Panel\All Control Panel Items\System`, and select **Advanced System Settings**. The System Properties window appears.
  2. On the Advanced tab, click **Environment Variables**. The Environment Variables window appears.
  3. Add/Edit `JAVA_HOME` to point to the location where the JDK software is located.

     **Example**
     
        C:\Program Files\Java\jdk1.7.0_65
  4. Click **OK** to exit the windows.

#### Installing Drill

Complete the following steps to install Drill:

  1. Create a `drill` directory on your `C:\` drive, (or in some other location if you prefer).

     **Example**
     
        C:\Drill
     Do not include spaces in your directory path. If you include spaces in the
directory path, Drill fails to run.
  2. Click the following link to download the latest, stable version of Apache Drill:
  
     [http://getdrill.org/drill/download/apache-drill-0.8.0.tar.gz)
  3. Move the `apache-drill-<version>.tar.gz` file to the `drill` directory that you created on your `C:\` drive.
  4. Unzip the `TAR.GZ` file and the resulting `TAR` file.  
     1. Right-click `apache-drill-<version>.tar.gz,` and select `7-Zip>Extract Here`. The utility extracts the `apache-drill-<version>.tar` file.  
     2. Right-click `apache-drill-<version>.tar,` and select `7-Zip>Extract Here`. The utility extracts the `apache-drill-<version>` folder.
  5. Open the `apache-drill-<version>` folder.
  6. Open the `bin` folder, and double-click on the `sqlline.bat` file. The Windows command prompt opens.
  7. At the `sqlline>` prompt, type `!connect jdbc:drill:zk=local` and then press `Enter`.
  8. Enter the username and password. 
     1. When prompted, enter the user name `admin` and then press Enter. 
     2. When prompted, enter the password `admin` and then press Enter. The cursor blinks for a few seconds and then `0: jdbc:drill:zk=local>` displays in the prompt.

At this point, you can submit queries to Drill. Refer to [Querying Data]({{ site.baseurl }}/docs/query-data-introduction).