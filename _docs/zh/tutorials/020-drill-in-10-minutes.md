---
title: "10分钟了解 Drill"
slug: "Drill in 10 Minutes"
parent: "教程"
description: 10分钟内了解Drill.
lang: "zh"
---
## 目标

10分钟内使用 Apache Drill 查询样本数据。为了方便，相对于分布式模式，你更应该在嵌入式模式中试用Drill，以此避免很多复杂的设置。  

## 安装概述

在 Linux，Mac OS X 或 Windows 中，你都可以安装并嵌入式模式启动Drill。更多嵌入式模式安装 Drill 的相关信息请参考 [嵌入式模式安装Drill]({{ site.baseurl }}/docs/installing-drill-in-distributed-mode)。  

安装步骤包括如何下载 Apache Drill 存档文件，提取存档文件内容到你电脑的指定文件夹。Apache Drill 存档文件包括可以立即查询的示例 JSON 和 Parquet 文件。  

安装完 Drill 后，启动 Drill shell。[Drill shell]({{site.baseurl}}/docs/configuring-the-drill-shell/) 是一个基于控制台的纯Java工具，用于连接关系型数据库和执行 SQL 命令。Drill 遵循 SQL:2011 标准，嵌套数据及其他功能遵循[插件标准]({{site.baseurl}}/docs/sql-extensions/)。  

## 嵌入式模式安装条件

你的机器需要满足如下条件来嵌入式运行 Drill： 

* Linux, Mac OS X, and Windows: Oracle JDK [version 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html).
* 针对 Windows:  
  * 设置 JAVA_HOME 环境变量指向 JDK 路径
  * 在 PATH 环境变量中添加 JDK bin 目录
  * 可以解压缩 .tar.gz 的第三方工具  
  
### JAVA 安装条件检查

在终端（Linux 和 Mac OS X）或命令提示符（Windows）中运行下列指令来确认 JAVA 8 是正在使用的 JAVA 版本：

`java -version`

输出应当是：  

    java version "1.8.0"
    Java(TM) SE Runtime Environment (build 1.8.0_7965-b15)
    Java HotSpot(TM) 64-Bit Server VM (build 24.79-b02, mixed mode)

## 在 Linux 或 Mac OS X 上安装 Drill：

完成下列步骤来安装 Drill：  

1. 在终端窗口中，切换到你想安装 Drill 的目录。  
2. 下载最新版的 Apache Drill [下载链接](http://apache.mirrors.hoobly.com/drill/drill-1.19.0/apache-drill-1.19.0.tar.gz) 或者 [Apache Drill 镜像站](http://www.apache.org/dyn/closer.cgi/drill/drill-1.19.0/apache-drill-1.19.0.tar.gz)。选择适配你的操作系统的下载指令:  
       * `wget http://apache.mirrors.hoobly.com/drill/drill-1.19.0/apache-drill-1.19.0.tar.gz`
       * `curl -o apache-drill-1.19.0.tar.gz http://apache.mirrors.hoobly.com/drill/drill-1.19.0/apache-drill-1.19.0.tar.gz`  
3. 拷贝下载文件到你想安装 Drill 的目录。     
4. 提取 Drill .tar.gz 文件的内容。如有必要，使用 `sudo`:  
`tar -xvzf <.tar.gz file name>`  

提取过程创建了一个包含 Drill 软件的安装目录。  

现在，你可以启动 Drill 了。  

### 在 Linux and Mac OS X 中启动 Drill
使用 `drill-embedded` 命令嵌入式模式启动 Drill：  

1. 切换到 Drill 安装目录。例如:    

    `cd apache-drill-<version>`  

2. 输入下列命令来嵌入式模式启动 Drill:  

    `bin/drill-embedded`  

   会显示下列命令： [`0: jdbc:drill:zk=local>`  prompt]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/#about-the-drill-prompt)。    

   此时, 你已经可以对 Drill [提交查询]({{site.baseurl}}/docs/drill-in-10-minutes/#query-sample-data)。  

## 在 Windows 中安装并启动 Drill  

参考 [在 Windows 中启动 Drill]({{site.baseurl}}/docs/installing-drill-on-windows/)。   


## 退出 Drill

通过下述命令来退出 Drill shell:

	!quit

## 查询示例数据

Drill 安装目录下有一个 `sample-data` 文件夹，其中有你可以查询的示例 JSON 和
Parquet 文件。 当你嵌入式模式启动 Drill 时，默认的 `dfs` 存储插件配置指向了你个人电脑的本地文件系统。
更多关于存储插件配置的信息, 请参考[存储插件注册]({{ site.baseurl }}/docs/connect-a-data-source-introduction)。

使用 SQL 来查询你本地文件系统 `sample-data` 文件夹中的示例 `JSON` 和 `Parquet` 文件。

### 查询 JSON 文件

示例 JSON 文件, [`employee.json`]({{site.baseurl}}/docs/querying-json-files/), 包含了虚构的员工数据。想查看 `employee.json` 文件的内容, 提交下述 SQL 查询语句到Drill，使用配置 [cp (classpath) storage plugin]({{site.baseurl}}/docs/storage-plugin-registration/) 来指向到文件。
    
	SELECT * FROM cp.`employee.json` LIMIT 3;
	
	    |--------------|------------------|-------------|------------|--------------|---------------------|-----------|----------------|-------------|------------------------|----------|----------------|------------------|-----------------|---------|--------------------|
	    | employee_id  |    full_name     | first_name  | last_name  | position_id  |   position_title    | store_id  | department_id  | birth_date  |       hire_date        |  salary  | supervisor_id  | education_level  | marital_status  | gender  |  management_role   |
	    |--------------|------------------|-------------|------------|--------------|---------------------|-----------|----------------|-------------|------------------------|----------|----------------|------------------|-----------------|---------|--------------------|
	    | 1            | Sheri Nowmer     | Sheri       | Nowmer     | 1            | President           | 0         | 1              | 1961-08-26  | 1994-12-01 00:00:00.0  | 80000.0  | 0              | Graduate Degree  | S               | F       | Senior Management  |
	    | 2            | Derrick Whelply  | Derrick     | Whelply    | 2            | VP Country Manager  | 0         | 1              | 1915-07-03  | 1994-12-01 00:00:00.0  | 40000.0  | 1              | Graduate Degree  | M               | M       | Senior Management  |
	    | 4            | Michael Spence   | Michael     | Spence     | 2            | VP Country Manager  | 0         | 1              | 1969-06-20  | 1998-01-01 00:00:00.0  | 40000.0  | 1              | Graduate Degree  | S               | M       | Senior Management  |
	    |--------------|------------------|-------------|------------|--------------|---------------------|-----------|----------------|-------------|------------------------|----------|----------------|------------------|-----------------|---------|--------------------|
	   

### 查询 Parquet 文件

在你本地文件系统中的 `sample-data` 文件夹中查询 `region.parquet` 和 `nation.parquet` 文件。

#### 地区文件

如果你按照本教程安装并嵌入式模式启动了Drill，Parquet 文件的路径会因操作系统的不同而改变。

要查看 `region.parquet` 文件中的数据，使用你实际安装 Drill 的路径来构建这个查询：

	//SELECT * FROM dfs.`<path-to-installation>/apache-drill-<version>/sample-data/region.parquet`;

	SELECT * FROM dfs.`Users/drilluser/apache-drill/sample-data/region.parquet`;

    |--------------|--------------|-----------------------|
    | R_REGIONKEY  |    R_NAME    |       R_COMMENT       |
    |--------------|--------------|-----------------------|
    | 0            | AFRICA       | lar deposits. blithe  |
    | 1            | AMERICA      | hs use ironic, even   |
    | 2            | ASIA         | ges. thinly even pin  |
    | 3            | EUROPE       | ly final courts cajo  |
    | 4            | MIDDLE EAST  | uickly special accou  |
    |--------------|--------------|-----------------------|
    
#### 国家文件

Parquet 文件的路径会因操作系统的不同而改变,使用你实际安装 Drill 的路径来构建这个查询：

	//SELECT * FROM dfs.`<path-to-installation>/apache-drill-<version>/sample-data/nation.parquet`;``

    SELECT * FROM dfs.`Users/drilluser/apache-drill/sample-data/nation.parquet`;
    |-------------|----------------|-------------|----------------------|
    | N_NATIONKEY | N_NAME         | N_REGIONKEY | N_COMMENT            |
    |-------------|----------------|-------------|----------------------|
    | 0           | ALGERIA        | 0           | haggle. carefully f  |
    | 1           | ARGENTINA      | 1           | al foxes promise sly |
    | 2           | BRAZIL         | 1           | y alongside of the p |
    | 3           | CANADA         | 1           | eas hang ironic, sil |
    | 4           | EGYPT          | 4           | y above the carefull |
    | 5           | ETHIOPIA       | 0           | ven packages wake qu |
    | 6           | FRANCE         | 3           | refully final reques |
    | 7           | GERMANY        | 3           | l platelets. regular |
    | 8           | INDIA          | 2           | ss excuses cajole sl |
    | 9           | INDONESIA      | 2           | slyly express asymp  |
    | 10          | IRAN           | 4           | efully alongside of  |
    | 11          | IRAQ           | 4           | nic deposits boost a |
    | 12          | JAPAN          | 2           | ously. final, expres |
    | 13          | JORDAN         | 4           | ic deposits are blit |
    | 14          | KENYA          | 0           | pending excuses hag  |
    | 15          | MOROCCO        | 0           | rns. blithely bold c |
    | 16          | MOZAMBIQUE     | 0           | s. ironic, unusual a |
    | 17          | PERU           | 1           | platelets. blithely  |
    | 18          | CHINA          | 2           | c dependencies. furi |
    | 19          | ROMANIA        | 3           | ular asymptotes are  |
    | 20          | SAUDI ARABIA   | 4           | ts. silent requests  |
    | 21          | VIETNAM        | 2           | hely enticingly expr |
    | 22          | RUSSIA         | 3           | requests against th  |
    | 23          | UNITED KINGDOM | 3           | eans boost carefully |
    | 24          | UNITED STATES  | 1           | y final packages. sl |
    |-------------|----------------|-------------|----------------------|
   

## 总结

Apache Drill 支持嵌套数据，schema-less 执行，和分散管理的元数据。你现在可以使用 Drill 来对 JSON 或者 Parquet 数据进行简单的查询了。

## 下一步

现在，你会想 Drill 可以做些什么，你也许会有如下需求：

  * [集群模式安装 Drill]({{ site.baseurl }}/docs/installing-drill-on-the-cluster)
  * [配置存储插件使 Drill 可以连接到你的数据源]({{ site.baseurl }}/docs/connect-a-data-source-introduction).
  * 查询 [Hive]({{ site.baseurl }}/docs/querying-hive) and [HBase]({{ site.baseurl }}/docs/hbase-storage-plugin) data.
  * [查询复杂数据]({{ site.baseurl }}/docs/querying-complex-data)
  * [查询纯文本文件]({{ site.baseurl }}/docs/querying-plain-text-files)

## 更多信息

关于 Apache Drill 的更多信息，请探索 [Apache Drill 官方网站](http://drill.apache.org)。