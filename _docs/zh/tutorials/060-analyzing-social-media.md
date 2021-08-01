---
title: "分析社交媒体"
slug: "Analyzing Social Media"
parent: "教程"
lang: "zh"
---

本教程介绍了如何使用 Apache Drill 分析原生 JSON 格式的 Twitter 数据。首先，使用 Apache Flume 处理 Twitter 数据流并过滤关键字和语言类型，然后使用 Drill 分析数据。最后，运行 MicroStrategy 以获得交互式报告和分析。

## 社交媒体分析所需准备

* Twitter 开发者账户
* 亚马逊云服务账户
* 亚马逊云服务中加载一个 MapR 节点
* 亚马逊云服务中加载一个 MicroStrategy 实例

## 配置亚马逊云服务环境

在亚马逊云服务 (AWS) 上配置环境包括以下任务：

* 创建一个 Twitter 开发者账户并注册一个 Twitter 应用程序  
* 在开启的 AWS MapR 节点中配置 Flume 和 Drill
* 在 AWS 虚拟机中配置 MicroStrategy
* 配置 MicroStrategy 来使用 Drill 运行报告和分析  

本教程假设你已熟悉 MicroStrategy。有关使用 MicroStrategy 的信息，请参考 [MicroStrategy documentation](http://www.microstrategy.com/Strategy/media/downloads/products/cloud/cloud_aws-user-guide.pdf)。

----------

## 订阅 Twitter 新消息并建立 Flume 凭证

以下步骤订阅 Twitter 新消息并获取 Twitter 证书使 Flume 将 Twitter 设置为数据源：

1. 打开 dev.twitter.com 页面并登陆。
2. 点击页面底部 Tools 中的 **Manage Your Apps**。
3. 点击 **Create New App** 并填写表格，创建 App。
4. 点击 **Keys and Access Tokens** 选项卡，创建一个新的秘钥，并点击 **Create My Access Token**。只读权限即可创建秘钥。
5. 复制 Twitter App 中的以下证书用于配置 Flume： 
   * Consumer Key
   * Consumer Secret
   * Access Token
   * Access Token Secret

----------

## 在 AWS 虚拟机中配置 MapR 节点

你需要在 AWS 中加载名为 ami-4dedc47d 的 MapR 节点。AMI 已经配置了 Flume、Drill 和特定工具，以支持来自 Twitter 和 Drill 查询数据流。AMI 社区免费开放使用 AMI，具有 6GB 根驱动器和 100GB 数据驱动器。作为一个小节点，丰富的硬件资源将显著加快 Twitter 数据查询的响应时间。

1. 在 AWS 中启动一个实例。AMI 镜像预先配置为具有4个 vCPU 和 32GB 内存的 m2.2xlarge 型实例。
2. 选择 AMI id 为 ami-4dedc47d。
3. 确保虚拟机已分配外部 IP 地址；弹性 IP 更好，但不是必需的。 
4. 确认安全验证是否应用于与实例上打开的 TCP 和 UDP 端口。此时，节点上的所有端口应处于打开状态。
5. 配置并启动实例后，在 AWS EC2 管理界面中重启节点以完成配置。

该节点配置了所需的 Flume 和 Drill。接下来，使用所需的证书和关键字更新 Flume 配置文件。

----------

## 更新 Flume 配置文件

1. 使用 AWS 凭证登录 ec2-user。
2. 使用 `su – mapr` 切换到节点上的 MapR 用户。
3. 使用步骤一中的 Twitter App 证书更新 `<FLUME HOME>/conf` 目录中的 Flume 配置文件 `flume-env.sh` 和 `flume.conf`。请参考[sample files](https://github.com/mapr/mapr-demos/tree/master/drill-twitter-MSTR/flume)。
4. 输入所需的关键字，以逗号分隔。使用空格分隔多个关键字。 
5. 如果要过滤特定语言的推文，需要通过以逗号分隔的 ISO 639-1 [语言代码](http://en.wikipedia.org/wiki/List_of_ISO_639-1_codes)。如果不需要语言类型过滤，请将参数留空。  
6. 以用户 `MapR` 切换到 FLUME HOME 目录，并以用户 `MapR` 在命令行执行命令：  
7. 通过下命令启动 Flume：  

        ./bin/flume-ng agent --conf ./conf/ -f ./conf/flume.conf -Dflume.root.logger=INFO,console -n TwitterAgent
8. 输入 `CTRL+a` 使程序退出前台，然后输入 `d` 使程序后台运行。要使程序返回屏幕终端，只需输入 screen -r 即可重新连接。Twitter 数据会加载进系统。 
9. 运行以下命令来查看 Twitter 数据所占磁盘空间

         du –h /mapr/drill_demo/twitter/feed.

数据加载需要等待 20-30 分钟，加载完成之后才可以开始查询数据。

----------

## 在 AWS 虚拟机中配置 MicroStrategy

MicroStrategy 为 AWS 虚拟机提供各种适配类型。MicroStrategy 有30天的免费试用。AWS 也会针对平台和操作系统收取费用。

如要在 AWS 中配置 MicroStrategy：

1. 在 [MicroStrategy website](http://www.microstrategy.com/us/analytics/analytics-on-aws)页面, 点击 **Get started**。  
2. 选择用户的数量，比如选择25个用户。
3. 选择 AWS 的使用地区。强烈建议给 MapR 节点和 MicroStrategy 实例选择和 AWS 相同的使用地区。
4. 点击 **Continue**。
5. 在手动启动选项卡上, 点击 **Launch with EC2 Console**, 然后选择 **r3.large instance**
   一个 r3.large 的 EC2 实例足以满足 25 个用户的用户量。
6. 点击 **Configure Instance Details**。
7. 选择适当的网络设置和网段，最好与您配置的 MapR 节点处在相同的网络中。
8. 保留默认存储类型。
9. 给实例分配标签以方便识别。
10. 选择一个允许外部 IP 访问的安全组，并打开所有端口，不必担心存在安全问题。
11. 在 AWS 控制台中，启动一个实例，当 AWS 报告该实例正在运行时，选择该实例并单击 **Connect**。
12. 点击 **Get Password** 来获得 OS 的管理员密码。

{% include startimportant.html %}确保 MicroStrategy 实例具有公共 IP；弹性 IP 更佳，但不是必需{% include endimportant.html %}

该实例现在可通过 RDP 访问并绑定 AWS 的证书和安全验证。

----------

## 配置 MicroStrategy

你需要使用 ODBC 驱动程序来配置，使 MicroStrategy 可以与 Drill 集成。安装完成后的 MicroStrategy 套件，包含许多实用的 Twitter 数据报告模板。你可以修改报告模板或使用模板来创建新的报告和分析模型。

1. 使用 ODBC 管理员配置名为 `Twitter` 的 DSN 系统。MapR ODBC 驱动程序的快速入门版本需要 DSN 系统。
2. [下载用于 Drill 的 MapR ODBC 驱动程序的快速入门版本](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/MapRDrill_odbc_v0.08.1.0618/MapRDrillODBC32.msi)。  
3. [配置 ODBC 驱动程序](http://drill.apache.org/docs/using-microstrategy-analytics-with-apache-drill) 使 MicroStrategy Analytics 中可以使用 Drill。
   Drill 是程序的一部分，不需要配置。 
4. 如果 MapR 节点和 MicroStrategy 实例位于同一网络，使用 AWS 虚拟机私有 IP（推荐）。
5. 下载 [Drill 和 Twitter 配置](https://github.com/mapr/mapr-demos/blob/master/drill-twitter-MSTR/MSTR/DrillTwitterProjectPackage.mmp) 配置 Windows 中的 MicroStrategy 可以使用 Git，同时使 Windows 全面兼容 Git。

----------

## 导入报告

1. 在 MicroStrategy Developer，选择 **Schema > Create New Project** 来创建新的项目。 
2. 点击 **Create Project** 给新项目命名。 
3. 点击 **OK**。
   新项目出现在 MicroStrategy Developer。
4. 打开 MicroStrategy Object Manager。  
5. 连接到 Project Source 并管理员登录
   ![project sources]({{ site.baseurl }}/images/docs/socialmed1.png)
6. 在 MicroStrategy Object Manager 中的 MicroStrategy Analytics Modules，选择组件所应用的项目。比如，选择 **Twitter analysis Apache Drill**。    
   ![project sources]({{ site.baseurl }}/images/docs/socialmed2.png)
7. 选择 **Tools > Import Configuration Package**。  
8. 打开组件配置文件，并点击 **Proceed**。  
   ![project sources]({{ site.baseurl }}/images/docs/socialmed3.png)  
   项目对应的报告便会生成到 MicroStrategy 中。  

你可以在 MicroStrategy Developer 中测试和修改报告。并在必要时修改权限。

----------

## 更新 Schema

1. 在 MicroStrategy Developer 中, 选择 **Schema > Update Schema**。  
2. 在 Schema Update 中，勾中所有选项，并点击 **Update**。  
   ![project sources]({{ site.baseurl }}/images/docs/socialmed4.png)

----------

## 创建用户并设置密码

1. 点击 Administration.  
2. 点击 User Manager, 然后点击 **Everyone**.  
3. 点击右键创建新用户，或点击 **Administrator** 来编辑密码.  

----------

## 关于报告

组件中有18个报告模板。大多数报告会提示根据需要指定日期范围、输出条件和所需字段。该组件包包含三个主要类别的报告：

* 数量统计：指定日期和时间的推文总量。
* 热门列表：显示热门推文、转推、热门话题和用户。
* 关键字：根据推文或者转推中的内容来统计关键字。

你可以复制和修改报告或将报告作为模板并通过 Drill 查询 Twitter。 

你可以通过 MicroStrategy Developer 或 Web 界面访问报告。MicroStrategy Developer 提供了比 Web 界面更强大的接口来修改或添加新报告，但需要对节点进行 RDP 访问。

----------

## 使用网页界面

1. 使用 Web 浏览器，输入 Web 界面的 URL：  
         http://<MSTR node name or IP address>/MicroStrategy/asp/Main.aspx
2. 使用初始账户或管理员账户登录，并通过证书初始化 Developer。
3. 在 MicroStrategy Web 用户页面上，选择用于加载分析组件的项目：**Drill Twitter Analysis**。  
   ![choose project]({{ site.baseurl }}/images/docs/socialmed5.png)
4. 选择 **Shared Reports**.  
   将出现包含三个主要报告类别的文件夹。
   ![project sources]({{ site.baseurl }}/images/docs/socialmed6.png)
5. 选择一个报告，并按提示操作。例如，要按日期范围统计热门推文语言种类，请输入所需的 Date_Start 和 Date_End。 
   ![project sources]({{ site.baseurl }}/images/docs/socialmed7.png)
6. 点击 **Run Report**.  
   直方图报告会按日期范围显示排名靠前的推文语言种类。
   ![project sources]({{ site.baseurl }}/images/docs/socialmed8.png)
7. 要刷新数据或重新输入参数，请选择 **Data > Refresh** 或 **Data > Re-prompt**.

## 浏览 Apache Drill Twitter 分析报告

MicroStrategy Developer 报告位于组件安装路径中的 Public Objects 文件夹中
   ![project sources]({{ site.baseurl }}/images/docs/socialmed9.png)
许多报告要求按提示选择合适的参数。例如，选择右侧栏中的 Top Hashtags 报告。此报告需要提供开始日期和结束日期来指定你感兴趣的时间范围；默认情况下，是以当前时间为基准最近两个月的数据。你还可以指定要返回的热门话题的数量；默认是热度前10的热门话题。  
   ![project sources]({{ site.baseurl }}/images/docs/socialmed10.png)
会出现一个带有热门话题和它在指定参数范围内出现次数的条形图报告。
   ![project sources]({{ site.baseurl }}/images/docs/socialmed11.png)

组件中还提供了其他报告。例如，此报告按小时显示推文总数：
   ![tweets by hour]({{ site.baseurl }}/images/docs/socialmed12.png)
此报告显示日期范围内的最高转推次数，以及该日期范围内的原始推文日期和计数。  
   ![retweets report]({{ site.baseurl }}/images/docs/socialmed13.png)

----------

## 总结

在本教程中，学习了如何配置环境以使用 Apache Flume 传输 Twitter 数据流。然后学习了如何通过 Apache Drill 使用 SQL 分析原生 JSON 格式的数据，以及如何使用 MicroStrategy 运行交互式报告和分析。
