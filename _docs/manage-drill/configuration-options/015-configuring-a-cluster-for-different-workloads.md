---
title: "Configuring a Cluster for Different Workloads"
parent: "Configuration Options"
---
This is something like resource(CPU/memory/disk) allocation best practice.
Actually memory is easiest to consider as of now. Because CPU/disks are kind of shared by everyone.
I do not have actual customer's configuration since many of the POC projects are dedicated Drill cluster.

But to understand this, you have to dig into the resource allocation from warden firstly.
http://doc.mapr.com/display/MapR/Cluster+Resource+Allocation

The memory for Drill/Impala/etc will be "virtually" reserved by below parameters:
service.command.os.heapsize.percent
service.command.os.heapsize.max
service.command.os.heapsize.min
And then set memory related parameters to "hard" limit the memory.

# Memory allocation for JobTracker is only used
# to calculate total memory required for all services to run
# but -Xmx JobTracker itself is not set allowing memory 
# on JobTracker to grow as needed
# if upper limit on memory is strongly desired
# set HADOOP_HEAPSIZE env. variable in /opt/mapr/hadoop/hadoop-0.20.2/conf/hadoo
p-env.sh
service.command.jt.heapsize.percent=10
service.command.jt.heapsize.max=5000
service.command.jt.heapsize.min=256
# Memory allocation for TaskTracker is only used
# to calculate total memory required for all services to run
# but -Xmx TaskTracker itself is not set allowing memory 
# on TaskTracker to grow as needed
# if upper limit on memory is strongly desired
# set HADOOP_HEAPSIZE env. variable in /opt/mapr/hadoop/hadoop-0.20.2/conf/hadoo
p-env.sh
service.command.tt.heapsize.percent=2
service.command.tt.heapsize.max=325
service.command.tt.heapsize.min=64





*****************************************************************

services=webserver:all:cldb;nfs:all:cldb;kvstore:all;cldb:all:kvstore;hoststats:
all:kvstore
service.command.jt.start=/opt/mapr/hadoop/hadoop-0.20.2/bin/hadoop-daemon.sh sta
rt jobtracker
service.command.tt.start=/opt/mapr/hadoop/hadoop-0.20.2/bin/hadoop-daemon.sh sta
rt tasktracker
service.command.hbmaster.start=/opt/mapr/hbase/hbase-0.98.9/bin/hbase-daemon.sh 
start master
service.command.hbregion.start=/opt/mapr/hbase/hbase-0.98.9/bin/hbase-daemon.sh 
start regionserver
service.command.cldb.start=/opt/mapr/initscripts/mapr-cldb start
service.command.kvstore.start=/opt/mapr/initscripts/mapr-mfs start
service.command.mfs.start=/opt/mapr/initscripts/mapr-mfs start
service.command.nfs.start=/opt/mapr/initscripts/mapr-nfsserver start
service.command.hoststats.start=/opt/mapr/initscripts/mapr-hoststats start
service.command.webserver.start=/opt/mapr/adminuiapp/webserver start
service.command.jt.stop=/opt/mapr/hadoop/hadoop-0.20.2/bin/hadoop-daemon.sh stop
 jobtracker
service.command.tt.stop=/opt/mapr/hadoop/hadoop-0.20.2/bin/hadoop-daemon.sh stop
 tasktracker
service.command.hbmaster.stop=/opt/mapr/hbase/hbase-0.98.9/bin/hbase-daemon.sh s
top master
service.command.hbregion.stop=/opt/mapr/hbase/hbase-0.98.9/bin/hbase-daemon.sh s
top regionserver
service.command.cldb.stop=/opt/mapr/initscripts/mapr-cldb stop
service.command.kvstore.stop=/opt/mapr/initscripts/mapr-mfs stop
service.command.mfs.stop=/opt/mapr/initscripts/mapr-mfs stop
service.command.nfs.stop=/opt/mapr/initscripts/mapr-nfsserver stop
service.command.hoststats.stop=/opt/mapr/initscripts/mapr-hoststats stop
service.command.webserver.stop=/opt/mapr/adminuiapp/webserver stop
service.command.jt.type=BACKGROUND
service.command.tt.type=BACKGROUND
service.command.hbmaster.type=BACKGROUND
service.command.hbregion.type=BACKGROUND
service.command.cldb.type=BACKGROUND
service.command.kvstore.type=BACKGROUND
service.command.mfs.type=BACKGROUND
service.command.nfs.type=BACKGROUND
service.command.hoststats.type=BACKGROUND
service.command.webserver.type=BACKGROUND
service.command.jt.monitor=org.apache.hadoop.mapred.JobTracker
service.command.tt.monitor=org.apache.hadoop.mapred.TaskTracker
service.command.hbmaster.monitor=org.apache.hadoop.hbase.master.HMaster start
service.command.hbregion.monitor=org.apache.hadoop.hbase.regionserver.HRegionSer
ver start
service.command.cldb.monitor=com.mapr.fs.cldb.CLDB
service.command.kvstore.monitor=server/mfs
service.command.mfs.monitor=server/mfs
service.command.nfs.monitor=server/nfsserver
service.command.jt.monitorcommand=/opt/mapr/hadoop/hadoop-0.20.2/bin/hadoop-daem
on.sh status jobtracker
service.command.tt.monitorcommand=/opt/mapr/hadoop/hadoop-0.20.2/bin/hadoop-daem
on.sh status tasktracker
service.command.hbmaster.monitorcommand=/opt/mapr/hbase/hbase-0.98.9/bin/hbase-d
aemon.sh status master
service.command.hbregion.monitorcommand=/opt/mapr/hbase/hbase-0.98.9/bin/hbase-d
aemon.sh status regionserver
service.command.cldb.monitorcommand=/opt/mapr/initscripts/mapr-cldb status
service.command.kvstore.monitorcommand=/opt/mapr/initscripts/mapr-mfs status
service.command.mfs.monitorcommand=/opt/mapr/initscripts/mapr-mfs status
service.command.nfs.monitorcommand=/opt/mapr/initscripts/mapr-nfsserver status
service.command.hoststats.monitorcommand=/opt/mapr/initscripts/mapr-hoststats st
atus
service.command.webserver.monitorcommand=/opt/mapr/adminuiapp/webserver status
# Memory allocation for JobTracker is only used
# to calculate total memory required for all services to run
# but -Xmx JobTracker itself is not set allowing memory 
# on JobTracker to grow as needed
# if upper limit on memory is strongly desired
# set HADOOP_HEAPSIZE env. variable in /opt/mapr/hadoop/hadoop-0.20.2/conf/hadoo
p-env.sh
service.command.jt.heapsize.percent=10
service.command.jt.heapsize.max=5000
service.command.jt.heapsize.min=256
# Memory allocation for TaskTracker is only used
# to calculate total memory required for all services to run
# but -Xmx TaskTracker itself is not set allowing memory 
# on TaskTracker to grow as needed
# if upper limit on memory is strongly desired
# set HADOOP_HEAPSIZE env. variable in /opt/mapr/hadoop/hadoop-0.20.2/conf/hadoo
p-env.sh
service.command.tt.heapsize.percent=2
service.command.tt.heapsize.max=325
service.command.tt.heapsize.min=64
service.command.hbmaster.heapsize.percent=4
service.command.hbmaster.heapsize.max=128
service.command.hbmaster.heapsize.min=128
service.command.hbregion.heapsize.percent=25
service.command.hbregion.heapsize.max=256
service.command.hbregion.heapsize.min=256
service.command.cldb.heapsize.percent=8
service.command.cldb.heapsize.max=256
service.command.cldb.heapsize.min=256
service.command.mfs.heapsize.percent=35
service.command.mfs.heapsize.maxpercent=85
service.command.mfs.heapsize.min=512
service.command.webserver.heapsize.percent=3
service.command.webserver.heapsize.max=128
service.command.webserver.heapsize.min=128
service.command.nfs.heapsize.percent=3
service.command.nfs.heapsize.min=64
service.command.nfs.heapsize.max=64
service.command.os.heapsize.percent=10
service.command.os.heapsize.max=4000
service.command.os.heapsize.min=256
service.command.warden.heapsize.percent=1
service.command.warden.heapsize.max=750
service.command.warden.heapsize.min=64
service.command.zk.heapsize.percent=1
service.command.zk.heapsize.max=1500
service.command.zk.heapsize.min=256
service.nice.value=-10
zookeeper.servers=maprdemo:5181
nodes.mincount=1
services.retries=3
cldb.port=7222
mfs.port=5660
hbmaster.port=60000
hoststats.port=5660
jt.port=9001
jt.http.port=50030
kvstore.port=5660
mapr.home.dir=/opt/mapr
centralconfig.enabled=true
pollcentralconfig.interval.seconds=300
rpc.drop=false
hs.rpcon=true
hs.port=1111
hs.host=localhost
service.command.cldb.retryinterval.time.sec=600
services.retryinterval.time.sec=1800
jt.response.timeout.minutes=10
isM7=0
mr1.memory.percent=50
mr1.cpu.percent=50
mr1.disk.percent=50
log.retention.exceptions=cldb.log,hoststats.log,configure.log,mfs.log-*
services.memoryallocation.alarm.threshold=97
isDB=true














*****************************************************************
Typo:
IMPALA_STATE_STORE_ARGS=>IMPALA_SERVER_ARGS

On Mon, Apr 20, 2015 at 2:11 PM, David Tucker <dtucker@maprtech.com> wrote:
Thanks for the pointer, Hao.

On Apr 20, 2015, at 1:58 PM, Hao Zhu <hzhu@maprtech.com> wrote:

Impala's mem setting is in env.sh:
--mem_limit of "IMPALA_STATE_STORE_ARGS" env var.
It is how much percentage of OS RAM.

Thanks,
Hao

On Mon, Apr 20, 2015 at 1:33 PM, David Tucker <dtucker@maprtech.com> wrote:
Eric,

What you have to do is carve out the memory for Drill and Impala using the os.heapsize settings (forcing .min and .max to the same value will do the right thing).   Both Drill and Impala have memory configuration settings (Drill’s are in drill-env.sh, I suspect Impala’s are in some xml file).   Max memory for a given drill bit is DRILL_MAX_HEAP + DIRECT_MEMORY; you’ll probably only adjust the DIRECT_MEMORY setting to give the query engine more data capacity.

To get specific with an example :
For a 64 GB system with 20% allocated for MFS, you can probably safely use 32 GB total for the query engines (assuming you want some room for Spark)
warden.conf properties (giving you a bit of wiggle room to adjust heapsize.percent if you like)
os.heapsize.percent=50
os.heapsize.min=20480
os.heapsize.max=40960
drill-env.sh settings (16 GB total)
DRILL_MAX_HEAP=4G
DIRECT_MEMORY=12G
Impala settings (left as an exercise for the reader :)
16 GB total

This configuration is very conservative; I’m pretty sure there’s no way to overcommit memory.   If you’re sure that you won’t be running competing workloads, you could obviously increase the Drill and Impala settings, or even reduce os.heapsize.percent to leave more room for Spark on YARN.

— David


On Apr 20, 2015, at 1:04 PM, Eric Ward <eward@maprtech.com> wrote:

yes for spark...i'm actually much more concerned with impala and drill in this context.

thanks,

eric

On 4/20/15 3:57 PM, Keys Botzum wrote:
Can you use yarn with spark? Then you don't have to worry about spark resources.
Keys
_______________________________
Keys Botzum 
Senior Principal Technologist
kbotzum@maprtech.com
443-718-0098
MapR Technologies 
http://www.mapr.com

On Apr 20, 2015 3:56 PM, "Eric Ward" <eward@maprtech.com> wrote:
All,

I'm going to be setting up a cluster w/ spark, drill, and impala, and I'd like to chat with someone regarding the correct way to set up memory allocations and ensure that warden is correctly configured to be aware of these allocations...who has experience with similar configuration(s) and might have a few minutes to discuss?

Thanks,

eric


========================================

On Fri, Apr 17, 2015 at 8:15 AM, Leon Clayton <lclayton@maprtech.com> wrote:
3 nodes, 36 drives per node. Initial usage as file archive with large files. 

10 mapr clients, all on 10GbE network. iperf between storage and hosts is around 8-9Gb/s both directions
My biggest throughput test so far:
- 10*10GB files, mem to mapr    (write)
- 10*10GB files, mapr to /dev/null    (read)
- 10 mapr volumes

In other words, each host generated one read and one write at the same time on a dedicated volume and the aggregated performance is:
- READ 288MB/s
- WRITE 654MB/s

with 3x replication for writes and with the addition of reads we are at or close to network saturation. Stands to be corrected. 

Regards
 
Leon Clayton
Solutions Architect  | +44 (0)7530 980566
MapR Technologies UK Ltd.

Now Available - Free Hadoop On-Demand Training

On 17 Apr 2015, at 07:09, Vivekananda Vellanki <vvellanki@maprtech.com> wrote:

Yes, warden needs to be restarted to use the additional NIC.

Can you share more details about the setup? How many nodes? What's the workload? How did you determine that the link is saturated? What else is running on these nodes?

On Thu, Apr 16, 2015 at 9:23 PM, David Tucker <dtucker@maprtech.com> wrote:
I share Keys’ believe that you’ll need to restart MFS to enable the extra interface (no ‘configure.sh -R’ needed).

Remember that you can’t restart mfs independently on cldb nodes … you’ll need to restart warden completely for those nodes.

— David

On Apr 16, 2015, at 6:56 AM, Leon Clayton <lclayton@maprtech.com> wrote:

understood and thank you. 
Regards
 
Leon Clayton
Solutions Architect  | +44 (0)7530 980566
MapR Technologies UK Ltd.

Now Available - Free Hadoop On-Demand Training

On 16 Apr 2015, at 14:54, kbotzum <kbotzum@maprtech.com> wrote:

You shouldn’t even need to run configure.sh -R unless you are planning on changing the IPs used by the CLDB or other services.

MapR will automatically detect the new IPs and start using them. I’m fairly sure that requires restarting the MFS but I’m not 100% certain.

Just remember this improves throughput,  not reliability. If you lose the primary network some services will not work properly - such as ZK.

Keys
_______________________________
Keys Botzum 
Senior Principal Technologist
kbotzum@mapr.com
443-718-0098 
MapR Technologies 
http://www.mapr.com



On Apr 16, 2015, at 9:50 AM, Leon Clayton <lclayton@maprtech.com> wrote:

Hello All

Need more bandwidth in a cluster. reaching limits of a single 10GbE per node. Looking for validation of the steps for bringing online another 10GbE interface per node. There will be no LACP. 

1. wire up the new port. 
2. assign an ip address. 
3. configure.sh -R
4. restart warden. 

Missed anything? Will MapR then use both interfaces as long as i have not set any mapr subnets? 

Regards


******************************************
Hi Latha,

Please make sure you have enough free RAM for :
DRILL_MAX_DIRECT_MEMORY + DRILL_MAX_HEAP .

Some other services like MFS may take significant part of OS RAM.

Thanks,
Hao

On Thu, Apr 23, 2015 at 8:24 AM, Sivasubramaniam, Latha <
Latha.Sivasubramaniam@aspect.com> wrote:

> Hao,
>
> The drillbit that crashed has outofmemory - heap space error - heap memory
> is set to 8GB (total RAM in the node is 32GB, document said to set the heap
> at 8GB). Also, I ran some new small queries, those ran fine. But I one of
> the query that was part of the test again, it showed on the web UI as
> pending for sometime and then disappeared. I will increase memory further
> and run the tests again.
>
>
> java.lang.OutOfMemoryError: Java heap space
>         at java.util.Arrays.copyOf(Arrays.java:2367) ~[na:1.7.0_45]
>         at
> java.lang.AbstractStringBuilder.expandCapacity(AbstractStringBuilder.java:130)
> ~[na:1.7.0_45]
>         at
> java.lang.AbstractStringBuilder.ensureCapacityInternal(AbstractStringBuilder.java:114)
> ~[na:1.7.0_45]
>         at
> java.lang.AbstractStringBuilder.append(AbstractStringBuilder.java:415)
> ~[na:1.7.0_45]
>         at java.lang.StringBuffer.append(StringBuffer.java:237)
> ~[na:1.7.0_45]
>         at java.io.StringWriter.write(StringWriter.java:112) ~[na:1.7.0_45]
>         at java.io.PrintWriter.write(PrintWriter.java:456) ~[na:1.7.0_45]
>         at java.io.PrintWriter.write(PrintWriter.java:473) ~[na:1.7.0_45]
>         at
> org.eigenbase.rel.AbstractRelNode$1.explain_(AbstractRelNode.java:386)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at org.eigenbase.rel.RelWriterImpl.done(RelWriterImpl.java:166)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at
> org.eigenbase.rel.AbstractRelNode.explain(AbstractRelNode.java:281)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at
> org.eigenbase.rel.AbstractRelNode.computeDigest(AbstractRelNode.java:391)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at
> org.eigenbase.rel.AbstractRelNode.recomputeDigest(AbstractRelNode.java:323)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at
> org.eigenbase.rel.AbstractRelNode.onRegister(AbstractRelNode.java:317)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at
> org.eigenbase.relopt.volcano.VolcanoPlanner.registerImpl(VolcanoPlanner.java:1464)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at
> org.eigenbase.relopt.volcano.VolcanoPlanner.register(VolcanoPlanner.java:837)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at
> org.eigenbase.relopt.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:860)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at
> org.eigenbase.relopt.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:49)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at
> org.eigenbase.rel.AbstractRelNode.onRegister(AbstractRelNode.java:301)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at
> org.eigenbase.relopt.volcano.VolcanoPlanner.registerImpl(VolcanoPlanner.java:1464)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at
> org.eigenbase.relopt.volcano.VolcanoPlanner.register(VolcanoPlanner.java:837)
> ~[optiq-core-0.9-drill-r20.jar:na]
>         at
> org.eigenbase.relopt.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:860)
> ~[optiq-core-0.9-drill-r20.jar:na]
>
> -----Original Message-----
> From: Hao Zhu [mailto:hzhu@maprtech.com]
> Sent: Wednesday, April 22, 2015 6:10 PM
> To: user@drill.apache.org
> Subject: Re: Query performance Comparison - Drill and Impala
>
> Any errors from each drillbit log?
> Can you run any simple drill query from any drillbit as of now?
>
>
> On Wed, Apr 22, 2015 at 5:09 PM, Sivasubramaniam, Latha <
> Latha.Sivasubramaniam@aspect.com> wrote:
>
> > Hi Hao,
> >
> > I am yet to run the tests with the queries you had asked for. I have
> > to get the results for 15 concurrent queries before that.  I am using
> > sqlline, so I had setup 15 terminals ( since running as a background
> > process does not seem to work) and submitted 15 queries. Initial
> > behavior was same as when I ran one query. But then, I don't see any
> > running queries on the web gui on any of the drillbit nodes. One of
> > the drill bit crashed and the rest of them seems to be hanging,
> > nothing in the in the drillbit log for the remaining drillbit nodes.
> >
> > It's been more than 2 hours. What can I check to see if these queries
> > are even being attempted or not?
> >
> > Thanks,
> > Latha
> >
> > -----Original Message-----
> > From: Hao Zhu [mailto:hzhu@maprtech.com]
> > Sent: Tuesday, April 21, 2015 3:27 PM
> > To: user@drill.apache.org
> > Subject: Re: Query performance Comparison - Drill and Impala
> >
> > Correction for item 1: Change "FAA" => "FCD1".
> >
> >
> > On Tue, Apr 21, 2015 at 3:11 PM, Hao Zhu <hzhu@maprtech.com> wrote:
> >
> > > Hi Team,
> > >
> > > Besides the 300+ seconds planning time, here are the performance
> > > differences in execution phase also.
> > >
> > > In general, this SQL contains 2 fact table joins, and with another
> > > 10+ dimension table joins.
> > > The 2 fact tables are:
> > >
> > >    - fact_agent_activity_detail_12m_partparq AS FAA
> > >    - fact_contact_detail_12m_partparq AS FCD1
> > >
> > > So here are the 2 major time differences:
> > > *1. PARQUET_ROW_GROUP_SCAN for FAA.* Impala spent 3s248ms to read
> > > FAA.
> > > Operator          #Hosts   Avg Time   Max Time    #Rows  Est. #Rows
> > > Peak Mem  Est. Peak Mem  Detail
> > > 15:HASH JOIN           4    2s728ms    2s774ms  145.81K      89.61K
> > >  531.03 MB        2.68 KB  RIGHT OUTER JOIN, PARTITIONED
> > > |--30:EXCHANGE         4    3.135ms   12.277ms  145.81K         192
> > >    0              0  HASH(FAA.Data_Source_Key,FA...
> > > |  00:SCAN HDFS        1    3s248ms    3s248ms  145.81K         192
> > >  107.93 MB      880.00 MB  default.fact_agent_activity...
> > > 29:EXCHANGE            4      9.8ms    9.826ms  399.94K      89.61K
> > >    0              0  HASH(FCD1.data_source_key,F...
> > > 01:SCAN HDFS           4    2s648ms    3s452ms  399.94K      89.61K
> > > 55.96 MB      960.00 MB  default.fact_contact_detail...
> > >
> > > Drill spent 26.934 seconds(Avg Process time) to read FAA.
> > > 02-xx-05 PARQUET_ROW_GROUP_SCAN 26.934
> > > 02-05       Scan(groupscan=[ParquetGroupScan
> [entries=[ReadEntryWithPath
> > > [path=hdfs://
> > > 10.153.25.119:8020/user/root/fact_contact_detail_12m_partparq/2014/1
> > > 2/
> > > 1_0_0.parquet],
> > > ReadEntryWithPath [path=hdfs://
> > > 10.153.25.119:8020/user/root/fact_contact_detail_12m_partparq/2014/1
> > > 2/
> > > 1_10_0.parquet],
> > > ReadEntryWithPath [path=hdfs://
> > > 10.153.25.119:8020/user/root/fact_contact_detail_12m_partparq/2014/1
> > > 2/
> > > 1_11_0.parquet],
> > > ReadEntryWithPath [path=hdfs://
> > > 10.153.25.119:8020/user/root/fact_contact_detail_12m_partparq/2014/1
> > > 2/
> > > 1_1_0.parquet],
> > > ReadEntryWithPath [path=hdfs://
> > > 10.153.25.119:8020/user/root/fact_contact_detail_12m_partparq/2014/1
> > > 2/
> > > 1_2_0.parquet],
> > > ReadEntryWithPath [path=hdfs://
> > > 10.153.25.119:8020/user/root/fact_contact_detail_12m_partparq/2014/1
> > > 2/
> > > 1_3_0.parquet],
> > > ReadEntryWithPath [path=hdfs://
> > > 10.153.25.119:8020/user/root/fact_contact_detail_12m_partparq/2014/1
> > > 2/
> > > 1_4_0.parquet],
> > > ReadEntryWithPath [path=hdfs://
> > > 10.153.25.119:8020/user/root/fact_contact_detail_12m_partparq/2014/1
> > > 2/
> > > 1_5_0.parquet],
> > > ReadEntryWithPath [path=hdfs://
> > > 10.153.25.119:8020/user/root/fact_contact_detail_12m_partparq/2014/1
> > > 2/
> > > 1_6_0.parquet],
> > > ReadEntryWithPath [path=hdfs://
> > > 10.153.25.119:8020/user/root/fact_contact_detail_12m_partparq/2014/1
> > > 2/
> > > 1_7_0.parquet],
> > > ReadEntryWithPath [path=hdfs://
> > > 10.153.25.119:8020/user/root/fact_contact_detail_12m_partparq/2014/1
> > > 2/
> > > 1_8_0.parquet],
> > > ReadEntryWithPath [path=hdfs://
> > > 10.153.25.119:8020/user/root/fact_contact_detail_12m_partparq/2014/1
> > > 2/
> > > 1_9_0.parquet]
> > > <http://10.153.25.119:8020/user/root/fact_contact_detail_12m_partpar
> > > q/
> > > 2014/12/1_9_0.parquet%5D>],
> > > selectionRoot=/user/root/fact_contact_detail_12m_partparq,
> > > numFiles=12,
> > > columns=[`*`]]]) : rowType = (DrillRecordRow[*,
> > > Call_Start_Date_Time, dir0,
> > > dir1]): rowcount = 8960530.0, cumulative cost = {8960530.0 rows,
> > > 3.584212E7 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 606703
> > >
> > > Could you compare the performance of below 2 SQLs:
> > > * Impala:*
> > >  select count(*) from
> > >  (select * from fact_contact_detail_12m_partparq AS FCD1 where
> > > Call_Start_Date_Time between '2014-12-03 00:00:00' AND '2014-12-03
> > > 13:00:00' AND FCD1.Year_key = 20140000 and FCD1.month_key =
> > > 20141200) tmp;
> > > * Drill:*
> > >  select count(*) from
> > >  (select * from fact_contact_detail_12m_partparq AS FCD1 where
> > > Call_Start_Date_Time between '2014-12-03 00:00:00' AND '2014-12-03
> > > 13:00:00' AND FCD1.dir0 = 2014 and FCD1.dir1 = 12) tmp;
> > >
> > > *2. Drill is using broadcast join for the 2 fact table while impala
> > > is using shuffle join.*
> > >
> > > Impala's shuffle join for 2 fact tables took 2s728ms.
> > > 15:HASH JOIN           4    2s728ms    2s774ms  145.81K      89.61K
> > >  531.03 MB        2.68 KB  RIGHT OUTER JOIN, PARTITIONED
> > >
> > > Drill's broadcast join for 2 fact tables caused 16.035 seconds avg
> > > waiting time.
> > > 01-xx-46 UNORDERED_RECEIVER 16.035
> > >
> > > 01-46    BroadcastExchange : rowType = RecordType(ANY $f1, ANY $f2, ANY
> > > $f3, ANY ITEM, ANY ITEM4, ANY ITEM5, ANY ITEM6, ANY ITEM7, ANY
> > > ITEM8, ANY ITEM9, ANY ITEM10, ANY ITEM11, ANY ITEM12, ANY ITEM13,
> > > ANY ITEM14, ANY
> > > ITEM15): rowcount = 50402.98125, cumulative cost =
> > > {2.7032798943749998E7 rows, 1.0056522825625E8 cpu, 0.0 io,
> > > 1.32128391168E10 network, 0.0 memory}, id = 606708
> > >
> > > The reason why Drill chose broadcast is because it estimates row
> > > count is only 50K(Below planner.broadcast_threshold, default 1000K).
> > > However the actual row count to broadcast is much higher than
> estimation.
> > >
> > > Needs to check why the estimation is not that accurate in this case.
> > >
> > > Thanks,
> > > Hao
> > >
> > > On Tue, Apr 21, 2015 at 2:32 PM, Jinfeng Ni <jni@apache.org> wrote:
> > >
> > >> The query you used is a 15-table join query. We know that Drill's
> > >> cost based optimizer will see performance overhead increase
> > >> significantly with increased # of tables joined, due to the
> > >> increased search space.  I'm not surprised to see that you had
> > >> 306.812 seconds for planning for such 15 table join.
> > >>
> > >> We plan to implement a heuristic planner in Drill, which will
> > >> provide another option, for large # of table join query.  Heuristic
> > >> planner should help in this case.
> > >>
> > >>
> > >> On Tue, Apr 21, 2015 at 1:48 PM, Sivasubramaniam, Latha <
> > >> Latha.Sivasubramaniam@aspect.com> wrote:
> > >>
> > >> > Hao,
> > >> >
> > >> > I have copied both query profiles to the link
> > >> >
> > >> https://drive.google.com/folderview?id=0ByB1-EsAGxA8fkRCSDhVcGtQS0N
> > >> GT jRuSlpGelVLMkxIVVFxYXMtU2JtQ3FaN2t3UTZpUUE&usp=sharing
> > >> >
> > >> > Please let me know if you cannot access.
> > >> >
> > >> > Appreciate any help.
> > >> >
> > >> > Thanks,
> > >> > Latha
> > >> >
> > >> > -----Original Message-----
> > >> > From: Hao Zhu [mailto:hzhu@maprtech.com]
> > >> > Sent: Tuesday, April 21, 2015 11:31 AM
> > >> > To: user@drill.apache.org
> > >> > Subject: Re: Query performance Comparison - Drill and Impala
> > >> >
> > >> > Hi Latha,
> > >> >
> > >> > Do you have the complete SQL profiles for the same query on
> > >> > impala and Drill?
> > >> > For Impala, you can run "profile;" command after the SQL finished.
> > >> > For Drill, you can go to the web GUI, and copy/paste the complete
> > >> > "Full JSON Profile".
> > >> >
> > >> > I want to compare what is the major performance difference
> > >> > between them two.
> > >> >
> > >> > Thanks,
> > >> > Hao
> > >> >
> > >> > On Tue, Apr 21, 2015 at 10:43 AM, Sivasubramaniam, Latha <
> > >> > Latha.Sivasubramaniam@aspect.com> wrote:
> > >> >
> > >> > > Neeraja, I removed some columns from select list and shortened
> > >> > > aliases to get the query manageable and got the explain plan
> > working finally.
> > >> > > The errors were really random.
> > >> > >
> > >> > > Jacques,
> > >> > >
> > >> > > Most of it seems to be Hashjoin and yes the planning time took
> > >> > > 306 seconds.  Is there a way to improve the query performance.?
> > >> > > Below is the snapshot of what I see in the query plan and I
> > >> > > have copied the query also to the end of the email.
> > >> > >
> > >> > >
> > >> > > Resource_Group_Desc=[$7], Call_Action_Desc=[$20], ITEM7=[$9],
> > >> > > ITEM8=[$10], ITEM9=[$11], ITEM10=[$12], ITEM11=[$13],
> > >> > > ITEM12=[$14], ITEM13=[$15], ITEM0=[$16], ITEM14=[$17],
> > >> > > ITEM15=[$18])
> > >> > > 01-29
> > >> > > HashJoin(condition=[=($19, $8)], joinType=[left])
> > >> > > 01-31
> > >> > > Project(Call_Id_Source=[$0], Agent_Key=[$1], Site_Key=[$2],
> > >> > > Workgroup_Key=[$3], Status_Key=[$4], Reason_Key=[$5],
> > >> > > Call_Type_Desc=[$6], Resource_Group_Desc=[$20], ITEM6=[$8],
> > >> > > ITEM7=[$9], ITEM8=[$10], ITEM9=[$11], ITEM10=[$12],
> > >> > > ITEM11=[$13], ITEM12=[$14], ITEM13=[$15], ITEM0=[$16],
> > >> > > ITEM14=[$17],
> > >> > > ITEM15=[$18])
> > >> > > 01-32
> > >> > > HashJoin(condition=[=($7, $19)], joinType=[left])
> > >> > > 01-34
> > >> > > Project(Call_Id_Source=[$0], Agent_Key=[$1], Site_Key=[$2],
> > >> > > Workgroup_Key=[$3], Status_Key=[$4], Reason_Key=[$5],
> > >> > > Call_Type_Desc=[$20], ITEM5=[$7], ITEM6=[$8], ITEM7=[$9],
> > >> > > ITEM8=[$10], ITEM9=[$11], ITEM10=[$12], ITEM11=[$13],
> > >> > > ITEM12=[$14], ITEM13=[$15], ITEM0=[$16], ITEM14=[$17],
> > >> > > ITEM15=[$18])
> > >> > > 01-35
> > >> > > HashJoin(condition=[=($6, $19)], joinType=[left])
> > >> > > 01-37
> > >> > > Project(Call_Id_Source=[$0], Agent_Key=[$1], Site_Key=[$2],
> > >> > > Workgroup_Key=[$3], Status_Key=[$4], Reason_Key=[$5],
> > >> > > ITEM4=[$7], ITEM5=[$8], ITEM6=[$9], ITEM7=[$10], ITEM8=[$11],
> > >> > > ITEM9=[$12], ITEM10=[$13], ITEM11=[$14], ITEM12=[$15],
> > >> > > ITEM13=[$16], ITEM0=[$17], ITEM14=[$18], ITEM15=[$19])
> > >> > > 01-38
> > >> > > HashJoin(condition=[=($6, $20)], joinType=[left])
> > >> > > 01-40
> > >> > > Project(Call_Id_Source=[$0], Agent_Key=[$2], Site_Key=[$3],
> > >> > > Workgroup_Key=[$4], Status_Key=[$5], Reason_Key=[$6],
> > >> > > ITEM=[$7], ITEM4=[$8], ITEM5=[$9], ITEM6=[$10], ITEM7=[$11],
> > >> > > ITEM8=[$12], ITEM9=[$13], ITEM10=[$14], ITEM11=[$15],
> > >> > > ITEM12=[$16], ITEM13=[$17], ITEM0=[$7], ITEM14=[$18],
> > >> > > ITEM15=[$19])
> > >> > > 01-41
> > >> > > HashJoin(condition=[=($20, $1)], joinType=[left])
> > >> > > 01-43
> > >> > > Project(Call_Id_Source=[$1], Service_Key=[$3], Agent_Key=[$4],
> > >> > > Site_Key=[$5], Workgroup_Key=[$6], Status_Key=[$7],
> > >> > > Reason_Key=[$8], ITEM=[$15], ITEM4=[$16], ITEM5=[$17],
> > >> > > ITEM6=[$18], ITEM7=[$19], ITEM8=[$20], ITEM9=[$21],
> > >> > > ITEM10=[$22], ITEM11=[$23], ITEM12=[$24], ITEM13=[$25],
> > >> > > ITEM14=[$26],
> > >> > > ITEM15=[$27])
> > >> > > 01-45
> > >> > > HashJoin(condition=[AND(=($0, $12), =($1, $13), =($2, $14))],
> > >> > > joinType=[left])
> > >> > > 01-47
> > >> > > SelectionVectorRemover
> > >> > > 01-48
> > >> > > Filter(condition=[AND(>=($9, '2014-12-03 00:00:00'), <=($9,
> > >> > > '2014-12-03 13:00:00'), =($10, 2014), =($11, 12))])
> > >> > > 01-49
> > >> > > Project(Data_Source_Key=[$9], Call_Id_Source=[$4],
> > >> > > Sequence_Number=[$8], Service_Key=[$11], Agent_Key=[$10],
> > >> > > Site_Key=[$2], Workgroup_Key=[$5], Status_Key=[$3],
> > >> > > Reason_Key=[$7], Start_Date_Time=[$6], dir0=[$1],
> > >> > > dir1=[$0])
> > >> > > 01-50
> > >> > >   Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath
> > >> > > [path=hdfs://
> > >> > >
> > >> 10.153.25.119:8020/user/root/fact_agent_activity_detail_12m_partpar
> > >> q/
> > >> 2
> > >> > > 014/12/1_0_0.parquet],
> > >> > > ReadEntryWithPath [path=hdfs://
> > >> > >
> > >> 10.153.25.119:8020/user/root/fact_agent_activity_detail_12m_partpar
> > >> q/
> > >> 2
> > >> > > 014/12/1_1_0.parquet],
> > >> > > ReadEntryWithPath [path=hdfs://
> > >> > >
> > >> 10.153.25.119:8020/user/root/fact_agent_activity_detail_12m_partpar
> > >> q/
> > >> 2
> > >> > > 014/12/1_2_0.parquet],
> > >> > > ReadEntryWithPath [path=hdfs://
> > >> > >
> > >> 10.153.25.119:8020/user/root/fact_agent_activity_detail_12m_partpar
> > >> q/
> > >> 2
> > >> > > 014/12/1_3_0.parquet],
> > >> > > ReadEntryWithPath [path=hdfs://
> > >> > >
> > >> 10.153.25.119:8020/user/root/fact_agent_activity_detail_12m_partpar
> > >> q/
> > >> 2
> > >> > > 014/12/1_4_0.parquet],
> > >> > > ReadEntryWithPath [path=hdfs://
> > >> > >
> > >> 10.153.25.119:8020/user/root/fact_agent_activity_detail_12m_partpar
> > >> q/
> > >> 2
> > >> > > 014/12/1_5_0.parquet],
> > >> > > ReadEntryWithPath [path=hdfs://
> > >> > >
> > >> 10.153.25.119:8020/user/root/fact_agent_activity_detail_12m_partpar
> > >> q/
> > >> 2
> > >> > > 014/12/1_6_0.parquet],
> > >> > > ReadEntryWithPath [path=hdfs://
> > >> > >
> > >> 10.153.25.119:8020/user/root/fact_agent_activity_detail_12m_partpar
> > >> q/
> > >> 2
> > >> > > 014/12/1_7_0.parquet],
> > >> > > ReadEntryWithPath [path=hdfs://
> > >> > >
> > >> 10.153.25.119:8020/user/root/fact_agent_activity_detail_12m_partpar
> > >> q/
> > >> 2
> > >> > > 014/12/1_8_0.parquet],
> > >> > > ReadEntryWithPath [path=hdfs://
> > >> > >
> > >> 10.153.25.119:8020/user/root/fact_agent_activity_detail_12m_partpar
> > >> q/
> > >> 2
> > >> > > 014/12/1_9_0.parquet]],
> > >> > > selectionRoot=/user/root/fact_agent_activity_detail_12m_partpar
> > >> > > q, numFiles=10, columns=[`Data_Source_Key`, `Call_Id_Source`,
> > >> > > `Sequence_Number`, `Service_Key`, `Agent_Key`, `Site_Key`,
> > >> > > `Workgroup_Key`, `Status_Key`, `Reason_Key`, `Start_Date_Time`,
> > >> > > `dir0`, `dir1`]]])
> > >> > > 01-46
> > >> > > BroadcastExchange
> > >> > > 02-01
> > >> > > Project($f1=[ITEM($0, 'Data_Source_Key')], $f2=[ITEM($0,
> > >> > > 'Call_Id_Source')], $f3=[ITEM($0, 'Sequence_Number')],
> > >> > > ITEM=[ITEM($0, 'Service_Key')], ITEM4=[ITEM($0,
> > >> > > 'Call_Type_Key')], ITEM5=[ITEM($0, 'Resource_Group_Key')],
> > >> > > ITEM6=[ITEM($0, 'Call_Action_Key')], ITEM7=[ITEM($0,
> > >> > > 'Agent_Disp_Key')], ITEM8=[ITEM($0, 'Second_Party_Agent_Key')],
> > >> > > ITEM9=[ITEM($0, 'Call_Action_Reason_Key')], ITEM10=[ITEM($0,
> > >> > > 'ANI')], ITEM11=[ITEM($0, 'DNIS')], ITEM12=[ITEM($0,
> > >> > > 'Begin_Date_Time')], ITEM13=[ITEM($0, 'Sequence_Number')],
> > >> > > ITEM14=[ITEM($0, 'Dur_Short_Call')], ITEM15=[ITEM($0,
> > >> > > 'Call_Category_Key')])
> > >> > > 02-02
> > >> > > SelectionVectorRemover
> > >> > > 02-03
> > >> > >   Filter(condition=[AND(>=($1, '2014-12-03 00:00:00'), <=($1,
> > >> > > '2014-12-03 13:00:00'), =($2, 2014), =($3, 12))])
> > >> > > 02-04
> > >> > >     Project(T8¦¦*=[$0], Call_Start_Da |
> > >> > > +------------+------------+
> > >> > > 1 row selected (306.812 seconds)
> > >> > > 0: jdbc:drill:zk=rtr-poc-imp1:2181>
> > >> > >
> > >> > >
> > >> > > ************ Query ******************** SELECT DSQ.Site_Desc,
> > >> > > DWAE.Workgroup_Desc, DASAE.Agent_Status_Key,
> > >> > > DCTCD.Call_Type_Desc, DCA.Call_Action_Desc, FCD.ANI, FCD.DNIS,
> > >> > > FCD.Begin_Date_Time, DD.Disposition_Desc, DAAE.Full_Name ,
> > >> > >
> > >> > > DRGAE.Resource_Group_Desc, FCD.Sequence_Number,
> > >> > > FCD.Service_Key, FCD.Dur_Short_Call, FAA.Call_Id_Source,
> > >> > > FCD.Call_Category_Key FROM
> > >> > > fact_agent_activity_detail_12m_partparq AS FAA LEFT OUTER JOIN
> > >> > > (select
> > >> > > * from fact_contact_detail_12m_partparq AS FCD1 where
> > >> > > Call_Start_Date_Time between '2014-12-03 00:00:00' AND
> > >> > > '2014-12-03 13:00:00' AND FCD1.dir0 = 2014 and FCD1.dir1 = 12
> > >> > >
> > >> > > ) AS FCD
> > >> > > ON FAA.Data_Source_Key = FCD.Data_Source_Key AND
> > >> > > FAA.Call_Id_Source = FCD.Call_Id_Source AND FAA.Sequence_Number
> > >> > > = FCD.Sequence_Number LEFT OUTER JOIN DIM_Services_Parq AS
> > >> > > DSAAD ON DSAAD.Service_Key = FAA.Service_Key LEFT OUTER JOIN
> > >> > > DIM_Services_Parq AS DSCD ON FCD.Service_Key = DSCD.Service_Key
> > >> > > LEFT OUTER JOIN DIM_Call_Types_Parq AS DCTCD ON
> > >> > > FCD.Call_Type_Key = DCTCD.Call_Type_Key LEFT OUTER JOIN
> > >> > > DIM_Resource_Groups_parq AS DRGAE ON FCD.Resource_Group_Key =
> > >> > > DRGAE.Resource_Group_Key LEFT OUTER JOIN DIM_Call_Actions_parq
> > >> > > AS DCA ON DCA.Call_Action_Key = FCD.Call_Action_Key INNER JOIN
> > >> > > DIM_Agents_Parquet AS DAAE ON FAA.Agent_Key = DAAE.Agent_Key
> > >> > > INNER JOIN DIM_Sites_Parq AS DSQ ON FAA.Site_Key = DSQ.Site_Key
> > >> > > INNER JOIN DIM_Workgroups_parq AS
> > DWAE ON FAA.Workgroup_Key = DWAE.Workgroup_Key
> > >> > > LEFT OUTER JOIN    DIM_Dispositions_parq AS DD ON
> > FCD.Agent_Disp_Key =
> > >> > > DD.Disposion_Key
> > >> > > LEFT OUTER JOIN DIM_Agents_Parquet AS DACD ON
> > >> > > FCD.Second_Party_Agent_Key = DACD.Agent_Key LEFT OUTER JOIN
> > >> > > DIM_Call_Action_Reasons_parq AS DAAR ON
> > >> > > FCD.Call_Action_Reason_Key = DAAR.Call_Action_Reason_Key LEFT
> > >> > > OUTER JOIN DIM_Agent_Status_parq AS DASAE ON FAA.Status_Key =
> > >> > > DASAE.Agent_Status_Key LEFT OUTER JOIN
> > >> > > DIM_Agent_Status_Reasons_parq AS DARAE ON FAA.Reason_Key =
> > >> > > DARAE.Agent_Status_Reason_Key WHERE (DWAE.Workgroup_Key IN (
> > >> > > 3,1)
> > >> > >
> > >> > >  AND
> > >> > > (FAA.Start_Date_Time between '2014-12-03 00:00:00' AND
> > >> > > '2014-12-03 13:00:00' AND FAA.dir0 = 2014 and FAA.dir1 = 12));
> > >> > >
> > >> > >
> > >> > > Appreciate your help.
> > >> > >
> > >> > > Thanks,
> > >> > > Latha
> > >> > >
> > >> > >
> > >> > >
> > >> > >
> > >> > >
> > >> > > -----Original Message-----
> > >> > > From: Neeraja Rentachintala
> > >> > > [mailto:nrentachintala@maprtech.com]
> > >> > > Sent: Monday, April 20, 2015 9:22 PM
> > >> > > To: user@drill.apache.org
> > >> > > Subject: Re: Query performance Comparison - Drill and Impala
> > >> > >
> > >> > > Latha
> > >> > > What error are you getting when doing explain plan.
> > >> > >
> > >> > > On Mon, Apr 20, 2015 at 4:40 PM, Sivasubramaniam, Latha <
> > >> > > Latha.Sivasubramaniam@aspect.com> wrote:
> > >> > >
> > >> > > > I am unable to do an explain plan for this query.  Is there a
> > >> > > > way to pass a sql file for the Explain plan command. Query
> > >> > > > runs fine from the file with the !run command though. Will
> > >> > > > try to fix the errors and see if I can get the query plan.
> > >> > > >
> > >> > > > Thanks,
> > >> > > > latha
> > >> > > >
> > >> > > > -----Original Message-----
> > >> > > > From: Jacques Nadeau [mailto:jacques@apache.org]
> > >> > > > Sent: Monday, April 20, 2015 4:12 PM
> > >> > > > To: user@drill.apache.org
> > >> > > > Subject: Re: Query performance Comparison - Drill and Impala
> > >> > > >
> > >> > > > Can you confirm you are getting broadcast joins for all the
> > >> > > > dimension tables? Additionally, can you confirm the planning
> time?
> > >> > > > I'm guessing both might be an issue given the times you are
> > seeing.
> > >> > > > On Apr 20, 2015 6:46 PM, "Sivasubramaniam, Latha" <
> > >> > > > Latha.Sivasubramaniam@aspect.com> wrote:
> > >> > > >
> > >> > > > > Hi,
> > >> > > > >
> > >> > > > > I am running performance tests for our use cases on Impala
> > >> > > > > and Drill to make a choice and pick one for our Data
> > >> > > > > warehouse implementation on top of Hadoop.  I used the same
> > >> > > > > cluster for both
> > >> > the tests.
> > >> > > > > Select Query that takes ~20 seconds in impala, takes around
> > >> > > > > 440 seconds in drill on same hardware cluster (nearly 20
> > >> > > > > times
> > more).
> > >> > > > > I would like to make sure that I am not missing anything in
> > >> > > > > tuning or
> > >> > > configuration.
> > >> > > > > Below is our configuration, appreciate comments on if these
> > >> > > > > performance numbers are as expected or if any tuning can be
> > >> > > > > done to
> > >> > > > improve drill performance.
> > >> > > > >
> > >> > > > > Hadoop Cluster configuration and data set:
> > >> > > > >
> > >> > > > >
> > >> > > > > *         5 nodes cluster, 1 name node and 4 data nodes, 32GB
> > >> memory
> > >> > ,
> > >> > > 4
> > >> > > > > cpus on each node.
> > >> > > > >
> > >> > > > > *         There are two main fact tables. 98 million records
> in
> > >> one
> > >> > and
> > >> > > > 87
> > >> > > > > million records in another. There are around 10 dimension
> > tables.
> > >> > > > > For now, we are running the tests with normalized schema
> > >> > > > > from out data
> > >> > > > warehouse.
> > >> > > > >
> > >> > > > > *         Running the same query against Impala and Drill
> except
> > >> for
> > >> > > the
> > >> > > > > syntax differences everything is same.
> > >> > > > >
> > >> > > > > Impala Configuration
> > >> > > > >
> > >> > > > >
> > >> > > > > *         CDH 5.3.2 with Impala 2.1.2 version
> > >> > > > >
> > >> > > > > *         4 impala daemons running on 4 data nodes.
> > >> > > > >
> > >> > > > > *          Impala daemons memory set to 25GB.
> > >> > > > >
> > >> > > > > *         Partitioned parquet tables were created from text
> > format
> > >> > hdfs
> > >> > > > > files using impala itself (CTAS command).
> > >> > > > >
> > >> > > > > *         Parquet block size 256MB
> > >> > > > >
> > >> > > > > Drill Configuration
> > >> > > > >
> > >> > > > >
> > >> > > > > *         Drill 0.8 version, added drill on the CDH cluster
> > setup
> > >> > > > >
> > >> > > > > *         4 drillbits running on 4 data nodes
> > >> > > > >
> > >> > > > > *         Drillbits memory set to 16GB and heap memory set to
> > >> 8GB.  I
> > >> > > ran
> > >> > > > > with 8GB memory and 4GB for heap, there was no change in
> > >> > > > > the query
> > >> > > > timings.
> > >> > > > >
> > >> > > > > *         Partitioned parquet files.
> > >> > > > >
> > >> > > > > SQL Query
> > >> > > > >
> > >> > > > > SELECT
> > >> > > > >
> > >> > > > >                 DIM_Sites_parq.Site_Desc,
> > >> > > > >
> > >> > > > >
>  DIM_Workgroups_AgentEventDetails.Workgroup_Desc,
> > >> > > > >                 DIM_Agents_AgentEventDetails.Last_Name,
> > >> > > > >
> > >> > > > >                 DIM_Agents_AgentEventDetails.First_Name,
> > >> > > > >
> > >> > > > >
> > >> > > > >
> > >>  DIM_Agent_Status_AgentEventDetails.Agent_Status_Key,
> > >> > > > >
> > >> > > > > DIM_CallTypes_ContactDetails.Call_Type_Desc,
> > >> > > > >
> > >> > > > >
> > >> > > > >
> > >> > > > >                 DIM_Call_Actions.Call_Action_Desc,
> > >> > > > >
> > >> > > > >                 FACT_Contact_Detail.ANI,
> > >> > > > >                 FACT_Contact_Detail.DNIS,
> > >> > > > >
> > >> > > > >                 FACT_Contact_Detail.Begin_Date_Time,
> > >> > > > >                 FACT_Contact_Detail.Call_Start_Date_Time,
> > >> > > > >
> > >> > > > >                 FACT_Contact_Detail.Orig_DNIS,
> > >> > > > >                 FACT_Agent_Activity_Detail.Start_Date_Time,
> > >> > > > >                 FACT_Agent_Activity_Detail.End_Date_Time,
> > >> > > > >                 FACT_Agent_Activity_Detail.Dur_Time,
> > >> > > > >
> > >> > > > >                 FACT_Agent_Activity_Detail.Sequence_Number,
> > >> > > > >
> > >> > > > >                 FACT_Contact_Detail.Begin_Date_Time,
> > >> > > > >                 FACT_Contact_Detail.Call_Start_Date_Time,
> > >> > > > >                 FACT_Agent_Activity_Detail.Start_Date_Time,
> > >> > > > >                 FACT_Agent_Activity_Detail.End_Date_Time,
> > >> > > > >
> > >> > > > >
> > >> > > > >                 DIM_Dispositions.Disposition_Desc,
> > >> > > > >                 DIM_Agents_AgentEventDetails.Full_Name ,
> > >> > > > >
> > >> > > > >
> > >> > > > > DIM_Resource_Groups_AgentEventDetails.Resource_Group_Desc,
> > >> > > > >
> > >> > > > >                 FACT_Contact_Detail.Sequence_Number,
> > >> > > > >
> > >> > > > >                 FACT_Contact_Detail.Service_Key,
> > >> > > > >                 FACT_Contact_Detail.Station,
> > >> > > > >                 FACT_Contact_Detail.Near_Off_Hook_Date_Time,
> > >> > > > >                 FACT_Contact_Detail.Far_Off_Hook_Date_Time,
> > >> > > > >                 FACT_Contact_Detail.Near_On_Hook_Date_Time,
> > >> > > > >                 FACT_Contact_Detail.Far_On_Hook_Date_Time,
> > >> > > > >
> > >> > > > > FACT_Contact_Detail.Begin_Greeting_Date_Time,
> > >> > > > >
> > >> > > > >                 FACT_Contact_Detail.End_Guard_Date_Time,
> > >> > > > >                 FACT_Contact_Detail.Resource_Group_Type,
> > >> > > > >                 FACT_Contact_Detail.Info_Digits,
> > >> > > > >                 FACT_Contact_Detail.Agent_Site_Key,
> > >> > > > >                 FACT_Contact_Detail.Agent_Switch_Key,
> > >> > > > >
> > >> > > > >                 FACT_Contact_Detail.Line_Number,
> > >> > > > >                 FACT_Contact_Detail.Queue_Start_Date_Time,
> > >> > > > >
> > >> > > > >                 FACT_Contact_Detail.CallQ_Start_Date_Time,
> > >> > > > >                 FACT_Contact_Detail.Queue_End_Date_Time,
> > >> > > > >                 FACT_Contact_Detail.Dur_Queue_Time,
> > >> > > > >                 FACT_Contact_Detail.Conn_Clear_Date_Time,
> > >> > > > >                 FACT_Contact_Detail.Wrap_End_Date_Time,
> > >> > > > >
> > >> > > > > FACT_Contact_Detail.Answer_Flag,
> > >> > > > >
> > >> > > > > FACT_Contact_Detail.Call_End_Date_Time,
> > >> > > > >  FACT_Contact_Detail.Script_Id_Source,
> > >> > > > >
> > >> > > > >   FACT_Contact_Detail.IPNIQ_ASBR_Flag,
> > >> > > > >  FACT_Contact_Detail.Reserved_Start_Date_Time,
> > >> > > > >
> > >> > > > >   FACT_Contact_Detail.Reserved_End_Date_Time,
> > >> > > > >  FACT_Contact_Detail.Dur_IPNIQ_Q_Time,
> > >> > > > >  FACT_Contact_Detail.SeqLeg_Number,
> > >> > > > >
> > >> > > > > FACT_Contact_Detail.Num_Accept_Responses,
> > >> > > > >  FACT_Contact_Detail.Num_Reject_Responses,
> > >> > > > >  FACT_Contact_Detail.Preview_Start_Date_Time,
> > >> > > > >
> > >> > > > >   FACT_Contact_Detail.Preview_End_Date_Time,
> > >> > > > >
> > >> > > > > FACT_Contact_Detail.Detection_Date_Time,
> > >> > > > >  FACT_Contact_Detail.Answer_Date_Time,
> > >> > > > >  FACT_Contact_Detail.Record_Number,
> > >> > > > >  FACT_Contact_Detail.Detection_Type,
> > >> > > > >  FACT_Contact_Detail.Calling_Party_Name,
> > >> > > > >  FACT_Contact_Detail.Num_Phone_Rings,
> > >> > > > >
> > >> > > > >   FACT_Contact_Detail.End_Greeting_Date_Time,
> > >> > > > >  FACT_Contact_Detail.Phone_Start_Ringing_Date_Time,
> > >> > > > >
> > >> > > > >   FACT_Contact_Detail.Begin_Greeting_Date_Time,
> > >> > > > >  FACT_Contact_Detail.Begin_Msg_Playback_Date_Time,
> > >> > > > >  FACT_Contact_Detail.Xfer_Command_Date_Time,
> > >> > > > >  FACT_Contact_Detail.Connect_Date_Time,
> > >> > > > >  FACT_Contact_Detail.Second_Party_Park_Flag,
> > >> > > > >  FACT_Contact_Detail.Dur_Short_Call,
> > >> > > > >  FACT_Agent_Activity_Detail.Call_Id_Source,
> > >> > > > >
> > >> > > > >   FACT_Contact_Detail.Call_Category_Key,
> > >> > > > >
> > >> > > > >   DIM_Agents_AgentEventDetailsSecondParty.User_Id_Source
> > >> > > > >
> > >> > > > >   FROM
> > >> > > > >     hdfs.rootparq.`fact_agent_activity_detail_12m_partparq`
> > >> > > > > AS FACT_Agent_Activity_Detail
> > >> > > > >
> > >> > > > >                 LEFT OUTER JOIN
> > >> > > > >
> > >> > > > >  (select * from
> > >> > > > > hdfs.rootparq.`fact_contact_detail_12m_partparq`
> > >> > > > > where Call_Start_Date_Time
> > >> > > > >
> > >> > > > > between
> > >> > > > >
> > >> > > > > '2014-12-03 00:00:00' AND '2014-12-03 13:00:00' AND
> > >> > > > > fact_contact_detail_12m_partparq.dir0 = 2014 and
> > >> > > > > fact_contact_detail_12M_partParq.dir1 = 12 ) AS
> > >> > > > > FACT_Contact_Detail ON
> > >> > > > >        FACT_Agent_Activity_Detail.Data_Source_Key =
> > >> > > > > FACT_Contact_Detail.Data_Source_Key
> > >> > > > >                 AND
> > >> > > > >        FACT_Agent_Activity_Detail.Call_Id_Source =
> > >> > > > > FACT_Contact_Detail.Call_Id_Source
> > >> > > > >                 AND
> > >> > > > >        FACT_Agent_Activity_Detail.Sequence_Number =
> > >> > > > > FACT_Contact_Detail.Sequence_Number
> > >> > > > >
> > >> > > > >
> > >> > > > >                 LEFT OUTER JOIN
> > >> > > > >                    DIM_Services_Parq AS
> > >> > > > > DIM_Services_Agent_Activity_Detail
> > >> > > > > ON DIM_Services_Agent_Activity_Detail.Service_Key =
> > >> > > > > FACT_Agent_Activity_Detail.Service_Key
> > >> > > > >
> > >> > > > >                 LEFT OUTER JOIN
> > >> > > > >     DIM_Services_Parq AS DIM_Services_Contact_Detail ON
> > >> > > > > FACT_Contact_Detail.Service_Key =
> > >> > > > > DIM_Services_Contact_Detail.Service_Key
> > >> > > > >
> > >> > > > >                 LEFT OUTER JOIN
> > >> > > > >     DIM_Call_Types_Parq AS DIM_CallTypes_ContactDetails ON
> > >> > > > >        FACT_Contact_Detail.Call_Type_Key =
> > >> > > > > DIM_CallTypes_ContactDetails.Call_Type_Key
> > >> > > > >
> > >> > > > >                 LEFT OUTER JOIN
> > >> > > > >     DIM_Resource_Groups_parq AS
> > >> > > > > DIM_Resource_Groups_AgentEventDetails
> > >> > > ON
> > >> > > > >        FACT_Contact_Detail.Resource_Group_Key =
> > >> > > > > DIM_Resource_Groups_AgentEventDetails.Resource_Group_Key
> > >> > > > >
> > >> > > > >
> > >> > > > >                 LEFT OUTER JOIN
> > >> > > > >     DIM_Call_Actions_parq AS DIM_Call_Actions ON
> > >> > > > > DIM_Call_Actions.Call_Action_Key =
> > >> > > > > FACT_Contact_Detail.Call_Action_Key
> > >> > > > >
> > >> > > > >                 INNER JOIN
> > >> > > > >         DIM_Agents_Parquet AS DIM_Agents_AgentEventDetails
> > >> > > > > ON FACT_Agent_Activity_Detail.Agent_Key =
> > >> > > > > DIM_Agents_AgentEventDetails.Agent_Key
> > >> > > > >
> > >> > > > >                 INNER JOIN
> > >> > > > >         DIM_Sites_Parq ON
> > >> > > > > FACT_Agent_Activity_Detail.Site_Key
> > >> > > > > = DIM_Sites_Parq.Site_Key
> > >> > > > >
> > >> > > > >                 INNER JOIN
> > >> > > > >         DIM_Workgroups_parq AS
> > >> > > > > DIM_Workgroups_AgentEventDetails
> > ON
> > >> > > > >        FACT_Agent_Activity_Detail.Workgroup_Key =
> > >> > > > > DIM_Workgroups_AgentEventDetails.Workgroup_Key
> > >> > > > >
> > >> > > > >                 LEFT OUTER JOIN
> > >> > > > >     DIM_Dispositions_parq AS DIM_Dispositions ON
> > >> > > > > FACT_Contact_Detail.Agent_Disp_Key =
> > >> > > > > DIM_Dispositions.Disposion_Key
> > >> > > > >
> > >> > > > >                 LEFT OUTER JOIN
> > >> > > > >     DIM_Agents_Parquet AS
> > >> > > > > DIM_Agents_AgentEventDetailsSecondParty
> > >> ON
> > >> > > > >        FACT_Contact_Detail.Second_Party_Agent_Key =
> > >> > > > > DIM_Agents_AgentEventDetailsSecondParty.Agent_Key
> > >> > > > >
> > >> > > > >                 LEFT OUTER JOIN
> > >> > > > >     DIM_Call_Action_Reasons_parq AS DIM_Call_Action_Reasons
> > >> > > > > ON FACT_Contact_Detail.Call_Action_Reason_Key =
> > >> > > > > DIM_Call_Action_Reasons.Call_Action_Reason_Key
> > >> > > > >
> > >> > > > >                 LEFT OUTER JOIN
> > >> > > > >     DIM_Agent_Status_parq AS
> > >> > > > > DIM_Agent_Status_AgentEventDetails
> > ON
> > >> > > > >        FACT_Agent_Activity_Detail.Status_Key =
> > >> > > > > DIM_Agent_Status_AgentEventDetails.Agent_Status_Key
> > >> > > > >
> > >> > > > >                 LEFT OUTER JOIN
> > >> > > > >     DIM_Agent_Status_Reasons_parq AS
> > >> > > > > DIM_Agent_Reason_AgentEventDetails
> > >> > > > ON
> > >> > > > >        FACT_Agent_Activity_Detail.Reason_Key =
> > >> > > > > DIM_Agent_Reason_AgentEventDetails.Agent_Status_Reason_Key
> > >> > > > >
> > >> > > > >
> > >> > > > > WHERE
> > >> > > > >  (
> > >> > > > >
> > >> > > > > DIM_Workgroups_AgentEventDetails.Workgroup_Key  IN  ( 3,1)
> > >> > > > >
> > >> > > > >   AND
> > >> > > > >
> > >> > > > >
> > >> > > > >    ( FACT_Agent_Activity_Detail.Start_Date_Time
> > >> > > > >
> > >> > > > > between
> > >> > > > > '2014-12-03 00:00:00' AND '2014-12-03 13:00:00' AND
> > >> > > > > FACT_Agent_Activity_Detail.dir0 = 2014 and
> > >> > > > > FACT_Agent_Activity_Detail.dir1 =
> > >> > > > >
> > >> > > > > 12
> > >> > > > >    )
> > >> > > > >
> > >> > > > >   );
> > >> > > > >
> > >> > > > >
> > >> > > > > Thanks,
> > >> > > > > Latha
> > >> > > > >
> > >> > > > >
> > >> > > > > This email (including any attachments) is proprietary to
> > >> > > > > Aspect Software, Inc. and may contain information that is
> > confidential.
> > >> > > > > If you have received this message in error, please do not
> > >> > > > > read, copy or
> > >> > > > forward this message.
> > >> > > > > Please notify the sender immediately, delete it from your
> > >> > > > > system and destroy any copies. You may not further disclose
> > >> > > > > or distribute this email or its attachments.
> > >> > > > >
> > >> > > > This email (including any attachments) is proprietary to
> > >> > > > Aspect Software, Inc. and may contain information that is
> > >> > > > confidential. If you have received this message in error,
> > >> > > > please do not read, copy or
> > >> > > forward this message.
> > >> > > > Please notify the sender immediately, delete it from your
> > >> > > > system and destroy any copies. You may not further disclose
> > >> > > > or distribute this email or its attachments.
> > >> > > >
> > >> > > This email (including any attachments) is proprietary to Aspect
> > >> > > Software, Inc. and may contain information that is confidential.
> > >> > > If you have received this message in error, please do not read,
> > >> > > copy or
> > >> > forward this message.
> > >> > > Please notify the sender immediately, delete it from your
> > >> > > system and destroy any copies. You may not further disclose or
> > >> > > distribute this email or its attachments.
> > >> > >
> > >> >
> > >>
> > >
> > >
