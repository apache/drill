---
title: "Configuring a Cluster for Different Workloads"
parent: "Configuration Options"
---
In this release of Drill, to configure a Drill cluster for different workloads, you re-allocate memory resources only. Currently, you do not configure disk or CPU resources because Drill shares disk and CPU resources with other services. To re-allocate memory from the default settings to other settings based on your performance testing. Currently, you configuration changes to memory re-allocation 

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

continued later
*********************************

Warden only allocates resources for MapR Hadoop services associated with roles that are installed on the node.

**********************************
ypo:
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


************************************************
Configure these options in <drill installation directory>/conf/drill-env.sh by modifying DRILL_JAVA_OPTS. The defaults are:


DRILL_MAX_DIRECT_MEMORY="8G"
DRILL_MAX_HEAP="4G"

export DRILL_JAVA_OPTS="-Xms1G -Xmx$DRILL_MAX_HEAP -XX:MaxDirectMemorySize=$DRILL_MAX_DIRECT_MEMORY -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=1G -ea"



************************************************

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










********************************
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


[mapr@maprdemo conf]$ hadoop conf
Try 'hadoop conf-details' for more detailed configuration.
<?xml version="1.0" encoding="UTF-8" standalone="no"?><configuration>
<property><name>mapreduce.job.ubertask.enable</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.delayed.delegation-token.removal-interval-ms</name><value>30000</value><source>yarn-default.xml</source></property>
<property><name>yarn.resourcemanager.max-completed-applications</name><value>10000</value><source>yarn-default.xml</source></property>
<property><name>io.bytes.per.checksum</name><value>512</value><source>core-default.xml</source></property>
<property><name>yarn.timeline-service.leveldb-timeline-store.read-cache-size</name><value>104857600</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.client.submit.file.replication</name><value>10</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.shuffle.connection-keep-alive.enable</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.container-manager.thread-count</name><value>20</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.jobhistory.cleaner.interval-ms</name><value>86400000</value><source>mapred-default.xml</source></property>
<property><name>yarn.app.mapreduce.am.container.log.limit.kb</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>yarn.timeline-service.handler-thread-count</name><value>10</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.pmem-check-enabled</name><value>true</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.jobhistory.done-dir</name><value>${yarn.app.mapreduce.am.staging-dir}/history/done</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.tasktracker.healthchecker.interval</name><value>60000</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobtracker.staging.root.dir</name><value>${hadoop.tmp.dir}/mapred/staging</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.ha.custom-ha-enabled</name><value>true</value><source>yarn-site.xml</source></property>
<property><name>yarn.resourcemanager.recovery.enabled</name><value>true</value><source>yarn-site.xml</source></property>
<property><name>mapreduce.task.profile.reduce.params</name><value>${mapreduce.task.profile.params}</value><source>mapred-default.xml</source></property>
<property><name>fs.AbstractFileSystem.file.impl</name><value>org.apache.hadoop.fs.local.LocalFs</value><source>core-default.xml</source></property>
<property><name>fs.du.interval</name><value>600000</value><source>core-default.xml</source></property>
<property><name>fs.AbstractFileSystem.har.impl</name><value>org.apache.hadoop.fs.HarFs</value><source>core-default.xml</source></property>
<property><name>mapreduce.client.completion.pollinterval</name><value>5000</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobhistory.client.thread-count</name><value>10</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.ubertask.maxreduces</name><value>1</value><source>mapred-default.xml</source></property>
<property><name>yarn.timeline-service.generic-application-history.enabled</name><value>false</value><source>yarn-default.xml</source></property>
<property><name>yarn.client.max-nodemanagers-proxies</name><value>500</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.reduce.shuffle.memory.limit.percent</name><value>0.25</value><source>mapred-default.xml</source></property>
<property><name>hadoop.ssl.keystores.factory.class</name><value>org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory</value><source>core-default.xml</source></property>
<property><name>yarn.nodemanager.keytab</name><value>/etc/krb5.keytab</value><source>yarn-default.xml</source></property>
<property><name>hadoop.http.authentication.kerberos.keytab</name><value>${user.home}/hadoop.keytab</value><source>core-default.xml</source></property>
<property><name>io.seqfile.sorter.recordlimit</name><value>1000000</value><source>core-default.xml</source></property>
<property><name>ipc.client.connect.retry.interval</name><value>1000</value><source>core-default.xml</source></property>
<property><name>s3.blocksize</name><value>67108864</value><source>core-default.xml</source></property>
<property><name>mapreduce.task.io.sort.factor</name><value>256</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.disk-health-checker.interval-ms</name><value>120000</value><source>yarn-default.xml</source></property>
<property><name>yarn.admin.acl</name><value>*</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.job.speculative.speculativecap</name><value>0.1</value><source>mapred-default.xml</source></property>
<property><name>yarn.timeline-service.leveldb-timeline-store.path</name><value>${hadoop.tmp.dir}/yarn/timeline</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.resource.memory-mb</name><value>${nodemanager.resource.memory-mb}</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.task.local.output.class</name><value>org.apache.hadoop.mapred.MapRFsOutputFile</value></property>
<property><name>io.map.index.interval</name><value>128</value><source>core-default.xml</source></property>
<property><name>s3.client-write-packet-size</name><value>65536</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.fs.state-store.uri</name><value>/var/mapr/cluster/yarn/rm/system</value><source>yarn-default.xml</source></property>
<property><name>hadoop.workaround.non.threadsafe.getpwuid</name><value>false</value></property>
<property><name>mapreduce.task.files.preserve.failedtasks</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.aux-services.RMVolumeManager.class</name><value>com.mapr.hadoop.yarn.resourcemanager.RMVolumeManager</value></property>
<property><name>ha.zookeeper.session-timeout.ms</name><value>5000</value><source>core-default.xml</source></property>
<property><name>fs.ramfs.impl</name><value>org.apache.hadoop.fs.InMemoryFileSystem</value></property>
<property><name>s3.replication</name><value>3</value><source>core-default.xml</source></property>
<property><name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name><value>org.apache.hadoop.mapred.ShuffleHandler</value><source>yarn-default.xml</source></property>
<property><name>yarn.app.mapreduce.am.container.log.backups</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.reduce.shuffle.connect.timeout</name><value>180000</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle,mapr_direct_shuffle</value></property>
<property><name>hadoop.ssl.enabled</name><value>false</value><source>core-default.xml</source></property>
<property><name>mapreduce.job.counters.max</name><value>120</value><source>mapred-default.xml</source></property>
<property><name>yarn.timeline-service.keytab</name><value>/etc/krb5.keytab</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.recovery.enabled</name><value>false</value><source>yarn-default.xml</source></property>
<property><name>hadoop.security.groups.cache.warn.after.ms</name><value>5000</value><source>core-default.xml</source></property>
<property><name>ipc.client.connect.max.retries.on.timeouts</name><value>45</value><source>core-default.xml</source></property>
<property><name>mapreduce.job.complete.cancel.delegation.tokens</name><value>true</value><source>mapred-default.xml</source></property>
<property><name>dfs.namenode.checkpoint.dir</name><value>${hadoop.tmp.dir}/dfs/namesecondary</value></property>
<property><name>yarn.resourcemanager.admin.address</name><value>${yarn.resourcemanager.hostname}:8033</value><source>yarn-default.xml</source></property>
<property><name>fs.trash.interval</name><value>0</value><source>core-default.xml</source></property>
<property><name>ha.health-monitor.check-interval.ms</name><value>1000</value><source>core-default.xml</source></property>
<property><name>hadoop.jetty.logs.serve.aliases</name><value>true</value><source>core-default.xml</source></property>
<property><name>hadoop.http.authentication.kerberos.principal</name><value>HTTP/_HOST@LOCALHOST</value><source>core-default.xml</source></property>
<property><name>mapreduce.tasktracker.taskmemorymanager.monitoringinterval</name><value>5000</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.reduce.shuffle.consumer.plugin.class</name><value>org.apache.hadoop.mapreduce.task.reduce.DirectShuffle</value><source>mapred-default.xml</source></property>
<property><name>mapr.home</name><value>/opt/mapr</value></property>
<property><name>s3native.blocksize</name><value>67108864</value><source>core-default.xml</source></property>
<property><name>yarn.timeline-service.leveldb-timeline-store.start-time-read-cache-size</name><value>10000</value><source>yarn-default.xml</source></property>
<property><name>ha.health-monitor.sleep-after-disconnect.ms</name><value>1000</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.staging</name><value>/var/mapr/cluster/yarn/rm/staging</value></property>
<property><name>yarn.resourcemanager.nodemanagers.heartbeat-interval-ms</name><value>1000</value><source>yarn-default.xml</source></property>
<property><name>yarn.log-aggregation.retain-check-interval-seconds</name><value>-1</value><source>yarn-default.xml</source></property>
<property><name>hadoop.proxyuser.mapr.groups</name><value>*</value><source>core-site.xml</source></property>
<property><name>mapreduce.jobtracker.jobhistory.task.numberprogresssplits</name><value>12</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.map.cpu.vcores</name><value>1</value><source>mapred-default.xml</source></property>
<property><name>yarn.acl.enable</name><value>false</value><source>yarn-default.xml</source></property>
<property><name>hadoop.security.instrumentation.requires.admin</name><value>false</value><source>core-default.xml</source></property>
<property><name>yarn.nodemanager.localizer.fetch.thread-count</name><value>4</value><source>yarn-default.xml</source></property>
<property><name>mapr.localoutput.dir</name><value>output</value></property>
<property><name>hadoop.security.authorization</name><value>false</value><source>core-default.xml</source></property>
<property><name>hadoop.security.group.mapping.ldap.search.filter.group</name><value>(objectClass=group)</value><source>core-default.xml</source></property>
<property><name>yarn.timeline-service.ttl-ms</name><value>604800000</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.output.fileoutputformat.compress.codec</name><value>org.apache.hadoop.io.compress.DefaultCodec</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.shuffle.max.connections</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>fs.s3.blockSize</name><value>33554432</value></property>
<property><name>mapreduce.shuffle.port</name><value>13562</value><source>mapred-default.xml</source></property>
<property><name>yarn.log-aggregation-enable</name><value>false</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.reduce.log.level</name><value>INFO</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.app-submission.cross-platform</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobtracker.instrumentation</name><value>org.apache.hadoop.mapred.JobTrackerMetricsInst</value><source>mapred-default.xml</source></property>
<property><name>hadoop.security.group.mapping.ldap.search.attr.group.name</name><value>cn</value><source>core-default.xml</source></property>
<property><name>mapr.map.keyprefix.ints</name><value>1</value></property>
<property><name>mapreduce.map.java.opts</name><value>-Xmx900m</value></property>
<property><name>fs.client.resolve.remote.symlinks</name><value>true</value><source>core-default.xml</source></property>
<property><name>yarn.app.mapreduce.client-am.ipc.max-retries-on-timeouts</name><value>3</value><source>mapred-default.xml</source></property>
<property><name>s3native.bytes-per-checksum</name><value>512</value><source>core-default.xml</source></property>
<property><name>mapreduce.tasktracker.tasks.sleeptimebeforesigkill</name><value>5000</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.local-dirs</name><value>${hadoop.tmp.dir}/nm-local-dir</value><source>yarn-default.xml</source></property>
<property><name>tfile.fs.output.buffer.size</name><value>262144</value><source>core-default.xml</source></property>
<property><name>mapreduce.jobtracker.persist.jobstatus.active</name><value>true</value><source>mapred-default.xml</source></property>
<property><name>mapred.ifile.outputstream</name><value>org.apache.hadoop.mapred.MapRIFileOutputStream</value></property>
<property><name>fs.AbstractFileSystem.hdfs.impl</name><value>com.mapr.fs.MFS</value><source>core-default.xml</source></property>
<property><name>mapreduce.jobhistory.recovery.store.fs.uri</name><value>${hadoop.tmp.dir}/mapred/history/recoverystore</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.map.output.collector.class</name><value>org.apache.hadoop.mapred.MapRFsOutputBuffer</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.tasktracker.local.dir.minspacestart</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>hadoop.security.uid.cache.secs</name><value>14400</value><source>core-default.xml</source></property>
<property><name>fs.har.impl.disable.cache</name><value>true</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.scheduler.monitor.policies</name><value>org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy</value><source>yarn-default.xml</source></property>
<property><name>hadoop.ssl.client.conf</name><value>ssl-client.xml</value><source>core-default.xml</source></property>
<property><name>yarn.timeline-service.webapp.address</name><value>${yarn.timeline-service.hostname}:8188</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.tasktracker.local.dir.minspacekill</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobtracker.retiredjobs.cache.size</name><value>1000</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.scheduler.class</name><value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.job.reduce.slowstart.completedmaps</name><value>0.95</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.end-notification.retry.attempts</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.task.profile.map.params</name><value>${mapreduce.task.profile.params}</value><source>mapred-default.xml</source></property>
<property><name>fs.s3n.impl</name><value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value></property>
<property><name>mapreduce.map.memory.mb</name><value>1024</value></property>
<property><name>mapreduce.tasktracker.outofband.heartbeat</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>fs.s3n.multipart.uploads.enabled</name><value>false</value><source>core-default.xml</source></property>
<property><name>io.native.lib.available</name><value>true</value><source>core-default.xml</source></property>
<property><name>mapreduce.jobtracker.persist.jobstatus.hours</name><value>1</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.ha.automatic-failover.enabled</name><value>true</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.client.progressmonitor.pollinterval</name><value>1000</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.reduce.input.buffer.percent</name><value>0.0</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.map.output.compress.codec</name><value>org.apache.hadoop.io.compress.DefaultCodec</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.map.skip.proc.count.autoincr</name><value>true</value><source>mapred-default.xml</source></property>
<property><name>hadoop.user.group.static.mapping.overrides</name><value>dr.who=;</value><source>core-default.xml</source></property>
<property><name>hadoop.logfile.size</name><value>10000000</value></property>
<property><name>mapreduce.jobtracker.address</name><value>local</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.cluster.local.dir</name><value>${hadoop.tmp.dir}/mapred/local</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.input.fileinputformat.list-status.num-threads</name><value>1</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.tasktracker.taskcontroller</name><value>org.apache.hadoop.mapred.DefaultTaskController</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.shuffle.connection-keep-alive.timeout</name><value>5</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.env-whitelist</name><value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,HADOOP_YARN_HOME</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.reduce.shuffle.parallelcopies</name><value>12</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.resource.io-spindles</name><value>${nodemanager.resource.io-spindles}</value></property>
<property><name>mapreduce.jobtracker.heartbeats.in.second</name><value>100</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.maxtaskfailures.per.tracker</name><value>3</value><source>mapred-default.xml</source></property>
<property><name>ipc.client.connection.maxidletime</name><value>10000</value><source>core-default.xml</source></property>
<property><name>mapreduce.shuffle.ssl.enabled</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.max.split.locations</name><value>10</value><source>mapred-default.xml</source></property>
<property><name>fs.checkpoint.size</name><value>67108864</value></property>
<property><name>fs.s3.sleepTimeSeconds</name><value>10</value><source>core-default.xml</source></property>
<property><name>yarn.scheduler.maximum-allocation-vcores</name><value>32</value><source>yarn-default.xml</source></property>
<property><name>hadoop.ssl.server.conf</name><value>ssl-server.xml</value><source>core-default.xml</source></property>
<property><name>yarn.dfs-logging.handler-class</name><value>com.mapr.hadoop.yarn.util.MapRFSLoggingHandler</value></property>
<property><name>fs.s3n.multipart.uploads.block.size</name><value>67108864</value><source>core-default.xml</source></property>
<property><name>ha.zookeeper.parent-znode</name><value>/hadoop-ha</value><source>core-default.xml</source></property>
<property><name>io.seqfile.lazydecompress</name><value>true</value><source>core-default.xml</source></property>
<property><name>mapreduce.reduce.merge.inmem.threshold</name><value>1000</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.input.fileinputformat.split.minsize</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>ipc.client.tcpnodelay</name><value>true</value><source>core-default.xml</source></property>
<property><name>mapred.maxthreads.generate.mapoutput</name><value>1</value></property>
<property><name>yarn.resourcemanager.zk-retry-interval-ms</name><value>1000</value><source>yarn-default.xml</source></property>
<property><name>s3.stream-buffer-size</name><value>4096</value><source>core-default.xml</source></property>
<property><name>mapreduce.jobtracker.tasktracker.maxblacklists</name><value>4</value><source>mapred-default.xml</source></property>
<property><name>yarn.dfs-logging.dir-glob</name><value>maprfs:////var/mapr/local/*/logs/yarn/userlogs</value></property>
<property><name>mapreduce.job.jvm.numtasks</name><value>1</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.task.io.sort.mb</name><value>100</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.ha.enabled</name><value>false</value><source>yarn-default.xml</source></property>
<property><name>io.compression.codecs</name><value>org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.SnappyCodec</value></property>
<property><name>io.file.buffer.size</name><value>8192</value><source>core-default.xml</source></property>
<property><name>yarn.timeline-service.generic-application-history.aux-services</name><value>HSVolumeManager</value></property>
<property><name>yarn.nodemanager.admin-env</name><value>MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.job.split.metainfo.maxsize</name><value>10000000</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.scheduler.monitor.enable</name><value>false</value><source>yarn-default.xml</source></property>
<property><name>yarn.app.mapreduce.am.scheduler.heartbeat.interval-ms</name><value>1000</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.reduce.maxattempts</name><value>4</value><source>mapred-default.xml</source></property>
<property><name>nfs.exports.allowed.hosts</name><value>* rw</value><source>core-default.xml</source></property>
<property><name>fs.har.impl</name><value>org.apache.hadoop.fs.HarFileSystem</value></property>
<property><name>hadoop.security.authentication</name><value>SIMPLE</value><source>core-default.xml</source></property>
<property><name>fs.s3.buffer.dir</name><value>${hadoop.tmp.dir}/s3</value><source>core-default.xml</source></property>
<property><name>mapred.maxthreads.partition.closer</name><value>1</value></property>
<property><name>rpc.metrics.quantile.enable</name><value>false</value><source>core-default.xml</source></property>
<property><name>mapreduce.jobtracker.taskscheduler</name><value>org.apache.hadoop.mapred.JobQueueTaskScheduler</value><source>mapred-default.xml</source></property>
<property><name>yarn.app.mapreduce.am.job.task.listener.thread-count</name><value>30</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.reduces</name><value>1</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.map.sort.spill.percent</name><value>0.99</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.end-notification.retry.interval</name><value>1000</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobhistory.minicluster.fixed.ports</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.maps</name><value>2</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.speculative.slownodethreshold</name><value>1.0</value><source>mapred-default.xml</source></property>
<property><name>tfile.fs.input.buffer.size</name><value>262144</value><source>core-default.xml</source></property>
<property><name>mapreduce.map.speculative</name><value>true</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.acl-view-job</name><value> </value><source>mapred-default.xml</source></property>
<property><name>mapreduce.reduce.shuffle.retry-delay.max.ms</name><value>60000</value><source>mapred-default.xml</source></property>
<property><name>yarn.ipc.serializer.type</name><value>protocolbuffers</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.job.end-notification.max.retry.interval</name><value>5000</value><source>mapred-default.xml</source></property>
<property><name>mapr.localvolumes.path</name><value>/var/mapr/local</value></property>
<property><name>ftp.blocksize</name><value>67108864</value><source>core-default.xml</source></property>
<property><name>mapreduce.tasktracker.http.threads</name><value>40</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.reduce.java.opts</name><value>-Xmx2560m</value></property>
<property><name>ha.failover-controller.cli-check.rpc-timeout.ms</name><value>20000</value><source>core-default.xml</source></property>
<property><name>mapreduce.job.token.tracking.ids.enabled</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.principal</name><value>mapr</value></property>
<property><name>fs.file.impl</name><value>org.apache.hadoop.fs.LocalFileSystem</value></property>
<property><name>yarn.nodemanager.resourcemanager.connect.wait.secs</name><value>900</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.task.skip.start.attempts</name><value>2</value><source>mapred-default.xml</source></property>
<property><name>yarn.use-central-logging-for-mapreduce-only</name><value>false</value></property>
<property><name>mapreduce.jobtracker.persist.jobstatus.dir</name><value>/jobtracker/jobsInfo</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.aux-services</name><value>RMVolumeManager</value></property>
<property><name>ipc.client.kill.max</name><value>10</value><source>core-default.xml</source></property>
<property><name>yarn.nodemanager.linux-container-executor.cgroups.mount</name><value>false</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.jobhistory.keytab</name><value>/etc/security/keytab/jhs.service.keytab</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.linux-container-executor.cgroups.hierarchy</name><value>/hadoop-yarn</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.job.end-notification.max.attempts</name><value>5</value><source>mapred-default.xml</source></property>
<property><name>yarn.http.policy</name><value>HTTP_ONLY</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.jobhistory.max-age-ms</name><value>604800000</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.task.tmp.dir</name><value>./tmp</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.reducer.preempt.delay.sec</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.reduce.memory.mb</name><value>3072</value></property>
<property><name>yarn.resourcemanager.zk-state-store.parent-path</name><value>/rmstore</value><source>yarn-default.xml</source></property>
<property><name>hadoop.http.filter.initializers</name><value>org.apache.hadoop.http.lib.StaticUserWebFilter</value><source>core-default.xml</source></property>
<property><name>yarn.timeline-service.address</name><value>${yarn.timeline-service.hostname}:10200</value><source>yarn-default.xml</source></property>
<property><name>yarn.timeline-service.hostname</name><value>0.0.0.0</value><source>yarn-default.xml</source></property>
<property><name>yarn.timeline-service.generic-application-history.fs-history-store.compression-type</name><value>none</value><source>yarn-default.xml</source></property>
<property><name>yarn.client.application-client-protocol.poll-interval-ms</name><value>200</value><source>yarn-default.xml</source></property>
<property><name>hadoop.http.authentication.type</name><value>simple</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.client.thread-count</name><value>50</value><source>yarn-default.xml</source></property>
<property><name>ipc.server.listen.queue.size</name><value>128</value><source>core-default.xml</source></property>
<property><name>mapreduce.reduce.skip.maxgroups</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>file.stream-buffer-size</name><value>4096</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.store.class</name><value>org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore</value><source>yarn-default.xml</source></property>
<property><name>io.mapfile.bloom.size</name><value>1048576</value><source>core-default.xml</source></property>
<property><name>yarn.nodemanager.container-executor.class</name><value>org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor</value><source>yarn-default.xml</source></property>
<property><name>fs.hsftp.impl</name><value>org.apache.hadoop.hdfs.HsftpFileSystem</value></property>
<property><name>fs.swift.impl</name><value>org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem</value><source>core-default.xml</source></property>
<property><name>mapreduce.map.maxattempts</name><value>4</value><source>mapred-default.xml</source></property>
<property><name>yarn.log-aggregation.retain-seconds</name><value>2592000</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.jobtracker.jobhistory.block.size</name><value>3145728</value><source>mapred-default.xml</source></property>
<property><name>yarn.app.mapreduce.am.job.committer.cancel-timeout</name><value>60000</value><source>mapred-default.xml</source></property>
<property><name>ftp.replication</name><value>3</value><source>core-default.xml</source></property>
<property><name>mapreduce.jobtracker.http.address</name><value>0.0.0.0:50030</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.health-checker.script.timeout-ms</name><value>1200000</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.jobhistory.intermediate-done-dir</name><value>${yarn.app.mapreduce.am.staging-dir}/history/done_intermediate</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobhistory.address</name><value>maprdemo:10020</value><source>mapred-site.xml</source></property>
<property><name>mapreduce.jobtracker.taskcache.levels</name><value>2</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.recovery.dir</name><value>${hadoop.tmp.dir}/yarn-nm-recovery</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.log.retain-seconds</name><value>10800</value><source>yarn-default.xml</source></property>
<property><name>yarn.timeline-service.enabled</name><value>false</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.local-cache.max-files-per-directory</name><value>8192</value><source>yarn-default.xml</source></property>
<property><name>mapred.child.java.opts</name><value>-Xmx200m</value><source>mapred-default.xml</source></property>
<property><name>map.sort.class</name><value>org.apache.hadoop.util.QuickSort</value><source>mapred-default.xml</source></property>
<property><name>hadoop.util.hash.type</name><value>murmur</value><source>core-default.xml</source></property>
<property><name>mapreduce.jobhistory.move.interval-ms</name><value>180000</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.reduce.skip.proc.count.autoincr</name><value>true</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.ha.automatic-failover.embedded</name><value>true</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.container-monitor.interval-ms</name><value>3000</value><source>yarn-default.xml</source></property>
<property><name>yarn.client.nodemanager-client-async.thread-pool-max-size</name><value>500</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.disk-health-checker.min-healthy-disks</name><value>0.25</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.jobhistory.http.policy</name><value>HTTP_ONLY</value><source>mapred-default.xml</source></property>
<property><name>ha.zookeeper.acl</name><value>world:anyone:rwcda</value><source>core-default.xml</source></property>
<property><name>yarn.nodemanager.sleep-delay-before-sigkill.ms</name><value>250</value><source>yarn-default.xml</source></property>
<property><name>hadoop.proxyuser.mapr.hosts</name><value>*</value><source>core-site.xml</source></property>
<property><name>mapr.mapred.localvolume.mount.path</name><value>${mapr.localvolumes.path}/${mapr.host}/mapred</value></property>
<property><name>io.map.index.skip</name><value>0</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.zk-num-retries</name><value>1000</value><source>yarn-default.xml</source></property>
<property><name>hadoop.ssl.exclude.cipher.suites</name><value>SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA,SSL_RSA_EXPORT_WITH_DES40_CBC_SHA,SSL_RSA_EXPORT_WITH_RC4_40_MD5</value></property>
<property><name>net.topology.node.switch.mapping.impl</name><value>org.apache.hadoop.net.ScriptBasedMapping</value><source>core-default.xml</source></property>
<property><name>fs.s3.maxRetries</name><value>4</value><source>core-default.xml</source></property>
<property><name>ha.failover-controller.new-active.rpc-timeout.ms</name><value>60000</value><source>core-default.xml</source></property>
<property><name>s3native.client-write-packet-size</name><value>65536</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.amliveliness-monitor.interval-ms</name><value>1000</value><source>yarn-default.xml</source></property>
<property><name>mapred.ifile.inputstream</name><value>org.apache.hadoop.mapred.MapRIFileInputStream</value></property>
<property><name>hadoop.http.staticuser.user</name><value>unknown</value><source>core-default.xml</source></property>
<property><name>mapreduce.reduce.speculative</name><value>true</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.client.output.filter</name><value>FAILED</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobhistory.datestring.cache.size</name><value>200000</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.ifile.readahead.bytes</name><value>4194304</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.tasktracker.report.address</name><value>127.0.0.1:0</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.task.userlog.limit.kb</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.tasktracker.map.tasks.maximum</name><value>2</value><source>mapred-default.xml</source></property>
<property><name>hadoop.http.authentication.simple.anonymous.allowed</name><value>true</value><source>core-default.xml</source></property>
<property><name>mapreduce.job.classloader.system.classes</name><value>java.,javax.,org.apache.commons.logging.,org.apache.log4j.,
          org.apache.hadoop.,core-default.xml,hdfs-default.xml,
          mapred-default.xml,yarn-default.xml</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.reduce.disk</name><value>1.33</value></property>
<property><name>hadoop.rpc.socket.factory.class.default</name><value>org.apache.hadoop.net.StandardSocketFactory</value><source>core-default.xml</source></property>
<property><name>yarn.nodemanager.resourcemanager.connect.retry_interval.secs</name><value>30</value><source>yarn-default.xml</source></property>
<property><name>fs.hftp.impl</name><value>org.apache.hadoop.hdfs.HftpFileSystem</value></property>
<property><name>yarn.resourcemanager.connect.max-wait.ms</name><value>900000</value><source>yarn-default.xml</source></property>
<property><name>fs.automatic.close</name><value>true</value><source>core-default.xml</source></property>
<property><name>fs.kfs.impl</name><value>org.apache.hadoop.fs.kfs.KosmosFileSystem</value></property>
<property><name>mapreduce.tasktracker.healthchecker.script.timeout</name><value>600000</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.address</name><value>${yarn.resourcemanager.hostname}:8032</value><source>yarn-default.xml</source></property>
<property><name>fs.hdfs.impl</name><value>com.mapr.fs.MapRFileSystem</value></property>
<property><name>yarn.client.failover-retries</name><value>0</value><source>yarn-default.xml</source></property>
<property><name>yarn.resourcemanager.ha.automatic-failover.zk-base-path</name><value>/yarn-leader-election</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.health-checker.interval-ms</name><value>600000</value><source>yarn-default.xml</source></property>
<property><name>yarn.resourcemanager.container-tokens.master-key-rolling-interval-secs</name><value>86400</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.reduce.markreset.buffer.percent</name><value>0.0</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.fs.state-store.retry-policy-spec</name><value>2000, 500</value><source>yarn-default.xml</source></property>
<property><name>hadoop.security.group.mapping.ldap.directory.search.timeout</name><value>10000</value><source>core-default.xml</source></property>
<property><name>mapreduce.map.log.level</name><value>INFO</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.localizer.address</name><value>${yarn.nodemanager.hostname}:8040</value><source>yarn-default.xml</source></property>
<property><name>dfs.bytes-per-checksum</name><value>512</value></property>
<property><name>yarn.resourcemanager.keytab</name><value>/etc/krb5.keytab</value><source>yarn-default.xml</source></property>
<property><name>ftp.stream-buffer-size</name><value>4096</value><source>core-default.xml</source></property>
<property><name>ha.health-monitor.rpc-timeout.ms</name><value>45000</value><source>core-default.xml</source></property>
<property><name>hadoop.security.group.mapping.ldap.search.attr.member</name><value>member</value><source>core-default.xml</source></property>
<property><name>mapreduce.task.profile.params</name><value>-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,file=%s</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb</name><value>0</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.job.classloader</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>fs.maprfs.impl</name><value>com.mapr.fs.MapRFileSystem</value></property>
<property><name>yarn.nm.liveness-monitor.expiry-interval-ms</name><value>600000</value><source>yarn-default.xml</source></property>
<property><name>io.compression.codec.bzip2.library</name><value>system-native</value><source>core-default.xml</source></property>
<property><name>hadoop.http.authentication.token.validity</name><value>36000</value><source>core-default.xml</source></property>
<property><name>yarn.timeline-service.ttl-enable</name><value>true</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.resourcemanager.minimum.version</name><value>NONE</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.job.hdfs-servers</name><value>${fs.defaultFS}</value><source>yarn-default.xml</source></property>
<property><name>fs.ftp.impl</name><value>org.apache.hadoop.fs.ftp.FTPFileSystem</value></property>
<property><name>yarn.external.token.manager</name><value>com.mapr.hadoop.yarn.security.MapRTicketManager</value></property>
<property><name>s3native.replication</name><value>3</value><source>core-default.xml</source></property>
<property><name>yarn.app.mapreduce.task.container.log.backups</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.localizer.client.thread-count</name><value>5</value><source>yarn-default.xml</source></property>
<property><name>yarn.resourcemanager.container.liveness-monitor.interval-ms</name><value>600000</value><source>yarn-default.xml</source></property>
<property><name>dfs.ha.fencing.ssh.connect-timeout</name><value>30000</value><source>core-default.xml</source></property>
<property><name>yarn.am.liveness-monitor.expiry-interval-ms</name><value>600000</value><source>yarn-default.xml</source></property>
<property><name>fs.mapr.working.dir</name><value>/user/$USERNAME/</value></property>
<property><name>net.topology.impl</name><value>org.apache.hadoop.net.NetworkTopology</value><source>core-default.xml</source></property>
<property><name>mapreduce.shuffle.max.threads</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.task.profile</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.linux-container-executor.resources-handler.class</name><value>org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.tasktracker.instrumentation</name><value>org.apache.hadoop.mapred.TaskTrackerMetricsInst</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.tasktracker.http.address</name><value>0.0.0.0:50060</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobhistory.webapp.address</name><value>maprdemo:19888</value><source>mapred-site.xml</source></property>
<property><name>yarn.ipc.rpc.class</name><value>org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC</value><source>yarn-default.xml</source></property>
<property><name>ha.failover-controller.graceful-fence.rpc-timeout.ms</name><value>5000</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.application-tokens.master-key-rolling-interval-secs</name><value>86400</value><source>yarn-default.xml</source></property>
<property><name>yarn.resourcemanager.zk-acl</name><value>world:anyone:rwcda</value><source>yarn-default.xml</source></property>
<property><name>yarn.resourcemanager.am.max-attempts</name><value>2</value><source>yarn-default.xml</source></property>
<property><name>hbase.table.namespace.mappings</name><value>*:/tables</value><source>core-site.xml</source></property>
<property><name>mapreduce.job.ubertask.maxmaps</name><value>9</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.state-store.max-completed-applications</name><value>${yarn.resourcemanager.max-completed-applications}</value><source>yarn-default.xml</source></property>
<property><name>yarn.scheduler.maximum-allocation-mb</name><value>8192</value><source>yarn-default.xml</source></property>
<property><name>yarn.resourcemanager.webapp.https.address</name><value>${yarn.resourcemanager.hostname}:8090</value><source>yarn-default.xml</source></property>
<property><name>mapr.localspill.dir</name><value>spill</value></property>
<property><name>mapreduce.job.userlog.retain.hours</name><value>24</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.linux-container-executor.nonsecure-mode.user-pattern</name><value>^[_.A-Za-z0-9][-@_.A-Za-z0-9]{0,255}?[$]?$</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.task.timeout</name><value>600000</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobhistory.loadedjobs.cache.size</name><value>5</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.framework.name</name><value>yarn</value><source>mapred-default.xml</source></property>
<property><name>ipc.client.idlethreshold</name><value>4000</value><source>core-default.xml</source></property>
<property><name>ipc.server.tcpnodelay</name><value>true</value><source>core-default.xml</source></property>
<property><name>ftp.bytes-per-checksum</name><value>512</value><source>core-default.xml</source></property>
<property><name>hadoop.logfile.count</name><value>10</value></property>
<property><name>yarn.resourcemanager.hostname</name><value>0.0.0.0</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.shuffle.transfer.buffer.size</name><value>131072</value><source>mapred-default.xml</source></property>
<property><name>s3.bytes-per-checksum</name><value>512</value><source>core-default.xml</source></property>
<property><name>mapreduce.job.speculative.slowtaskthreshold</name><value>1.0</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobhistory.recovery.store.class</name><value>org.apache.hadoop.mapreduce.v2.hs.HistoryServerFileSystemStateStoreService</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.localizer.cache.target-size-mb</name><value>10240</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.remote-app-log-dir</name><value>/tmp/logs</value><source>yarn-default.xml</source></property>
<property><name>fs.s3.block.size</name><value>33554432</value><source>core-default.xml</source></property>
<property><name>mapreduce.job.queuename</name><value>default</value><source>mapred-default.xml</source></property>
<property><name>yarn.scheduler.minimum-allocation-mb</name><value>1024</value><source>yarn-default.xml</source></property>
<property><name>hadoop.rpc.protection</name><value>authentication</value><source>core-default.xml</source></property>
<property><name>yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user</name><value>nobody</value><source>yarn-default.xml</source></property>
<property><name>yarn.timeline-service.store-class</name><value>org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore</value><source>yarn-default.xml</source></property>
<property><name>yarn.timeline-service.leveldb-timeline-store.start-time-write-cache-size</name><value>10000</value><source>yarn-default.xml</source></property>
<property><name>yarn.app.mapreduce.client-am.ipc.max-retries</name><value>3</value><source>mapred-default.xml</source></property>
<property><name>ftp.client-write-packet-size</name><value>65536</value><source>core-default.xml</source></property>
<property><name>yarn.nodemanager.address</name><value>${yarn.nodemanager.hostname}:0</value><source>yarn-default.xml</source></property>
<property><name>fs.defaultFS</name><value>maprfs:///</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.scheduler.client.thread-count</name><value>50</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.task.merge.progress.records</name><value>10000</value><source>mapred-default.xml</source></property>
<property><name>file.client-write-packet-size</name><value>65536</value><source>core-default.xml</source></property>
<property><name>mapred.local.mapoutput</name><value>false</value></property>
<property><name>mapreduce.reduce.cpu.vcores</name><value>1</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.delete.thread-count</name><value>4</value><source>yarn-default.xml</source></property>
<property><name>yarn.resourcemanager.scheduler.address</name><value>${yarn.resourcemanager.hostname}:8030</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.jobhistory.admin.address</name><value>0.0.0.0:10033</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.job.shuffle.provider.services</name><value>mapr_direct_shuffle</value></property>
<property><name>fs.trash.checkpoint.interval</name><value>0</value><source>core-default.xml</source></property>
<property><name>hadoop.http.authentication.signature.secret.file</name><value>${user.home}/hadoop-http-auth-signature-secret</value><source>core-default.xml</source></property>
<property><name>s3native.stream-buffer-size</name><value>4096</value><source>core-default.xml</source></property>
<property><name>ipc.client.max.connection.setup.timeout</name><value>20</value></property>
<property><name>yarn.resourcemanager.webapp.delegation-token-auth-filter.enabled</name><value>true</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.reduce.shuffle.read.timeout</name><value>180000</value><source>mapred-default.xml</source></property>
<property><name>fs.AbstractFileSystem.maprfs.impl</name><value>com.mapr.fs.MFS</value></property>
<property><name>yarn.app.mapreduce.am.command-opts</name><value>-Xmx1024m</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.map.disk</name><value>0.5</value></property>
<property><name>mapreduce.local.clientfactory.class.name</name><value>org.apache.hadoop.mapred.LocalClientFactory</value><source>mapred-default.xml</source></property>
<property><name>dfs.namenode.checkpoint.edits.dir</name><value>${fs.checkpoint.dir}</value></property>
<property><name>fs.permissions.umask-mode</name><value>022</value><source>core-default.xml</source></property>
<property><name>mapreduce.jobhistory.admin.acl</name><value>*</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobhistory.move.thread-count</name><value>3</value><source>mapred-default.xml</source></property>
<property><name>hadoop.common.configuration.version</name><value>0.23.0</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.dir</name><value>/var/mapr/cluster/yarn/rm</value></property>
<property><name>mapreduce.tasktracker.dns.interface</name><value>default</value><source>mapred-default.xml</source></property>
<property><name>fs.s3n.blockSize</name><value>33554432</value></property>
<property><name>yarn.nodemanager.container-monitor.procfs-tree.smaps-based-rss.enabled</name><value>false</value><source>yarn-default.xml</source></property>
<property><name>yarn.resourcemanager.connect.retry-interval.ms</name><value>30000</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.output.fileoutputformat.compress.type</name><value>RECORD</value><source>mapred-default.xml</source></property>
<property><name>hadoop.security.group.mapping.ldap.ssl</name><value>false</value><source>core-default.xml</source></property>
<property><name>mapreduce.ifile.readahead</name><value>true</value><source>mapred-default.xml</source></property>
<property><name>io.serializations</name><value>org.apache.hadoop.io.serializer.WritableSerialization</value><source>core-default.xml</source></property>
<property><name>yarn.timeline-service.webapp.https.address</name><value>${yarn.timeline-service.hostname}:8190</value><source>yarn-default.xml</source></property>
<property><name>fs.df.interval</name><value>60000</value><source>core-default.xml</source></property>
<property><name>mapreduce.reduce.shuffle.input.buffer.percent</name><value>0.70</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name><value>100.0</value><source>yarn-default.xml</source></property>
<property><name>io.seqfile.compress.blocksize</name><value>1000000</value><source>core-default.xml</source></property>
<property><name>ipc.client.connect.max.retries</name><value>10</value><source>core-default.xml</source></property>
<property><name>hadoop.security.groups.cache.secs</name><value>300</value><source>core-default.xml</source></property>
<property><name>yarn.nodemanager.process-kill-wait.ms</name><value>2000</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value><source>yarn-default.xml</source></property>
<property><name>yarn.app.mapreduce.client.max-retries</name><value>3</value><source>mapred-default.xml</source></property>
<property><name>yarn.timeline-service.generic-application-history.fs-history-store.uri</name><value>${hadoop.tmp.dir}/yarn/timeline/generic-history</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.log-aggregation.compression-type</name><value>none</value><source>yarn-default.xml</source></property>
<property><name>hadoop.security.group.mapping.ldap.search.filter.user</name><value>(&amp;(objectClass=user)(sAMAccountName={0}))</value><source>core-default.xml</source></property>
<property><name>yarn.nodemanager.localizer.cache.cleanup.interval-ms</name><value>600000</value><source>yarn-default.xml</source></property>
<property><name>mapr.host</name><value>maprdemo</value></property>
<property><name>yarn.nodemanager.log-dirs</name><value>${yarn.log.dir}/userlogs</value><source>yarn-default.xml</source></property>
<property><name>fs.s3n.multipart.copy.block.size</name><value>5368709120</value><source>core-default.xml</source></property>
<property><name>fs.s3n.block.size</name><value>33554432</value><source>core-default.xml</source></property>
<property><name>fs.ftp.host</name><value>0.0.0.0</value><source>core-default.xml</source></property>
<property><name>hadoop.security.group.mapping</name><value>org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback</value><source>core-default.xml</source></property>
<property><name>yarn.app.mapreduce.am.resource.cpu-vcores</name><value>1</value><source>mapred-default.xml</source></property>
<property><name>mapr.mapred.localvolume.root.dir.name</name><value>nodeManager</value></property>
<property><name>yarn.client.failover-proxy-provider</name><value>org.apache.hadoop.yarn.client.MapRZKBasedRMFailoverProxyProvider</value><source>yarn-site.xml</source></property>
<property><name>mapreduce.jobhistory.cleaner.enable</name><value>true</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.map.skip.maxrecords</name><value>0</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.external.token.localizer</name><value>com.mapr.hadoop.yarn.nodemanager.MapRTicketLocalizer</value></property>
<property><name>yarn.scheduler.minimum-allocation-vcores</name><value>1</value><source>yarn-default.xml</source></property>
<property><name>fs.s3.impl</name><value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value></property>
<property><name>file.replication</name><value>1</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.configuration.provider-class</name><value>org.apache.hadoop.yarn.LocalConfigurationProvider</value><source>yarn-default.xml</source></property>
<property><name>yarn.resourcemanager.resource-tracker.address</name><value>${yarn.resourcemanager.hostname}:8031</value><source>yarn-default.xml</source></property>
<property><name>yarn.resourcemanager.aux-services.HSVolumeManager.class</name><value>com.mapr.hadoop.yarn.resourcemanager.RMVolumeManager</value></property>
<property><name>yarn.resourcemanager.work-preserving-recovery.enabled</name><value>false</value><source>yarn-default.xml</source></property>
<property><name>yarn.resourcemanager.history-writer.multi-threaded-dispatcher.pool-size</name><value>10</value><source>yarn-default.xml</source></property>
<property><name>hadoop.security.java.security.login.config.jar.path</name><value>/mapr.login.conf</value></property>
<property><name>mapreduce.jobtracker.restart.recover</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>hadoop.work.around.non.threadsafe.getpwuid</name><value>false</value><source>core-default.xml</source></property>
<property><name>mapreduce.tasktracker.indexcache.mb</name><value>10</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.output.fileoutputformat.compress</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>hadoop.tmp.dir</name><value>/tmp/hadoop-${user.name}</value><source>core-default.xml</source></property>
<property><name>yarn.nodemanager.resource.cpu-vcores</name><value>${nodemanager.resource.cpu-vcores}</value><source>yarn-default.xml</source></property>
<property><name>hadoop.kerberos.kinit.command</name><value>kinit</value><source>core-default.xml</source></property>
<property><name>mapreduce.job.committer.setup.cleanup.needed</name><value>true</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.task.profile.reduces</name><value>0-2</value><source>mapred-default.xml</source></property>
<property><name>file.bytes-per-checksum</name><value>512</value><source>core-default.xml</source></property>
<property><name>mapr.mapred.localvolume.root.dir.path</name><value>${mapr.mapred.localvolume.mount.path}/${mapr.mapred.localvolume.root.dir.name}</value></property>
<property><name>mapreduce.jobtracker.handler.count</name><value>10</value><source>mapred-default.xml</source></property>
<property><name>yarn.app.mapreduce.am.job.committer.commit-window</name><value>10000</value><source>mapred-default.xml</source></property>
<property><name>net.topology.script.number.args</name><value>100</value><source>core-default.xml</source></property>
<property><name>mapreduce.task.profile.maps</name><value>0-2</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.webapp.address</name><value>${yarn.resourcemanager.hostname}:8088</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.jobtracker.system.dir</name><value>${hadoop.tmp.dir}/mapred/system</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.vmem-pmem-ratio</name><value>2.1</value><source>yarn-default.xml</source></property>
<property><name>hadoop.ssl.hostname.verifier</name><value>DEFAULT</value><source>core-default.xml</source></property>
<property><name>yarn.timeline-service.generic-application-history.store-class</name><value>org.apache.hadoop.yarn.server.applicationhistoryservice.FileSystemApplicationHistoryStore</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.hostname</name><value>0.0.0.0</value><source>yarn-default.xml</source></property>
<property><name>ipc.client.connect.timeout</name><value>20000</value><source>core-default.xml</source></property>
<property><name>mapreduce.jobhistory.principal</name><value>jhs/_HOST@REALM.TLD</value><source>mapred-default.xml</source></property>
<property><name>io.mapfile.bloom.error.rate</name><value>0.005</value><source>core-default.xml</source></property>
<property><name>yarn.mapr.ticket.expiration</name><value>604800000</value></property>
<property><name>mapreduce.shuffle.ssl.file.buffer.size</name><value>65536</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.aux-services.mapr_direct_shuffle.class</name><value>com.mapr.hadoop.mapred.LocalVolumeAuxService</value></property>
<property><name>io.sort.record.percent</name><value>0.17</value></property>
<property><name>mapreduce.jobtracker.expire.trackers.interval</name><value>600000</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.cluster.acls.enabled</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>yarn.nodemanager.remote-app-log-dir-suffix</name><value>logs</value><source>yarn-default.xml</source></property>
<property><name>ha.failover-controller.graceful-fence.connection.retries</name><value>1</value><source>core-default.xml</source></property>
<property><name>yarn.client.failover-retries-on-socket-timeouts</name><value>0</value><source>yarn-default.xml</source></property>
<property><name>ha.health-monitor.connect-retry-interval.ms</name><value>1000</value><source>core-default.xml</source></property>
<property><name>yarn.app.mapreduce.am.resource.mb</name><value>1536</value><source>mapred-default.xml</source></property>
<property><name>io.seqfile.local.dir</name><value>${hadoop.tmp.dir}/io/local</value><source>core-default.xml</source></property>
<property><name>mapreduce.reduce.shuffle.merge.percent</name><value>0.66</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.nm.liveness-monitor.interval-ms</name><value>1000</value><source>yarn-default.xml</source></property>
<property><name>tfile.io.chunk.size</name><value>1048576</value><source>core-default.xml</source></property>
<property><name>file.blocksize</name><value>67108864</value><source>core-default.xml</source></property>
<property><name>mapreduce.jobtracker.jobhistory.lru.cache.size</name><value>5</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobtracker.maxtasks.perjob</name><value>-1</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.nodemanager.minimum.version</name><value>NONE</value><source>yarn-default.xml</source></property>
<property><name>yarn.nodemanager.webapp.address</name><value>${yarn.nodemanager.hostname}:8042</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.job.acl-modify-job</name><value> </value><source>mapred-default.xml</source></property>
<property><name>mapreduce.am.max-attempts</name><value>2</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.tasktracker.reduce.tasks.maximum</name><value>2</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.cluster.temp.dir</name><value>${hadoop.tmp.dir}/mapred/temp</value><source>mapred-default.xml</source></property>
<property><name>io.skip.checksum.errors</name><value>false</value><source>core-default.xml</source></property>
<property><name>mapreduce.jobhistory.joblist.cache.size</name><value>20000</value><source>mapred-default.xml</source></property>
<property><name>yarn.app.mapreduce.am.staging-dir</name><value>${fs.defaultFS}/var/mapr/cluster/yarn/rm/staging</value><source>mapred-default.xml</source></property>
<property><name>mapreduce.jobhistory.recovery.enable</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>hadoop.http.authentication.signature.secret</name><value>com.mapr.security.maprauth.MaprSignatureSecretFactory</value></property>
<property><name>yarn.resourcemanager.zk-timeout-ms</name><value>10000</value><source>yarn-default.xml</source></property>
<property><name>fs.ftp.host.port</name><value>21</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.admin.client.thread-count</name><value>1</value><source>yarn-default.xml</source></property>
<property><name>dfs.namenode.checkpoint.period</name><value>3600</value></property>
<property><name>fs.AbstractFileSystem.viewfs.impl</name><value>org.apache.hadoop.fs.viewfs.ViewFs</value><source>core-default.xml</source></property>
<property><name>yarn.resourcemanager.resource-tracker.client.thread-count</name><value>50</value><source>yarn-default.xml</source></property>
<property><name>yarn.timeline-service.leveldb-timeline-store.ttl-interval-ms</name><value>300000</value><source>yarn-default.xml</source></property>
<property><name>mapreduce.tasktracker.dns.nameserver</name><value>default</value><source>mapred-default.xml</source></property>
<property><name>yarn.resourcemanager.system</name><value>/var/mapr/cluster/yarn/rm/system</value></property>
<property><name>ipc.client.fallback-to-simple-auth-allowed</name><value>false</value><source>core-default.xml</source></property>
<property><name>mapreduce.map.output.compress</name><value>false</value><source>mapred-default.xml</source></property>
<property><name>fs.webhdfs.impl</name><value>org.apache.hadoop.hdfs.web.WebHdfsFileSystem</value></property>
<property><name>yarn.nodemanager.delete.debug-delay-sec</name><value>0</value><source>yarn-default.xml</source></property>
<property><name>hadoop.ssl.require.client.cert</name><value>false</value><source>core-default.xml</source></property>
</configuration>








