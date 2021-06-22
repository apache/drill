---
title: "Modify logback.xml"
slug: "Modify logback.xml"
parent: "Log and Debug"
---

You can access `logback.xml` in `~<drill_installation_directory>/conf/`. The default log level is set to INFO. You can enable debug logging and Lilith in `logback.xml`. Drill should automatically pick up changes to `logback.xml` if you modify the file while Drill is running. If not, restart the cluster for Logback to pick up new settings.

Logback.xml contains two appenders, STDOUT and FILE, that determine where Drill outputs logs. STDOUT output is directed to the console. FILE output is directed to drillbit.log and drillbit.out located in /var/log/drill/conf. You can modify these appenders to redirect log ouput to different locations.

## Enable Debug Logging
You can enable DEBUG on some loggers for additional information. Each of the classes listed below have a logger that you can add to logback.xml:

* Task execution (similar to MR task submit output)  
  * org.apache.drill.exec.rpc.control.WorkEventBus  
  * org.apache.drill.exec.work.fragment.FragmentExecutor  
  * org.apache.drill.exec.work.foreman.QueryManager  
* Cluster cohesion and coordination  
  * org.apache.drill.exec.coord.zk.ZkClusterCoordinator  
* Query text and plans
  * org.apache.drill.exec.planner.sql.handlers.DefaultSqlHandler

To enable DEBUG on a logger, add the logger to logback.xml, and set the level value to “debug.”
The following example shows a task execution logger that you can add to logback.xml:

              <logger name="org.apache.drill.exec.work.foreman.QueryManager" additivity="false">
                 <level value="debug" />
                <appender-ref ref="FILE" />
              </logger>

## Enable Lilith
You can use Lilith and the socket appender for local debugging. Lillith connects to a Drillbit and shows logging as it happens.

To enable log messages to output to Lilith, uncomment the following two sections in logback.xml and change LILITH_HOSTNAME to that of the machine running the Lilith application:

       <appender name="SOCKET" class="de.huxhorn.lilith.logback.appender.ClassicMultiplexSocketAppender"> <Compressing>true</Compressing> <ReconnectionDelay>10000</ReconnectionDelay> <IncludeCallerData>true</IncludeCallerData> <RemoteHosts>${LILITH_HOSTNAME:-localhost}</RemoteHosts> </appender>
       
       <logger name="org.apache.drill" additivity="false">
           <level value="debug" />
           <appender-ref ref="SOCKET" />
       </logger>
 
## Add the Hostname
Logback includes a layout called PatternLayout that takes a logging event and returns a string. You can modify PatternLayout's conversion pattern to include the hostname in the log message. Add the hostname to the conversion pattern in logback.xml to identify which machine is sending log messages.

The following example shows the conversion pattern with the hostname included:

       <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>$%d{HH:mm:ss.SSS} %property{HOSTNAME} [%thread] %-5level %logger{36} - %msg%n
            </pattern>
        </encoder>
         </appender>


