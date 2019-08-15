/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.httpd;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class TestHTTPDLogReader extends ClusterTest {

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    defineHTTPDPlugin();
  }

  private static void defineHTTPDPlugin() throws ExecutionSetupException {

    // Create an instance of the regex config.
    // Note: we can"t use the ".log" extension; the Drill .gitignore
    // file ignores such files, so they"ll never get committed. Instead,
    // make up a fake suffix.
    HttpdLogFormatConfig sampleConfig = new HttpdLogFormatConfig();
    sampleConfig.setLogFormat("%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"");

    // Define a temporary format plugin for the "cp" storage plugin.
    Drillbit drillbit = cluster.drillbit();
    final StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();
    final FileSystemPlugin plugin = (FileSystemPlugin) pluginRegistry.getPlugin("cp");
    final FileSystemConfig pluginConfig = (FileSystemConfig) plugin.getConfig();
    pluginConfig.getFormats().put("sample", sampleConfig);
    pluginRegistry.createOrUpdate("cp", pluginConfig, false);
  }

  @Test
  public void testDateField() throws RpcException {
    String sql = "SELECT `request_receive_time` FROM cp.`httpd/hackers-access-small.httpd` LIMIT 5";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
            .addNullable("request_receive_time", MinorType.TIMESTAMP)
            .buildSchema();
    RowSet expected = client.rowSetBuilder(expectedSchema)
            .addRow(1445742685000L)
            .addRow(1445742686000L)
            .addRow(1445742687000L)
            .addRow(1445743471000L)
            .addRow(1445743472000L)
            .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testSelectColumns() throws Exception {
    String sql = "SELECT request_referer_ref,\n" +
            "request_receive_time_last_time,\n" +
            "request_firstline_uri_protocol,\n" +
            "request_receive_time_microsecond,\n" +
            "request_receive_time_last_microsecond__utc,\n" +
            "request_firstline_original_protocol,\n" +
            "request_firstline_original_uri_host,\n" +
            "request_referer_host,\n" +
            "request_receive_time_month__utc,\n" +
            "request_receive_time_last_minute,\n" +
            "request_firstline_protocol_version,\n" +
            "request_receive_time_time__utc,\n" +
            "request_referer_last_ref,\n" +
            "request_receive_time_last_timezone,\n" +
            "request_receive_time_last_weekofweekyear,\n" +
            "request_referer_last,\n" +
            "request_receive_time_minute,\n" +
            "connection_client_host_last,\n" +
            "request_receive_time_last_millisecond__utc,\n" +
            "request_firstline_original_uri,\n" +
            "request_firstline,\n" +
            "request_receive_time_nanosecond,\n" +
            "request_receive_time_last_millisecond,\n" +
            "request_receive_time_day,\n" +
            "request_referer_port,\n" +
            "request_firstline_original_uri_port,\n" +
            "request_receive_time_year,\n" +
            "request_receive_time_last_date,\n" +
            "request_receive_time_last_time__utc,\n" +
            "request_receive_time_last_hour__utc,\n" +
            "request_firstline_original_protocol_version,\n" +
            "request_firstline_original_method,\n" +
            "request_receive_time_last_year__utc,\n" +
            "request_firstline_uri,\n" +
            "request_referer_last_host,\n" +
            "request_receive_time_last_minute__utc,\n" +
            "request_receive_time_weekofweekyear,\n" +
            "request_firstline_uri_userinfo,\n" +
            "request_receive_time_epoch,\n" +
            "connection_client_logname,\n" +
            "response_body_bytes,\n" +
            "request_receive_time_nanosecond__utc,\n" +
            "request_firstline_protocol,\n" +
            "request_receive_time_microsecond__utc,\n" +
            "request_receive_time_hour,\n" +
            "request_firstline_uri_host,\n" +
            "request_referer_last_port,\n" +
            "request_receive_time_last_epoch,\n" +
            "request_receive_time_last_weekyear__utc,\n" +
            "request_useragent,\n" +
            "request_receive_time_weekyear,\n" +
            "request_receive_time_timezone,\n" +
            "response_body_bytesclf,\n" +
            "request_receive_time_last_date__utc,\n" +
            "request_receive_time_millisecond__utc,\n" +
            "request_referer_last_protocol,\n" +
            "request_status_last,\n" +
            "request_firstline_uri_query,\n" +
            "request_receive_time_minute__utc,\n" +
            "request_firstline_original_uri_protocol,\n" +
            "request_referer_query,\n" +
            "request_receive_time_date,\n" +
            "request_firstline_uri_port,\n" +
            "request_receive_time_last_second__utc,\n" +
            "request_referer_last_userinfo,\n" +
            "request_receive_time_last_second,\n" +
            "request_receive_time_last_monthname__utc,\n" +
            "request_firstline_method,\n" +
            "request_receive_time_last_month__utc,\n" +
            "request_receive_time_millisecond,\n" +
            "request_receive_time_day__utc,\n" +
            "request_receive_time_year__utc,\n" +
            "request_receive_time_weekofweekyear__utc,\n" +
            "request_receive_time_second,\n" +
            "request_firstline_original_uri_ref,\n" +
            "connection_client_logname_last,\n" +
            "request_receive_time_last_year,\n" +
            "request_firstline_original_uri_path,\n" +
            "connection_client_host,\n" +
            "request_firstline_original_uri_query,\n" +
            "request_referer_userinfo,\n" +
            "request_receive_time_last_monthname,\n" +
            "request_referer_path,\n" +
            "request_receive_time_monthname,\n" +
            "request_receive_time_last_month,\n" +
            "request_referer_last_query,\n" +
            "request_firstline_uri_ref,\n" +
            "request_receive_time_last_day,\n" +
            "request_receive_time_time,\n" +
            "request_receive_time_last_weekofweekyear__utc,\n" +
            "request_useragent_last,\n" +
            "request_receive_time_last_weekyear,\n" +
            "request_receive_time_last_microsecond,\n" +
            "request_firstline_original,\n" +
            "request_referer_last_path,\n" +
            "request_receive_time_month,\n" +
            "request_receive_time_last_day__utc,\n" +
            "request_referer,\n" +
            "request_referer_protocol,\n" +
            "request_receive_time_monthname__utc,\n" +
            "response_body_bytes_last,\n" +
            "request_receive_time,\n" +
            "request_receive_time_last_nanosecond,\n" +
            "request_firstline_uri_path,\n" +
            "request_firstline_original_uri_userinfo,\n" +
            "request_receive_time_date__utc,\n" +
            "request_receive_time_last,\n" +
            "request_receive_time_last_nanosecond__utc,\n" +
            "request_receive_time_last_hour,\n" +
            "request_receive_time_hour__utc,\n" +
            "request_receive_time_second__utc,\n" +
            "connection_client_user_last,\n" +
            "request_receive_time_weekyear__utc,\n" +
            "connection_client_user\n" +
            "FROM cp.`httpd/hackers-access-small.httpd`\n" +
            "LIMIT 1";

    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("request_referer_ref", "request_receive_time_last_time", "request_firstline_uri_protocol", "request_receive_time_microsecond", "request_receive_time_last_microsecond__utc", "request_firstline_original_protocol", "request_firstline_original_uri_host", "request_referer_host", "request_receive_time_month__utc", "request_receive_time_last_minute", "request_firstline_protocol_version", "request_receive_time_time__utc", "request_referer_last_ref", "request_receive_time_last_timezone", "request_receive_time_last_weekofweekyear", "request_referer_last", "request_receive_time_minute", "connection_client_host_last", "request_receive_time_last_millisecond__utc", "request_firstline_original_uri", "request_firstline", "request_receive_time_nanosecond", "request_receive_time_last_millisecond", "request_receive_time_day", "request_referer_port", "request_firstline_original_uri_port", "request_receive_time_year", "request_receive_time_last_date", "request_receive_time_last_time__utc", "request_receive_time_last_hour__utc", "request_firstline_original_protocol_version", "request_firstline_original_method", "request_receive_time_last_year__utc", "request_firstline_uri", "request_referer_last_host", "request_receive_time_last_minute__utc", "request_receive_time_weekofweekyear", "request_firstline_uri_userinfo", "request_receive_time_epoch", "connection_client_logname", "response_body_bytes", "request_receive_time_nanosecond__utc", "request_firstline_protocol", "request_receive_time_microsecond__utc", "request_receive_time_hour", "request_firstline_uri_host", "request_referer_last_port", "request_receive_time_last_epoch", "request_receive_time_last_weekyear__utc", "request_useragent", "request_receive_time_weekyear", "request_receive_time_timezone", "response_body_bytesclf", "request_receive_time_last_date__utc", "request_receive_time_millisecond__utc", "request_referer_last_protocol", "request_status_last", "request_firstline_uri_query", "request_receive_time_minute__utc", "request_firstline_original_uri_protocol", "request_referer_query", "request_receive_time_date", "request_firstline_uri_port", "request_receive_time_last_second__utc", "request_referer_last_userinfo", "request_receive_time_last_second", "request_receive_time_last_monthname__utc", "request_firstline_method", "request_receive_time_last_month__utc", "request_receive_time_millisecond", "request_receive_time_day__utc", "request_receive_time_year__utc", "request_receive_time_weekofweekyear__utc", "request_receive_time_second", "request_firstline_original_uri_ref", "connection_client_logname_last", "request_receive_time_last_year", "request_firstline_original_uri_path", "connection_client_host", "request_firstline_original_uri_query", "request_referer_userinfo", "request_receive_time_last_monthname", "request_referer_path", "request_receive_time_monthname", "request_receive_time_last_month", "request_referer_last_query", "request_firstline_uri_ref", "request_receive_time_last_day", "request_receive_time_time", "request_receive_time_last_weekofweekyear__utc", "request_useragent_last", "request_receive_time_last_weekyear", "request_receive_time_last_microsecond", "request_firstline_original", "request_referer_last_path", "request_receive_time_month", "request_receive_time_last_day__utc", "request_referer", "request_referer_protocol", "request_receive_time_monthname__utc", "response_body_bytes_last", "request_receive_time", "request_receive_time_last_nanosecond", "request_firstline_uri_path", "request_firstline_original_uri_userinfo", "request_receive_time_date__utc", "request_receive_time_last", "request_receive_time_last_nanosecond__utc", "request_receive_time_last_hour", "request_receive_time_hour__utc", "request_receive_time_second__utc", "connection_client_user_last", "request_receive_time_weekyear__utc", "connection_client_user")
            .baselineValues(null, "04:11:25", null, 0L, 0L, "HTTP", null, "howto.basjes.nl", 10L, 11L, "1.1", "03:11:25", null, null, 43L, "http://howto.basjes.nl/", 11L, "195.154.46.135", 0L, "/linux/doing-pxe-without-dhcp-control", "GET /linux/doing-pxe-without-dhcp-control HTTP/1.1", 0L, 0L, 25L, null, null, 2015L, "2015-10-25", "03:11:25", 3L, "1.1", "GET", 2015L, "/linux/doing-pxe-without-dhcp-control", "howto.basjes.nl", 11L, 43L, null, 1445742685000L, null, 24323L, 0L, "HTTP", 0L, 4L, null, null, 1445742685000L, 2015L, "Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0", 2015L, null, 24323L, "2015-10-25", 0L, "http", "200", "", 11L, null, "", "2015-10-25", null, 25L, null, 25L, "October", "GET", 10L, 0L, 25L, 2015L, 43L, 25L, null, null, 2015L, "/linux/doing-pxe-without-dhcp-control", "195.154.46.135", "", null, "October", "/", "October", 10L, "", null, 25L, "04:11:25", 43L, "Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0", 2015L, 0L, "GET /linux/doing-pxe-without-dhcp-control HTTP/1.1", "/", 10L, 25L, "http://howto.basjes.nl/", "http", "October", 24323L, LocalDateTime.parse("2015-10-25T03:11:25"), 0L, "/linux/doing-pxe-without-dhcp-control", null, "2015-10-25", LocalDateTime.parse("2015-10-25T03:11:25"), 0L, 4L, 3L, 25L, null, 2015L, null)
            .go();
  }


  @Test
  public void testCount() throws Exception {
    String sql = "SELECT COUNT(*) FROM cp.`httpd/hackers-access-small.httpd`";
    long result = client.queryBuilder().sql(sql).singletonLong();
    assertEquals(10, result);
  }

  @Test
  public void testStar() throws Exception {
    String sql = "SELECT * FROM cp.`httpd/hackers-access-small.httpd` LIMIT 1";

    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("request_referer_ref","request_receive_time_last_time","request_firstline_uri_protocol","request_receive_time_microsecond","request_receive_time_last_microsecond__utc","request_firstline_original_uri_query_$","request_firstline_original_protocol","request_firstline_original_uri_host","request_referer_host","request_receive_time_month__utc","request_receive_time_last_minute","request_firstline_protocol_version","request_receive_time_time__utc","request_referer_last_ref","request_receive_time_last_timezone","request_receive_time_last_weekofweekyear","request_referer_last","request_receive_time_minute","connection_client_host_last","request_receive_time_last_millisecond__utc","request_firstline_original_uri","request_firstline","request_receive_time_nanosecond","request_receive_time_last_millisecond","request_receive_time_day","request_referer_port","request_firstline_original_uri_port","request_receive_time_year","request_receive_time_last_date","request_referer_query_$","request_receive_time_last_time__utc","request_receive_time_last_hour__utc","request_firstline_original_protocol_version","request_firstline_original_method","request_receive_time_last_year__utc","request_firstline_uri","request_referer_last_host","request_receive_time_last_minute__utc","request_receive_time_weekofweekyear","request_firstline_uri_userinfo","request_receive_time_epoch","connection_client_logname","response_body_bytes","request_receive_time_nanosecond__utc","request_firstline_protocol","request_receive_time_microsecond__utc","request_receive_time_hour","request_firstline_uri_host","request_referer_last_port","request_receive_time_last_epoch","request_receive_time_last_weekyear__utc","request_receive_time_weekyear","request_receive_time_timezone","response_body_bytesclf","request_receive_time_last_date__utc","request_useragent_last","request_useragent","request_receive_time_millisecond__utc","request_referer_last_protocol","request_status_last","request_firstline_uri_query","request_receive_time_minute__utc","request_firstline_original_uri_protocol","request_referer_query","request_receive_time_date","request_firstline_uri_port","request_receive_time_last_second__utc","request_referer_last_userinfo","request_receive_time_last_second","request_receive_time_last_monthname__utc","request_firstline_method","request_receive_time_last_month__utc","request_receive_time_millisecond","request_receive_time_day__utc","request_receive_time_year__utc","request_receive_time_weekofweekyear__utc","request_receive_time_second","request_firstline_original_uri_ref","connection_client_logname_last","request_receive_time_last_year","request_firstline_original_uri_path","connection_client_host","request_referer_last_query_$","request_firstline_original_uri_query","request_referer_userinfo","request_receive_time_last_monthname","request_referer_path","request_receive_time_monthname","request_receive_time_last_month","request_referer_last_query","request_firstline_uri_ref","request_receive_time_last_day","request_receive_time_time","request_receive_time_last_weekofweekyear__utc","request_receive_time_last_weekyear","request_receive_time_last_microsecond","request_firstline_original","request_firstline_uri_query_$","request_referer_last_path","request_receive_time_month","request_receive_time_last_day__utc","request_referer","request_referer_protocol","request_receive_time_monthname__utc","response_body_bytes_last","request_receive_time","request_receive_time_last_nanosecond","request_firstline_uri_path","request_firstline_original_uri_userinfo","request_receive_time_date__utc","request_receive_time_last","request_receive_time_last_nanosecond__utc","request_receive_time_last_hour","request_receive_time_hour__utc","request_receive_time_second__utc","connection_client_user_last","request_receive_time_weekyear__utc","connection_client_user")
            .baselineValues(null,"04:11:25",null,0L,0L,new HashMap<>(),"HTTP",null,"howto.basjes.nl",10L,11L,"1.1","03:11:25",null,null,43L,"http://howto.basjes.nl/",11L,"195.154.46.135",0L,"/linux/doing-pxe-without-dhcp-control","GET /linux/doing-pxe-without-dhcp-control HTTP/1.1",0L,0L,25L,null,null,2015L,"2015-10-25",new HashMap<>(),"03:11:25",3L,"1.1","GET",2015L,"/linux/doing-pxe-without-dhcp-control","howto.basjes.nl",11L,43L,null,1445742685000L,null,24323L,0L,"HTTP",0L,4L,null,null,1445742685000L,2015L,2015L,null,24323L,"2015-10-25","Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0","Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0",0L,"http","200","",11L,null,"","2015-10-25",null,25L,null,25L,"October","GET",10L,0L,25L,2015L,43L,25L,null,null,2015L,"/linux/doing-pxe-without-dhcp-control","195.154.46.135",new HashMap<>(),"",null,"October","/","October",10L,"",null,25L,"04:11:25",43L,2015L,0L,"GET /linux/doing-pxe-without-dhcp-control HTTP/1.1",new HashMap<>(),"/",10L,25L,"http://howto.basjes.nl/","http","October",24323L,LocalDateTime.parse("2015-10-25T03:11:25"),0L,"/linux/doing-pxe-without-dhcp-control",null,"2015-10-25",LocalDateTime.parse("2015-10-25T03:11:25"),0L,4L,3L,25L,null,2015L,null)
            .go();
  }
}
