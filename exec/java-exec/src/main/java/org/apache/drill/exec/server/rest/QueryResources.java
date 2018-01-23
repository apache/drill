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
package org.apache.drill.exec.server.rest;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.WatchedEvent;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.server.rest.QueryWrapper.QueryResult;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Path("/")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class QueryResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryResources.class);

  @Inject UserAuthEnabled authEnabled;
  @Inject WorkManager work;
  @Inject SecurityContext sc;
  @Inject WebUserConnection webUserConnection;


  @GET
  @Path("/query")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery() {
    return ViewableWithPermissions.create(
        authEnabled.get(),
        "/rest/query/query.ftl",
        sc,
        // if impersonation is enabled without authentication, will provide mechanism to add user name to request header from Web UI
        WebServer.isImpersonationOnlyEnabled(work.getContext().getConfig()));
  }


  @POST
  @Path("/query.json")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public QueryResult submitQueryJSON(QueryWrapper query) throws Exception {
    try {
      // Run the query
      return query.run(work, webUserConnection);
    } finally {
      // no-op for authenticated user
      webUserConnection.cleanupSession();
    }
  }

  // 限制网页query最大返回1000条
  public static String limit_query(String query){
    //去除首尾空格，替换多空格为一个空格，转化小写
    String sql = query.trim().replaceAll(" +", " ").toLowerCase();
    //以空格切分字符串获取sql语句各组成部分
    String word[] = sql.split(" ");

    /*
      非select语句
        返回原语句
      select语句
        结尾有limit
          limit后数字大于1000
            替换为1000
          小于1000
            返回原语句
        无limit
          语句后 + " limit 1000"
     */
    try{
      if (!sql.startsWith("select")) {
        return query;
      } else if (word[word.length - 2].equals("limit")) {
        if (Integer.parseInt(word[word.length - 1]) > 1000){
          return sql.replace("limit " + word[word.length - 1], "limit 1000");
        } else {
          return query;
        }
      } else {
        return query + " limit 1000";
      }
    } catch (IndexOutOfBoundsException e) {
      // 可能传入的query不完整
      return query;
    } catch (NumberFormatException e) {
      // 可能limit后错误输入了数字
      return query;
    } catch (Exception e) {
      //e.printStackTrace();
      logger.error("limit_query " + e);
      return query;
    }

  }

  public String getZKData(String path) throws IOException {
    String zk_ip = work.getContext().getConfig().getString(ExecConstants.ZK_CONNECTION);
    logger.info("zk ip " + zk_ip);
    ZooKeeper zk = new ZooKeeper(zk_ip, 5000, new ZookeeperSimple());

    try {
      logger.info("Zookeeper session established");
      if(zk.exists(path, true) == null) {
        zk.close();
        return "write and save.";
      } else {
        String data = new String(zk.getData("/zk_test/infomation", true, null));
        zk.close();
        return data;
      }
    } catch (InterruptedException e){
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
    } finally {
      return "something error... please contact manager";
    }
  }

  public String setZKData(String path, String data) throws IOException {
    String zk_ip = work.getContext().getConfig().getString(ExecConstants.ZK_CONNECTION);
    logger.info("zk ip " + zk_ip);
    ZooKeeper zk = new ZooKeeper(zk_ip, 5000, new ZookeeperSimple());

    try{
        if(zk.exists(path, true) == null) {
          zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
          zk.close();
          return "success";
        } else {
          zk.setData(path, data.getBytes(), -1);
          zk.close();
          return "success";
        }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
    }

    return "fail";
  }

  @GET
  @Path("/data")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.TEXT_HTML)
  public String getData(@FormParam("username") String username) throws Exception {
    logger.info("picasso: username: " + username);
    String data = getZKData("/zk_test/" + username);
    return data;
  }

  @POST
  @Path("/data")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.TEXT_HTML)
  public String setData(@FormParam("username") String username, @FormParam("data") String data) throws Exception {
    logger.info("picasso: username: " + username + " data: " + data);
    String res = setZKData("/zk_test/" + username, data);
    return res;
  }

  @POST
  @Path("/query")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.TEXT_HTML)
  public Viewable submitQuery(@FormParam("query") String query,
                              @FormParam("queryType") String queryType) throws Exception {
    logger.info("picasso: submitQuery: query:" + query);

    //query = limit_query(query);
    //logger.info("picasso: submitQuery: after limit, query:" + query);
    try {
      final String trimmedQueryString = CharMatcher.is(';').trimTrailingFrom(query.trim());
      final QueryResult result = submitQueryJSON(new QueryWrapper(trimmedQueryString, queryType));

      return ViewableWithPermissions.create(authEnabled.get(), "/rest/query/result.ftl", sc, new TabularResult(result));
    } catch (Exception | Error e) {
      logger.error("Query from Web UI Failed", e);
      return ViewableWithPermissions.create(authEnabled.get(), "/rest/query/errorMessage.ftl", sc, e);
    }
  }

  public class ZookeeperSimple implements Watcher {
    private  CountDownLatch connectedSemaphore = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent watchedEvent) {

      System.out.println("Receive watched event: " + watchedEvent);
      if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
        connectedSemaphore.countDown();
      }
    }
  }

  public static class TabularResult {
    private final List<String> columns;
    private final List<List<String>> rows;

    public TabularResult(QueryResult result) {
      final List<List<String>> rows = Lists.newArrayList();
      for (Map<String, String> rowMap:result.rows) {
        final List<String> row = Lists.newArrayList();
        for (String col:result.columns) {
          row.add(rowMap.get(col));
        }
        rows.add(row);
      }

      this.columns = ImmutableList.copyOf(result.columns);
      this.rows = rows;
    }

    public boolean isEmpty() {
      return columns.isEmpty();
    }

    public List<String> getColumns() {
      return columns;
    }

    public List<List<String>> getRows() {
      if(rows.size() <= 1000) {
        return rows;
      }
      else {
        return rows.subList(0, 1000);
      }
    }
  }


}
