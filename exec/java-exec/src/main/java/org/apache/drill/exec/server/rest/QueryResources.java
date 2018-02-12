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

import com.google.gson.Gson;

import com.google.gson.JsonObject;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.coord.store.TransientStore;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryInfo;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.server.QueryProfileStoreContext;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.hadoop.util.hash.Hash;
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
import javax.ws.rs.QueryParam;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Path("/")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class QueryResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryResources.class);
  final String ZKPATH = "/drill/infomation/";

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

  @GET
  @Path("/cancelQuery")
  @Produces(MediaType.TEXT_PLAIN)
  public String cancelCurrentQuery() {
    Gson gson = new Gson();
    Map<String, String> res = new HashMap<>();
    String msg;
    String type;


    final QueryProfileStoreContext profileStoreContext = work.getContext().getProfileStoreContext();
    final TransientStore<QueryInfo> running = profileStoreContext.getRunningProfileStore();
    final Iterator<Map.Entry<String, QueryInfo>> runningEntries = running.entries();

    // check running
    if(runningEntries.hasNext()) {
    String queryId = runningEntries.next().getKey();
    final QueryInfo info = running.get(queryId);
    UserBitShared.QueryId id = QueryIdHelper.getQueryIdFromString(queryId);

    try {
      // try to cancel
      GeneralRPCProtos.Ack a = work.getContext().getController().getTunnel(info.getForeman()).requestCancelQuery(id).checkedGet(2, TimeUnit.SECONDS);
      if(a.getOk()){
        type = "success";
        msg = String.format("Query %s canceled on node %s.", queryId, info.getForeman().getAddress());
      }else{
        type = "fail";
        msg = String.format("Attempted to cancel query %s on %s but the query is no longer active on that node.", queryId, info.getForeman().getAddress());
      }

    }catch(Exception e){
      logger.debug("Failure to cancel the running query.", e);
      type = "fail";
      msg = String.format
              ("Failure attempting to cancel the query .  Unable to find information about where query is actively running.");
    }
    } else {
      type = "fail";
      msg = String.format
            ("Failure attempting to cancel the query .  Unable to find information about where query is actively running.");
    }

    res.put("type", type);
    res.put("msg", msg);
    return gson.toJson(res);
  }


  @GET
  @Path("/data")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.TEXT_PLAIN)
  public String getData(@QueryParam("username") String UserName) throws Exception {
    String username = webUserConnection.getSession().getCredentials().getUserName();
    logger.info("picasso: getData: username: " + username);

    if(username.equals("")) {
      username = "bingxing.wang";
    }
    String nodePath = ZKPATH + username;
    String zk_ip = work.getContext().getConfig().getString(ExecConstants.ZK_CONNECTION);
    ZooKeeper zk = new ZooKeeper(zk_ip, 5000, new ZookeeperSimple());
    Gson gson = new Gson();
    Map<String, String> res = new HashMap<>();
    String msg;
    String type;

    try {
      if(zk.exists(nodePath, true) == null) {
        zk.close();
        // 没有数据视为成功
        msg = "";
        type = "success";
      } else {
        String data = new String(zk.getData(nodePath, true, null));
        zk.close();
        // json格式的字符串
        msg = data;
        type = "success";
      }
    } catch (InterruptedException e) {
        msg = e.getMessage();
        type = "fail";
    } catch (KeeperException e) {
        msg = e.getMessage();
        type = "fail";
    } catch (Exception e) {
      msg = e.getMessage();
      type = "fail";
    }

    res.put("type", type);
    res.put("msg", msg);

    return gson.toJson(res);
  }

  @POST
  @Path("/data")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.TEXT_PLAIN)
  public String setData(@FormParam("username") String UserName, @FormParam("data") String data) throws Exception {
    String username = webUserConnection.getSession().getCredentials().getUserName();
    logger.info("picasso: setData: username: " + username);

    if(username.equals("")) {
      username = "bingxing.wang";
    }

    logger.info("picasso: setData: data:" + data);
    //logger.info("picasso: setData: sql:" + sql);

    Gson gson = new Gson();
    Map<String, String> res = new HashMap<>();
    String msg = "";
    String sqls = data;
    String type;

    String nodePath = ZKPATH + username;
    String zk_ip = work.getContext().getConfig().getString(ExecConstants.ZK_CONNECTION);
    ZooKeeper zk = new ZooKeeper(zk_ip, 5000, new ZookeeperSimple());

    try{
      if(zk.exists(nodePath, true) == null) {
        zk.create(nodePath, sqls.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.close();
        type = "success";
      } else {
        zk.setData(nodePath, sqls.getBytes(), -1);
        zk.close();
        type = "success";
      }
    } catch (Exception e) {
      type = "fail";
      msg = e.getMessage();

    }

    res.put("type", type);
    res.put("msg", msg);
    return gson.toJson(res);
  }

  @POST
  @Path("/query")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.TEXT_PLAIN)
  public String submitQuery(@FormParam("query") String query) throws Exception {

    // 这里需要设置为constant
    String queryType = "SQL";
    Gson gson = new Gson();
    Map<String, Object> res = new HashMap<>();
    Map<String, Object> data = new HashMap<>();

    logger.info("picasso: submitQuery: query:" + query);


    try {
      final String trimmedQueryString = CharMatcher.is(';').trimTrailingFrom(query.trim());
      final QueryResult result = submitQueryJSON(new QueryWrapper(trimmedQueryString, queryType));

      List<Map<String, String>> rows = result.rows;
      List<String> columns = ImmutableList.copyOf(result.columns);
      if(columns.isEmpty()) {
        res.put("type", "empty");
        data.put("msg", "");
        res.put("data", data);
        return gson.toJson(res);
      }
      data.put("thead", columns);
      // 只返回1000条
      if(rows.size() > 1000) {
        data.put("tbody", rows.subList(0, 1000));
      } else {
        data.put("tbody", rows);
      }
      res.put("data", data);
      res.put("type", "success");

      return gson.toJson(res);

      // 原始的返回
      //return ViewableWithPermissions.create(authEnabled.get(), "/rest/query/result.ftl", sc, new TabularResult(result));
    } catch (Exception | Error e) {
      logger.error("Query from Web UI Failed", e);
      data.put("msg", e.toString());
      res.put("type", "fail");
      res.put("data", data);

      return gson.toJson(res);
      // 原始的返回
      //return ViewableWithPermissions.create(authEnabled.get(), "/rest/query/errorMessage.ftl", sc, e);
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
