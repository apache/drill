<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->

<#include "*/generic.ftl">

<#-- Format comma-delimited string-->
<#macro format_string str>
  <#if str?has_content>
    ${(str?split(","))?join(", ")}
  <#else>
    ${"<empty>"}
  </#if>
</#macro>

<#macro page_head>
<style>
.list-value {
    text-align: right !important;
}
</style>
</#macro>

<#macro page_body>
  <a href="/queries">back</a><br/>
  <div class="page-header">
  </div>

  <#if (model.getMismatchedVersions()?size > 0)>
    <div id="message" class="alert alert-danger alert-dismissable">
      <button type="button" class="close" data-dismiss="alert" aria-hidden="true">&times;</button>
      <strong>Drill does not support clusters containing a mix of Drillbit versions.
          Current drillbit version is ${model.getCurrentVersion()}.
          One or more drillbits in cluster have different version:
          ${model.getMismatchedVersions()?join(", ")}.
      </strong>
    </div>
  </#if>

  <div class="row">
    <div class="col-md-12">
      <h3>Drillbits <span class="label label-primary" id="size" >${model.getDrillbits()?size}</span></h3>
      <div class="table-responsive">
        <table class="table table-hover">
          <thead>
            <tr>
              <th>#</th>
              <th>Address</th>
              <th>User Port</th>
              <th>Control Port</th>
              <th>Data Port</th>
              <th>Version</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            <#assign i = 1>
            <#list model.getDrillbits() as drillbit>
              <tr id="row-${i}">
                <td>${i}</td>
                <td id="address" >${drillbit.getAddress()}<#if drillbit.isCurrent()>
                    <span class="label label-info">Current</span>
                  </#if>
                </td>
                <td id="port" >${drillbit.getUserPort()}</td>
                <td>${drillbit.getControlPort()}</td>
                <td>${drillbit.getDataPort()}</td>
                <td>
                  <span class="label
                    <#if drillbit.isVersionMatch()>label-success<#else>label-danger</#if>">
                    ${drillbit.getVersion()}
                  </span>
                </td>
                <td id="status" >${drillbit.getState()}</td>
                <#if model.shouldShowAdminInfo()>
                  <td>
                      <button type="button" id="shutdown" onClick="shutdown('${drillbit.getAddress()}',$(this));"> SHUTDOWN </button>
                  </td>
                </#if>
                <td id="queriesCount">  </td>
              </tr>
              <#assign i = i + 1>
            </#list>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="row">
      <div class="col-md-12">
        <h3>Encryption</h3>
        <div class="table-responsive">
          <table class="table table-hover" style="width: auto;">
            <tbody>
                <tr>
                  <td>Client to Bit Encryption</td>
                  <td class="list-value">${model.isUserEncryptionEnabled()?string("Enabled", "Disabled")}</td>
                </tr>
                <tr>
                  <td>Bit to Bit Encryption</td>
                  <td class="list-value">${model.isBitEncryptionEnabled()?string("Enabled", "Disabled")}</td>
                </tr>
            </tbody>
          </table>
        </div>
      </div>
  </div>

   <#if model.shouldShowAdminInfo()>
       <div class="row">
            <div class="col-md-12">
              <h3>User Info </h3>
              <div class="table-responsive">
                <table class="table table-hover" style="width: auto;">
                  <tbody>
                      <tr>
                        <td>Admin Users</td>
                        <td class="list-value"><@format_string str=model.getAdminUsers()/></td>
                      </tr>
                      <tr>
                        <td>Admin User Groups</td>
                        <td class="list-value"><@format_string str=model.getAdminUserGroups()/></td>
                      </tr>
                      <tr>
                        <td>Process User</td>
                        <td class="list-value">${model.getProcessUser()}</td>
                      </tr>
                      <tr>
                        <td>Process User Groups</td>
                        <td class="list-value">${model.getProcessUserGroups()}</td>
                      </tr>
                  </tbody>
                </table>
              </div>
            </div>
        </div>
   </#if>

  <#assign queueInfo = model.queueInfo() />
  <div class="row">
      <div class="col-md-12">
        <h3>Query Throttling</h3>
        <div class="table-responsive">
          <table class="table table-hover" style="width: auto;">
            <tbody>
               <tr>
                  <td>Queue Status</td>
                  <td class="list-value">${queueInfo.isEnabled()?string("Enabled", "Disabled")}</td>
                </tr>
  <#if queueInfo.isEnabled() >
                <tr>
                  <td>Maximum Concurrent "Small" Queries</td>
                  <td class="list-value">${queueInfo.smallQueueSize()}</td>
                 </tr>
                 <tr>
                  <td>Maximum Concurrent "Large" Queries</td>
                  <td class="list-value">${queueInfo.largeQueueSize()}</td>
                </tr>
                  <tr>
                  <td>Cost Threshhold for Large vs. Small Queries</td>
                  <td class="list-value">${queueInfo.threshold()}</td>
                </tr>
                <tr>
                  <td>Total Memory</td>
                  <td class="list-value">${queueInfo.totalMemory()}</td>
                </tr>
                <tr>
                  <td>Memory per Small Query</td>
                  <td class="list-value">${queueInfo.smallQueueMemory()}</td>
                </tr>
                <tr>
                  <td>Memory per Large Query</td>
                  <td class="list-value">${queueInfo.largeQueueMemory()}</td>
                </tr>
  </#if>
            </tbody>
          </table>
        </div>
      </div>
  </div>
   <script charset="utf-8">
      var refreshTime = 10000;
      var refresh = getRefreshTime();
      var portNum = 0;
      var port = getPortNum();
      var timeout;
      var size = $("#size").html();


      function getPortNum() {
          var port = $.ajax({
                          type: 'GET',
                          url: '/portNum',
                          dataType: "json",
                          complete: function(data) {
                                portNum = data.responseJSON["port"];
                                }
                          });
      }

      function getRefreshTime() {
          $.ajax({
              type: 'GET',
              url: '/gracePeriod',
              dataType: "json",
              complete: function (data) {
                  var gracePeriod = data.responseJSON["gracePeriod"];
                  if (gracePeriod > 0) {
                      refreshTime = gracePeriod / 3;
                  }
                  timeout = setTimeout(reloadStatus, refreshTime);
              }
          });
      }

      function reloadStatus () {
          var result = $.ajax({
                      type: 'GET',
                      url: '/state',
                      dataType: "json",
                      complete: function(data) {
                            fillStatus(data,size);
                            }
                      });
          timeout = setTimeout(reloadStatus, refreshTime);
      }

      function fillStatus(data,size) {
          var status_map = (data.responseJSON);
          for (i = 1; i <= size; i++) {
            var address = $("#row-"+i).find("#address").contents().get(0).nodeValue;
            address = address.trim();
            var port = $("#row-"+i).find("#port").html();
            var key = address+"-"+port;

            if (status_map[key] == null) {
                $("#row-"+i).find("#status").text("OFFLINE");
                $("#row-"+i).find("#shutdown").prop('disabled',true).css('opacity',0.5);
                $("#row-"+i).find("#queriesCount").text("");
            }
            else {
                if( status_map[key] == "ONLINE") {
                    $("#row-"+i).find("#status").text(status_map[key]);
                }
                else {
                    fillQueryCount(address,i);
                    $("#row-"+i).find("#status").text(status_map[key]);
                }
            }
          }
      }
      function fillQueryCount(address,row_id) {
          url = "http://"+address+":"+portNum+"/queriesCount";
          var result = $.ajax({
                        type: 'GET',
                        url: url,
                        complete: function(data) {
                              queries = data.responseJSON["queriesCount"];
                              fragments = data.responseJSON["fragmentsCount"];
                              $("#row-"+row_id).find("#queriesCount").text(queries+" queries and "+fragments+" fragments remaining before shutting down");
                              }
                        });
      }
       <#if model.shouldShowAdminInfo()>
          function shutdown(address,button) {
              url = "http://"+address+":"+portNum+"/gracefulShutdown";
              var result = $.ajax({
                    type: 'POST',
                    url: url,
                    contentType : 'text/plain',
                    complete: function(data) {
                        alert(data.responseJSON["response"]);
                        button.prop('disabled',true).css('opacity',0.5);
                    }
              });
          }
      </#if>
    </script>
</#macro>

<@page_html/>
