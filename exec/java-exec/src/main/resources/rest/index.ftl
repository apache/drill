<#--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->
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
              <th title="Drillbits in Cluster" style="cursor: help;">Address <span class="glyphicon glyphicon-info-sign" style="font-size:100%"/></th>
              <th title="Heap Memory in use (as percent of Total Heap)" style="cursor: help;">Heap Memory Usage <span class="glyphicon glyphicon-info-sign" style="font-size:100%"/></th>
              <th title="Direct Memory ACTIVELY in use (as percent of Peak Usage so far).&#013;The percentage is an estimate of the total because the Netty library directly negotiates with the OS for memory" style="cursor: help;">Direct Memory Usage <span class="glyphicon glyphicon-info-sign" style="font-size:100%"/></th>
              <th title="Average load on the system in the last 1 minute" style="cursor: help;">Avg Sys Load <span class="glyphicon glyphicon-info-sign" style="font-size:100%"/></th>
              <th>User Port</th>
              <th>Control Port</th>
              <th>Data Port</th>
              <th>Version</th>
              <th>Status</th>
              <#if (model.shouldShowAdminInfo() || !model.isAuthEnabled()) >
              <th>Shutdown</th>
              </#if>
            </tr>
          </thead>
          <tbody>
            <#assign i = 1>
            <#list model.getDrillbits() as drillbit>
              <tr id="row-${i}">
                <td>${i}</td>
                <td id="address" >${drillbit.getAddress()}<#if drillbit.isCurrent()>
                    <span class="label label-info" id="current">Current</span>
                    <#else>
                    <!-- HTTPS / HTTP ? -->
                    <a href="http://${drillbit.getAddress()}:${drillbit.getHttpPort()}" target="dbit_${drillbit.getAddress()}" title="Open in new window"><span class="glyphicon glyphicon-new-window"/></a>
                  </#if>
                </td>
                <td id="httpPort" style="display:none">${drillbit.getHttpPort()}</td>
                <td class="heap">0GB (0%)</td>
                <td class="direct">0GB (0%)</td>
                <td class="avgload"><span class="label label-info" id="NA">Unknown</span></td>
                <td id="port">${drillbit.getUserPort()}</td>
                <td>${drillbit.getControlPort()}</td>
                <td>${drillbit.getDataPort()}</td>
                <td>
                  <span class="label
                    <#if drillbit.isVersionMatch()>label-success<#else>label-danger</#if>">
                    ${drillbit.getVersion()}
                  </span>
                </td>
                <td id="status" >${drillbit.getState()}</td>
                <!-- #if (model.shouldShowAdminInfo() || !model.isAuthEnabled()) -->
                <#if (model.shouldShowAdminInfo() || !model.isAuthEnabled()) && drillbit.isCurrent() >
                  <td>
                      <button type="button" id="shutdown" onClick="shutdown($(this));"><span class="glyphicon glyphicon-off"></span> SHUTDOWN </button>
                  </td>
                <#elseif (model.shouldShowAdminInfo() || !model.isAuthEnabled()) && !drillbit.isCurrent() >
                  <td>
                      <button type="button" id="shutdown" onClick="remoteShutdown($(this), '${drillbit.getAddress()}:${drillbit.getHttpPort()}');"><span class="glyphicon glyphicon-off"></span> SHUTDOWN </button>
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
                  <td class="list-value">${model.isUserEncryptionEnabled()?string("Enabled", "Disabled")}
                    <#if model.isBitEncryptionEnabled()><span class="glyphicon glyphicon-lock"/></#if>
                  </td>
                </tr>
                <tr>
                  <td>Bit to Bit Encryption</td>
                  <td class="list-value">${model.isBitEncryptionEnabled()?string("Enabled", "Disabled")}
                    <#if model.isBitEncryptionEnabled()><span class="glyphicon glyphicon-lock"/></#if>
                  </td>
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
      reloadMetrics();
      setInterval(reloadMetrics, refreshTime);

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
                if (status_map[key] == "ONLINE") {
                    $("#row-"+i).find("#status").text(status_map[key]);
                    $("#row-"+i).find("#shutdown").prop('disabled',false).css('opacity',1.0);
                }
                else {
                    if ($("#row-"+i).find("#current").html() == "Current") {
                        fillQueryCount(i);
                    }
                    $("#row-"+i).find("#status").text(status_map[key]);
                }
            }
          }
      }
      function fillQueryCount(row_id) {
          var requestPath = "/queriesCount";
          var url = getRequestUrl(requestPath);
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
       <#if (model.shouldShowAdminInfo() || !model.isAuthEnabled()) >
          function shutdown(button) {
              if (confirm("Are you sure you want to shutdown Drillbit running on " + location.host + " node?")) {
                  var requestPath = "/gracefulShutdown";
                  var url = getRequestUrl(requestPath);
                  var result = $.ajax({
                               type: 'POST',
                               url: url,
                               contentType : 'text/plain',
                               error: function (request, textStatus, errorThrown) {
                                   alert(errorThrown);
                               },
                               success: function(data) {
			           alert(data["response"]);
                                   button.prop('disabled',true).css('opacity',0.5);
                               }
                  });
              }
          }

          function remoteShutdown(button,host) {
              var url = location.protocol + "//" + host + "/gracefulShutdown";
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
      function getRequestUrl(requestPath) {
            var protocol = location.protocol;
            var host = location.host;
            var url = protocol + "//" + host + requestPath;
            return url;
      }

      //Iterates through all the nodes for update
      function reloadMetrics() {
          for (i = 1; i <= size; i++) {
              var address = $("#row-"+i).find("#address").contents().get(0).nodeValue.trim();
              var httpPort = $("#row-"+i).find("#httpPort").contents().get(0).nodeValue.trim();
              updateMetricsHtml(address, httpPort, i);
          }
      }

      //Update memory
      //TODO: HTTPS?
      function updateMetricsHtml(drillbit,webport,idx) {
        var result = $.ajax({
          type: 'GET',
          url: location.protocol+"//"+drillbit+":"+webport+"/status/metrics",
          dataType: "json",
          error: function(data) {
            resetMetricsHtml(idx);
          }, 
          complete: function(data) {
            if (typeof data.responseJSON == 'undefined') {
                resetMetricsHtml(idx);
                return;
            }
            var metrics = data.responseJSON['gauges'];
            //Memory
            var usedHeap = metrics['heap.used'].value;
            var maxHeap  = metrics['heap.max'].value;
            var usedDirect = metrics['drill.allocator.root.used'].value;
            var peakDirect = metrics['drill.allocator.root.peak'].value;
            var heapUsage    = computeMemUsage(usedHeap,maxHeap);
            var directUsage  = computeMemUsage(usedDirect,peakDirect);
            var rowElem = document.getElementById("row-"+idx);
            var heapElem = rowElem.getElementsByClassName("heap")[0];
            heapElem.innerHTML = heapUsage;
            var directElem = rowElem.getElementsByClassName("direct")[0];
            directElem.innerHTML = directUsage;
            //AvgSysLoad
            var avgSysLoad = metrics['os.load.avg'].value;
            var sysLoadElem = rowElem.getElementsByClassName("avgload")[0];
            sysLoadElem.innerHTML = avgSysLoad;
            }
          });
      }

      function resetMetricsHtml(idx) {
        $.each(["heap","direct","avgload"], function(i, key) {
          var resetElem = document.getElementById("row-"+idx).getElementsByClassName(key)[0];
          resetElem.innerHTML = "Not Available";
        });
    };

      //Compute Usage
      function computeMemUsage(used, max) {
        var percent = 0;
        if ( max > 0) {
          var percent = Math.round((100 * used / max), 2);
        }
        var usage   =  bytesInGB(used, 2) + "GB (" + Math.max(0, percent) + "% of "+ bytesInGB(max, 2) +"GB)";
        return usage;
      }

      function bytesInGB(byteVal, decimal) {
      var scale = Math.pow(10,decimal);
        return Math.round(scale * byteVal / 1073741824)/scale;
      }
  </script>
</#macro>

<@page_html/>
