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
      <h3><span id="sizeLabel">Drillbits <span class="label label-primary" id="size">${model.getDrillbits()?size}</span>
          <button type="button" class="btn btn-warning" id='reloadBtn' style="display:none;font-size:50%" title='New Drillbits detected! Click to Refresh' onclick='location.reload();'>
          <span class="glyphicon glyphicon-refresh"></span></button></h3>
      <div class="table-responsive">
        <table class="table table-hover" id="bitTable">
          <thead>
            <tr>
              <th>#</th>
              <th title="Drillbits in Cluster" style="cursor: help;">Address <span class="glyphicon glyphicon-info-sign" style="font-size:100%"/></th>
              <th title="Heap Memory in use (as percent of Total Heap)" style="cursor: help;">Heap Memory Usage <span class="glyphicon glyphicon-info-sign" style="font-size:100%"/></th>
              <th title="Estimated Direct Memory ACTIVELY in use (as percent of Peak Usage)" style="cursor: help;">Direct Memory Usage <span class="glyphicon glyphicon-info-sign" style="font-size:100%"/></th>
              <th title="Current CPU usage by Drill" style="cursor: help;">CPU Usage<span class="glyphicon glyphicon-info-sign" style="font-size:100%"/></th>
              <th title="Average load on the system in the last 1 minute" style="cursor: help;">Avg Sys Load <span class="glyphicon glyphicon-info-sign" style="font-size:100%"/></th>
              <th>User Port</th>
              <th>Control Port</th>
              <th>Data Port</th>
              <th>Version</th>
              <th>Status</th>
              <th>Uptime</th>
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
                    <a onclick="popOutRemoteDbitUI('${drillbit.getAddress()}','${drillbit.getHttpPort()}');" style="cursor:pointer;color:blue" title="Open in new window"><span class="glyphicon glyphicon-new-window"/></a>
                  </#if>
                </td>
                <td id="httpPort" style="display:none">${drillbit.getHttpPort()}</td>
                <td class="heap">Not Available</td>
                <td class="direct">Not Available</td>
                <td class="bitload"><span class="label label-info" id="NA">Not Available</span></td>
                <td class="avgload"><span class="label label-info" id="NA">Not Available</span></td>
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
                <td class="uptime" >Not Available</td>
                <td>
                  <#if ( model.shouldShowAdminInfo() || !model.isAuthEnabled() || drillbit.isCurrent() ) >
                      <button type="button" id="shutdown" onClick="shutdown($(this));" disabled="true" style="opacity:0.5;cursor:not-allowed;">
                  <#else>
                      <button type="button" id="shutdown" title="Drillbit cannot be shutdown remotely" disabled="true" style="opacity:0.5;cursor:not-allowed;">
                  </#if>
                      <span class="glyphicon glyphicon-off"></span></button>
                </td>
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
      var updateRemoteInfo = <#if (model.isAuthEnabled())>false<#else>true</#if>;
      var refreshTime = 10000;
      var nAText = "Not Available";
      var refresh = getRefreshTime();
      var timeout;
      var size = $("#size").html();
      reloadMetrics();
      setInterval(reloadMetrics, refreshTime);

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

      function fillStatus(dataResponse,size) {
          var status_map = (dataResponse.responseJSON);
          //In case localhost has gone down (i.e. we don't know status from ZK)
          if (typeof status_map == 'undefined') {
            //Query other nodes for state details
            for (j = 1; j <= size; j++) {
              if ($("#row-"+j).find("#current").html() == "Current") {
                continue; //Skip LocalHost
              }
              var address = $("#row-"+j).find("#address").contents().get(0).nodeValue.trim();
              var restPort = $("#row-"+j).find("#httpPort").contents().get(0).nodeValue.trim();
              var altStateUrl = location.protocol + "//" + address+":"+restPort + "/state";
              var goatResponse = $.getJSON(altStateUrl)
                    .done(function(stateDataJson) {
                        //Update Status & Buttons for alternate stateData
                        if (typeof status_map == 'undefined') {
                          status_map = (stateDataJson); //Update
                          updateStatusAndShutdown(stateDataJson);
                        }
                      });
              //Don't loop any more
              if (typeof status_map != 'undefined') {
                break;
              }
            }
          } else {
            updateStatusAndShutdown(status_map);
          }
      }

      function updateStatusAndShutdown(status_map) {
        let bitMap = {};
        if (typeof status_map != 'undefined') {
            for (var k in status_map) {
              bitMap[k] = status_map[k];
            }
        }
        for (i = 1; i <= size; i++) {
            let key = "";
            if ($("#row-"+i).find("#stateKey").length > 0) { //Check if newBit that has no stateKey
              key = $("#row-"+i).find("#stateKey").textContent;
            } else {
              let address = $("#row-"+i).find("#address").contents().get(0).nodeValue.trim();
              let port = $("#row-"+i).find("#httpPort").html();
              key = address+"-"+port;
            }

            if (typeof status_map == 'undefined') {
                $("#row-"+i).find("#status").text(nAText);
                $("#row-"+i).find("#shutdown").prop('disabled',true).css('opacity',0.5);
                $("#row-"+i).find("#queriesCount").text("");
            } else if (status_map[key] == null) {
                $("#row-"+i).find("#status").text("OFFLINE");
                $("#row-"+i).find("#shutdown").prop('disabled',true).css('opacity',0.5);
                $("#row-"+i).find("#queriesCount").text("");
            } else {
                if (status_map[key] == "ONLINE") {
                    $("#row-"+i).find("#status").text(status_map[key]);
                    <#if ( model.shouldShowAdminInfo() || !model.isAuthEnabled() ) >
                    if ( location.protocol != "https" || ($("#row-"+i).find("#current").html() == "Current") ) {
                      $("#row-"+i).find("#shutdown").prop('disabled',false).css('opacity',1.0).css('cursor','pointer');
                    }
                    </#if>
                } else {
                    if ($("#row-"+i).find("#current").html() == "Current") {
                        fillQueryCount(i);
                    }
                    $("#row-"+i).find("#status").text(status_map[key]);
                }
                //Removing accounted key
                delete bitMap[key];
            }
        }
        //If bitMap is not empty, then new bits have been discovered!
        listNewDrillbits(bitMap, status_map);
      }

      //Add new Bits for listing
      function listNewDrillbits(newBits, status_map) {
        let newBitList = Object.keys(newBits);
        let tableRef = document.getElementById('bitTable').getElementsByTagName('tbody')[0];
        let bitId = size;
        for (i = 0; i < newBitList.length; i++) {
           var displayNodeName = newBitList[i].substring(0, newBitList[i].lastIndexOf("-"));
           var newBitHttpPort = newBitList[i].substring(newBitList[i].lastIndexOf("-")+1);
           var newBitElemId = "neo-"+newBitList[i];
           var newBitElem = document.getElementsByName(newBitElemId);
           if ( newBitElem.length == 0 ) {
                 bitId++;
               var bitState = status_map[newBitList[i]];
               //Injecting new row for previously unseen Drillbit
               $('#bitTable').find('tbody')
                 .append("<tr id='row-" + bitId + "' class='newbit' title='Recommend page refresh for more info'>"
                 + "<td>"+bitId+"</td>"
                 + "<td id='address' name='"+newBitElemId+"'>"+displayNodeName+" <span class='label label-primary' id='size' >new</span></td>"
                 + "<div id='stateKey' hidden='true'>"+newBitList[i]+"</div>"
                 + "<td id='httpPort' style='display:none'>"+newBitHttpPort+"</td>"
                 + "<td class='heap'>"+nAText+"</td>"
                 + "<td class='direct'>"+nAText+"</td>"
                 + "<td class='bitload'>"+nAText+"</td>"
                 + "<td  class='avgload'>"+nAText+"</td>"
                 + "<td uiElem='userPort'>--</td>"
                 + "<td uiElem='ctrlPort'>--</td>"
                 + "<td uiElem='dataPort'>--</td>"
                 + "<td uiElem='version'>"+nAText+"</td>"
                 + "<td id='status'>"+bitState+"</td>"
                 + "<td class='uptime'>"+nAText+"</td>"
                 + "<td>"
                   + "<button type='button' id='shutdown' onclick='shutdown($(this));' disabled='true' style='opacity:0.5;cursor:not-allowed;'>"
                   + "<span class='glyphicon glyphicon-off'></span></button></td>"
                 + "<td uiElem='queriesCount' />"
                 + "</tr>");
           }
        }
        //Update Drillbits count on top
        if (newBitList.length > 0) {
          //Setting Drillbit Count & Display Refresh Icon
          $('#size').text(bitId);
          $('#reloadBtn').css('display', 'inline');
          size = bitId;
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
        function shutdown(shutdownBtn) {
            let rowElem = $(shutdownBtn).parent().parent();
            let hostAddr = $(rowElem).find('#address').contents().get(0).nodeValue.trim();
            let hostPort = $(rowElem).find('#httpPort').html();
            let host = hostAddr+":"+hostPort

            if (confirm("Are you sure you want to shutdown Drillbit running on " + host + " node?")) {
              var url = location.protocol + "//" + host + "/gracefulShutdown";
              var result = $.ajax({
                    type: 'POST',
                    url: url,
                    contentType : 'text/plain',
                    error: function (request, textStatus, errorThrown) {
                        alert(errorThrown);
                    },
                    success: function(data) {
                        alert(data.responseJSON["response"]);
                        shutdownBtn.prop('disabled',true).css('opacity',0.5);
                    }
              });
            }
        }
        </#if>

      function popOutRemoteDbitUI(dbitHost, dbitPort) {
            var dbitWebUIUrl = location.protocol+'//'+ dbitHost+':'+dbitPort;
            var tgtWindow = 'dbit_'+dbitHost;
            window.open(dbitWebUIUrl, tgtWindow);
      }

      function getRequestUrl(requestPath) {
            var protocol = location.protocol;
            var host = location.host;
            var url = protocol + "//" + host + requestPath;
            return url;
      }

      //Iterates through all the nodes for update
      function reloadMetrics() {
          for (i = 1; i <= size; i++) {
              if ( $("#row-"+i).find("#stateKey").length == 0 ) {
                var address = $("#row-"+i).find("#address").contents().get(0).nodeValue.trim();
                var httpPort = $("#row-"+i).find("#httpPort").contents().get(0).nodeValue.trim();
                updateMetricsHtml(address, httpPort, i);
              }
          }
      }

      //Update memory
      function updateMetricsHtml(drillbit,webport,idx) {
        /* NOTE: For remote drillbits:
           If Authentication or SSL is enabled; we'll assume we don't have valid certificates
        */
        var remoteHost = location.protocol+"//"+drillbit+":"+webport;
        if ( !updateRemoteInfo || (location.protocol == "https" && remoteHost != location.host) ) {
          return;
        }
        //
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
            //DrillbitLoad
            var dbitLoad = metrics['drillbit.load.avg'].value;
            var dbitLoadElem = rowElem.getElementsByClassName("bitload")[0];
            if (dbitLoad >= 0) {
              dbitLoadElem.innerHTML = parseFloat(Math.round(dbitLoad * 10000)/100).toFixed(2) + "%";
            } else {
              dbitLoadElem.innerHTML = nAText;
            }
            //AvgSysLoad
            var avgSysLoad = metrics['os.load.avg'].value;
            var sysLoadElem = rowElem.getElementsByClassName("avgload")[0];
            sysLoadElem.innerHTML = avgSysLoad;
            //Uptime
            var uptimeValue = metrics['drillbit.uptime'].value;
            var uptimeElem = rowElem.getElementsByClassName("uptime")[0];
            uptimeElem.innerHTML = elapsedTime(uptimeValue);
          }
        });
      }

      function resetMetricsHtml(idx) {
        $.each(["heap","direct","bitload","avgload","uptime"], function(i, key) {
          var resetElem = document.getElementById("row-"+idx).getElementsByClassName(key)[0];
          resetElem.innerHTML = nAText;
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

      //Calculate Uptime
      function elapsedTime(valueInMsec) {
        var elapsedTime = "";
        var d, h, m, s;
        s = Math.floor(valueInMsec / 1000);
        m = Math.floor(s / 60);
        s = s % 60;
        h = Math.floor(m / 60);
        m = m % 60;
        d = Math.floor(h / 24);
        h = h % 24;
        var detailCount = 0;
        if (!isNaN(d) && d > 0) {
          elapsedTime = d+"d ";
          detailCount += 1;
        }
        if (!isNaN(h) && h > 0) {
          elapsedTime = elapsedTime + h+"h ";
          detailCount += 1;
        }
        if (!isNaN(m) && m > 0 && detailCount < 2) {
          elapsedTime = elapsedTime + m+"m ";
          detailCount += 1;
        }
        if (s > 0 && detailCount < 2) {
          elapsedTime = elapsedTime + s+"s";
        }
        return elapsedTime;
};
  </script>
</#macro>

<@page_html/>
