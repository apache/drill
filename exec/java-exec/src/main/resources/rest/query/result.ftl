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
<#macro page_head>
    <script type="text/javascript" language="javascript"  src="../static/js/jquery.dataTables-1.10.0.min.js"> </script>
    <script type="text/javascript" language="javascript" src="../static/js/dataTables.colVis-1.1.0.min.js"></script>
    <link href="/static/css/dataTables.colVis-1.1.0.min.css" rel="stylesheet">
    <link href="/static/css/dataTables.jqueryui.css" rel="stylesheet">
    <link href="/static/css/jquery-ui-1.10.3.min.css" rel="stylesheet">
</#macro>

<#macro page_body>
  <div class="page-header">
  </div>
  <div>
  <table><tr>
    <td align='left'>
      <button type="button"  title="Open in new window" onclick="popOutProfile('${model.getQueryId()}');" class="btn btn-default btn-sm">
      <b>Query Profile:</b> ${model.getQueryId()} <span class="glyphicon glyphicon-new-window"/></button>
     </td><td align="right" width="100%">
       <div class="input-group">
         <span class="input-group-addon" style="font-size:95%">Delimiter </span>
         <input id="delimitBy" type="text" class="form-control input-sm" name="delimitBy" title="Specify delimiter" placeholder="Required" maxlength="2" size="2" value=",">
       </div></td><td>
       <button type="button"  title="Export visible table as CSV. Show ALL rows to export entire resultSet" onclick="exportTableAsCsv('${model.getQueryId()}');" class="btn btn-default btn-sm">
       <b>Export </b> <span class="glyphicon glyphicon-export"/></button>
     </td>
  </tr>
  </table>
  </div>
  <#if model.isEmpty()>
    <div class="jumbotron">
      <p class="lead">No result found.</p>
    </div>
  <#else>
    <table id="result" class="table table-striped table-bordered table-condensed" style="table-layout: auto; width=100%; white-space: pre;">
      <thead>
        <tr>
          <#list model.getColumns() as value>
          <th>${value}</th>
          </#list>
        </tr>
      </thead>
      <tbody>
      <#list model.getRows() as record>
        <tr>
          <#list record as value>
          <td>${value!"null"}</td>
          </#list>
        </tr>
      </#list>
      </tbody>
    </table>
  </#if>
  <script charset="utf-8">
    $(document).ready(function() {
      $('#result').dataTable( {
        "aaSorting": [],
        "scrollX" : true,
        "lengthMenu": [[10, 25, 50, 100, -1], [10, 25, 50, 100, "All"]],
        "lengthChange": true,
        "dom": '<"H"lCfr>t<"F"ip>',
        "jQueryUI" : true
      } );
    } );

    //Pop out profile (needed to avoid losing query results)
    function popOutProfile(queryId) {
      var profileUrl = location.protocol+'//'+ location.host+'/profiles/'+queryId;
      var tgtWindow = '_blank';
      window.open(profileUrl, tgtWindow);
    }

    //Ref: https://jsfiddle.net/gengns/j1jm2tjx/
    function downloadCsv(csvRecords, filename) {
      var csvFile;
      var downloadElem;

      //CSV File
      csvFile = new Blob([csvRecords], {type: "text/csv"});
      // Download link
      downloadElem = document.createElement("a");
      // File name
      downloadElem.download = filename;

      // We have to create a link to the file
      downloadElem.href = window.URL.createObjectURL(csvFile);

      // Make sure that the link is not displayed
      downloadElem.style.display = "none";

      // Add the link to your DOM
      document.body.appendChild(downloadElem);

      // Launch the download prompt
      downloadElem.click();
    }

    function exportTableAsCsv(queryId) {
      var filename = queryId + '.csv';
      var csv = []; //Array of records
      var rows = document.getElementById('result').querySelectorAll("tr");
      var delimiter = document.getElementById('delimitBy').value;
      if (delimiter == 'undefined' || delimiter.length==0) {
        delimiter = ",";
      }
      for (var i = 0; i < rows.length; i++) {
        var row = [], cols = rows[i].querySelectorAll("th, td");
        for (var j = 0; j < cols.length; j++)
          row.push(cols[j].textContent);
          csv.push(row.join(delimiter));
        }
        // Download CSV
        downloadCsv(csv.join("\n"), filename);
    }

    </script>
</#macro>

<@page_html/>
