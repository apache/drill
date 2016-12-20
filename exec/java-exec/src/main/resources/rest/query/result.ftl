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
<#macro page_head>
    <script type="text/javascript" language="javascript"  src="../static/js/jquery.dataTables-1.10.0.min.js"> </script>
    <script type="text/javascript" language="javascript" src="../static/js/dataTables.colVis-1.1.0.min.js"></script>
    <link href="/static/css/dataTables.colVis-1.1.0.min.css" rel="stylesheet">
    <link href="/static/css/dataTables.jqueryui.css" rel="stylesheet">
    <link href="/static/css/jquery-ui-1.10.3.min.css" rel="stylesheet">
</#macro>

<#macro page_body>
  <a href="/queries">back</a><br/>
  <div class="page-header">
  </div>
  <#if model.isEmpty()>
    <div class="jumbotron">
      <p class="lead">No result found.</p>
    </div>
  <#else>
    <table id="result" class="table table-striped table-bordered table-condensed" style="table-layout: auto; width=100%;">
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
        "dom": '<"H"lCfr>t<"F"ip>',
        "jQueryUI" : true
      } );
    } );
  </script>
</#macro>

<@page_html/>
