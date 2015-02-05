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
  <link rel="stylesheet" type="text/css" href="//code.jquery.com/ui/1.10.3/themes/smoothness/jquery-ui.css">
  <link rel="stylesheet" type="text/css" href="//cdn.datatables.net/plug-ins/be7019ee387/integration/jqueryui/dataTables.jqueryui.css">

  <script type="text/javascript" language="javascript" src="//code.jquery.com/jquery-1.10.2.min.js"></script>
  <script type="text/javascript" language="javascript" src="//cdn.datatables.net/1.10.0/js/jquery.dataTables.js"></script>

  <link rel="stylesheet" type="text/css" href="//cdn.datatables.net/colvis/1.1.0/css/dataTables.colVis.css">
  <script type="text/javascript" language="javascript" src="//cdn.datatables.net/colvis/1.1.0/js/dataTables.colVis.min.js"></script>
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
          <th>${value?html}</th>
          </#list>
        </tr>
      </thead>
      <tbody>
      <#list model.getRows() as record>
        <tr>
          <#list record as value>
          <td>${value!"null"?html}</td>
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
