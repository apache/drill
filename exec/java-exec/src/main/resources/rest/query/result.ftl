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
  <script type="text/javascript" language="javascript" src="//cdn.datatables.net/1.10.0/js/jquery.dataTables.min.js"></script>
  <script type="text/javascript" language="javascript" src="//cdn.datatables.net/plug-ins/be7019ee387/integration/jqueryui/dataTables.jqueryui.js"></script>
</#macro>

<#macro page_body>
  <a href="/queries">back</a><br/>
  <div class="page-header">
  </div>
  <h2>Result</h2>
  <div style="width=100%; overflow: auto;">
    <table id="relation" class="table table-striped table-bordered table-condensed" style="display: table; table-layout: fized; width=100%;">
      <#assign rows = model[0]>
      <thead style="overflow: auto;">
        <tr>
          <#list rows as row>
          <th>${row}</th>
          </#list>
        </tr>
      </thead>
      <tbody style="overflow: auto;">
      <#list model as rows>
        <#if (rows_index > 0)>
          <tr>
            <#list rows as row>
            <td>${row}</td>
            </#list>
          </tr>
        </#if>
      </#list>
      </tbody>
    </table>
  </div>
  <script charset="utf-8">
    $(document).ready(function() {
      $('#relation').dataTable( { "scrollX" : true } );
    } );
  </script>
</#macro>

<@page_html/>
