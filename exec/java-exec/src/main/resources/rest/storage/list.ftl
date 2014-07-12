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
</#macro>

<#macro page_body>
  <a href="/queries">back</a><br/>
  <div class="page-header">
  </div>
  <h3>Registered Storage Plugins</h3>
  <div class="table-responsive">
    <table class="table">
      <tbody>
        <tr><th colspan="2" style="border:none;">Enabled Plugins</th></tr>
        <#list model as plugin>
        <#if plugin.enabled == true>
          <tr>
            <td style="border:none;">
              ${plugin.name}
            </td>
            <td style="border:none;">
              <a class="btn btn-default" href="/storage/${plugin.name}/config/update">Update</a>
              <a class="btn btn-default" href="/storage/${plugin.name}/config/enable/false">Disable</a>
            </td>
          </tr>
        </#if>
        </#list>
        <tr><th colspan="2" style="border:none;">Disabled Plugins</th></tr>
        <#list model as plugin>
        <#if plugin.enabled == false>
          <tr>
            <td style="border:none;">
              ${plugin.name}
            </td>
            <td style="border:none;">
              <a class="btn btn-default" href="/storage/${plugin.name}/config/update">Update</a>
              <a class="btn btn-default" href="/storage/${plugin.name}/config/enable/true">Enable</a>
            </td>
          </tr>
        </#if>
        </#list>
      </tbody>
    </table>
  </div>
  <div>
    <h4>Create new storage configuration</h4>
    <form class="form-inline" id="newStorage" role="form" action="/" method="GET">
      <div class="form-group">
        <input type="text" class="form-control" id="storageName" placeholder="Storage Name">
      </div>
      <script>
        function doSubmit() {
          var name = document.getElementById("storageName");
          var form = document.getElementById("newStorage");
          form.action = "/storage/" + name.value + "/config/update";
          form.submit();
        }
      </script>
      <button type="submit" class="btn btn-default" onclick="javascript:doSubmit();">Create</button>
    </form>
  </div>
</#macro>

<@page_html/>