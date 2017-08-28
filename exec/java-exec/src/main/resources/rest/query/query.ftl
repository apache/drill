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
    <#if model?? && model>
      <script src="/static/js/jquery.form.js"></script>
    </#if>
</#macro>

<#macro page_body>
  <a href="/queries">back</a><br/>
  <div class="page-header">
  </div>
  <div id="message" class="alert alert-info alert-dismissable" style="font-family: Courier;">
    <button type="button" class="close" data-dismiss="alert" aria-hidden="true">&times;</button>
    Sample SQL query: <strong>SELECT * FROM cp.`employee.json` LIMIT 20</strong>
  </div>

  <#if model?? && model>
     <div class="form-group">
       <label for="userName">User Name</label>
       <input type="text" size="30" name="userName" id="userName" placeholder="User Name">
     </div>
  </#if>

  <form role="form" id="queryForm" action="/query" method="POST">
      <div class="form-group">
      <label for="queryType">Query Type</label>
      <div class="radio">
        <label>
          <input type="radio" name="queryType" id="sql" value="SQL" checked>
          SQL
        </label>
      </div>
      <div class="radio">
        <label>
          <input type="radio" name="queryType" id="physical" value="PHYSICAL">
          PHYSICAL
        </label>
      </div>
      <div class="radio">
        <label>
          <input type="radio" name="queryType" id="logical" value="LOGICAL">
          LOGICAL
        </label>
      </div>
    </div>
    <div class="form-group">
      <label for="query">Query</label>
      <textarea class="form-control" id="query" rows="5" name="query" style="font-family: Courier;"></textarea>
    </div>

    <button class="btn btn-default" type=<#if model?? && model>"button" onclick="doSubmit()"<#else>"submit"</#if>>
      Submit
    </button>
  </form>

    <#if model?? && model>
      <script>
        function doSubmit() {
          var userName = document.getElementById("userName").value;
          if (!userName.trim()) {
              alert("Please fill in User Name field");
              return;
          }
          $.ajax({
            type: "POST",
            beforeSend: function (request) {
              request.setRequestHeader("User-Name", userName);
            },
            url: "/query",
            data: $("#queryForm").serializeArray(),
            success: function (response) {
              var newDoc = document.open("text/html", "replace");
              newDoc.write(response);
              newDoc.close();
            },
            error: function (request, textStatus, errorThrown) {
              alert(errorThrown);
            }
          });
        }
      </script>
    </#if>
</#macro>

<@page_html/>
