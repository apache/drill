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
  <#if (model.getErrors()?size > 0) >
    <div id="message" class="alert alert-danger alert-dismissable">
        <button type="button" class="close" data-dismiss="alert" aria-hidden="true">&times;</button>
        <strong>Failed to get profiles:</strong><br>
        <#list model.getErrors() as error>
          ${error}<br>
        </#list>
    </div>
  </#if>
  <#if (model.getRunningQueries()?size > 0) >
    <h3>Running Queries</h3>
    <@list_queries queries=model.getRunningQueries()/>
    <div class="page-header">
    </div>
  <#else>
    <div id="message" class="alert alert-info alert-dismissable">
      <button type="button" class="close" data-dismiss="alert" aria-hidden="true">&times;</button>
      <strong>No running queries.</strong>
    </div>
  </#if>
  <h3>Completed Queries</h3>
  <@list_queries queries=model.getFinishedQueries()/>
</#macro>

<#macro list_queries queries>
    <div class="table-responsive">
        <table class="table table-hover">
            <thead>
            <tr>
                <th>Time</th>
                <th>User</th>
                <th>Query</th>
                <th>State</th>
                <th>Duration</th>
                <th>Foreman</th>
            </tr>
            </thead>
            <tbody>
            <#list queries as query>
            <tr>
                <td>${query.getTime()}</td>
                <td>${query.getUser()}</td>
                <td>
                    <a href="/profiles/${query.getQueryId()}">
                        <div style="height:100%;width:100%;white-space:pre-line">${query.getQuery()}</div>
                    </a>
                </td>
                <td>${query.getState()}</td>
                <td>${query.getDuration()}</td>
                <td>${query.getForeman()}</td>
            </tr>
            </#list>
            </tbody>
        </table>
    </div>
</#macro>

<@page_html/>