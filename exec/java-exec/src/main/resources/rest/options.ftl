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
  <h4>System options</h4>
  <div align="right">
    <a href="http://drill.apache.org/docs/planning-and-execution-options/">Documentation</a>
  </div>
  <div class="table-responsive">
    <table class="table table-hover">
      <tbody>
        <#list model as option>
          <tr>
            <td style="border:none;">${option.getName()}</td>
            <td style="border:none;">
              <form class="form-inline" role="form" action="/option/${option.getName()}" method="POST">
                <div class="form-group">
                  <input type="text" class="form-control" name="value" value="${option.getValueAsString()}">
                  <input type="hidden" class="form-control" name="kind" value="${option.getKind()}">
                  <input type="hidden" class="form-control" name="name" value="${option.getName()}">
                </div>
                <button type="submit" class="btn btn-default">Update</button>
              </form>
            </td>
          </tr>
        </#list>
      </tbody>
    </table>
  </div>
</#macro>

<@page_html/>
