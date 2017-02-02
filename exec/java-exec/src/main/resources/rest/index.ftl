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
      <h3>Drillbits <span class="label label-primary">${model.getDrillbits()?size}</span></h3>
      <div class="table-responsive">
        <table class="table table-hover">
          <thead>
            <tr>
              <th>#</th>
              <th>Address</th>
              <th>User Port</th>
              <th>Control Port</th>
              <th>Data Port</th>
              <th>Version</th>
            </tr>
          </thead>
          <tbody>
            <#assign i = 1>
            <#list model.getDrillbits() as drillbit>
              <tr>
                <td>${i}</td>
                <td>${drillbit.getAddress()}
                  <#if drillbit.isCurrent()>
                    <span class="label label-info">Current</span>
                  </#if>
                </td>
                <td>${drillbit.getUserPort()}</td>
                <td>${drillbit.getControlPort()}</td>
                <td>${drillbit.getDataPort()}</td>
                <td>
                  <span class="label
                    <#if drillbit.isVersionMatch()>label-success<#else>label-danger</#if>">
                    ${drillbit.getVersion()}
                  </span>
                </td>
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
        <h3>Encryption Info <span class="label label-primary"></span></h3>
        <div class="table-responsive">
          <table class="table table-hover">
            <tbody>
                <tr>
                  <td>Client to Bit Encryption:</td>
                  <td>${model.isUserEncryptionEnabled()?string("enabled", "disabled")}</td>
                </tr>
                <tr>
                  <td>Bit to Bit Encryption:</td>
                  <td>${model.isBitEncryptionEnabled()?string("enabled", "disabled")}</td>
                </tr>
            </tbody>
          </table>
        </div>
      </div>
  </div>
</#macro>

<@page_html/>
