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
</#macro>

<#macro page_body>
  <div class="page-header">
  </div>
<!-- Rendering Panel -->
  <div class="panel panel-danger">
    <div class="panel-heading" style="white-space:pre;font-size:110%;padding:0px 0px">
      <span class="glyphicon glyphicon-alert" style="font-size:125%;">&#xe209;</span>   <strong>${model.getClass().getSimpleName()} :</strong> 	${model.getMessage()?split("\n")[0]}
    </div>
    <div class="panel-body"><span style="font-family:courier,monospace;white-space:pre-wrap">${model}</span></div>
  </div>
  <a class="btn btn-default" id="backBtn" style="display:inline" onclick="window.history.back()"><span class="glyphicon glyphicon-step-backward"></span> Back</a>
</#macro>

<@page_html/>