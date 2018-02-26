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
    <meta name="viewport" content="width=device-width,initial-scale=1,shrink-to-fit=no">
    <meta name="theme-color" content="#000000">
    <link rel="stylesheet" href="http://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/css/bootstrap.min.css">
    <script src="/static/js/jquery-3.2.1.min.js"></script>
    <script src="http://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/js/bootstrap.js"></script>
    <link href="/static/css/main.b9c6151e.css" rel="stylesheet">
</#macro>

<#macro page_body>
  <a href="/queries">back</a><br/>
   <noscript>You need to enable JavaScript to run this app.</noscript>
   <div id="root"></div>
   <script type="text/javascript" src="/static/js/main.31525d42.js"></script>
</#macro>

<@page_html/>
