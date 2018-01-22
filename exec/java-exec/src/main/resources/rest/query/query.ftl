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
      <script src="/static/js/querySubmission.js"></script>
    </#if>
  <!-- Ace Libraries for Syntax Formatting -->
  <script src="/static/js/ace-code-editor/ace.js" type="text/javascript" charset="utf-8"></script>
  <script src="/static/js/ace-code-editor/mode-sql.js" type="text/javascript" charset="utf-8"></script>
  <script src="/static/js/ace-code-editor/ext-language_tools.js" type="text/javascript" charset="utf-8"></script>
  <script src="/static/js/ace-code-editor/theme-sqlserver.js" type="text/javascript" charset="utf-8"></script>
  <script src="/static/js/ace-code-editor/snippets/sql.js" type="text/javascript" charset="utf-8"></script>
  <script src="/static/js/ace-code-editor/mode-snippets.js" type="text/javascript" charset="utf-8"></script>
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
      <div id="query-editor-format"></div>
      <input class="form-control" type="hidden" id="query" name="query"/>
    </div>

    <button class="btn btn-default" type=<#if model?? && model>"button" onclick="doSubmitQueryWithUserName()"<#else>"submit"</#if>>
      Submit
    </button>
  </form>

  <script>
    ace.require("ace/ext/language_tools");
    var editor = ace.edit("query-editor-format");
    var queryText = $('input[name="query"]');
    //Hidden text input for form-submission
    editor.getSession().on("change", function () {
      queryText.val(editor.getSession().getValue());
    });
    editor.setAutoScrollEditorIntoView(true);
    editor.setOption("maxLines", 25);
    editor.setOption("minLines", 12);
    editor.renderer.setShowGutter(true);
    editor.renderer.setOption('showLineNumbers', true);
    editor.renderer.setOption('showPrintMargin', false);
    editor.getSession().setMode("ace/mode/sql");
    editor.getSession().setTabSize(4);
    editor.getSession().setUseSoftTabs(true);
    editor.setTheme("ace/theme/sqlserver");
    editor.$blockScrolling = "Infinity";
    //CSS Formatting
    document.getElementById('query-editor-format').style.fontSize='13px';
    document.getElementById('query-editor-format').style.fontFamily='courier';
    document.getElementById('query-editor-format').style.lineHeight='1.5';
    document.getElementById('query-editor-format').style.width='98%';
    document.getElementById('query-editor-format').style.margin='auto';
    editor.setOptions({
      enableSnippets: true,
      enableBasicAutocompletion: true,
      enableLiveAutocompletion: false
    });
  </script>

</#macro>

<@page_html/>
