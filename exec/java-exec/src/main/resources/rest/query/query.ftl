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
  <script src="/static/js/jquery.form.js"></script>
  <script src="/static/js/querySubmission.js"></script>
  <!-- Ace Libraries for Syntax Formatting -->
  <script src="/static/js/ace-code-editor/ace.js" type="text/javascript" charset="utf-8"></script>
  <!-- Disabled in favour of dynamic: script src="/static/js/ace-code-editor/mode-sql.js" type="text/javascript" charset="utf-8" -->
  <script src="/dynamic/mode-sql.js" type="text/javascript" charset="utf-8"></script>
  <script src="/static/js/ace-code-editor/ext-language_tools.js" type="text/javascript" charset="utf-8"></script>
  <script src="/static/js/ace-code-editor/theme-sqlserver.js" type="text/javascript" charset="utf-8"></script>
  <script src="/static/js/ace-code-editor/snippets/sql.js" type="text/javascript" charset="utf-8"></script>
  <script src="/static/js/ace-code-editor/mode-snippets.js" type="text/javascript" charset="utf-8"></script>
</#macro>

<#macro page_body>
  <div class="page-header">
  </div>
  <div id="message" class="alert alert-info alert-dismissable" style="font-family: Courier;">
    <button type="button" class="close" data-dismiss="alert" aria-hidden="true">&times;</button>
    Sample SQL query: <strong>SELECT * FROM cp.`employee.json` LIMIT 20</strong>
  </div>

<#include "*/alertModals.ftl">

<#include "*/runningQuery.ftl">

  <#if model.isOnlyImpersonationEnabled()>
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
      <div style="display: inline-block"><label for="query">Query</label></div>
      <div style="display: inline-block; float:right; padding-right:5%"><b>Hint: </b>Use <div id="keyboardHint" style="display:inline-block; font-style:italic"></div> to submit</div>
      <div id="query-editor-format"></div>
      <input class="form-control" type="hidden" id="query" name="query"/>
    </div>

    <button class="btn btn-default" type="button" onclick="<#if model.isOnlyImpersonationEnabled()>doSubmitQueryWithUserName()<#else>doSubmitQueryWithAutoLimit()</#if>">
      Submit
    </button>
    <input type="checkbox" name="forceLimit" value="limit" <#if model.isAutoLimitEnabled()>checked</#if>> Limit results to <input type="text" id="queryLimit" min="0" value="${model.getDefaultRowsAutoLimited()}" size="6" pattern="[0-9]*"> rows <span class="glyphicon glyphicon-info-sign" onclick="alert('Limits the number of records retrieved in the query')" style="cursor:pointer"></span>
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
    document.getElementById('query-editor-format').style.fontFamily='courier,monospace';
    document.getElementById('query-editor-format').style.lineHeight='1.5';
    document.getElementById('query-editor-format').style.width='98%';
    document.getElementById('query-editor-format').style.margin='auto';
    editor.setOptions({
      enableSnippets: true,
      enableBasicAutocompletion: true,
      enableLiveAutocompletion: false
    });

    //Provides hint based on OS
    var browserOS = navigator.platform.toLowerCase();
    if ((browserOS.indexOf("mac") > -1)) {
      document.getElementById('keyboardHint').innerHTML="Meta+Enter";
    } else {
      document.getElementById('keyboardHint').innerHTML="Ctrl+Enter";
    }

    // meta+enter / ctrl+enter to submit query
    document.getElementById('queryForm')
            .addEventListener('keydown', function(e) {
      if (!(e.keyCode == 13 && (e.metaKey || e.ctrlKey))) return;
      if (e.target.form) //Submit [Wrapped] Query 
        <#if model.isOnlyImpersonationEnabled()>doSubmitQueryWithUserName()<#else>doSubmitQueryWithAutoLimit()</#if>;
    });
  </script>
</#macro>

<@page_html/>
