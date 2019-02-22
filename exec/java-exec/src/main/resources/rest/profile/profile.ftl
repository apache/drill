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
<script src="/static/js/d3.v3.js"></script>
<script src="/static/js/dagre-d3.min.js"></script>
<script src="/static/js/graph.js"></script>
<script src="/static/js/jquery.dataTables-1.10.16.min.js"></script>
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
<link href="/static/css/drill-dataTables.sortable.css" rel="stylesheet">

<script>
    var globalconfig = {
        "queryid" : "${model.getQueryId()}",
        "operators" : ${model.getOperatorsJSON()?no_esc}
    };

    $(document).ready(function() {
      $(".sortable").DataTable( {
        "searching": false,
        "lengthChange": false,
        "paging": false,
        "info": false
      });
      //Enable Warnings by making it visible
      checkForWarnings();
    });

    //Check for Warnings
    function checkForWarnings() {
      //No Progress Warning
      let noProgressFragmentCount = document.querySelectorAll('td[class=no-progress-tag]').length;
      let majorFragmentCount = document.querySelectorAll('#fragment-overview table tbody tr').length;
      toggleWarning("noProgressWarning", majorFragmentCount, noProgressFragmentCount);

      //Spill To Disk Warnings
      let spillCount = document.querySelectorAll('td[class=spill-tag]').length;
      toggleWarning("spillToDiskWarning", true, (spillCount > 0));

      //Slow Scan Warnings
      let longScanWaitCount = document.querySelectorAll('td[class=scan-wait-tag]').length;
      toggleWarning("longScanWaitWarning", true, (longScanWaitCount > 0));
    }

    //Show Warnings
    function toggleWarning(warningElemId, expectedVal, actualVal) {
        if (expectedVal == actualVal) {
            document.getElementById(warningElemId).style.display="block";
        } else {
            closeWarning(warningElemId);
        }
    }

    //Close Warning
    function closeWarning(warningElemId) {
        document.getElementById(warningElemId).style.display="none";
    }

    //Close the cancellation status popup
    function refreshStatus() {
      //Close PopUp Modal
      $("#queryCancelModal").modal("hide");
      location.reload(true);
    }

    //Cancel query & show cancellation status
    function cancelQuery() {
      document.getElementById("cancelTitle").innerHTML = "Drillbit on " + location.hostname + " says";
      $.get("/profiles/cancel/"+globalconfig.queryid, function(data, status){/*Not Tracking Response*/});
      //Show PopUp Modal
      $("#queryCancelModal").modal("show");
    };

</script>
</#macro>

<#macro page_body>
  <div class="page-header">
  </div>
  <h3>Query and Planning</h3>
  <ul id="query-tabs" class="nav nav-tabs" role="tablist">
    <li><a href="#query-query" role="tab" data-toggle="tab">Query</a></li>
    <li><a href="#query-physical" role="tab" data-toggle="tab">Physical Plan</a></li>
    <li><a href="#query-visual" role="tab" data-toggle="tab">Visualized Plan</a></li>
    <li><a href="#query-edit" role="tab" data-toggle="tab">Edit Query</a></li>
    <#if model.hasError()>
        <li><a href="#query-error" role="tab" data-toggle="tab">Error</a></li>
    </#if>
  </ul>
  <div id="query-content" class="tab-content">
    <div id="query-query" class="tab-pane">
      <p><pre id="query-text" name="query-text"  style="background-color: #f5f5f5;">${model.getProfile().query}</pre></p>
    </div>
    <div id="query-physical" class="tab-pane">
      <p><pre>${model.profile.plan}</pre></p>
    </div>
    <div id="query-visual" class="tab-pane">
      <div style='padding: 15px 15px;'>
        <button type='button' class='btn btn-default' onclick='popUpAndPrintPlan();'><span class="glyphicon glyphicon-print"></span> Print Plan</button>
      </div>
      <div>
        <svg id="query-visual-canvas" class="center-block"></svg>
      </div>
    </div>
    <div id="query-edit" class="tab-pane">
      <p>

        <#if model.isOnlyImpersonationEnabled()>
          <div class="form-group">
            <label for="userName">User Name</label>
            <input type="text" size="30" name="userName" id="userName" placeholder="User Name" value="${model.getProfile().user}">
          </div>
        </#if>

        <form role="form" id="queryForm" action="/query" method="POST">
          <div id="query-editor" class="form-group">${model.getProfile().query}</div>
          <input class="form-control" id="query" name="query" type="hidden" value="${model.getProfile().query}"/>
          <div style="padding:5px"><b>Hint: </b>Use <div id="keyboardHint" style="display:inline-block; font-style:italic"></div> to submit</div>
          <div class="form-group">
            <div class="radio-inline">
              <label>
                <input type="radio" name="queryType" id="sql" value="SQL" checked>
                    SQL
              </label>
            </div>
            <div class="radio-inline">
              <label>
                 <input type="radio" name="queryType" id="physical" value="PHYSICAL">
                     PHYSICAL
              </label>
            </div>
            <div class="radio-inline">
              <label>
                <input type="radio" name="queryType" id="logical" value="LOGICAL">
                    LOGICAL
              </label>
            </div>
            </div>
            <button class="btn btn-default" type="button" onclick="<#if model.isOnlyImpersonationEnabled()>doSubmitQueryWithUserName()<#else>doSubmitQueryWithAutoLimit()</#if>">
            Re-run query
            </button>
          </form>
      </p>

<#include "*/alertModals.ftl">
<#include "*/runningQuery.ftl">

      <p>
      <form action="/profiles/cancel/${model.queryId}" method="GET">
        <div class="form-group">
          <button type="submit" class="btn btn-default">Cancel query</button>
        </div>
      </form>
        </p>
    </div>
    <#if model.hasError()>
      <div id="query-error" class="tab-pane fade">
        <p><pre>${model.getProfile().error?trim}</pre></p>
        <p>Failure node: ${model.getProfile().errorNode}</p>
        <p>Error ID: ${model.getProfile().errorId}</p>

        <div class="page-header"></div>
        <h3>Verbose Error Message</h3>
        <div class="panel panel-default">
          <div class="panel-heading">
            <h4 class="panel-title">
              <a data-toggle="collapse" href="#error-message-overview">
                 Overview
              </a>
            </h4>
          </div>
          <div id="error-message-overview" class="panel-collapse collapse">
            <div class="panel-body">
              <pre>${model.getProfile().verboseError?trim}</pre>
            </div>
          </div>
        </div>
      </div>
    </#if>

  </div>

  <#assign queueName = model.getProfile().getQueueName() />
  <#assign queued = queueName != "" && queueName != "-" />

  <div class="page-header"></div>
  <h3>Query Profile: <span style='font-size:85%'>${model.getQueryId()}</span>
  <#if model.getQueryStateDisplayName() == "Prepared" || model.getQueryStateDisplayName() == "Planning" || model.getQueryStateDisplayName() == "Enqueued" || model.getQueryStateDisplayName() == "Starting" || model.getQueryStateDisplayName() == "Running">
    <div  style="display: inline-block;">
      <button type="button" id="cancelBtn" class="btn btn-warning btn-sm" onclick="cancelQuery()" > Cancel </button>
    </div>

  <!-- Cancellation Modal -->
  <div class="modal fade" id="queryCancelModal" role="dialog">
    <div class="modal-dialog">
      <div class="modal-content">
        <div class="modal-header">
          <button type="button" class="close" data-dismiss="modal" onclick="refreshStatus()">&times;</button>
          <h4 class="modal-title" id="cancelTitle"></h4>
        </div>
        <div class="modal-body" style="line-height:2">
          Cancellation issued for Query ID:<br>${model.getQueryId()}
        </div>
        <div class="modal-footer"><button type="button" class="btn btn-default" onclick="refreshStatus()">Close</button></div>
      </div>
    </div>
  </div>
  </#if>
  </h3>

  <div class="panel-group" id="query-profile-accordion">
    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#query-profile-overview">
              Overview
          </a>
        </h4>
      </div>
      <div id="query-profile-overview" class="panel-collapse collapse in">
        <div class="panel-body">
          <table class="table table-bordered">
            <thead>
            <tr>
                <th>State</th>
                <th>Foreman</th>
                <th>Total Fragments</th>
     <#if queued>
                <th>Total Cost</th>
                <th>Queue</th>
     </#if>
            </tr>
            </thead>
            <tbody>
              <tr>
                  <td>${model.getQueryStateDisplayName()}</td>
                  <td>${model.getProfile().getForeman().getAddress()}</td>
                  <td>${model.getProfile().getTotalFragments()}</td>
     <#if queued>
                  <td>${model.getProfile().getTotalCost()}</td>
                  <td>${queueName}</td>
     </#if>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#query-profile-duration">
             Duration
          </a>
        </h4>
      </div>
      <div id="query-profile-duration" class="panel-collapse collapse in">
        <div class="panel-body">
          <table class="table table-bordered">
            <thead>
              <tr>
                <th>Planning</th>
     <#if queued>
                <th>Queued</th>
     </#if>
                <th>Execution</th>
                <th>Total</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>${model.getPlanningDuration()}</td>
     <#if queued>
                <td>${model.getQueuedDuration()}</td>
     </#if>
                <td>${model.getExecutionDuration()}</td>
                <td>${model.getProfileDuration()}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <#assign sessionOptions = model.getSessionOptions()>
  <#assign queryOptions = model.getQueryOptions()>
  <#if (sessionOptions?keys?size > 0 || queryOptions?keys?size > 0) >
    <div class="page-header"></div>
    <h3>Options</h3>
    <div class="panel-group" id="options-accordion">
      <div class="panel panel-default">
        <div class="panel-heading">
          <h4 class="panel-title">
            <a data-toggle="collapse" href="#options-overview">
              Overview
            </a>
          </h4>
        </div>
        <div id="options-overview" class="panel-collapse collapse in">
          <@list_options options=sessionOptions scope="Session" />
          <@list_options options=queryOptions scope="Query" />
        </div>
      </div>
    </div>
  </#if>

  <div class="page-header"></div>
  <h3>Fragment Profiles</h3>

  <div class="panel-group" id="fragment-accordion">
    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#fragment-overview">
            Overview
          </a>
        </h4>
      </div>
      <div id="fragment-overview" class="panel-collapse collapse">
        <div class="panel-body">
          <svg id="fragment-overview-canvas" class="center-block"></svg>
          <div id="noProgressWarning" style="display:none;cursor:help" class="panel panel-warning">
            <div class="panel-heading" title="Check if any of the Drillbits are waiting for data from a SCAN operator, or might actually be hung with its VM thread being busy." style="cursor:pointer">
            <span class="glyphicon glyphicon-alert" style="font-size:125%">&#xe209;</span> <b>WARNING:</b> No fragments have made any progress in the last <b>${model.getNoProgressWarningThreshold()}</b> seconds. (See <span style="font-style:italic;font-weight:bold">Last Progress</span> below)
            </div>
          </div>
          ${model.getFragmentsOverview()?no_esc}
        </div>
      </div>
      <#list model.getFragmentProfiles() as frag>
      <div class="panel panel-default">
        <div class="panel-heading">
          <h4 class="panel-title">
            <a data-toggle="collapse" href="#${frag.getId()}">
              ${frag.getDisplayName()}
            </a>
          </h4>
        </div>
        <div id="${frag.getId()}" class="panel-collapse collapse">
          <div class="panel-body">
            ${frag.getContent()?no_esc}
          </div>
        </div>
      </div>
      </#list>
    </div>
  </div>

  <div class="page-header"></div>
  <h3>Operator Profiles</h3>

  <div class="panel-group" id="operator-accordion">
    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#operator-overview">
            Overview
          </a>
        </h4>
      </div>
      <div id="operator-overview" class="panel-collapse collapse">
        <div class="panel-body">
          <div id="spillToDiskWarning" style="display:none;cursor:help" class="panel panel-warning" title="Spills occur because a buffered operator didn't get enough memory to hold data in memory. Increase the memory or ensure that number of spills &lt; 2">
            <div class="panel-heading"><span class="glyphicon glyphicon-alert" style="font-size:125%">&#xe209;</span> <b>WARNING:</b> Some operators have data spilled to disk. This will result in performance loss. (See <span style="font-style:italic;font-weight:bold">Avg Peak Memory</span> and <span style="font-style:italic;font-weight:bold">Max Peak Memory</span> below)
            <button type="button" class="close" onclick="closeWarning('spillToDiskWarning')" style="font-size:180%">&times;</button>
            </div>
          </div>
          <div id="longScanWaitWarning" style="display:none;cursor:help" class="panel panel-warning">
            <div class="panel-heading" title="Check if any of the Drillbits are waiting for data from a SCAN operator, or might actually be hung with its VM thread being busy." style="cursor:pointer">
            <span class="glyphicon glyphicon-alert" style="font-size:125%">&#xe209;</span> <b>WARNING:</b> Some of the SCAN operators spent more time waiting for the data than processing it. (See <span style="font-style:italic;font-weight:bold">Avg Wait Time</span> as compared to <span style="font-style:italic;font-weight:bold">Average Process Time</span> for the <b>SCAN</b> operators below)
            <button type="button" class="close" onclick="closeWarning('longScanWaitWarning')" style="font-size:180%">&times;</button>
            </div>
          </div>
          ${model.getOperatorsOverview()?no_esc}
        </div>
      </div>
    </div>

    <#list model.getOperatorProfiles() as op>
    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#${op.getId()}">
            ${op.getDisplayName()}
          </a>
        </h4>
      </div>
      <div id="${op.getId()}" class="panel-collapse collapse">
        <div class="panel-body">
          ${op.getContent()?no_esc}
        </div>
        <div class="panel panel-info">
          <div class="panel-heading">
            <h4 class="panel-title">
              <a data-toggle="collapse" href="#${op.getId()}-metrics">
                Operator Metrics
              </a>
            </h4>
          </div>
          <div id="${op.getId()}-metrics" class="panel-collapse collapse">
            <div class="panel-body" style="display:block;overflow-x:auto">
              ${op.getMetricsTable()?no_esc}
            </div>
          </div>
        </div>
      </div>
    </div>
    </#list>
  </div>

  <div class="page-header"></div>
  <h3>Full JSON Profile</h3>

  <div class="span4 collapse-group" id="full-json-profile">
    <a class="btn btn-default" data-toggle="collapse" data-target="#full-json-profile-json">JSON profile</a>
    <br> <br>
    <pre class="collapse" id="full-json-profile-json">
    </pre>
  </div>
  <div class="page-header">
  </div> <br>

    <script>
    //Inject Spilled Tags
    $(window).on('load', function () {
      injectIconByClass("spill-tag","glyphicon-download-alt");
      injectIconByClass("time-skew-tag","glyphicon-time");
      injectSlowScanIcon();
    });

    //Inject Glyphicon by Class tag
    function injectIconByClass(tagLabel, tagIcon) {
        //Inject Spill icons
        var tagElemList = document.getElementsByClassName(tagLabel);
        var i;
        for (i = 0; i < tagElemList.length; i++) {
            var content = tagElemList[i].innerHTML;
            tagElemList[i].innerHTML = "<span class=\"glyphicon "+tagIcon+"\">&nbsp;</span>"+content;
        }
    }

    //Inject PNG icon for slow
    function injectSlowScanIcon() {
        //Inject Spill icons
        var tagElemList = document.getElementsByClassName("scan-wait-tag");
        var i;
        for (i = 0; i < tagElemList.length; i++) {
            var content = tagElemList[i].innerHTML;
            tagElemList[i].innerHTML = "<img src='/static/img/turtle.png' alt='slow'> "+content;
        }
    }

    //Configuration for Query Viewer in Profile
    ace.require("ace/ext/language_tools");
    var viewer = ace.edit("query-text");
    viewer.setAutoScrollEditorIntoView(true);
    viewer.setOption("minLines", 3);
    viewer.setOption("maxLines", 20);
    viewer.renderer.setShowGutter(false);
    viewer.renderer.setOption('showLineNumbers', false);
    viewer.renderer.setOption('showPrintMargin', false);
    viewer.renderer.setOption("vScrollBarAlwaysVisible", true);
    viewer.renderer.setOption("hScrollBarAlwaysVisible", true);
    viewer.renderer.setScrollMargin(10, 10, 10, 10);
    viewer.getSession().setMode("ace/mode/sql");
    viewer.setTheme("ace/theme/sqlserver");
    //CSS Formatting
    document.getElementById('query-query').style.fontSize='13px';
    document.getElementById('query-query').style.fontFamily='courier';
    document.getElementById('query-query').style.lineHeight='1.5';
    document.getElementById('query-query').style.width='98%';
    document.getElementById('query-query').style.margin='auto';
    document.getElementById('query-query').style.backgroundColor='#f5f5f5';
    viewer.resize();
    viewer.setReadOnly(true);
    viewer.setOptions({
      enableBasicAutocompletion: false,
      enableSnippets: false,
      enableLiveAutocompletion: false
    });

    //Configuration for Query Editor in Profile
    ace.require("ace/ext/language_tools");
    var editor = ace.edit("query-editor");
    //Hidden text input for form-submission
    var queryText = $('input[name="query"]');
    editor.getSession().on("change", function () {
      queryText.val(editor.getSession().getValue());
    });
    editor.setAutoScrollEditorIntoView(true);
    editor.setOption("maxLines", 16);
    editor.setOption("minLines", 10);
    editor.renderer.setShowGutter(true);
    editor.renderer.setOption('showLineNumbers', true);
    editor.renderer.setOption('showPrintMargin', false);
    editor.renderer.setOption("vScrollBarAlwaysVisible", true);
    editor.renderer.setOption("hScrollBarAlwaysVisible", true);;
    editor.renderer.setScrollMargin(10, 10, 10, 10);
    editor.getSession().setMode("ace/mode/sql");
    editor.getSession().setTabSize(4);
    editor.getSession().setUseSoftTabs(true);
    editor.setTheme("ace/theme/sqlserver");
    editor.$blockScrolling = "Infinity";
    //CSS Formatting
    document.getElementById('query-editor').style.fontSize='13px';
    document.getElementById('query-editor').style.fontFamily='courier';
    document.getElementById('query-editor').style.lineHeight='1.5';
    document.getElementById('query-editor').style.width='98%';
    document.getElementById('query-editor').style.margin='auto';
    document.getElementById('query-editor').style.backgroundColor='#ffffff';
    editor.setOptions({
      enableSnippets: true,
      enableBasicAutocompletion: true,
      enableLiveAutocompletion: false
    });

    //Pops out a new window and provide prompt to print
    var popUpAndPrintPlan = function() {
      var srcSvg = $('#query-visual-canvas');
      var screenRatio=0.9;
      let printWindow = window.open('', 'PlanPrint', 'width=' + (screenRatio*screen.width) + ',height=' + (screenRatio*screen.height) );
      printWindow.document.writeln($(srcSvg).parent().html());
      printWindow.print();
    };

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
      if (e.target.form) 
        <#if model.isOnlyImpersonationEnabled()>doSubmitQueryWithUserName()<#else>doSubmitQueryWithAutoLimit()</#if>;
    });
    </script>

</#macro>

<#macro list_options options scope>
 <#if (options?keys?size > 0) >
   <div class="panel-body">
     <h4>${scope} Options</h4>
     <table id="${scope}_options_table" class="table table-bordered">
       <thead>
         <tr>
           <th>Name</th>
           <th>Value</th>
         </tr>
       </thead>
         <tbody>
           <#list options?keys as name>
             <tr>
               <td>${name}</td>
               <td>${options[name]}</td>
             </tr>
           </#list>
         </tbody>
     </table>
   </div>
 </#if>
</#macro>

<@page_html/>
