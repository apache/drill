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
<script src="/static/js/d3.v3.js"></script>
<script src="/static/js/dagre-d3.min.js"></script>
<script src="/static/js/graph.js"></script>
<script src="/static/js/jquery.dataTables-1.10.16.min.js"></script>

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
      }
    );} );
</script>
<style>
/* DataTables Sorting: inherited via sortable class */
table.sortable thead .sorting,.sorting_asc,.sorting_desc {
  background-repeat: no-repeat;
  background-position: center right;
  cursor: pointer;
}
/* Sorting Symbols */
table.sortable thead .sorting { background-image: url("/static/img/black-unsorted.gif"); }
table.sortable thead .sorting_asc { background-image: url("/static/img/black-asc.gif"); }
table.sortable thead .sorting_desc { background-image: url("/static/img/black-desc.gif"); }
</style>
</#macro>

<#macro page_body>
  <a href="/queries">back</a><br/>
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
      <p><pre>${model.getProfile().query}</pre></p>
    </div>
    <div id="query-physical" class="tab-pane">
      <p><pre>${model.profile.plan}</pre></p>
    </div>
    <div id="query-visual" class="tab-pane">
      <svg id="query-visual-canvas" class="center-block"></svg>
    </div>
    <div id="query-edit" class="tab-pane">
      <p>
        <form role="form" action="/query" method="POST">
          <div class="form-group">
            <textarea class="form-control" id="query" name="query" style="font-family: Courier;">${model.getProfile().query}</textarea>
          </div>
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
            <button type="submit" class="btn btn-default">Re-run query</button>
          </form>
      </p>
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
  <h3>Query Profile</h3>
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
      <div id="query-profile-duration" class="panel-collapse collapse">
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

  <#assign options = model.getOptions()>
  <#if (options?keys?size > 0)>
    <div class="page-header"></div>
    <h3>Session Options</h3>
    <div class="panel-group" id="session-options-accordion">
      <div class="panel panel-default">
        <div class="panel-heading">
          <h4 class="panel-title">
            <a data-toggle="collapse" href="#session-options-overview">
              Overview
            </a>
          </h4>
        </div>
        <div id="session-options-overview" class="panel-collapse collapse in">
          <div class="panel-body">
            <table class="table table-bordered">
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
          ${model.getFragmentsOverview()?no_esc}
        </div>
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
</#macro>

<@page_html/>
