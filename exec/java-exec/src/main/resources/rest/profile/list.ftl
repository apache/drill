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

<script src="/static/js/jquery.dataTables-1.10.16.min.js"></script>
<script>
    $(document).ready(function() {
      $("#profileList").DataTable( {
        //Preserve order
        "ordering": false,
        "searching": true,
        "paging": true,
        "pagingType": "first_last_numbers",
        "lengthMenu": [[10, 25, 50, -1], [10, 25, 50, "All"]],
        "lengthChange": true,
        "info": true,
        //Ref: https://legacy.datatables.net/ref#sDom
        "sDom": '<"top"lftip><"bottom"><"clear">',
        //Customized info labels
        "language": {
            "lengthMenu": "Display _MENU_ profiles per page",
            "zeroRecords": "No matching profiles found!",
            "info": "Showing page _PAGE_ of _PAGES_ ",
            "infoEmpty": "No profiles available",
            "infoFiltered": "(filtered _TOTAL_ from _MAX_)",
            "search": "Search Profiles  "
        }
      } );
    } );
</script>

<!-- CSS to control DataTable Elements -->
<style type="text/css" class="init">
  /* Control Padding for length and filter as a pair */
  div.dataTables_length {
    float: right;
  }
  div.dataTables_filter {
    float: left;
  }
  div.dataTables_info {
    padding-right: 2em;
    float: right;
  }

  /* Add spaces between pagination links */
  #profileList_paginate * {
    padding-right: 0.55em;
    float:left
  }
  /* Normal wt for search text */
  #profileList_filter input {
    font-weight: normal;
    padding-left: 0.45em;
  }
  #profileList_length * {
    font-weight: normal;
  }

</style>

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
  <table width="100%">
    <script type="text/javascript" language="javascript">
    //Validate that the fetch number is valid
    function checkMaxFetch() {
      var maxFetch = document.forms["profileFetch"]["max"].value;
      console.log("maxFetch: " + maxFetch);
      if (isNaN(maxFetch) || (maxFetch < 1) || (maxFetch > 100000) ) {
        alert("Invalid Entry: " + maxFetch + "\n" +
               "Please enter a valid number of profiles to fetch (1 to 100000) ");
        return false;
      }
      return true;
    }
    </script>
    <tr>
      <td><h3>Completed Queries</h3></td>
      <td align="right">
        <form name="profileFetch" action="/profiles" onsubmit="return checkMaxFetch();" method="get"><span title="Max number of profiles to load">Loaded <b>${model.getFinishedQueries()?size}</b> profiles </span>
        <input id="fetchMax" type="text" size="5" name="max" value="" style="text-align: right" />
        <input type="submit" value="Reload"/>
      </form></td>
    </tr></table>
    <!-- Placed after textbox to allow for DOM to contain "fetchMax" element -->
    <script type="text/javascript" language="javascript">
    //Get max fetched from URL for populating textbox
    var maxFetched="${model.getMaxFetchedQueries()}";
    if (window.location.search.indexOf("max=") >= 1) {
      //Select 1st occurrence (Chrome accepts 1st of duplicates)
      var kvPair=window.location.search.substr(1).split('&')[0];
      maxFetched=kvPair.split('=')[1]
    }
    //Update textbox
    $(document).ready(function() {
            $("#fetchMax").val(maxFetched);
    });
    </script>
  <@list_queries queries=model.getFinishedQueries()/>
</#macro>

<#macro list_queries queries>
    <div class="table-responsive">
        <table id="profileList" class="table table-hover dataTable" role="grid">
            <thead>
            <tr role="row">
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
    <div style="padding-bottom: 2em;"/>
</#macro>

<@page_html/>