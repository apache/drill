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
    <script type="text/javascript" language="javascript"  src="/static/js/jquery.dataTables-1.10.16.min.js"> </script>
    <script type="text/javascript" language="javascript" src="/static/js/dataTables.colVis-1.1.0.min.js"></script>
    <script>
    //Alter System Values
    function alterSysOption(optionName, optionValue, optionKind) {
        $.post("/option/"+optionName, {kind: optionKind, name: optionName, value: optionValue}, function () {
            location.reload(true);
        });
    }

    //Read Values and apply
    function alterSysOptionUsingId(optionRawName) {
        //Escaping '.' for id search
        let optionName = optionRawName.replace(/\./gi, "\\.");
        //Extracting datatype from the form
        let optionKind = $("#"+optionName+" input[name='kind']").attr("value");
        //Extracting value from the form's INPUT element
        let optionValue = $("#"+optionName+" input[name='value']").val();
        if (optionKind == "BOOLEAN") {
            //Extracting boolean value from the form's SELECT element (since this is a dropdown input)
            optionValue = $("#"+optionName+" select[name='value']").val();
        } else if (optionKind != "STRING") { //i.e. it is a number (FLOAT/DOUBLE/LONG)
            if (isNaN(optionValue)) {
                alert(optionValue+" is not a valid number for option: "+optionName);
                return;
            }
        }
        alterSysOption(optionRawName, optionValue, optionKind);
    }
    </script>
    <!-- List of Option Descriptions -->
    <script src="/dynamic/options.describe.js"></script>
    <link href="/static/css/dataTables.colVis-1.1.0.min.css" rel="stylesheet">
    <link href="/static/css/dataTables.jqueryui.css" rel="stylesheet">
    <link href="/static/css/jquery-ui-1.10.3.min.css" rel="stylesheet">
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
  <div class="page-header">
  </div>
  <div class="btn-group btn-group-sm" style="display:inline-block;">
  <button type="button" class="btn" style="cursor:default;font-weight:bold;" > Quick Filters </button>
  <button type="button" class="btn btn-info" onclick="inject(this.innerHTML);">planner</button>
  <button type="button" class="btn btn-info" onclick="inject(this.innerHTML);">store</button>
  <button type="button" class="btn btn-info" onclick="inject(this.innerHTML);">parquet</button>
  <button type="button" class="btn btn-info" onclick="inject(this.innerHTML);">hashagg</button>
  <button type="button" class="btn btn-info" onclick="inject(this.innerHTML);">hashjoin</button>
  </div>
  <div class="col-xs-4">
  <input id="searchBox"  name="searchBox" class="form-control" type="text" value="" placeholder="Search options...">
  </div>

  <div class="table-responsive">
    <table id='optionsTbl' class="table table-striped table-condensed display sortable" style="table-layout: auto; width=100%;">
      <thead>
        <tr>
          <th style="width:30%">OPTION</th>
          <th style="width:25%">VALUE</th>
          <th style="width:45%">DESCRIPTION</th>
        </tr>
      </thead>
      <tbody>
        <#assign i = 1>
        <#list model as option>
          <tr id="row-${i}">
            <td style="font-family:Courier New; vertical-align:middle" id='optionName'>${option.getName()}</td>
            <td>
              <form class="form-inline" role="form" id="${option.getName()}">
                <div class="form-group">
                <input type="hidden" class="form-control" name="kind" value="${option.getKind()}">
                <input type="hidden" class="form-control" name="name" value="${option.getName()}">
                  <div class="input-group input-sm">
                  <#if option.getKind() == "BOOLEAN" >
                  <select class="form-control" name="value">
                    <option value="false" ${(option.getValueAsString() == "false")?string("selected", "")}>false</option>
                    <option value="true" ${(option.getValueAsString() == "true")?string("selected", "")}>true</option>
                  </select>
                  <#else>
                    <input type="text" class="form-control" placeholder="${option.getValueAsString()}" name="value" value="${option.getValueAsString()}">
                  </#if>
                    <div class="input-group-btn">
                      <button class="btn btn-default" type="button" onclick="alterSysOptionUsingId('${option.getName()}')">Update</button>
                      <button class="btn btn-default" type="button" onclick="alterSysOption('${option.getName()}','${option.getDefaultValue()}', '${option.getKind()}')" <#if option.getDefaultValue() == option.getValueAsString()>disabled="true" style="pointer-events:none" <#else>
                      title="Reset to ${option.getDefaultValue()}"</#if>>Default</button>
                    </div>
                  </div>
                </div>
              </form>
            </td>
            <td id='description'></td>
          </tr>
          <#assign i = i + 1>
        </#list>
      </tbody>
    </table>
  </div>
  <script>
   //Defining the DataTable with a handle
   var optTable = $('#optionsTbl').DataTable( {
        "lengthChange": false,
         "pageLength": -1,
        "dom": 'lrit',
        "jQueryUI" : true,
        "searching": true,
        "language": {
            "lengthMenu": "Display _MENU_ records per page",
            "zeroRecords": "No matching options found. Check search entry",
            "info": "Found _END_ matches out of _MAX_ options",
            "infoEmpty": "No options available",
            "infoFiltered": ""
        }
      } );

    //Draw when the table is ready
    $(document).ready(function() {
      //Inject Descriptions for table
      let size = $('#optionsTbl tbody tr').length;
      for (i = 1; i <= size; i++) {
        let currRow = $("#row-"+i);
        let optionName = currRow.find("#optionName").text();
        let setOptDescrip = currRow.find("#description").text(getDescription(optionName));
      }

      // Draw DataTable
      optTable.rows().invalidate().draw();
    });

    //EventListener to update table when changes are detected
    $('#searchBox').on('keyup change', function () {
      optTable.search(this.value).draw().toString();
    });

    //Inject word and force table redraw
    function inject(searchTerm) {
      $('#searchBox').val(searchTerm);
      optTable.search(searchTerm).draw().toString();
    }
  </script>
</#macro>
<@page_html/>
