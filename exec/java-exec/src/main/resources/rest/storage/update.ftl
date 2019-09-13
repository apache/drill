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
  <!-- Ace Libraries for Syntax Formatting -->
  <script src="/static/js/ace-code-editor/ace.js" type="text/javascript" charset="utf-8"></script>
  <script src="/static/js/ace-code-editor/theme-eclipse.js" type="text/javascript" charset="utf-8"></script>
  <script src="/static/js/serverMessage.js"></script>
</#macro>

<#macro page_body>
  <div class="page-header">
  </div>
  <h3>Configuration</h3>
  <form id="updateForm" role="form" action="/storage/create_update" method="POST">
    <input type="hidden" name="name" value="${model.getPlugin().getName()}" />
    <div class="form-group">
      <div id="editor" class="form-control"></div>
      <textarea class="form-control" id="config" name="config" data-editor="json" style="display: none;" >
      </textarea>
    </div>
    <a class="btn btn-default" href="/storage">Back</a>
    <button class="btn btn-default" type="submit" onclick="doUpdate();">Update</button>
    <#if model.getPlugin().enabled()>
      <a id="enabled" class="btn btn-default">Disable</a>
    <#else>
      <a id="enabled" class="btn btn-primary">Enable</a>
    </#if>
    <button type="button" class="btn btn-default export" name="${model.getPlugin().getName()}" data-toggle="modal"
            data-target="#pluginsModal">
      Export
    </button>
    <a id="del" class="btn btn-danger" onclick="deleteFunction()">Delete</a>
    <input type="hidden" name="csrfToken" value="${model.getCsrfToken()}">
  </form>
  <br>
  <div id="message" class="hidden alert alert-info">
  </div>

  <#include "*/confirmationModals.ftl">

  <#-- Modal window-->
  <div class="modal fade" id="pluginsModal" tabindex="-1" role="dialog" aria-labelledby="exportPlugin" aria-hidden="true">
    <div class="modal-dialog modal-sm" role="document">
      <div class="modal-content">
        <div class="modal-header">
          <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
          <h4 class="modal-title" id="exportPlugin">Plugin config</h4>
        </div>
        <div class="modal-body">
          <div id="format" style="display: inline-block; position: relative;">
            <label for="format">Format</label>
            <div class="radio">
              <label>
                <input type="radio" name="format" id="json" value="json" checked="checked">
                JSON
              </label>
            </div>
            <div class="radio">
              <label>
                <input type="radio" name="format" id="hocon" value="conf">
                HOCON
              </label>
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
          <button type="button" id="export" class="btn btn-primary">Export</button>
        </div>
      </div>
    </div>
  </div>

  <script>
    const editor = ace.edit("editor");
    const textarea = $('textarea[name="config"]');

    editor.setAutoScrollEditorIntoView(true);
    editor.setOption("maxLines", 25);
    editor.setOption("minLines", 10);
    editor.renderer.setShowGutter(true);
    editor.renderer.setOption('showLineNumbers', true);
    editor.renderer.setOption('showPrintMargin', false);
    editor.getSession().setMode("ace/mode/json");
    editor.setTheme("ace/theme/eclipse");

    // copy back to textarea on form submit...
    editor.getSession().on('change', function(){
      textarea.val(editor.getSession().getValue());
    });

    $.get("/storage/" + encodeURIComponent("${model.getPlugin().getName()}") + ".json", function(data) {
      $("#config").val(JSON.stringify(data.config, null, 2));
      editor.getSession().setValue( JSON.stringify(data.config, null, 2) );
    });


    $("#enabled").click(function() {
      const enabled = ${model.getPlugin().enabled()?c};
      if (enabled) {
        showConfirmationDialog('"${model.getPlugin().getName()}"' + ' plugin will be disabled. Proceed?', proceed);
      } else {
        proceed();
      }
      function proceed() {
        $.get("/storage/" + encodeURIComponent("${model.getPlugin().getName()}") + "/enable/<#if model.getPlugin().enabled()>false<#else>true</#if>", function(data) {
          $("#message").removeClass("hidden").text(data.result).alert();
          setTimeout(function() { location.reload(); }, 800);
        });
      }
    });

    function doUpdate() {
      $("#updateForm").ajaxForm({
        dataType: 'json',
        success: serverMessage
      });
    }

    function deleteFunction() {
      showConfirmationDialog('"${model.getPlugin().getName()}"' + ' plugin will be deleted. Proceed?', function() {
        $.get("/storage/" + encodeURIComponent("${model.getPlugin().getName()}") + "/delete", serverMessage);
      });
    }

    // Modal window management
    $('#pluginsModal').on('show.bs.modal', function (event) {
      const button = $(event.relatedTarget); // Button that triggered the modal
      let exportInstance = button.attr("name");
      const modal = $(this);
      modal.find('.modal-title').text(exportInstance.toUpperCase() +' Plugin configs');
      modal.find('.btn-primary').click(function(){
        let format = "";
        if (modal.find('#json').is(":checked")) {
          format = 'json';
        }
        if (modal.find('#hocon').is(":checked")) {
          format = 'conf';
        }

        let url = '/storage/' + encodeURIComponent(exportInstance) + '/export/' + format;
        window.open(url);
      });
    })
  </script>
</#macro>

<@page_html/>