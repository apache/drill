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
  <script src="/static/js/jquery.form.js"></script>
  <script src="/static/js/src-min-noconflict/ace.js" type="text/javascript" charset="utf-8"></script>
</#macro>

<#macro page_body>
  <a href="/queries">back</a><br/>
  <div class="page-header">
  </div>
  <h3>Configuration</h3>
  <form id="updateForm" role="form" action="/storage/${model.getName()}" method="POST">
    <input type="hidden" name="name" value="${model.getName()}" />
    <div class="form-group">
      <textarea class="form-control" id="config" name="config" style="font-family: Courier;" data-editor="json">
      </textarea>
    <!-- edits-->
    <script>
        // Hook up ACE editor to all textareas with data-editor attribute
        $(function () {
            $('textarea[data-editor]').each(function () {
                var textarea = $(this);
                var mode = textarea.data('editor');
                var editDiv = $('<div>', {
                    //position: 'absolute',
                    //width: textarea.width(),
                    //height: textarea.height(),
                    'class': textarea.attr('class')
                }).insertBefore(textarea);
                textarea.css('visibility', 'hidden');
                var editor = ace.edit(editDiv[0]);
                editor.setAutoScrollEditorIntoView(true);
                editor.setOption("maxLines", 25);
                editor.setOption("minLines", 10);
                editor.renderer.setShowGutter(true);
                editor.renderer.setOption('showLineNumbers', true);
                editor.renderer.setOption('showPrintMargin', false);
                editor.getSession().setValue(textarea.val());
                editor.getSession().setMode("ace/mode/" + mode);


                // copy back to textarea on form submit...
                textarea.closest('form').submit(function () {
                    textarea.val(editor.getSession().getValue());
                })
            });
        });
    </script>
    </div>
    <a class="btn btn-default" href="/storage">Back</a>
    <button class="btn btn-default" type="submit" onclick="doUpdate();">
      <#if model.exists()>Update<#else>Create</#if>
    </button>
    <#if model.exists()>
      <#if model.enabled()>
        <a id="enabled" class="btn btn-default">Disable</a>
      <#else>
        <a id="enabled" class="btn btn-primary">Enable</a>
      </#if>
      <a id="del" class="btn btn-danger" onclick="deleteFunction()">Delete</a>
    </#if>
  </form>
  <br>
  <div id="message" class="hidden alert alert-info">
  </div>
  <script>
    $.get("/storage/${model.getName()}.json", function(data) {
      $("#config").val(JSON.stringify(data.config, null, 2));
    });
    $("#enabled").click(function() {
      $.get("/storage/${model.getName()}/enable/<#if model.enabled()>false<#else>true</#if>", function(data) {
        $("#message").removeClass("hidden").text(data.result).alert();
        setTimeout(function() { location.reload(); }, 800);
      });
    });
    function doUpdate() {
      $("#updateForm").ajaxForm(function(data) {
        var messageEl = $("#message");
        if (data.result == "success") {
          messageEl.removeClass("hidden")
                   .removeClass("alert-danger")
                   .addClass("alert-info")
                   .text(data.result).alert();
          setTimeout(function() { location.reload(); }, 800);
        } else {
          messageEl.addClass("hidden");
          // Wait a fraction of a second before showing the message again. This
          // makes it clear if a second attempt gives the same error as
          // the first that a "new" message came back from the server
          setTimeout(function() {
            messageEl.removeClass("hidden")
                     .removeClass("alert-info")
                     .addClass("alert-danger")
                     .text("Please retry: " + data.result).alert();
          }, 200);
        }
      });
    };
    function deleteFunction() {
      var temp = confirm("Are you sure?");
      if (temp == true) {
        $.get("/storage/${model.getName()}/delete", function(data) {
          window.location.href = "/storage";
        });
      }
    };
  </script>
</#macro>

<@page_html/>