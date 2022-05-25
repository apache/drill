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
  <script src="/static/js/credentialsServerMessage.js"></script>
</#macro>

<#macro page_body>

<#include "*/confirmationModals.ftl">

<h4 class="col-xs-6 mx-3">User Credential Management</h4>

<div class="pb-2 mt-4 mb-2 border-bottom" style="margin: 5px;"></div>

<div class="container-fluid">
  <div class="row">
    <div class="table-responsive col-sm-12 col-md-6 col-lg-5 col-xl-5">
      <h4>Enabled Storage Plugins</h4>
      <table class="table table-hover">
        <tbody>
        <#list model as pluginModel>
          <#if pluginModel.getPlugin()?? >
          <tr>
            <td style="border:none; max-width: 200px; overflow: hidden; text-overflow: ellipsis;">
                ${pluginModel.getPlugin().getName()}
            </td>
            <td style="border:none;">
                <#if pluginModel.getPlugin().isOauth()>
                  <button type="button" class="btn btn-primary"
                          id="getOauth" class="btn btn-success text-white"
                          onclick="authorize('${pluginModel.getPlugin().getAuthorizationURIWithParams()!}')">Authorize</button>
                <#else>
              <button type="button" class="btn btn-primary" data-toggle="modal" data-target="#new-plugin-modal" data-plugin="${pluginModel.getPlugin().getName()}"
                      data-username="${pluginModel.getUserName()}" data-password="${pluginModel.getPassword()}">
                Update Credentials
              </button>
                </#if>
            </td>
          </tr>
          </#if>
        </#list>
        </tbody>
      </table>
    </div>

      <#-- Modal window for creating plugin -->
    <div class="modal fade" id="new-plugin-modal" role="dialog" aria-labelledby="configuration">
      <div class="modal-dialog" role="document">
        <div class="modal-content">
          <div class="modal-header">
            <h4 class="modal-title" id="configuration">Update Credentials</h4>
            <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
          </div>
          <div class="modal-body">
            <form id="createForm" role="form" action="/credentials/update_credentials" method="POST">
              <input type="text" class="form-control" name="username" id="usernameField" placeholder="Username" />
              <input type="text" class="form-control" name="password" id="passwordField" placeholder="Password" />
              <input type="hidden" name="plugin" id="plugin" />
              <div style="text-align: right; margin: 10px">
                <button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>
                <button type="submit" class="btn btn-primary" onclick="doCreate()">Update Credentials</button>
              </div>
              <input type="hidden" name="csrfToken" value="${model[0].getCsrfToken()}">
            </form>

            <div id="message" class="d-none alert alert-info">
            </div>
          </div>
        </div>
      </div>
    </div>
      <#-- Modal window for creating plugin -->

    <script>
      // Populate the modal fields
      $(function () {
        $('#new-plugin-modal').on('show.bs.modal', function (event) {
          var button = $(event.relatedTarget);
          var username = button.data('username');
          var password = button.data('password');
          var plugin = button.data('plugin');

          $('#plugin').val(plugin);
          $('#usernameField').val(username);
          $('#passwordField').val(password);
        });
      });

      function authorize(finalURL) {
        console.log(finalURL);
        var tokenGetterWindow = window.open(finalURL, 'Authorize Drill', "toolbar=no,menubar=no,scrollbars=yes,resizable=yes,top=500,left=500,width=450,height=600");
        var timer = setInterval(function () {
          if (tokenGetterWindow.closed) {
            clearInterval(timer);
            window.location.reload(); // Refresh the parent page
          }
        }, 1000);
      }

      function doCreate() {
        $("#createForm").ajaxForm({
          dataType: 'json',
          success: serverMessage,
          error: serverMessage
        });
      }
    </script>
<#include "*/alertModals.ftl">
</#macro>

<@page_html/>
