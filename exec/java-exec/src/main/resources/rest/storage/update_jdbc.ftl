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
  <h3>JDBC Storage Plugin Configuration</h3>

  <form id="updateForm" role="form" action="/storage/create_update" method="POST">
    <input type="hidden" name="name" value="${model.getPlugin().getName()}" />
    <input type="hidden" name="pluginType" value="${model.getType()}" />
    <input type="hidden" name="csrfToken" value="${model.getCsrfToken()}">

    <!-- Database Type Preset Selector -->
    <div class="form-group">
      <label for="databaseType">Database Type</label>
      <select id="databaseType" class="form-control" onchange="applyPreset(this.value)">
        <option value="">-- Select Database or Custom --</option>
        <option value="mysql">MySQL</option>
        <option value="postgresql">PostgreSQL</option>
        <option value="oracle">Oracle</option>
        <option value="sqlserver">SQL Server</option>
        <option value="mariadb">MariaDB</option>
        <option value="db2">DB2</option>
        <option value="snowflake">Snowflake</option>
        <option value="custom">Custom / Other</option>
      </select>
    </div>

    <!-- Connection Section -->
    <fieldset class="form-group border p-3">
      <legend class="w-auto">Connection</legend>

      <div class="row">
        <div class="col-md-8">
          <label for="host">Host / Server</label>
          <input type="text" class="form-control" id="host" placeholder="localhost" onchange="rebuildUrl()">
        </div>
        <div class="col-md-4">
          <label for="port">Port</label>
          <input type="text" class="form-control" id="port" placeholder="5432" onchange="rebuildUrl()">
        </div>
      </div>

      <div class="form-group mt-3">
        <label for="database">Database / Schema</label>
        <input type="text" class="form-control" id="database" placeholder="mydb" onchange="rebuildUrl()">
      </div>
    </fieldset>

    <!-- Credentials Section -->
    <fieldset class="form-group border p-3">
      <legend class="w-auto">Credentials</legend>

      <div class="form-group">
        <label for="username">Username</label>
        <input type="text" class="form-control" id="username" placeholder="">
      </div>

      <div class="form-group">
        <label for="password">Password</label>
        <input type="password" class="form-control" id="password" placeholder="">
      </div>
    </fieldset>

    <!-- Advanced Options (Collapsible) -->
    <div class="form-group">
      <a data-toggle="collapse" href="#advancedOptions" class="btn btn-sm btn-secondary mb-3">
        Advanced Options
      </a>
      <div id="advancedOptions" class="collapse">
        <fieldset class="form-group border p-3">
          <legend class="w-auto">Advanced Settings</legend>

          <div class="form-group">
            <label for="driverClass">JDBC Driver Class</label>
            <input type="text" class="form-control" id="driverClass" placeholder="com.example.jdbc.Driver">
            <small class="form-text text-muted">Auto-filled when you select a database type above.</small>
          </div>

          <div class="form-group">
            <label for="connectionUrl">Connection URL</label>
            <input type="text" class="form-control" id="connectionUrl" placeholder="jdbc:example://host:port/database">
            <small class="form-text text-muted">Auto-generated from host, port, and database when possible. You can edit this directly.</small>
          </div>

          <div class="form-group">
            <label for="authMode">Authentication Mode</label>
            <select id="authMode" class="form-control">
              <option value="SHARED_USER">Shared User (single set of credentials)</option>
              <option value="USER_TRANSLATION">User Translation (per-user credentials)</option>
            </select>
          </div>

          <div class="form-check">
            <input type="checkbox" class="form-check-input" id="caseInsensitive">
            <label class="form-check-label" for="caseInsensitive">
              Case Insensitive Table Names
            </label>
          </div>

          <div class="form-check mt-2">
            <input type="checkbox" class="form-check-input" id="writable">
            <label class="form-check-label" for="writable">
              Writable (allow INSERT/UPDATE/DELETE)
            </label>
          </div>

          <div class="form-group mt-3">
            <label for="writerBatchSize">Writer Batch Size</label>
            <input type="number" class="form-control" id="writerBatchSize" value="10000" min="1">
          </div>

          <div class="form-group">
            <label>Source Parameters (Additional JDBC connection parameters)</label>
            <div id="sourceParametersContainer">
              <!-- Will be populated dynamically -->
            </div>
            <button type="button" class="btn btn-sm btn-secondary mt-2" onclick="addSourceParameter()">Add Parameter</button>
          </div>
        </fieldset>
      </div>
    </div>

    <!-- Raw JSON Editor Toggle -->
    <div class="form-group mt-4">
      <button type="button" class="btn btn-sm btn-info" data-toggle="collapse" data-target="#rawJsonEditor">
        View/Edit Raw JSON
      </button>
      <div id="rawJsonEditor" class="collapse mt-3">
        <div id="editor" class="form-control" style="height: 300px;"></div>
        <textarea class="form-control" id="config" name="config" data-editor="json" style="display: none;"></textarea>
      </div>
    </div>

    <!-- Buttons -->
    <a class="btn btn-secondary" href="/storage">Back</a>
    <button type="button" class="btn btn-info" onclick="doTestConnection();">Test Connection</button>
    <button class="btn btn-primary" type="submit" onclick="doUpdate();">Update</button>
    <#if model.getPlugin().enabled()>
      <a id="enabled" class="btn btn-warning">Disable</a>
    <#else>
      <a id="enabled" class="btn btn-success text-white">Enable</a>
    </#if>
    <button type="button" class="btn btn-secondary export" name="${model.getPlugin().getName()}" data-toggle="modal" data-target="#pluginsModal">
      Export
    </button>
    <a id="del" class="btn btn-danger text-white" onclick="deleteFunction()">Delete</a>

    <!-- Message Areas -->
    <br>
    <div id="message" class="d-none alert alert-info"></div>
    <div id="testMessage" class="d-none alert"></div>
  </form>

  <#include "*/confirmationModals.ftl">

  <!-- Modal for Export -->
  <div class="modal fade" id="pluginsModal" tabindex="-1" role="dialog" aria-labelledby="exportPlugin" aria-hidden="true">
    <div class="modal-dialog modal-sm" role="document">
      <div class="modal-content">
        <div class="modal-header">
          <h4 class="modal-title" id="exportPlugin">Plugin config</h4>
          <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
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
          <button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>
          <button type="button" id="export" class="btn btn-primary">Export</button>
        </div>
      </div>
    </div>
  </div>

  <script>
    // Database presets with driver, URL template, and default port
    const databasePresets = {
      mysql: {
        driver: 'com.mysql.cj.jdbc.Driver',
        urlTemplate: 'jdbc:mysql://{host}:{port}/{database}',
        defaultPort: 3306
      },
      postgresql: {
        driver: 'org.postgresql.Driver',
        urlTemplate: 'jdbc:postgresql://{host}:{port}/{database}',
        defaultPort: 5432
      },
      oracle: {
        driver: 'oracle.jdbc.driver.OracleDriver',
        urlTemplate: 'jdbc:oracle:thin:@{host}:{port}:{database}',
        defaultPort: 1521
      },
      sqlserver: {
        driver: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        urlTemplate: 'jdbc:sqlserver://{host}:{port};databaseName={database}',
        defaultPort: 1433
      },
      mariadb: {
        driver: 'org.mariadb.jdbc.Driver',
        urlTemplate: 'jdbc:mariadb://{host}:{port}/{database}',
        defaultPort: 3306
      },
      db2: {
        driver: 'com.ibm.db2.jcc.DB2Driver',
        urlTemplate: 'jdbc:db2://{host}:{port}/{database}',
        defaultPort: 50000
      },
      snowflake: {
        driver: 'net.snowflake.client.jdbc.SnowflakeDriver',
        urlTemplate: 'jdbc:snowflake://{host}.snowflakecomputing.com/?db={database}',
        defaultPort: null
      }
    };

    let pluginName = '${model.getPlugin().getName()}';
    let selectedPreset = null;

    // Initialize the page
    $(function() {
      initializeAceEditor();
      loadConfig();
      setupEventHandlers();
    });

    // Initialize ACE editor
    function initializeAceEditor() {
      const editor = ace.edit("editor");
      editor.setAutoScrollEditorIntoView(true);
      editor.setOption("maxLines", 25);
      editor.setOption("minLines", 10);
      editor.renderer.setShowGutter(true);
      editor.renderer.setOption('showLineNumbers', true);
      editor.renderer.setOption('showPrintMargin', false);
      editor.getSession().setMode("ace/mode/json");
      editor.setTheme("ace/theme/eclipse");

      // Sync back to textarea on editor changes
      editor.getSession().on('change', function(){
        $('#config').val(editor.getSession().getValue());
      });
    }

    // Load existing plugin configuration
    function loadConfig() {
      $.get("/storage/" + encodeURIComponent(pluginName) + ".json", function(data) {
        const config = data.config;

        // Populate form fields from config
        $('#driverClass').val(config.driver || '');
        $('#connectionUrl').val(config.url || '');
        $('#username').val(config.username || '');
        $('#password').val(config.password || '');
        $('#caseInsensitive').prop('checked', config.caseInsensitiveTableNames === true);
        $('#writable').prop('checked', config.writable === true);
        $('#writerBatchSize').val(config.writerBatchSize || 10000);
        $('#authMode').val(config.authMode || 'SHARED_USER');

        // Try to detect and select preset from driver
        detectPreset(config.driver || '');

        // Parse connection URL to extract host, port, database
        parseConnectionUrl(config.url || '');

        // Load source parameters
        if (config.sourceParameters && Object.keys(config.sourceParameters).length > 0) {
          for (const [key, value] of Object.entries(config.sourceParameters)) {
            addSourceParameter(key, value);
          }
        } else {
          // Start with empty row
          addSourceParameter();
        }

        // Sync to ACE editor
        syncEditor();
      });
    }

    // Detect preset from driver class
    function detectPreset(driver) {
      selectedPreset = null;
      for (const [presetName, preset] of Object.entries(databasePresets)) {
        if (preset.driver === driver) {
          $('#databaseType').val(presetName);
          selectedPreset = presetName;
          break;
        }
      }
    }

    // Parse connection URL to extract components
    function parseConnectionUrl(url) {
      if (!url) return;

      // Simple parsing - works for most JDBC URLs
      // Format examples:
      // jdbc:mysql://host:port/database
      // jdbc:postgresql://host:port/database
      // jdbc:oracle:thin:@host:port:database
      // jdbc:sqlserver://host:port;databaseName=database

      let host = '', port = '', database = '';

      if (url.includes('jdbc:oracle')) {
        // Oracle: jdbc:oracle:thin:@host:port:database
        const match = url.match(/jdbc:oracle:thin:@([^:]+):(\d+):(.+)/);
        if (match) {
          host = match[1];
          port = match[2];
          database = match[3];
        }
      } else if (url.includes('jdbc:sqlserver')) {
        // SQL Server: jdbc:sqlserver://host:port;databaseName=database
        const match = url.match(/jdbc:sqlserver:\/\/([^:;]+):?(\d+)?;?.*databaseName=([^;&]+)/);
        if (match) {
          host = match[1];
          port = match[2] || '';
          database = match[3];
        }
      } else if (url.includes('jdbc:snowflake')) {
        // Snowflake: jdbc:snowflake://account.snowflakecomputing.com/?db=database
        const match = url.match(/jdbc:snowflake:\/\/([^.]+)/);
        if (match) {
          host = match[1] + '.snowflakecomputing.com';
        }
        const dbMatch = url.match(/db=([^&]+)/);
        if (dbMatch) {
          database = dbMatch[1];
        }
      } else {
        // Standard format: jdbc:type://host:port/database
        const match = url.match(/jdbc:[^:]+:\/\/([^/:]+):?(\d+)?\/(.+)/);
        if (match) {
          host = match[1];
          port = match[2] || '';
          database = match[3];
        }
      }

      if (host) $('#host').val(host);
      if (port) $('#port').val(port);
      if (database) $('#database').val(database);
    }

    // Apply database preset
    function applyPreset(presetKey) {
      if (!presetKey || presetKey === 'custom') {
        selectedPreset = null;
        $('#driverClass').val('');
        $('#port').val('');
        $('#connectionUrl').val('');
        return;
      }

      const preset = databasePresets[presetKey];
      if (!preset) return;

      selectedPreset = presetKey;
      $('#driverClass').val(preset.driver);

      if (preset.defaultPort) {
        $('#port').val(preset.defaultPort);
      } else {
        $('#port').val('');
      }

      rebuildUrl();
    }

    // Rebuild connection URL from components
    function rebuildUrl() {
      if (!selectedPreset) {
        // If no preset selected, don't auto-build URL
        return;
      }

      const preset = databasePresets[selectedPreset];
      if (!preset) return;

      const host = $('#host').val().trim();
      const port = $('#port').val().trim();
      const database = $('#database').val().trim();

      if (!host || !database) return;

      let url = preset.urlTemplate
        .replace('{host}', host)
        .replace('{port}', port)
        .replace('{database}', database);

      $('#connectionUrl').val(url);
      syncEditor();
    }

    // Add a source parameter row
    function addSourceParameter(key = '', value = '') {
      const container = $('#sourceParametersContainer');
      const rowCount = container.children().length;
      const rowId = 'param_' + rowCount;

      const html = `
        <div id="${rowId}" class="input-group mb-2">
          <input type="text" class="form-control" placeholder="Parameter name" value="${key}">
          <input type="text" class="form-control" placeholder="Parameter value" value="${value}">
          <div class="input-group-append">
            <button type="button" class="btn btn-sm btn-danger" onclick="removeSourceParameter('${rowId}')">Remove</button>
          </div>
        </div>
      `;

      container.append(html);
    }

    // Remove a source parameter row
    function removeSourceParameter(rowId) {
      $('#' + rowId).remove();
    }

    // Build complete JDBC configuration JSON
    function buildConfigJson() {
      const sourceParameters = {};
      $('#sourceParametersContainer').children().each(function() {
        const inputs = $(this).find('input');
        const key = inputs.eq(0).val().trim();
        const value = inputs.eq(1).val().trim();
        if (key) {
          sourceParameters[key] = value;
        }
      });

      const config = {
        type: 'jdbc',
        driver: $('#driverClass').val().trim(),
        url: $('#connectionUrl').val().trim(),
        username: $('#username').val().trim(),
        password: $('#password').val().trim(),
        caseInsensitiveTableNames: $('#caseInsensitive').prop('checked'),
        writable: $('#writable').prop('checked'),
        writerBatchSize: parseInt($('#writerBatchSize').val()) || 10000,
        authMode: $('#authMode').val(),
        sourceParameters: sourceParameters
      };

      return config;
    }

    // Sync form values to JSON editor
    function syncEditor() {
      const editor = ace.edit("editor");
      const config = buildConfigJson();
      const jsonStr = JSON.stringify(config, null, 2);
      editor.getSession().setValue(jsonStr);
      $('#config').val(jsonStr);
    }

    // Test the connection
    function doTestConnection() {
      const driver = $('#driverClass').val().trim();
      const url = $('#connectionUrl').val().trim();

      if (!driver) {
        alert('Please enter a JDBC driver class');
        return;
      }
      if (!url) {
        alert('Please enter a connection URL');
        return;
      }

      const config = buildConfigJson();
      const payload = {
        name: pluginName,
        config: config
      };

      const testMessageEl = $('#testMessage');
      testMessageEl.removeClass('d-none alert-success alert-danger').text('Testing connection...');

      $.ajax({
        url: '/storage/test-connection',
        method: 'POST',
        contentType: 'application/json',
        data: JSON.stringify(payload),
        dataType: 'json',
        success: function(data) {
          if (data.success) {
            testMessageEl.removeClass('alert-danger').addClass('alert-success').text('✓ ' + data.message);
          } else {
            testMessageEl.removeClass('alert-success').addClass('alert-danger').text('✗ Connection failed: ' + data.message);
          }
        },
        error: function(xhr, status, error) {
          let errorMsg = 'Unknown error';
          if (xhr.responseJSON && xhr.responseJSON.message) {
            errorMsg = xhr.responseJSON.message;
          } else if (xhr.statusText) {
            errorMsg = xhr.statusText;
          }
          testMessageEl.removeClass('alert-success').addClass('alert-danger').text('✗ Error: ' + errorMsg);
        }
      });
    }

    // Update plugin configuration
    function doUpdate() {
      syncEditor();
      $("#updateForm").ajaxForm({
        dataType: 'json',
        success: serverMessage,
        error: serverMessage
      });
    }

    // Delete plugin
    function deleteFunction() {
      showConfirmationDialog('"' + pluginName + '" plugin will be deleted. Proceed?', function() {
        $.ajax({
            url: '/storage/' + encodeURIComponent(pluginName) + '.json',
            method: 'DELETE',
            contentType: 'application/json',
            success: serverMessage,
            error: function(request, msg, error) {
              serverMessage({ errorMessage: 'Error while trying to delete.' })
            }
        });
      });
    }

    // Enable/Disable plugin
    $("#enabled").click(function() {
      const enabled = ${model.getPlugin().enabled()?c};
      if (enabled) {
        showConfirmationDialog('"' + pluginName + '" plugin will be disabled. Proceed?', proceed);
      } else {
        proceed();
      }
      function proceed() {
        $.post("/storage/" + encodeURIComponent(pluginName) + "/enable/" + !enabled, function(data) {
          if (serverMessage(data)) {
              setTimeout(function() { location.reload(); }, 800);
          }
        }).fail(function(response) {
          if (serverMessage(response.responseJSON)) {
              setTimeout(function() { location.reload(); }, 800);
          }
        });
      }
    });

    // Export modal functionality
    $('#pluginsModal').on('show.bs.modal', function (event) {
      const button = $(event.relatedTarget);
      let exportInstance = button.attr("name");
      const modal = $(this);
      modal.find('.modal-title').text(exportInstance.toUpperCase() + ' Plugin configs');
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
    });

    // Setup event handlers for form changes
    function setupEventHandlers() {
      $('#host, #port, #database').on('change', function() {
        syncEditor();
      });

      $('#driverClass, #connectionUrl, #username, #password, #authMode').on('change', function() {
        syncEditor();
      });

      $('#caseInsensitive, #writable').on('change', function() {
        syncEditor();
      });

      $('#writerBatchSize').on('change', function() {
        syncEditor();
      });
    }
  </script>
</#macro>

<@page_html/>
