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
<style>
  .modalHeaderAlert, .closeX {
    color:red !important;
    text-align: center;
    font-size: 16px;
  }
</style>
  <!--
    Alert Modal to use across templates.
    By default, modal is hidden and expected to be populated and activated by relevant JavaScripts
  -->
  <div class="modal" id="errorModal" role="dialog" data-backdrop="static" data-keyboard="false" style="display: none;" aria-hidden="true">
    <div class="modal-dialog">
      <!-- Modal content-->
      <div class="modal-content">
        <div class="modal-header modalHeaderAlert">
          <button type="button" class="close closeX" data-dismiss="modal" style="color:red;font-size:200%">×</button>
          <h4 class="modal-title"><span class="glyphicon glyphicon-alert" style="font-size:125%"></span><span id="modalHeader" style="font-family:Helvetica Neue,Helvetica,Arial,sans-serif;white-space:pre">~ErrorMessage~ Title</span></h4>
        </div>
        <div class="modal-body" id="modalBody" style="line-height:3">
        ~ErrorMessage Details~
        </div>
        <div class="modal-footer">
          <button type="button" class="btn btn-info btn-default" data-dismiss="modal">Close</button>
        </div>
      </div>
    </div>
  </div>
<script>
    //Populate the alert modal with the right message params and show
    function populateAndShowAlert(errorMsg, inputValues) {
      if (!(errorMsg in errorMap)) {
        //Using default errorId to represent message
        modalHeader.innerHTML=errorMsg;
        modalBody.innerHTML="[Auto Description] "+JSON.stringify(inputValues);
      } else {
        modalHeader.innerHTML=errorMap[errorMsg].msgHeader;
        modalBody.innerHTML=errorMap[errorMsg].msgBody;
      }
      //Check if substitutions are needed
      let updatedHtml=modalBody.innerHTML;
      if (inputValues != null) {
        var inputValuesKeys = Object.keys(inputValues);
        for (i=0; i<inputValuesKeys.length; ++i) {
            let currKey=inputValuesKeys[i];
            updatedHtml=updatedHtml.replace(currKey, inputValues[currKey]);
        }
        modalBody.innerHTML=updatedHtml;
      }
      //Show Alert
      $('#errorModal').modal('show');
    }

    //Map of error messages to populate the alert modal
    var errorMap = {
        "userNameMissing": {
            msgHeader:"   ERROR: Username Needed",
            msgBody:"Please provide a user name. The field cannot be empty.<br>Username is required since impersonation is enabled"
        },
        "passwordMissing": {
            msgHeader:"   ERROR: Password Needed",
            msgBody:"Please provide a password. The field cannot be empty."
        },
        "invalidRowCount": {
            msgHeader:"   ERROR: Invalid RowCount",
            msgBody:"\"<span style='font-family:courier;white-space:pre'>_autoLimitValue_</span>\" is not a number. Please fill in a valid number.",
        },
        "invalidProfileFetchSize": {
            msgHeader:"   ERROR: Invalid Fetch Size",
            msgBody:"\"<span style='font-family:courier;white-space:pre'>_fetchSize_</span>\" is not a valid fetch size.<br>Please enter a valid number of profiles to fetch (1 to 100000)",
        },
        "invalidOptionValue": {
            msgHeader:"   ERROR: Invalid Option Value",
            msgBody:"\"<span style='font-family:courier;white-space:pre'>_numericOption_</span>\" is not a valid numeric value for <span style='font-family:courier;white-space:pre'>_optionName_</span><br>Please enter a valid number to update the option.",
        },
        "queryMissing": {
            msgHeader:"   ERROR: No Query to execute",
            msgBody:"Please provide a query. The query textbox cannot be empty."
        },
        "errorId": {
            msgHeader:"~header~",
            msgBody:"Description unavailable"
        }
    }
</script>