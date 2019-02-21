/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership. The ASF licenses this file to
 *  You under the Apache License, Version 2.0 (the "License"); you may not use
 *  this file except in compliance with the License. You may obtain a copy of
 *  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
 *  by applicable law or agreed to in writing, software distributed under the
 *  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 *  OF ANY KIND, either express or implied. See the License for the specific
 *  language governing permissions and limitations under the License.
 */
var userName = null;
//Elements for Timer in LoadingModal
var elapsedTime = 0;
var delay = 1000; //msec
var timeTracker = null; //Handle for stopping watch
var userName = null;

//Show cancellation status
function popupAndWait() {
  elapsedTime=0; //Init
  $("#queryLoadingModal").modal("show");
  var stopWatchElem = $('#stopWatch'); //Get handle on time progress elem within Modal
  //Timer updating
  timeTracker = setInterval(function() {
    elapsedTime = elapsedTime + delay/1000;
    let time = elapsedTime;
    let minutes = Math.floor(time / 60);
    let seconds = time - minutes * 60;
    let prettyTime = ("0" + minutes).slice(-2)+':'+ ("0" + seconds).slice(-2);
    stopWatchElem.text('Elapsed Time : ' + prettyTime);
  }, delay);
}

//Close the cancellation status popup
function closePopup() {
  clearInterval(timeTracker);
  $("#queryLoadingModal").modal("hide");
}

// Wrap & Submit Query (invoked if impersonation is enabled to check for username)
function doSubmitQueryWithUserName() {
    userName = document.getElementById("userName").value;
    if (!userName.trim()) {
        alert("Please fill in User Name field");
        return;
    }
    //Wrap and Submit query
    wrapAndSubmitQuery();
}

//Wrap & Submit Query (invoked directly if impersonation is not enabled)
function wrapAndSubmitQuery() {
    //Wrap if required
    wrapQuery();
    //Submit query
    submitQuery();
}

//Submit Query
function submitQuery() {
    popupAndWait();
    //Submit query
    $.ajax({
        type: "POST",
        beforeSend: function (request) {
            if (typeof userName !== 'undefined' && userName !== null && userName.length > 0) {
              request.setRequestHeader("User-Name", userName);
            }
        },
        url: "/query",
        data: $("#queryForm").serializeArray(),
        success: function (response) {
            closePopup();
            var newDoc = document.open("text/html", "replace");
            newDoc.write(response);
            newDoc.close();
        },
        error: function (request, textStatus, errorThrown) {
            closePopup();
            alert(errorThrown);
        }
    });
}

//Wraps a query with Limit by directly changing the query in the hidden textbox in the UI (see /query.ftl)
function wrapQuery() {
    var origQueryText = $('#query').attr('value');
    //dBug: console.log("Query Input:" + origQueryText);
    var mustWrapWithLimit = $('input[name="forceLimit"]:checked').length > 0;
    if (mustWrapWithLimit) {
        var semicolonIdx = origQueryText.lastIndexOf(';');
        //Check and eliminate trailing semicolon
        if (semicolonIdx  == origQueryText.length-1 ) {
          origQueryText = origQueryText.substring(0, semicolonIdx)
        }
        var qLimit = $('#queryLimit').val();
        var wrappedQuery = "-- [autoLimit: " + qLimit + " rows]\nselect * from (\n" + origQueryText + "\n) limit " + qLimit;
        //dBug: console.log("Query Output:" + wrappedQuery);
        //Wrapping Query
        $('#query').attr('value', wrappedQuery);
    } else {
        //Do not change the query
        //dBug: console.log("Query Output:" + origQueryText);
    }
}
