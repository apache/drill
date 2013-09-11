/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Drill frontend settings
var MAX_PROJECTS = 100;
var MAX_DATASOURCES = 100;
var DRILL_BACKEND_URL = 'http://localhost:6996';

// don't touch the following - internal settings
var drillstorage = window.localStorage;
var backendURL = DRILL_BACKEND_URL;
var projectnum = 0;

$(function(){
	initForms();
	
	// handling of configuration
	$('#config, #menu-config').click(function(){
		$('#config-dialog').modal('toggle');
		return false;
	});
	$('#config-form').submit(function() {
		return saveConfiguration();
	});
	$('#config-drill-reset').click(function(){
		var dconfig = { backendURL: DRILL_BACKEND_URL };
		backendURL = DRILL_BACKEND_URL;
		$('#config-drill-backend-url').val(backendURL);
		_store('config', dconfig);
		return false;
	});
	
	// handling of about
	$('#menu-about').click(function(){
		$('#about-dialog').modal('toggle');
		return false;
	});

	// handling of projects
	$('#project-create').click(function(){
		$('#project-title').val('');
		$('#current-project').text('NONE');
		$('#project-create-form').fadeIn('slow');
		return false;
	});
	$('#project-create-cancel').click(function(){
		$('#project-create-form').hide();
		return false;
	});
	$('#project-create-form').submit(function() {
		var ptitle = $('#project-title').val();
		var project = { timestamp : new Date() , ptitle: ptitle };
		var apid;
		
		if (ptitle) {
			if($('#current-project').text() == 'NONE') apid = _store('project', project);
			else apid = _store('project', project, $('#current-project').text());
			$('#current-project').html(apid);
			$('#project-create-form').hide();
			listProjects();
			return true;
		}
		return false;
	});
	$('.project-entry .project-main').live('click', function(event){ // deal with project selection	
		var key = $(this).parent().attr('id'); // using @id of the selected project entry, with current element: div/div
		$('#current-project').html(key); // ... remember it globaly ...
		listProjects(); // ... and highlight in project list as the active one
		return false;
	});
	$('.project-entry .icon-trash').live('click', function(event){ // deal with project deletion
		var response = confirm('Are you sure you want to delete this project and all data sources within it? Note that this action can not be undone ...');
		var pid = $(this).parent().parent().parent().attr('id');  // using @id of the project entry, with current element: div/div/a/i
		var idx = 0;
		var ds;
		
		if (response) {
			// first, remove data sources in project, if they exist ...
			while(true){
				ds = _read('drill_ds_' + pid + '_' + idx);
				if (idx > MAX_DATASOURCES) {
					break;
				} 
				else {
					if(ds){
						_remove('drill_ds_' + pid + '_' + idx);
						resetTargetDataSource('drill_ds_' + pid + '_' + idx);
					}
					idx += 1;
				}
			}
			// ... and then remove the project itself ...
			_remove(pid);
			listProjects();
		}
	});
	$('.project-entry .icon-edit').live('click', function(event){ // deal with project rename
		var pid = $(this).parent().parent().parent().attr('id'); // using @id of the project entry, with current element: div/div/a/i
		var ptitle = _read(pid).ptitle;
		$('#current-project').text(pid);
		$('#project-title').val(ptitle);
		$('#project-create-form').fadeIn('slow');
		listProjects();
		return false;
	});
	$('#project-help').click(function(){
		$('#project-help-alert').fadeIn('slow');
		return false;
	});

	// handling of data sources
	$('.project-entry .add-ds').live('click', function(event){ // deal with data source addition to project
		var key = $(this).parent().parent().attr('id'); // using @id of the selected project entry, with current element: div/div/button
		$('#current-project').html(key); // ... to remember it globaly ...
		$('#datasource-id').val('');
		$('#datasource-add-form').fadeIn('slow');
		return false;
    });
    $('#datasource-add-cancel').click(function(){
		$('#datasource-add-form').hide();
		return false;
    });
	$('#datasource-add-form').submit(function() {
		var pid = $('#current-project').text();
		var dsid = $('#datasource-id').val();
		var ds = { timestamp : new Date(), pid: pid, dsid: dsid };
		
		if (dsid) {
			_store('ds', ds);
			$('#datasource-add-form').hide();
			listProjects();
			return true;
		}
		return false;
	});
	$('.project-entry .project-main .datasource-entry').live('click', function(event){ // deal with data source selection
		var key = $('.dsid', this).attr('id'); // using @id of the child span that has a dsid class on it ...
		setTargetDataSource(key); // ... to remember it globaly ...
		listProjects(); // ... and to highlight in project list as the active one
		return false;
	});
	
	// execute a query - either via button click or by hitting ENTER in the text area
	$('#drill-query-execute').click(function(){
		executeQuery();
		return false;
	});
	$('#drill-query').keypress(function(e) {
		if (e.keyCode == 13 && e.shiftKey) {
			e.preventDefault();
			executeQuery();
			return false;
		}
	});
});

// init all forms (about, config, project list, etc.)
function initForms() {
	var dconfig = _read('drill_config');
	var apid = $('#current-project').text();
	var tmp;
	
	//TODO: store last selected target data source
	if(dconfig) {
		backendURL = dconfig.backendURL;
		$('#config-drill-backend-url').val(backendURL);
		console.log('CONFIG: using backend ' + backendURL);
		if(dconfig.targetds) {
			$('#current-ds').text(dconfig.targetds); // something like 'drill_ds_drill_project_3_1'
			tmp = dconfig.targetds.split('_');
			apid = tmp[2] + '_' + tmp[3] + '_' + tmp[4]; // extraxt from targetds: 'drill_ds_drill_project_3_1' -> 'drill_project_3'
			$('#current-project').html(apid);
			console.log('CONFIG: active project is [' + apid + '], target data source is [' + dconfig.targetds + ']');
		}
	}
	else {
		$('#config-drill-backend-url').val(backendURL);
	}
	listProjects();
	console.log('CONFIG: all projects ready.');
	
	// UI setup:
	$('#tutorial').popover({
		title : 'Apache Drill Tutorial',
		html: true, 
		content : '<p><small>A screen cast about how to use the front-end will be available on 15 Oct 2012.</small></p><p><small>For now, configure the back-end with <code>http://srvgal85.deri.ie/apache-drill</code> and create a project that contains a data source with the identifier <code>apache_drill</code>.</small></p>', 
		placement : 'bottom'
	});

	$('#config-dialog').modal({
		backdrop: true,
		keyboard: true,
		show: false
	});
	
	$('#about-dialog').modal({
		backdrop: true,
		keyboard: true,
		show: false
	});
	
	$('#help-dialog').modal({
		backdrop: false,
		keyboard: true,
		show: false
	});
}

// persists all user and system settings
function saveConfiguration(){
	var bURL = $('#config-drill-backend-url').val();
	var targetds = $('#current-ds').text();
	var dconfig = { backendURL: bURL, targetds : targetds };
	
	if (bURL && targetds) {
		backendURL = bURL;
		_store('config', dconfig);
		return true;
	}
	return false;
}

// sets and automatically saves target data source
function setTargetDataSource(dsid){
	$('#current-ds').text(dsid);
	return saveConfiguration();
}

// removes stored taarget data source if match
function resetTargetDataSource(dsid){
	var targetds = $('#current-ds').text();
	
	if (targetds == dsid) {
		$('#current-ds').text('NONE');
		return saveConfiguration();
	}
	else return false;
}

// executes the query against a Dummy Drill back-end
function executeQuery(){
	var drillquery = $('#drill-query').val();
	var seldsid = $('#current-ds').text();
	var ds = _read(seldsid);
	
	if(drillquery){
		if(ds){
			$('#drill-results-meta').html('');
			$('#drill-results').html('');
			$.ajax({
				type: "GET",
				url: backendURL +'/q/' + ds.dsid + '/' + drillquery,
				dataType : "json",
				success: function(data){
					if(data) {
						$('#drill-results-meta').html('<p class="text-info lead">Number of results: <strong>' + data.length + '</strong></p>');
						$('#drill-results').renderJSON(data);
					}
				},
				error:  function(msg){
					$('#drill-results-meta').html('');
					$('#drill-results').html('<div class="alert alert-error"><button type="button" class="close" data-dismiss="alert">×</button><h4>Something went wrong. Might check your configuration, check if the data source exists and make sure your query is properly formatted.</h4><div style="margin: 20px"><pre>' + JSON.stringify(msg) + '</pre></div></div>');
				} 
			});
		}
		else {
			alert("Can't execute the query: please select a data source to execute the query against.");
		}
	}
	else {
		alert("Can't execute the query: please provided a query value, try for example 'name:jane' ...");
	}
}


/////////////////////////////////////////////////////
// low-level storage API using localStorage 
// check http://caniuse.com/#feat=namevalue-storage
// if your browser supports it

// lists all projects, the selected project and data source
function listProjects(){
	var i = 0; // project pointer
	var j = 0; // data source pointer
	var buf = '';
	var selpid = $('#current-project').text();
	var seldsid = $('#current-ds').text();
	
	$('#project-list').html('');
	
	while(true){
		var pid = 'drill_project_' + i;
		var project = _read(pid);
		
		if (i > MAX_PROJECTS) return; // respect limit
		
		if(project) {
			buf = '';
			
			if(selpid && (pid == selpid)) { // highligt selected project
				buf = '<div class="active project-entry" ';
				console.log('The active project is: ' + selpid);
			}
			else {
				buf = '<div class="project-entry" ';
			}
			buf += 'id="' + pid + '">';
			buf += '<div style="text-align: right"><button class="btn btn-small btn-primary add-ds" type="button" title="Add a new data source to this project ..."><i class="icon-plus icon-white"></i> Add Data Source</button> ';
			buf += '<a class="btn btn-small" href="#" title="Edit project ..."><i class="icon-edit"></i></a> ';
			buf += '<a class="btn btn-small" href="#" title="Delete project ..."><i class="icon-trash"></i></a></div>';
			buf += '<div class="project-main"><h4>' + project.ptitle + '</h4>';
			buf += '<p class="project-meta muted">Last update: ' +  formatDateHuman(project.timestamp) + '</p>';
			if(selpid && (pid == selpid)) { // show data sources of selected project
				buf += '<div class="datasource-entry-container"><h5>Data sources:</h5>';
				while(true){
					var dsid = 'drill_ds_drill_project_' + i + '_' + j;
					var ds = _read(dsid);
					
					if (j > MAX_DATASOURCES) break; // respect limit
					
					if(ds){
						if(seldsid && (dsid == seldsid)) { // highligt selected data source
							buf += '<div class="target datasource-entry btn-success"><i class="icon-file icon-white"></i> <span class="dsid" id="' + dsid +'">' + ds.dsid +'</span></div>';
							console.log('The target data source for the query is: ' + seldsid);
						}
						else {
							buf += '<div class="datasource-entry"><i class="icon-file"></i> <span class="dsid" id="' + dsid +'">' + ds.dsid +'</span></div>';
						}
					}
					j += 1;
				}
				if(_find_latest_ds_in(pid) == 0) buf += '<div class="alert alert-info">No data sources added so far! Use the "Add Data Source" button above to add some ...</div></div>';
				else buf += '</div>';
			}
			else {
				buf += '<p class="project-meta muted">Data sources: ' +  _find_latest_ds_in(pid) + '</p>';
			}
			buf += '</div></div>';
			$('#project-list').append(buf);
			projectnum = i;
		}
		i += 1;
	}
}

// takes a string in ISO8601 (http://en.wikipedia.org/wiki/ISO_8601) format such as `2012-10-14T12:48:28.478Z` and turns it in something human readable
function formatDateHuman(iso8601datetime) {
	var now = new Date(Date.now());
	var today = now.toISOString().split('T')[0];
	var date = iso8601datetime.split('T')[0]; // 2012-10-14T12:48:28.478Z -> 2012-10-14
	var time = iso8601datetime.split('T')[1]; // 2012-10-14T12:48:28.478Z -> 12:48:28.478Z
	var hms = time.split('.')[0]; // 12:48:28
	
	if(date == today) return 'Today at ' + hms + ' UTC';
	else return date + ' at ' + hms + ' UTC';
}

// creates/stores an entry under the category; if key is provided, entry is updated
function _store(category, entry, ekey) {
	var key = 'drill_';
	var project;
	
	if(category == 'config') {
		key += 'config';
	}
	else {
		if(category == 'project') {
			if(ekey){
				key = ekey;
			}
			else{
				projectnum += 1;
				if (projectnum > MAX_PROJECTS) {
					alert('Maximum number of projects reached!');
					return;
				}
				key += 'project_' + projectnum;
			}
		}
		else {
			if(category == 'ds') {
				key += 'ds_' + entry.pid + '_' + (_find_latest_ds_in(entry.pid) + 1);
			}
			else return; // can only store known entry categories
		}
	}
	drillstorage.setItem(key, JSON.stringify(entry));
	return key;
}

function _find_latest_ds_in(pid){
	var idx = 0;
	var ds;
	var last_idx = idx;
	
	while(true){
		ds = _read('drill_ds_' + pid + '_' + idx);
		
		if (idx > MAX_DATASOURCES) {
			return last_idx;
		} 
		else {
			if(ds){
				last_idx = idx;
			}
			idx += 1;
		}
	}
}

function _remove(key){
	drillstorage.removeItem(key);
}

function _read(key){
	return JSON.parse(drillstorage.getItem(key));
}


///////////////////
// utility methods

// http://stackoverflow.com/questions/2573521/how-do-i-output-an-iso-8601-formatted-string-in-javascript
if (!Date.prototype.toISOString) {
	Date.prototype.toISOString = function() {
		function pad(n) { return n < 10 ? '0' + n : n }
		return	this.getUTCFullYear() + '-'
				+ pad(this.getUTCMonth() + 1) + '-'
				+ pad(this.getUTCDate()) + 'T'
				+ pad(this.getUTCHours()) + ':'
				+ pad(this.getUTCMinutes()) + ':'
				+ pad(this.getUTCSeconds()) + 'Z';
	};
}
