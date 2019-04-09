---
title: "Syslog Format Plugin"
date: 2019-04-08
parent: "Connect a Data Source"
---

Starting in Drill 1.16, Drill provides a syslog format plugin, which enables Drill to query syslog formatted data as specified in RFC-5424, as shown:

	<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"][examplePriority@32473 class="high"]  

## Configuration Options
This syslog format plugin has the following configuration options:



- **maxErrors**  
Sets the maximum number of malformatted lines that the format plugin will tolerate before throwing an error and halting execution.  
- **flattenStructuredData**  
Syslog data optionally contains a series of key/value pairs known as the structured data. By default, Drill will parse these into a map.  

		"syslog": {
		   "type": "syslog",
		   "extensions": [ "syslog" ],
		   "maxErrors": 10,
		   "flattenStructuredData": false
		}  

## Fields  

In terms of data types, the `event_date` field is a datetime, the `severity_code`, `facility_code`, and `proc_id` are integers and all other fields are VARCHARs.

**Note:** All fields, with the exception of the `event_date` field, are not required; therefore, all fields may not be present at all times.

- **event_date**  
This is the time of the event  
- **severity_code**  
The severity code of the event  
- **facility_code**   
The facility code of the incident  
- **severity**  
The severity of the event  
- **facility**  
- **ip**  
The IP address or hostname of the source machine  
- **app_name**  
The name of the application that is generating the event  
- **proc_id**  
The process ID of the event that generated the event  
- **msg_id**  
The identifier of the message  
- **message**  
The actual message text of the event  
- **raw**  
The full text of the event  

## Structured Data  

Syslog data can contain a list of key/value pairs which Drill will extract in a field called `structured_data`. This field is a Drill map.