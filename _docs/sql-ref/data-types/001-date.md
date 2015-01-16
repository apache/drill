---
title: "Supported Date/Time Data Type Formats"
parent: "Data Types"
---
You must use supported `date` and `time` formats when you `SELECT` date and
time literals or when you `CAST()` from `VARCHAR `to `date` and `time` data
types. Apache Drill currently supports specific formats for the following
`date` and `time` data types:

  * Date
  * Timestamp
  * Time
  * Interval
    * Interval Year
    * Interval Day
  * Literal

The following query provides an example of how to `SELECT` a few of the
supported date and time formats as literals:

    select date '2008-2-23', timestamp '2008-1-23 14:24:23', time '10:20:30' from dfs.`/tmp/input.json`;

The following query provides an example where `VARCHAR` data in a file is
`CAST()` to supported `date `and `time` formats:

    select cast(col_A as date), cast(col_B as timestamp), cast(col_C as time) from dfs.`/tmp/dates.json`;

`Date`, t`imestamp`, and` time` data types store values in `UTC`. Currently,
Apache Drill does not support `timestamp` with time zone.

## Date

Drill supports the `date` data type in the following format:

    YYYY-MM-DD (year-month-date)

The following table provides some examples for the `date` data type:

  | Use | Example |
  | --- | ------- |
  |Literal| `select date ‘2008-2-23’ from dfs.`/tmp/input.json`;`|
  |`JSON` input | `{"date_col" : "2008-2-23"} 
  | `CAST` from `VARCHAR`| `` select CAST(date_col as date) as CAST_DATE from dfs.`/tmp/input.json`; ``|

## Timestamp

Drill supports the `timestamp` data type in the following format:

    yyyy-MM-dd HH:mm:ss.SSS (year-month-date hour:minute:sec.milliseconds)

The following table provides some examples for the `timestamp` data type:

<table>
 <tbody>
  <tr>
   <th>Use</th>
   <th>CAST Example</th>
  </tr>
  <tr>
   <td valign="top">Literal</td>
   <td valign="top"><code><span style="color: rgb(0,0,0);">select timestamp ‘2008-2-23 10:20:30.345’, timestamp ‘2008-2-23 10:20:30’ from dfs.`/tmp/input.json`;</span></code>
   </td></tr>
  <tr>
   <td colspan="1" valign="top"><code>JSON</code> Input</td>
   <td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">{“timestamp_col”: “2008-2-23 15:20:30.345”}<br /></span><span style="color: rgb(0,0,0);">{“timestamp_col”: “2008-2-23 10:20:30”}</span></code><span style="color: rgb(0,0,0);">The fractional millisecond component is optional.</span></td>
   </tr>
   <tr>
    <td colspan="1" valign="top"><code>CAST</code> from <code>VARCHAR</code></td>
    <td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">select cast(timestamp_col as timestamp) from dfs.`/tmp/input.json`; </span></code></td>
   </tr>
  </tbody>
 </table>

## Time

Drill supports the `time` data type in the following format:

    HH:mm:ss.SSS (hour:minute:sec.milliseconds)

The following table provides some examples for the `time` data type:

<table><tbody><tr>
  <th>Use</th>
  <th>Example</th>
  </tr>
  <tr>
   <td valign="top">Literal</td>
   <td valign="top"><code><span style="color: rgb(0,0,0);">select time ‘15:20:30’, time ‘10:20:30.123’ from dfs.`/tmp/input.json`;</span></code></td>
  </tr>
  <tr>
  <td colspan="1" valign="top"><code>JSON</code> Input</td>
  <td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">{“time_col” : “10:20:30.999”}<br /></span><span style="color: rgb(0,0,0);">{“time_col”: “10:20:30”}</span></code></td>
 </tr>
 <tr>
  <td colspan="1" valign="top"><code>CAST</code> from <code>VARCHAR</code></td>
  <td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">select cast(time_col as time) from dfs.`/tmp/input.json`;</span></code></td>
</tr></tbody>
</table>

## Interval

Drill supports the `interval year` and `interval day` data types.

### Interval Year

The `interval year` data type stores time duration in years and months. Drill
supports the `interval` data type in the following format:

    P [qty] Y [qty] M

The following table provides examples for `interval year` data type:

<table ><tbody><tr>
<th>Use</th>
<th>Example</th></tr>
  <tr>
    <td valign="top">Literals</td>
    <td valign="top"><code><span style="color: rgb(0,0,0);">select interval ‘1-2’ year to month from dfs.`/tmp/input.json`;<br /></span><span style="color: rgb(0,0,0);">select interval ‘1’ year from dfs.`/tmp/input.json`;<br /></span><span style="color: rgb(0,0,0);">select interval '13’ month from dfs.`/tmp/input.json`;</span></code></td></tr><tr>
    <td colspan="1" valign="top"><code>JSON</code> Input</td>
    <td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">{“col” : “P1Y2M”}<br /></span><span style="color: rgb(0,0,0);">{“col” : “P-1Y2M”}<br /></span><span style="color: rgb(0,0,0);">{“col” : “P-1Y-2M”}<br /></span><span style="color: rgb(0,0,0);">{“col”: “P10M”}<br /></span><span style="color: rgb(0,0,0);">{“col”: “P5Y”}</span></code></td>
  </tr>
  <tr>
    <td colspan="1" valign="top"><code>CAST</code> from <code>VARCHAR</code></td>
    <td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">select cast(col as interval year) from dfs.`/tmp/input.json`;</span></code></td>
  </tr>
  </tbody></table> 

### Interval Day

The `interval day` data type stores time duration in days, hours, minutes, and
seconds. You do not need to specify all fields in a given interval. Drill
supports the `interval day` data type in the following format:

    P [qty] D T [qty] H [qty] M [qty] S

The following table provides examples for `interval day` data type:

<table ><tbody><tr><th >Use</th><th >Example</th></tr><tr><td valign="top">Literal</td><td valign="top"><code><span style="color: rgb(0,0,0);">select interval '1 10:20:30.123' day to second from dfs.`/tmp/input.json`;<br /></span><span style="color: rgb(0,0,0);">select interval '1 10' day to hour from dfs.`/tmp/input.json`;<br /></span><span style="color: rgb(0,0,0);">select interval '10' day  from dfs.`/tmp/input.json`;<br /></span><span style="color: rgb(0,0,0);">select interval '10' hour  from dfs.`/tmp/input.json`;</span></code><code><span style="color: rgb(0,0,0);">select interval '10.999' second  from dfs.`/tmp/input.json`;</span></code></td></tr><tr><td colspan="1" valign="top"><code>JSON</code> Input</td><td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">{&quot;col&quot; : &quot;P1DT10H20M30S&quot;}<br /></span><span style="color: rgb(0,0,0);">{&quot;col&quot; : &quot;P1DT10H20M30.123S&quot;}<br /></span><span style="color: rgb(0,0,0);">{&quot;col&quot; : &quot;P1D&quot;}<br /></span><span style="color: rgb(0,0,0);">{&quot;col&quot; : &quot;PT10H&quot;}<br /></span><span style="color: rgb(0,0,0);">{&quot;col&quot; : &quot;PT10.10S&quot;}<br /></span><span style="color: rgb(0,0,0);">{&quot;col&quot; : &quot;PT20S&quot;}<br /></span><span style="color: rgb(0,0,0);">{&quot;col&quot; : &quot;PT10H10S&quot;}</span></code></td></tr><tr><td colspan="1" valign="top"><code>CAST</code> from <code>VARCHAR</code></td><td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">select cast(col as interval day) from dfs.`/tmp/input.json`;</span></code></td></tr></tbody></table> 

## Literal

The following table provides a list of `date/time` literals that Drill
supports with examples of each:

<table ><tbody><tr><th >Format</th><th colspan="1" >Interpretation</th><th >Example</th></tr><tr><td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">interval '1 10:20:30.123' day to second</span></code></td><td colspan="1" valign="top"><code>1 day, 10 hours, 20 minutes, 30 seconds, and 123 thousandths of a second</code></td><td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">select interval '1 10:20:30.123' day to second from dfs.`/tmp/input.json`;</span></code></td></tr><tr><td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">interval '1 10' day to hour</span></code></td><td colspan="1" valign="top"><code>1 day 10 hours</code></td><td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">select interval '1 10' day to hour from dfs.`/tmp/input.json`;</span></code></td></tr><tr><td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">interval '10' day</span></code></td><td colspan="1" valign="top"><code>10 days</code></td><td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">select interval '10' day from dfs.`/tmp/input.json`;</span></code></td></tr><tr><td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">interval '10' hour</span></code></td><td colspan="1" valign="top"><code>10 hours</code></td><td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">select interval '10' hour from dfs.`/tmp/input.json`;</span></code></td></tr><tr><td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">interval '10.999' second</span></code></td><td colspan="1" valign="top"><code>10.999 seconds</code></td><td colspan="1" valign="top"><code><span style="color: rgb(0,0,0);">select interval '10.999' second from dfs.`/tmp/input.json`; </span></code></td></tr></tbody></table>



