---
title: "Data Types"
parent: "SQL Reference"
---
You can use the following SQL data types in your Drill queries:


#### Character

  * VARCHAR/CHAR 

#### Date/Time

  * DATE
  * INTERVAL
    * Interval Year (stores year and month)
    * Interval Day (stores day, hour, minute, seconds, and milliseconds)
  * TIME
  * TIMESTAMP

Refer to [Supported Date/Time Data Type formats](/drill/docs/supported-date-time-data-type-formats/).

#### Integer

  * BIGINT
  * INT
  * SMALLINT

#### Numeric

  * DECIMAL
  * FLOAT 
  * DOUBLE PRECISION (FLOAT 8)
  * REAL (FLOAT 4) 

#### Boolean

Values are FALSE or TRUE.

## Complex Data Types

Drill provides map and array data types to work with complex and nested data
structures. For analysis of complex data, a more modern JSON-style approach to
writing queries is more effective than using standard SQL functions.

The following table provides descriptions and examples of the complex data
types:

<table><tbody>
  <tr><th>Data Type</th>
  <th>Description</th>
  <th>Example</th></tr>
    <tr>
      <td valign="top">Map</td>
      <td valign="top">A map is a set of name/value pairs. </br>
      A value in an map can be a scalar type, </br>
      such as string or int, or a map can be a </br>
      complex type, such as an array or another map.</td>
      <td valign="top">Map with scalar type values:</br><code>&nbsp;&nbsp;&quot;phoneNumber&quot;: { &quot;areaCode&quot;: &quot;622&quot;, &quot;number&quot;: &quot;1567845&quot;}</code></br>Map with complex type value:<code></br>&nbsp;&nbsp;{ &quot;citiesLived&quot; : [ { &quot;place&quot; : &quot;Los Angeles&quot;,</br>        
      &nbsp;&nbsp;&nbsp;&nbsp;&quot;yearsLived&quot; : [ &quot;1989&quot;,</br>
      &nbsp;&nbsp;&nbsp;&nbsp;            &quot;1993&quot;,</br>            
      &nbsp;&nbsp;&nbsp;&nbsp;&quot;1998&quot;,</br>            
      &nbsp;&nbsp;&nbsp;&nbsp;&quot;2002&quot;</br>
      &nbsp;&nbsp;&nbsp;&nbsp;          ]</br>      
      &nbsp;&nbsp;
      &nbsp;} ] }</code></td>
    </tr>
    <tr>
      <td valign="top">Array</td>
      <td valign="top">An array is a repeated list of values. </br>
      A value in an array can be a scalar type, </br>
      such as string or int, or an array can be a</br> 
      complex type, such as a map or another array.</td>
      <td valign="top">Array with scalar values:</br><code>&nbsp;&nbsp;&quot;yearsLived&quot;: [&quot;1990&quot;, &quot;1993&quot;, &quot;1998&quot;, &quot;2008&quot;]</code></br>Array with complex type values:</br><code>&nbsp;&nbsp;&quot;children&quot;:</br>&nbsp;&nbsp;[ { &quot;age&quot; : &quot;10&quot;, </br>   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;gender&quot; : &quot;Male&quot;,</br>    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;name&quot; : &quot;Earl&quot;</br> &nbsp;&nbsp;&nbsp;&nbsp; }, </br> &nbsp;&nbsp;&nbsp;&nbsp;{ &quot;age&quot; : &quot;6&quot;,</br>    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;gender&quot; : &quot;Male&quot;,</br>    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;name&quot; : &quot;Sam&quot;</br>  &nbsp;&nbsp;&nbsp;&nbsp;},</br>  &nbsp;&nbsp;&nbsp;&nbsp;{ &quot;age&quot; : &quot;8&quot;,</br>    &nbsp;&nbsp;&nbsp;&nbsp;&quot;gender&quot; : &quot;Male&quot;,  </br>  &nbsp;&nbsp;&nbsp;&nbsp;&quot;name&quot; : &quot;Kit&quot; </br> &nbsp;&nbsp;&nbsp;&nbsp;}</br>&nbsp;&nbsp;]</code></td>
    </tr>
  </tbody></table>

