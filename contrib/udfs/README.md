# Drill User Defined Functions

This `README` documents functions which users have submitted to Apache Drill.  

## Geospatial Functions

<p>As of version 1.14, Drill contains a suite of<a contenteditable="false" data-primary="geographic information systems (GIS) functionality" data-secondary="reference of geo-spatial functions in Drill" data-type="indexterm">&nbsp;</a> geographic information system (GIS) functions. Most of the functionality follows that of PostGIS.<a contenteditable="false" data-primary="Well-Known Text (WKT) representation format for spatial data" data-type="indexterm">&nbsp;</a> To use these functions, your spatial data must be defined in the <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-Known Text (WKT) representation format</a>.<a contenteditable="false" data-primary="ST_AsText function" data-type="indexterm">&nbsp;</a><a contenteditable="false" data-primary="ST_GeoFromText function" data-type="indexterm">&nbsp;</a> The WKT format allows you to represent points, lines, polygons, and other geometric shapes. Following are two example WKT strings:</p>

<pre data-type="programlisting">
POINT (30 10)
POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))</pre>

<p>Drill stores points as binary, so to read or plot these points, you need to convert them using the <code>ST_AsText()</code> and<code> ST_GeoFromText()</code> functions. <a data-type="xref" href="https://github.com/apache/drill/tree/master/contrib/udfs#table0a05">#table0a05</a> lists the GIS functions.</p>

<table id="table0a05">
	<caption>Drill GIS functions</caption>
	<thead>
		<tr>
			<th width="45%">Function</th>
			<th>Output</th>
			<th>Description</th>
		</tr>
	</thead>
	<tbody>
		<tr>
			<td><code>ST_AsGeoJSON(<em>geometry</em>)</code></td>
			<td><code>VARCHAR</code></td>
			<td>Returns the geometry as a GeoJSON element.</td>
		</tr>
		<tr>
			<td><code>ST_AsJSON(<em>geometry</em>)</code></td>
			<td><code>VARCHAR</code></td>
			<td>Returns JSON representation of the geometry.</td>
		</tr>
		<tr>
			<td><code>ST_AsText(<em>geometry</em>)</code></td>
			<td><code>VARCHAR</code></td>
			<td>Return a the WKT representation of the geometry/geography without SRID metadata</td>
		</tr>
		<tr>
			<td><code>ST_Buffer(<em>geometry</em>, <em>radius</em>)</code></td>
			<td><code>GEOMETRY</code></td>
			<td>Returns a geometry that represents all points whose distance from this geometry is less than or equal to <em><code>radius</code></em>.</td>
		</tr>
		<tr>
			<td><code>ST_Contains(<em>geometry_a</em>, <em>geometry_b</em>)</code></td>
			<td><code>BOOLEAN</code></td>
			<td>Returns <code>true</code> if no points of <em><code>geometry_b</code></em> lie outside of <em><code>geometry_a</code></em> and at least one point of the interior of <em><code>geometry_b</code></em> is in the interior of <em><code>geometry_a</code></em>.</td>
		</tr>
		<tr>
			<td><code>ST_Crosses(<em>geometry_a</em>, <em>geometry_b</em>)</code></td>
			<td><code>BOOLEAN</code></td>
			<td>Returns <code>true</code> if the supplied geometries have some but not all interior points in common.</td>
		</tr>
		<tr>
			<td><code>ST_Difference(<em>geometry_a</em>, <em>geometry_b</em>)</code></td>
			<td><code>GEOMETRY</code></td>
			<td>Returns a geometry that represents the part of <em><code>geometry_a</code></em> that does not intersect with <em><code>geometry_b</code></em>.</td>
		</tr>
		<tr>
			<td><code>ST_Disjoint(<em>geometry_a</em>, <em>geometry_b</em>)</code></td>
			<td><code>BOOLEAN</code></td>
			<td>Returns <code>true</code> if the two geometries do not spatially intersect.</td>
		</tr>
		<tr>
			<td><code>ST_Distance(<em>geometry_a</em>, <em>geometry_b</em>)</code></td>
			<td><code>DOUBLE</code></td>
			<td>For geometry types, returns the 2D Cartesian distance between two geometries in projected units. For geography types, defaults to returning the minimum geodesic distance between two geographies in meters.</td>
		</tr>
		<tr>
			<td><code>ST_DWithin(<em>geometry_a</em>, <em>geometry_b</em>,<br />
			<em>distance</em>)</code></td>
			<td><code>BOOLEAN</code></td>
			<td>Returns <code>true</code> if the geometries are within the specified distance of one another.</td>
		</tr>
		<tr>
			<td><code>ST_Envelope(<em>geometry</em>)</code></td>
			<td><code>GEOMETRY</code></td>
			<td>Returns a geometry representing the double-precision bounding box of the supplied geometry. The polygon is defined by the corner points of the bounding box: <code>((MINX, MINY), (MINX, MAXY), (MAXX, MAXY), (MAXX, MINY), (MINX, MINY))</code>.</td>
		</tr>
		<tr>
			<td><code>ST_GeoFromText(<em>text</em>,[<em>SRID</em>])</code></td>
			<td><code>GEOMETRY</code></td>
			<td>Returns a specified <code>ST_Geometry</code> value from the WKT representation. If the spatial reference ID (SRID) is included as the second argument the function returns a geometry that includes this SRID as part of its metadata.</td>
		</tr>
		<tr>
			<td><code>ST_Equals(<em>geometry_a</em>, <em>geometry_b</em>)</code></td>
			<td><code>BOOLEAN</code></td>
			<td>Returns <code>true</code> if the given geometries represent the same geometry. Directionality is ignored.</td>
		</tr>
		<tr>
			<td><code>ST_Intersects(<em>geometry_a</em>, <em>geometry_b</em>)</code></td>
			<td><code>BOOLEAN</code></td>
			<td>Returns <code>true</code> if the geometries/geographies "spatially intersect in 2D" (share any portion of space) and <code>false</code> if they don't (they are disjoint).</td>
		</tr>
		<tr>
			<td><code>ST_Overlaps(<em>geometry_a</em>, <em>geometry_b</em>)</code></td>
			<td><code>BOOLEAN</code></td>
			<td>Returns <code>true</code> if the geometries share space and are of the same dimension, but are not completely contained by each other.</td>
		</tr>
		<tr>
			<td><code>ST_Point(<em>long</em>, <em>lat</em>)</code></td>
			<td><code>GEOMETRY</code></td>
			<td>Returns an <code>ST_Point</code> with the given coordinate values.</td>
		</tr>
		<tr>
			<td><code>ST_Relate(<em>geometry_a</em>, <em>geometry_b</em><br />
			[,intersectionMatrixPattern])</code></td>
			<td><code>BOOLEAN</code></td>
			<td>Returns <code>true</code> if <em><code>geometry_a</code></em> is spatially related to <em><code>geometry_b</code></em>, determined by testing for intersections between the interior, boundary, and exterior of the two geometries as specified by the values in the intersection matrix pattern. If no intersection matrix pattern is passed in, returns the maximum intersection matrix pattern that relates the two geometries.</td>
		</tr>
		<tr>
			<td><code>ST_Touches(<em>geometry_a</em>, <em>geometry_b</em>)</code></td>
			<td><code>BOOLEAN</code></td>
			<td>Returns <code>true</code> if the geometries have at least one point in common, but their interiors do not intersect.</td>
		</tr>
		<tr>
			<td><code>ST_Transform(<em>geometry_a</em> [<em>Source_SRID</em>, <em>Target_SRID</em>])</code></td>
			<td><code>GEOMETRY</code></td>
			<td>Returns a new geometry with its coordinates transformed to a different spatial reference.</td>
		</tr>
		<tr>
			<td><code>ST_Union(<em>geometry_a</em>, <em>geometry_b</em>)</code></td>
			<td><code>GEOMETRY</code></td>
			<td>Returns a geometry that represents the point set union of the geometries.</td>
		</tr>
		<tr>
			<td><code>ST_Union_Aggregate(<em>Geometry</em>)</code></td>
			<td><code>GEOMETRY</code></td>
			<td>Returns a geometry that represents the point set union of the geometries. Note: This function is an aggregate function and should be used with <code>GROUP BY</code>.</td>
		</tr>
		<tr>
			<td><code>ST_Within(<em>geometry_a</em>, <em>geometry_b</em>)</code></td>
			<td><code>BOOLEAN</code></td>
			<td>Returns <code>true</code> if <em><code>geometry_a</code></em> is completely inside <em><code>geometry_b</code></em>.</td>
		</tr>
		<tr>
			<td><code>ST_X(<em>geometry</em>)</code></td>
			<td><code>DOUBLE</code></td>
			<td>Returns the <em>x</em> coordinate of the point, or NaN if not available.</td>
		</tr>
		<tr>
			<td><code>ST_XMax(<em>geometry</em>)</code></td>
			<td><code>DOUBLE</code></td>
			<td>Returns the <em>x</em> maxima of a 2D or 3D bounding box or a geometry.</td>
		</tr>
		<tr>
			<td><code>ST_XMin(<em>geometry</em>)</code></td>
			<td><code>DOUBLE</code></td>
			<td>Returns the <em>x</em> minima of a 2D or 3D bounding box or a geometry.</td>
		</tr>
		<tr>
			<td><code>ST_Y(<em>geometry</em>)</code></td>
			<td><code>DOUBLE</code></td>
			<td>Return the <em>y</em> coordinate of the point, or NaN if not available.</td>
		</tr>
		<tr>
			<td><code>ST_YMax(<em>geometry</em>)</code></td>
			<td><code>DOUBLE</code></td>
			<td>Returns the <em>y</em> maxima of a 2D or 3D bounding box or a geometry.</td>
		</tr>
		<tr>
			<td><code>ST_YMin(<em>geometry</em>)</code></td>
			<td><code>DOUBLE</code></td>
			<td>Returns the <em>y</em> minima of a 2D or 3D bounding box or a geometry.</td>
		</tr>
	</tbody>
</table>

## User Agent Functions
Drill UDF for parsing User Agent Strings.
This function is based on Niels Basjes Java library for parsing user agent strings which is available here: <https://github.com/nielsbasjes/yauaa>.

### Usage
The function `parse_user_agent()` takes a user agent string as an argument and returns a map of the available fields. Note that not every field will be present in every user agent string. 
```
SELECT parse_user_agent( columns[0] ) as ua 
FROM dfs.`/tmp/data/drill-httpd/ua.csv`;
```
The query above returns:
```
{
  "DeviceClass":"Desktop",
  "DeviceName":"Macintosh",
  "DeviceBrand":"Apple",
  "OperatingSystemClass":"Desktop",
  "OperatingSystemName":"Mac OS X",
  "OperatingSystemVersion":"10.10.1",
  "OperatingSystemNameVersion":"Mac OS X 10.10.1",
  "LayoutEngineClass":"Browser",
  "LayoutEngineName":"Blink",
  "LayoutEngineVersion":"39.0",
  "LayoutEngineVersionMajor":"39",
  "LayoutEngineNameVersion":"Blink 39.0",
  "LayoutEngineNameVersionMajor":"Blink 39",
  "AgentClass":"Browser",
  "AgentName":"Chrome",
  "AgentVersion":"39.0.2171.99",
  "AgentVersionMajor":"39",
  "AgentNameVersion":"Chrome 39.0.2171.99",
  "AgentNameVersionMajor":"Chrome 39",
  "DeviceCpu":"Intel"
}
```
The function returns a Drill map, so you can access any of the fields using Drill's table.map.key notation. For example, the query below illustrates how to extract a field from this map and summarize it:

```
SELECT uadata.ua.AgentNameVersion AS Browser,
COUNT( * ) AS BrowserCount
FROM (
   SELECT parse_user_agent( columns[0] ) AS ua
   FROM dfs.drillworkshop.`user-agents.csv`
) AS uadata
GROUP BY uadata.ua.AgentNameVersion
ORDER BY BrowserCount DESC
```
The function can also be called with an optional field as an argument. IE:
```
SELECT parse_user_agent( `user_agent`, 'AgentName` ) as AgentName ...
```
which will just return the requested field. If the user agent string is empty, all fields will have the value of `Hacker`.  
