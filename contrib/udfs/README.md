# Drill User Defined Functions

This `README` documents functions which users have submitted to Apaceh Drill.  

## Protocol Lookup Functions
These functions provide a convenience lookup capability for port numbers. They will accept port numbers as either an int or string.

* `get_host_name(<ip address>)`: This function accepts an IP address and will return the host

* `get_service_name(<port number>, <protocol>)`:  This function returns the service name for a port and protocol combination.  
```
apache drill> select get_service_name(666, 'tcp') as service from (values(1));
+------------------+
|     service      |
+------------------+
| doom Id Software |
+------------------+
1 row selected (0.178 seconds)
```

* `get_short_service_name(<port number>, <protocol>)`: Same as above but returns a short protocol name. 

```
apache drill> select get_short_service_name(21, 'tcp') as service from (values(1));
   +---------+
   | service |
   +---------+
   | ftp     |
   +---------+
   1 row selected (0.112 seconds)
   ```
   
 ## GeoIP Functions for Apache Drill
 This is a collection of GeoIP functions for Apache Drill. These functions are a wrapper for the MaxMind GeoIP Database.
 
 IP Geo-Location is inherently imprecise and should never be relied on to get anything more than a general sense of where the traffic is coming from. 
 
 * **`getCountryName( <ip> )`**:  This function returns the country name of the IP address, "Unknown" if the IP is unknown or invalid.
 * **`getCountryConfidence( <ip> )`**:  This function returns the confidence score of the country ISO code of the IP address.
 * **`getCountryISOCode( <ip> )`**:  This function returns the country ISO code of the IP address, "Unknown" if the IP is unknown or invalid.
 * **`getCityName( <ip> )`**:  This function returns the city name of the IP address, "Unknown" if the IP is unknown or invalid.
 * **`getCityConfidence( <ip> )`**:  This function returns confidence score of the city name of the IP address.
 * **`getLatitude( <ip> )`**:  This function returns the latitude associated with the IP address.
 * **`getLongitude( <ip> )`**:  This function returns the longitude associated with the IP address.
 * **`getTimezone( <ip> )`**:  This function returns the timezone associated with the IP address.
 * **`getAccuracyRadius( <ip> )`**:  This function returns the accuracy radius associated with the IP address, 0 if unknown.
 * **`getAverageIncome( <ip> )`**:  This function returns the average income of the region associated with the IP address, 0 if unknown.
 * **`getMetroCode( <ip> )`**:  This function returns the metro code of the region associated with the IP address, 0 if unknown.
 * **`getPopulationDensity( <ip> )`**:  This function returns the population density associated with the IP address.
 * **`getPostalCode( <ip> )`**:  This function returns the postal code associated with the IP address.
 * **`getCoordPoint( <ip> )`**:  This function returns a point for use in GIS functions of the lat/long of associated with the IP address.
 * **`getASN( <ip> )`**:  This function returns the autonomous system of the IP address, "Unknown" if the IP is unknown or invalid.
 * **`getASNOrganization( <ip> )`**:  This function returns the autonomous system organization of the IP address, "Unknown" if the IP is unknown or invalid.
 * **`isEU( <ip> ), isEuropeanUnion( <ip> )`**:  This function returns `true` if the ip address is located in the European Union, `false` if not.
 * **`isAnonymous( <ip> )`**:  This function returns `true` if the ip address is anonymous, `false` if not.
 * **`isAnonymousVPN( <ip> )`**:  This function returns `true` if the ip address is an anonymous virtual private network (VPN), `false` if not.
 * **`isHostingProvider( <ip> )`**:  This function returns `true` if the ip address is a hosting provider, `false` if not.
 * **`isPublciProxy( <ip> )`**:  This function returns `true` if the ip address is a public proxy, `false` if not.
 * **`isTORExitNode( <ip> )`**:  This function returns `true` if the ip address is a known TOR exit node, `false` if not.
 
 This product includes GeoLite2 data created by MaxMind, available from <a href="https://www.maxmind.com">https://www.maxmind.com</a>.
