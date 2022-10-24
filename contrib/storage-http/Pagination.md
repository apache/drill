# Auto Pagination in Drill
Remote APIs frequently implement some sort of pagination as a way of limiting results.  However, if you are performing bulk data analysis, it is necessary to reassemble the
data into one larger dataset.  Drill's auto-pagination features allow this to happen in the background, so that the user will get clean data back.

To use a paginator, you simply have to configure the paginator in the connection for the particular API.

## Words of Caution
While extremely powerful, the auto-pagination feature has the potential to run afoul of APIs rate limits and even potentially DDoS an API. Please use with extreme care.

## Rate Limits
When using automatic pagination, you may encounter APIs that have burst limits or other limits
as to the maximum number of requests in a minute or other amount of time.  Drill allows you to
set a `retryDelay` parameter which is the number of milliseconds that Drill should wait before
resending the request.  This defaults to 1 second.  This option is set in the configuration for
the HTTP plugin.

## Offset Pagination
Offset Pagination uses commands similar to SQL which has a `LIMIT` and an `OFFSET`.  With an offset paginator, let's say you want 200 records and the  page size is 50 records, the offset paginator will break up your query into 4 requests as shown below:

* myapi.com?limit=50&offset=0
* myapi.com?limit=50?offset=50
* myapi.com?limit=50&offset=100
* myapi.com?limit=50&offset=150

### Configuring Offset Pagination
To configure an offset paginator, simply add the following to the configuration for your connection.

```json
"paginator": {
   "limitParam": "<limit>",
   "offsetParam": "<offset>",
   "pageSize": 100,
   "method": "OFFSET"
}
```

## Page Pagination
Page pagination is very similar to offset pagination except instead of using an `OFFSET` it uses a page number.

```json
 "paginator": {
        "pageParam": "page",
        "pageSizeParam": "per_page",
        "pageSize": 100,
        "method": "PAGE"
      }
```
In either case, the `pageSize` parameter should be set to the maximum page size allowable by the API.  This will minimize the number of requests Drill is making.

## Index / KeySet Pagination
Index or KeySet pagination is when the API itself returns values to generate the next page.

Consider an API that returned data like this:

```json
{
  "companies": [
    ...
  ],
  "has-more": true,
  "index": 3849945478
}

```
In this case, the `has-more` parameter is a boolean value which indicates whether or not there are more pages. The `index` parameter gets appended to the URL in question to generate the next page.

`https://api.myapi.com/paged?properties=name&properties=website&offset=3856722038`

There is a slight variant of this where the API will return the actual URL of the next page.

There are three possible parameters:

* `hasMoreParam`: This is the name of the boolean parameter which indicates whether the API has more pages.
* `indexParam`:  The parameter name of the key or offset that will be used to generate the next page.
* `nextPageParam`: The parameter name which returns a complete URL of the next page.


** Note: Index / Keyset Pagination is only implemented for APIs that return JSON **
