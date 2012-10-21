# Dummy Drill

This is a Drill dummy-backend, used in the Apache Drill front-end, to simulate the real backend (as long as we don't have one).

## Usage

### 0. Launching elasticsearch

Got to the directory where you have [elasticsearch](http://www.elasticsearch.org/), ES for short in the following, installed and launch it:

	$ bin/elasticsearch -f

If you now `curl http://localhost:9200/` you should see:

	HTTP/1.1 200 OK
	Content-Length: 172
	Content-Type: application/json; charset=UTF-8

	{
		"name": "Blindspot", 
		"ok": true, 
		"status": 200, 
		"tagline": "You Know, for Search", 
		"version": {
			"number": "0.19.9", 
			"snapshot_build": false
		}
	}

### 1. Generate data sources

First, you want to generate a number of data sources (here: JSON documents with random information about beer-preferences of people). Let's create 200 data sources:

	 $ python gen_ds.py 200

The `gen_ds.py` script will generate as many data sources as you tell it to in a sub-directory of the current directory called `ds` and add each to the `apache_drill` index in ES.  If you now inspect the `ds` directory, you should see 200 JSON documents, each looking something like:

	{
		"beer": "Bud,Paulaner Hefe-Weizen,Bud",
		"id": 1,
		"name": "Jane Masters",
		"created": "2012-09-30T18:02:16Z"
	}
	
You might want to check if all is well (note that I'm using [httpie](https://github.com/jkbr/httpie) here in the following to talk HTTP with the server but feel free to use `curl`). Now try the following - I'm assuming you're running ES at port `9200`:

	$ http http://localhost:9200/apache_drill/_search?q=name:jane
	
... and you should get something like this:

	{
		"took": 1,
		"timed_out": false,
		"_shards": {
			"total": 5,
			"successful": 5,
			"failed": 0
		},
		"hits": {
			"total": 41,
			"max_score": 2.338025,
			"hits": [{
				"_index": "apache_drill",
				"_type": "beer_pref",
				"_id": "9HORJCZtQFa-yTQyMGiAOg",
				"_score": 2.338025,
				"_source": {
					"beer": "Guinness,Bud,Guinness",
					"id": 259,
					"name": "Jane Smith",
					"created": "2012-10-13T17:29:48Z"
				}
			},
			...
			}]
		}
	}

For a date-range query such as:

	$ http http://localhost:9200/apache_drill/_search?q=created:%5B2012-10-13T17:29:48Z%20TO%202012-10-13:17:29:48Z%5D
	
... you should see approximately the following:

	{
		"took": 2,
		"timed_out": false,
		"_shards": {
			"total": 5,
			"successful": 5,
			"failed": 0
		},
		"hits": {
			"total": 500,
			"max_score": 1.0,
			"hits": [{
				"_index": "apache_drill",
				"_type": "beer_pref",
				"_id": "2s34_2xxSUCYmPLJzG7FyQ",
				"_score": 1.0,
				"_source": {
					"beer": "Paulaner Hefe-Weizen,Sierra Nevada's Pale Ale,Heineken",
					"id": 4,
					"name": "Sarah Masters",
					"created": "2012-10-13T17:29:48Z"
				}
			},
			...
			}]
		}
	}




### 2. Launching the back-end

	$ python dummy_drill.py

	2012-09-30T08:33:13 Apache Dummy Drill server started, use {Ctrl+C} to shut-down ...
	2012-09-30T08:33:13 Using elasticsearch interface at 127.0.0.1:9200
	
### 3. Testing the back-end

	$ http http://localhost:6996/q/name:jane
	
	[{
		"beer": "Guinness,Bud,Guinness",
		"created": "2012-10-13T17:29:48Z",
		"id": 259,
		"name": "Jane Smith"
	}, 
	...
	{
		"beer": "Heineken,Paulaner Hefe-Weizen,Sierra Nevada's Pale Ale",
		"created": "2012-10-13T17:29:49Z",
		"id": 472,
		"name": "Jane van Rhein"
	}]
	

	
## Dependencies

* Python 2.7
* [elasticsearch](http://www.elasticsearch.org/)
* [pyes](https://github.com/aparo/pyes) - Python ElasticSearch

## License

This software is licensed under Apache 2.0 Software License. In case you have any questions, ask [Michael Hausenblas](http://mhausenblas.info/ "Michael Hausenblas").