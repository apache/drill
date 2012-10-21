"""
Generates dummy data for the Apache Drill front-end. 

Copyright (c) 2012 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

@author: Michael Hausenblas, http://mhausenblas.info/#i
@since: 2012-09-30
@status: init
"""

import sys, os, logging, datetime, random, json
from pyes import *

# configuration
DEBUG = False
DS_DIR = 'ds'
ES_INTERFACE = '127.0.0.1:9200'
DRILL_INDEX = 'apache_drill'
BEER_PREF_TYPE = 'beer_pref'

if DEBUG:
	FORMAT = '%(asctime)-0s %(levelname)s %(message)s [at line %(lineno)d]'
	logging.basicConfig(level=logging.DEBUG, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')
else:
	FORMAT = '%(asctime)-0s %(message)s'
	logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')


def setup_ES(con_str):
	"""Sets up elasticsearch interface, resets existing Drill index and creates a new Drill index based on a simple mapping."""
	logging.info("Setting up elasticsearch interface at %s" %(con_str))
	connection = ES(con_str)
	try:
		connection.delete_index(DRILL_INDEX) # make sure to reset existing Drill index
	except:
		pass
		
	connection.create_index(DRILL_INDEX)
	mapping = {	u'id': {
					'index': 'not_analyzed',
					'store': 'yes',
					'type': u'integer'
				},
				u'name': {
					'boost': 1.0,
					'index': 'analyzed',
					'store': 'yes',
					'type': u'string',
					"term_vector" : "with_positions_offsets"
				},
				u'created': {
					'boost': 1.0,
					'index': 'analyzed',
					'store': 'yes',
					'type': u'date',
					"term_vector" : "with_positions_offsets"
				},
				u'beer': {
					'boost': 1.0,
					'index': 'analyzed',
					'store': 'yes',
					'type': u'string',
					"term_vector" : "with_positions_offsets"
				}
	}
	connection.put_mapping(BEER_PREF_TYPE, {'properties':mapping}, [DRILL_INDEX])
	return connection

def gen_datasources(numds, es_connection):
	"""Generates numds data sources in a sub-directory DS_DIR of the current directory and adds each data source to the elasticsearch index."""
	logging.info("Generating %s data sources in directory %s" %(numds,  os.path.abspath(DS_DIR)))

	beers = ['Bud', 'Heineken', 'Guinness', 'Paulaner Hefe-Weizen', 'Sierra Nevada\'s Pale Ale', 'Hoegaarden']
	fnames = ['Jim', 'Jane', 'Jill', 'Ted', 'Michael', 'Fred', 'Sophie', 'Stefan', 'Sarah', 'Luis', 'Frank', 'Ben', 'Roberto', 'Kathy']
	lnames = ['Jones', 'Ding', 'Meyer', 'Smith', 'Cho', 'MacDonell', 'Lu', 'Masters', 'van Rhein', 'Becker', 'Garcia', 'Perez']

	if not os.path.exists(DS_DIR):
		os.makedirs(DS_DIR)
	
	for dsi in range(1, numds + 1):
		ds_file_name = os.path.join(DS_DIR, ''.join(['ds_', str(dsi), '.json']))
		ds = open(ds_file_name, 'w')
		payload = {	'id' : dsi, 
					'created' : ''.join([datetime.datetime.utcnow().isoformat().split('.')[0], 'Z']),
					'name' : ' '.join([random.choice(fnames), random.choice(lnames)]),
					'beer' : ','.join([random.choice(beers), random.choice(beers), random.choice(beers)])
		}
		es_connection.index(payload, DRILL_INDEX, BEER_PREF_TYPE)
		ds.write(json.JSONEncoder().encode(payload))
		ds.close()

if __name__ == '__main__':
	try: 
		numds = int(sys.argv[1])
		es_connection = setup_ES(ES_INTERFACE)
		gen_datasources(numds, es_connection)
	except ValueError:
		print 'Usage:\n $python gen_ds.py NUMBER_OF_DATASOURCES'
		print 'Example:\n $python gen_ds.py 10'
		sys.exit()
