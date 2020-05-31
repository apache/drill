# Drill Storage Plugin for IPFS (Minerva)

## Contents

0. [Introduction](#Introduction)
1. [Configuration](#Configuration)
2. [Usage Notes](#Usage Notes)

## Introduction

Minerva is a storage plugin of Drill that connects IPFS's decentralized storage and Drill's flexible query engine. Any data file stored on IPFS can be easily accessed from Drill's query interface, just like a file stored on a local disk. Moreover, with Drill's capability of distributed execution, other instances who are also running Minerva can help accelerate the execution: the data stays where it is, and the queries go to the most suitable nodes which stores the data locally, and from there the operations can be performed most efficiently. 

## Configuration

1. Set Drill hostname to the IP address of the node to run Drill:
    
    Edit file `conf/drill-env.sh` and change the environment variable `DRILL_HOST_NAME` to the IP address of the node. Use private or global addresses, depending on whether you plan to run it in a private cluster or on the open Internet.

2. Configure the IPFS storage plugin:
    
    The default configuration of the IPFS storage plugin is located at `src/resources/bootstrap-storage-plugins.json`:
    
    ```
    "ipfs" : {
      "type":"ipfs",
      "host": "127.0.0.1",
      "port": 5001,
      "max-nodes-per-leaf": 3,
      "ipfs-timeouts": {
        "find-provider": 4,
        "find-peer-info": 4,
        "fetch-data": 5
      },
      "ipfs-caches": {
        "peer": {"size": 100, "ttl": 600},
        "provider": {"size": 1000, "ttl": 600}
      },
      "groupscan-worker-threads": 50,
      "formats": null,
      "enabled": true
    }
    ```
    
    where 
    
    `host` and `port` are the host and API port where your IPFS daemon will be listening. Change it so that it matches the configuration of your IPFS instance.

    `max-nodes-per-leaf` controls how many provider nodes will be considered when the query is being planned. A larger value increases the parallelization width but typically takes longer to find enough providers from DHT resolution. A smaller value does the opposite.
    
    `ipfs-timeouts` set the maximum amount of time in seconds for various time-consuming operations: 
    
    * `find-provider` is the time allowed to do DHT queries to find providers.
    * `find-peer-info` is the time allowed to resolve the network addresses of the providers.
    * `fetch-data` is the time the actual transmission is allowed to take. 
    
    `ipfs-caches` control the size and TTL in seconds of cache entries of various caches used to accelerate query execution:
    
    * `peer` cache caches peers addresses.
    * `provider` cache caches which providers provide a particular IPFS object.
    
    `groupscan-worker-threads` limits the number of worker threads when the planner communicate with the IPFS daemon to resolve providers and peer info.
    
    `formats` specifies the formats of the files. It is unimplemented for now and does nothing.
    
3. Configure IPFS

    Start the IPFS daemon first. 
    
    Set a Drill-ready flag to the node:
    
    ```
    $ IPFS_NULL_OBJECT=$(ipfs object new)
    $ ipfs object patch add-link $IPFS_NULL_OBJECT "drill-ready" $IPFS_NULL_OBJECT
    QmeXLv7D5uV2uXHejg7St7mSXDtqwTw8LuywSBawSxy5iA
    $ ipfs name publish /ipfs/QmeXLv7D5uV2uXHejg7St7mSXDtqwTw8LuywSBawSxy5iA
    Published to <your-node-id>: /ipfs/QmeXLv7D5uV2uXHejg7St7mSXDtqwTw8LuywSBawSxy5iA
    ```
    
    This flag indicates that an IPFS node is also capable of handling Drill queries, and the planner will consider it when scheduling a query to execute distributedly. A node without this flag will be ignored.
    
    Also, pin the flag so that it will stick on your node:
    
    ```
    $ ipfs pin add -r QmeXLv7D5uV2uXHejg7St7mSXDtqwTw8LuywSBawSxy5iA
    ```
    
## Usage Notes

1. Compatible data formats

    Currently only JSON files are supported by this storage plugin.
    
2. Add datasets to IPFS
    
    IPFS provides the `ipfs add` command to conveniently add a file to IPFS. Unfortunately that command does not split data files into chunks on line boundaries. Use [this script](https://gist.github.com/dbw9580/250e52a54e39a34083f815dea34a89e0) to do proper chunking and add files to IPFS. 
    
3. Timeout exceptions

    IPFS operations can be time-consuming, and sometimes an operation can take forever (e.g. querying the DHT for a non-existent object). Adjust the timeout values in the config to avoid most timeout exceptions.
