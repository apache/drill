/*
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


#ifndef DRILL_CLIENT_H
#define DRILL_CLIENT_H

#include <vector>
#include <boost/thread.hpp>
#include "drill/common.hpp"
#include "drill/protobuf/Types.pb.h"


#if defined _WIN32 || defined __CYGWIN__
  #ifdef DRILL_CLIENT_EXPORTS
      #define DECLSPEC_DRILL_CLIENT __declspec(dllexport)
  #else
    #ifdef USE_STATIC_LIBDRILL
      #define DECLSPEC_DRILL_CLIENT
    #else
      #define DECLSPEC_DRILL_CLIENT  __declspec(dllimport)
    #endif
  #endif
#else
  #if __GNUC__ >= 4
    #define DECLSPEC_DRILL_CLIENT __attribute__ ((visibility ("default")))
  #else
    #define DECLSPEC_DRILL_CLIENT
  #endif
#endif

namespace exec{
    namespace shared{
        class DrillPBError;
    };
};

namespace Drill{

//struct UserServerEndPoint;
class  DrillClientImplBase;
class  DrillClientImpl;
class  DrillClientQueryResult;
class  FieldMetadata;
class  RecordBatch;
class  SchemaDef;

enum QueryType{
    SQL = 1,
    LOGICAL = 2,
    PHYSICAL = 3
};

class DECLSPEC_DRILL_CLIENT DrillClientError{
    public:
        static const uint32_t CONN_ERROR_START = 100;
        static const uint32_t QRY_ERROR_START =  200;

        DrillClientError(uint32_t s, uint32_t e, char* m){status=s; errnum=e; msg=m;};
        DrillClientError(uint32_t s, uint32_t e, std::string m){status=s; errnum=e; msg=m;};

        static DrillClientError*  getErrorObject(const exec::shared::DrillPBError& e);

        // To get the error number we add a error range start number to
        // the status code returned (either status_t or connectionStatus_t)
        uint32_t status; // could be either status_t or connectionStatus_t
        uint32_t errnum;
        std::string msg;
};

// Only one instance of this class exists. A static member of DrillClientImpl;
class DECLSPEC_DRILL_CLIENT DrillClientInitializer{
    public:
        DrillClientInitializer();
        ~DrillClientInitializer();
};

// Only one instance of this class exists. A static member of DrillClientImpl;
class DECLSPEC_DRILL_CLIENT DrillClientConfig{
    public:
        DrillClientConfig();
        ~DrillClientConfig();
        static void initLogging(const char* path);
        static void setLogLevel(logLevel_t l);
        static void setBufferLimit(uint64_t l);
        static uint64_t getBufferLimit();
        static void setSocketTimeout(int32_t l);
        static void setHandshakeTimeout(int32_t l);
        static void setQueryTimeout(int32_t l);
        static void setHeartbeatFrequency(int32_t l);
        static int32_t getSocketTimeout();
        static int32_t getHandshakeTimeout();
        static int32_t getQueryTimeout();
        static int32_t getHeartbeatFrequency();
        static logLevel_t getLogLevel();
    private:
        // The logging level
        static logLevel_t s_logLevel;
        // The total amount of memory to be allocated by an instance of DrillClient.
        // For future use. Currently, not enforced.
        static uint64_t s_bufferLimit;

        /**
         * DrillClient configures timeout (in seconds) in a fine granularity.
         * Disabled by setting the value to zero.
         *
         * s_socketTimout: (default 0)
         *      set SO_RCVTIMEO and SO_SNDTIMEO socket options and place a
         *		timeout on socket receives and sends. It is disabled by default.
         *
         * s_handshakeTimeout: (default 5)
         *      place a timeout on validating handshake. When an endpoint (host:port)
         *		is reachable but drillbit hangs or running another service. It will
         *		avoid the client hanging.
         *
         * s_queryTimeout: (default 180)
         *      place a timeout on waiting result of querying.
         *
         * s_heartbeatFrequency: (default 30)
         *      Seconds of idle activity after which a heartbeat is sent to the drillbit
         */
        static int32_t s_socketTimeout;
        static int32_t s_handshakeTimeout;
        static int32_t s_queryTimeout;
        static int32_t s_heartbeatFrequency;
        static boost::mutex s_mutex;
};


class DECLSPEC_DRILL_CLIENT DrillUserProperties{
    public:
        static const std::map<std::string, uint32_t> USER_PROPERTIES;

        DrillUserProperties(){};

        void setProperty( const std::string& propName, const std::string& propValue){
            std::pair< std::string, std::string> in = make_pair(propName, propValue);
            m_properties.push_back(in);
        }

        size_t size() const { return m_properties.size(); }

        const std::string& keyAt(size_t i) const { return m_properties.at(i).first; }

        const std::string& valueAt(size_t i) const { return m_properties.at(i).second; }

        bool validate(std::string& err);

    private:
        std::vector< std::pair< std::string, std::string> > m_properties;
};

/*
 * Handle to the Query submitted for execution.
 * */
typedef void* QueryHandle_t;

/*
 * Query Results listener callback. This function is called for every record batch after it has
 * been received and decoded. The listener function should return a status.
 * If the listener returns failure, the query will be canceled.
 * The listener is also called one last time when the query is completed or gets an error. In that
 * case the RecordBatch Parameter is NULL. The DrillClientError parameter is NULL is there was no
 * error oterwise it will have a valid DrillClientError object.
 * DrillClientQueryResult will hold a listener & listener contxt for the call back function
 */
typedef status_t (*pfnQueryResultsListener)(QueryHandle_t ctx, RecordBatch* b, DrillClientError* err);

/*
 * The schema change listener callback. This function is called if the record batch detects a
 * change in the schema. The client application can call getColDefs in the RecordIterator or
 * get the field information from the RecordBatch itself and handle the change appropriately.
 */
typedef status_t (*pfnSchemaListener)(void* ctx, FieldDefPtr f, DrillClientError* err);

/*
 * A Record Iterator instance is returned by the SubmitQuery class. Calls block until some data
 * is available, or until all data has been returned.
 */

class DECLSPEC_DRILL_CLIENT RecordIterator{
    friend class DrillClient;
    public:

    ~RecordIterator();
    /*
     * Returns a vector of column(i.e. field) definitions. The returned reference is guaranteed to be valid till the
     * end of the query or until a schema change event is received. If a schema change event is received by the
     * application, the application should discard the reference it currently holds and call this function again.
     */
    FieldDefPtr getColDefs();

    /* Move the current pointer to the next record. */
    status_t next();

    /* Gets the ith column in the current record. */
    status_t getCol(size_t i, void** b, size_t* sz);

    /* true if ith column in the current record is NULL */
    bool isNull(size_t i);

    /* Cancels the query. */
    status_t cancel();

    /*  Returns true is the schem has changed from the previous record. Returns false for the first record. */
    bool hasSchemaChanged();

    void registerSchemaChangeListener(pfnSchemaListener l);

    bool hasError();
    /*
     * Returns the last error message
     */
    const std::string& getError();

    private:
    RecordIterator(DrillClientQueryResult* pResult){
        this->m_currentRecord=-1;
        this->m_pCurrentRecordBatch=NULL;
        this->m_pQueryResult=pResult;
        //m_pColDefs=NULL;
    }

    DrillClientQueryResult* m_pQueryResult;
    size_t m_currentRecord;
    RecordBatch* m_pCurrentRecordBatch;
    boost::mutex m_recordBatchMutex;
    FieldDefPtr m_pColDefs; // Copy of the latest column defs made from the
    // first record batch with this definition
};

class DECLSPEC_DRILL_CLIENT DrillClient{
    public:
        /*
         * Get the application context from query handle
         */
        static void* getApplicationContext(QueryHandle_t handle);

        /*
         * Get the query status from query handle
         */
        static status_t getQueryStatus(QueryHandle_t handle);


        DrillClient();
        ~DrillClient();

        // change the logging level
        static void initLogging(const char* path, logLevel_t l);

        /**
         * Connect the client to a Drillbit using connection string and default schema.
         *
         * @param[in] connectStr: connection string
         * @param[in] defaultSchema: default schema (set to NULL and ignore it
         * if not specified)
         * @return    connection status
         */
        DEPRECATED connectionStatus_t connect(const char* connectStr, const char* defaultSchema=NULL);

        /*  
         * Connect the client to a Drillbit using connection string and a set of user properties.
         * The connection string format can be found in comments of
         * [DRILL-780](https://issues.apache.org/jira/browse/DRILL-780)
         *
         * To connect via zookeeper, use the format:
         * "zk=zk1:port[,zk2:p2,...][/<path_to_drill_root>/<cluster_id>]".
         *
         * e.g.
         * ```
         * zk=localhost:2181
         * zk=localhost:2181/drill/drillbits1
         * zk=localhost:2181,zk2:2181/drill/drillbits1
         * ```
         *
         * To connect directly to a drillbit, use the format "local=host:port".
         *
         * e.g.
         * ```
         * local=127.0.0.1:31010
         * ```
         *
         * User properties is a set of name value pairs. The following properties are recognized:
         *     schema
         *     userName
         *     password
         *     useSSL [true|false]
         *     pemLocation
         *     pemFile
         *     (see drill/common.hpp for friendly defines and the latest list of supported proeprties)
         *
         * @param[in] connectStr: connection string
         * @param[in] properties
         * if not specified)
         * @return    connection status
         */
        connectionStatus_t connect(const char* connectStr, DrillUserProperties* properties);

        /* test whether the client is active */
        bool isActive();

        /*  close the connection. cancel any pending requests. */
        void close() ;

        /*
         * Submit a query asynchronously and wait for results to be returned through a callback. A query context handle is passed
         * back. The listener callback will return the handle in the ctx parameter.
         */
        status_t submitQuery(Drill::QueryType t, const std::string& plan, pfnQueryResultsListener listener, void* listenerCtx, QueryHandle_t* qHandle);

        /*
         * Submit a query asynchronously and wait for results to be returned through an iterator that returns
         * results synchronously. The client app needs to call delete on the iterator when done.
         */
        RecordIterator* submitQuery(Drill::QueryType t, const std::string& plan, DrillClientError* err);

        /*
         * The client application should call this function to wait for results if it has registered a
         * listener.
         */
        void waitForResults();

        /*
         * Returns the last error message
         */
        std::string& getError();

        /*
         * Returns the error message associated with the query handle
         */
        const std::string& getError(QueryHandle_t handle);
        /*
         * Applications using the async query submit method can register a listener for schema changes
         *
         */
        void registerSchemaChangeListener(QueryHandle_t* handle, pfnSchemaListener l);

        /*
         * Applications using the async query submit method should call freeQueryResources to free up resources
         * once the query is no longer being processed.
         */
        void freeQueryResources(QueryHandle_t* handle);

        /*
         * Applications using the sync query submit method should call freeQueryIterator to free up resources
         * once the RecordIterator is no longer being processed.
         */
        void freeQueryIterator(RecordIterator** pIter){ delete *pIter; *pIter=NULL;};

        /*
         * Applications using the async query submit method should call freeRecordBatch to free up resources
         * once the RecordBatch is processed and no longer needed.
         */
        void freeRecordBatch(RecordBatch* pRecordBatch);



    private:
        static DrillClientInitializer s_init;
        static DrillClientConfig s_config;

        DrillClientImplBase * m_pImpl;
};

} // namespace Drill

#endif
