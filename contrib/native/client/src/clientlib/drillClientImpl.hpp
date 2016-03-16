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


#ifndef DRILL_CLIENT_IMPL_H
#define DRILL_CLIENT_IMPL_H

#include "drill/common.hpp"

// Define some BOOST defines
// WIN32_SHUTDOWN_ON_TIMEOUT is defined in "drill/common.hpp" for Windows 32 bit platform
#ifndef WIN32_SHUTDOWN_ON_TIMEOUT
#define BOOST_ASIO_ENABLE_CANCELIO
#endif //WIN32_SHUTDOWN_ON_TIMEOUT

#include <algorithm>
#include <stdlib.h>
#include <time.h>
#include <queue>
#include <vector>
#include <boost/asio.hpp>

#if defined _WIN32  || defined _WIN64
#include <zookeeper.h>
//Windows header files redefine 'random'
#ifdef random
#undef random
#endif
#else
#include <zookeeper/zookeeper.h>
#endif
#include <boost/asio/deadline_timer.hpp>
#include <boost/thread.hpp>

#include "drill/drillClient.hpp"
#include "rpcEncoder.hpp"
#include "rpcDecoder.hpp"
#include "utils.hpp"
#include "User.pb.h"
#include "UserBitShared.pb.h"

namespace Drill {

class DrillClientImpl;
class InBoundRpcMessage;
class OutBoundRpcMessage;
class RecordBatch;
class RpcEncoder;
class RpcDecoder;

/*
 * Defines the interface used by DrillClient and implemented by DrillClientImpl and PooledDrillClientImpl
 * */
class DrillClientImplBase{
    public:
        DrillClientImplBase(){
        }

        virtual ~DrillClientImplBase(){
        }

        //Connect via Zookeeper or directly.
        //Makes an initial connection to a drillbit. successful connect adds the first drillbit to the pool.
        virtual connectionStatus_t connect(const char* connStr)=0;

        // Test whether the client is active. Returns true if any one of the underlying connections is active
        virtual bool Active()=0;

        // Closes all open connections. 
        virtual void Close()=0;

        // Returns the last error encountered by any of the underlying executing queries or connections
        virtual DrillClientError* getError()=0;

        // Submits a query to a drillbit. 
        virtual DrillClientQueryResult* SubmitQuery(::exec::shared::QueryType t, const std::string& plan, pfnQueryResultsListener listener, void* listenerCtx)=0;

        //Waits as a connection has results pending
        virtual void waitForResults()=0;

        //Validates handshake at connect time.
        virtual connectionStatus_t validateHandshake(DrillUserProperties* props)=0;

        virtual void freeQueryResources(DrillClientQueryResult* pQryResult)=0;

};

class DrillClientQueryResult{
    friend class DrillClientImpl;
    public:
    DrillClientQueryResult(DrillClientImpl * pClient, uint64_t coordId, const std::string& query):
        m_pClient(pClient),
        m_coordinationId(coordId),
        m_query(query),
        m_numBatches(0),
        m_columnDefs(new std::vector<Drill::FieldMetadata*>),
        m_bIsQueryPending(true),
        m_bIsLastChunk(false),
        m_bCancel(false),
        m_bHasSchemaChanged(false),
        m_bHasData(false),
        m_bHasError(false),
        m_queryState(exec::shared::QueryResult_QueryState_STARTING),
        m_pError(NULL),
        m_pQueryId(NULL),
        m_pSchemaListener(NULL),
        m_pResultsListener(NULL),
        m_pListenerCtx(NULL) {
    };

    ~DrillClientQueryResult(){
        this->clearAndDestroy();
    };

    // get data asynchronously
    void registerListener(pfnQueryResultsListener listener, void* listenerCtx){
        this->m_pResultsListener=listener;
        this->m_pListenerCtx = listenerCtx;
    }

    void registerSchemaChangeListener(pfnSchemaListener l){
        m_pSchemaListener=l;
    }

    // Synchronous call to get data. Caller assumes ownership of the recod batch
    // returned and it is assumed to have been consumed.
    RecordBatch*  getNext();
    // Synchronous call to get a look at the next Record Batch. This
    // call does not move the current pointer forward. Repeatied calls
    // to peekNext return the same value until getNext is called.
    RecordBatch*  peekNext();
    // Blocks until data is available.
    void waitForData();

    // placeholder to return an empty col def vector when calls are made out of order.
    static FieldDefPtr s_emptyColDefs;

    FieldDefPtr getColumnDefs(){
        boost::lock_guard<boost::mutex> bufferLock(this->m_schemaMutex);
        return this->m_columnDefs;
    }

    void cancel();
    bool isCancelled(){return this->m_bCancel;};
    bool hasSchemaChanged(){return this->m_bHasSchemaChanged;};
    int32_t getCoordinationId(){ return this->m_coordinationId;}
    const std::string&  getQuery(){ return this->m_query;}

    void setQueryId(exec::shared::QueryId* q){this->m_pQueryId=q;}
    void* getListenerContext() {return this->m_pListenerCtx;}
    exec::shared::QueryId& getQueryId(){ return *(this->m_pQueryId); }
    bool hasError(){ return m_bHasError;}
    status_t getErrorStatus(){ return m_pError!=NULL?(status_t)m_pError->status:QRY_SUCCESS;}
    const DrillClientError* getError(){ return m_pError;}
    void setQueryStatus(status_t s){ m_status = s;}
    status_t getQueryStatus(){ return m_status;}

    void setQueryState(exec::shared::QueryResult_QueryState s){ m_queryState = s;}
    exec::shared::QueryResult_QueryState getQueryState(){ return m_queryState;}
    void setIsQueryPending(bool isPending){
        boost::lock_guard<boost::mutex> cvLock(this->m_cvMutex);
        m_bIsQueryPending=isPending;
    }

    private:
    status_t setupColumnDefs(exec::shared::QueryData* pQueryData);
    status_t defaultQueryResultsListener(void* ctx, RecordBatch* b, DrillClientError* err);
    // Construct a DrillClientError object, set the appropriate state and signal any listeners, condition variables.
    // Also used when a query is cancelled or when a query completed response is received.
    // Error object is now owned by the DrillClientQueryResult object.
    void signalError(DrillClientError* pErr);
    void signalComplete();
    void clearAndDestroy();


    DrillClientImpl* m_pClient;

    int32_t m_coordinationId;
    const std::string& m_query;

    size_t m_numBatches; // number of record batches received so far

    // Vector of Buffers holding data returned by the server
    // Each data buffer is decoded into a RecordBatch
    std::vector<ByteBuf_t> m_dataBuffers;
    std::queue<RecordBatch*> m_recordBatches;
    FieldDefPtr m_columnDefs;

    // Mutex to protect schema definitions
    boost::mutex m_schemaMutex;
    // Mutex for Cond variable for read write to batch vector
    boost::mutex m_cvMutex;
    // Condition variable to signal arrival of more data. Condition variable is signaled
    // if the recordBatches queue is not empty
    boost::condition_variable m_cv;

    // state
    // if m_bIsQueryPending is true, we continue to wait for results
    bool m_bIsQueryPending;
    bool m_bIsLastChunk;
    bool m_bCancel;
    bool m_bHasSchemaChanged;
    bool m_bHasData;
    bool m_bHasError;

    // state in the last query result received from the server.
    exec::shared::QueryResult_QueryState m_queryState;

    const DrillClientError* m_pError;

    exec::shared::QueryId* m_pQueryId;
    status_t m_status;

    // Schema change listener
    pfnSchemaListener m_pSchemaListener;
    // Results callback
    pfnQueryResultsListener m_pResultsListener;

    // Listener context
    void * m_pListenerCtx;
};

class DrillClientImpl : public DrillClientImplBase{
    public:
        DrillClientImpl():
            m_coordinationId(1),
            m_handshakeVersion(0),
            m_handshakeStatus(exec::user::SUCCESS),
            m_bIsConnected(false),
            m_pendingRequests(0),
            m_pError(NULL),
            m_pListenerThread(NULL),
            m_pWork(NULL),
            m_socket(m_io_service),
            m_deadlineTimer(m_io_service),
            m_heartbeatTimer(m_io_service),
            m_rbuf(NULL),
            m_wbuf(MAX_SOCK_RD_BUFSIZE)
    {
        m_coordinationId=rand()%1729+1;
    };

        ~DrillClientImpl(){
            //TODO: Cleanup.
            //Free any record batches or buffers remaining
            //Cancel any pending requests
            //Clear and destroy DrillClientQueryResults vector?
            if(this->m_pWork!=NULL){
                delete this->m_pWork;
                this->m_pWork = NULL;
            }

            m_heartbeatTimer.cancel();
            m_deadlineTimer.cancel();
            m_io_service.stop();
            boost::system::error_code ignorederr;
            m_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignorederr);
            m_socket.close();
            if(m_rbuf!=NULL){
                Utils::freeBuffer(m_rbuf, MAX_SOCK_RD_BUFSIZE); m_rbuf=NULL;
            }
            if(m_pError!=NULL){
                delete m_pError; m_pError=NULL;
            }
            //Terminate and free the heartbeat thread
            //if(this->m_pHeartbeatThread!=NULL){
            //    this->m_pHeartbeatThread->interrupt();
            //    this->m_pHeartbeatThread->join();
            //    delete this->m_pHeartbeatThread;
            //    this->m_pHeartbeatThread = NULL;
            //}
            //Terminate and free the listener thread
            if(this->m_pListenerThread!=NULL){
                this->m_pListenerThread->interrupt();
                this->m_pListenerThread->join();
                delete this->m_pListenerThread;
                this->m_pListenerThread = NULL;
            }
        };

        //Connect via Zookeeper or directly
        connectionStatus_t connect(const char* connStr);
        // test whether the client is active
        bool Active();
        void Close() ;
        DrillClientError* getError(){ return m_pError;}
        DrillClientQueryResult* SubmitQuery(::exec::shared::QueryType t, const std::string& plan, pfnQueryResultsListener listener, void* listenerCtx);
        void waitForResults();
        connectionStatus_t validateHandshake(DrillUserProperties* props);
        void freeQueryResources(DrillClientQueryResult* pQryResult){
            // Doesn't need to do anything
            return;
        };

    private:
        friend class DrillClientQueryResult;
        friend class PooledDrillClientImpl;

        struct compareQueryId{
            bool operator()(const exec::shared::QueryId* q1, const exec::shared::QueryId* q2) const {
                return q1->part1()<q2->part1() || (q1->part1()==q2->part1() && q1->part2() < q2->part2());
            }
        };

        // Direct connection to a drillbit
        // host can be name or ip address, port can be port number or name of service in /etc/services
        connectionStatus_t connect(const char* host, const char* port);
        void startHeartbeatTimer();// start a heartbeat timer
        connectionStatus_t sendHeartbeat(); // send a heartbeat to the server
        void resetHeartbeatTimer(); // reset the heartbeat timer (called every time one sends a message to the server (after sendAck, or submitQuery)
        void handleHeartbeatTimeout(const boost::system::error_code & err); // send a heartbeat. If send fails, broadcast error, close connection and bail out.

        int32_t getNextCoordinationId(){ return ++m_coordinationId; };
        // send synchronous messages
        //connectionStatus_t recvSync(InBoundRpcMessage& msg);
        connectionStatus_t sendSync(OutBoundRpcMessage& msg);
        // handshake
        connectionStatus_t recvHandshake();
        void handleHandshake(ByteBuf_t b, const boost::system::error_code& err, std::size_t bytes_transferred );
        void handleHShakeReadTimeout(const boost::system::error_code & err);
        // starts the listener thread that receives responses/messages from the server
        void startMessageListener(); 
        // Query results
        void getNextResult();
        status_t readMsg(
                ByteBuf_t _buf,
                AllocatedBufferPtr* allocatedBuffer,
                InBoundRpcMessage& msg,
                boost::system::error_code& error);
        status_t processQueryResult(AllocatedBufferPtr allocatedBuffer, InBoundRpcMessage& msg);
        status_t processQueryData(AllocatedBufferPtr allocatedBuffer, InBoundRpcMessage& msg);
        status_t processCancelledQueryResult( exec::shared::QueryId& qid, exec::shared::QueryResult* qr);
        status_t processQueryId(AllocatedBufferPtr allocatedBuffer, InBoundRpcMessage& msg );
        DrillClientQueryResult* findQueryResult(exec::shared::QueryId& qid);
        status_t processQueryStatusResult( exec::shared::QueryResult* qr,
                DrillClientQueryResult* pDrillClientQueryResult);
        void handleReadTimeout(const boost::system::error_code & err);
        void handleRead(ByteBuf_t _buf, const boost::system::error_code & err, size_t bytes_transferred) ;
        status_t validateDataMessage(InBoundRpcMessage& msg, exec::shared::QueryData& qd, std::string& valError);
        status_t validateResultMessage(InBoundRpcMessage& msg, exec::shared::QueryResult& qr, std::string& valError);
        connectionStatus_t handleConnError(connectionStatus_t status, std::string msg);
        status_t handleQryError(status_t status, std::string msg, DrillClientQueryResult* pQueryResult);
        status_t handleQryError(status_t status,
                const exec::shared::DrillPBError& e,
                DrillClientQueryResult* pQueryResult);
        // handle query state indicating query is COMPELTED or CANCELED
        // (i.e., COMPELTED or CANCELED)
        status_t handleTerminatedQryState(status_t status,
                std::string msg,
                DrillClientQueryResult* pQueryResult);
        void broadcastError(DrillClientError* pErr);
        void clearMapEntries(DrillClientQueryResult* pQueryResult);
        void sendAck(InBoundRpcMessage& msg, bool isOk);
        void sendCancel(exec::shared::QueryId* pQueryId);

        void shutdownSocket();


        static RpcEncoder s_encoder;
        static RpcDecoder s_decoder;

        int32_t m_coordinationId;
        int32_t m_handshakeVersion;
        exec::user::HandshakeStatus m_handshakeStatus;
        std::string m_handshakeErrorId;
        std::string m_handshakeErrorMsg;
        bool m_bIsConnected;

        std::string m_connectStr; 

        // 
        // number of outstanding read requests.
        // handleRead will keep asking for more results as long as this number is not zero.
        size_t m_pendingRequests;
        //mutex to protect m_pendingRequests
        boost::mutex m_prMutex;

        // Error Object. NULL if no error. Set if the error is valid for ALL running queries.
        // All the query result objects will
        // also have the error object set.
        // If the error is query specific, only the query results object will have the error set.
        DrillClientError* m_pError;

        //Started after the connection is established and sends heartbeat messages after {heartbeat frequency} seconds
        //The thread is killed on disconnect.
        //boost::thread * m_pHeartbeatThread;

        // for boost asio
        boost::thread * m_pListenerThread;
        boost::asio::io_service m_io_service;
        // the work object prevent io_service running out of work
        boost::asio::io_service::work * m_pWork;
        boost::asio::ip::tcp::socket m_socket;
        boost::asio::deadline_timer m_deadlineTimer; // to timeout async queries that never return
        boost::asio::deadline_timer m_heartbeatTimer; // to send heartbeat messages

        std::string m_connectedHost; // The hostname and port the socket is connected to.

        //for synchronous messages, like validate handshake
        ByteBuf_t m_rbuf; // buffer for receiving synchronous messages
        DataBuf m_wbuf; // buffer for sending synchronous message

        // Mutex to protect drill client operations
        boost::mutex m_dcMutex;

        // Map of coordination id to  Query Ids.
        std::map<int, DrillClientQueryResult*> m_queryIds;

        // Map of query id to query result for currently executing queries
        std::map<exec::shared::QueryId*, DrillClientQueryResult*, compareQueryId> m_queryResults;

        // Condition variable to signal completion of all queries. 
        boost::condition_variable m_cv;

        bool m_bIsDirectConnection;
};

inline bool DrillClientImpl::Active() {
    return this->m_bIsConnected;;
}


/* *
 *  Provides the same public interface as a DrillClientImpl but holds a pool of DrillClientImpls.
 *  Every submitQuery uses a different DrillClientImpl to distribute the load.
 *  DrillClient can use this class instead of DrillClientImpl to get better load balancing.
 * */
class PooledDrillClientImpl : public DrillClientImplBase{
    public:
        PooledDrillClientImpl(){
            m_bIsDirectConnection=false;
            m_maxConcurrentConnections = DEFAULT_MAX_CONCURRENT_CONNECTIONS;
            char* maxConn=std::getenv(MAX_CONCURRENT_CONNECTIONS_ENV);
            if(maxConn!=NULL){
                m_maxConcurrentConnections=atoi(maxConn);
            }
            m_lastConnection=-1;
            m_pError=NULL;
            m_queriesExecuted=0;
            m_pUserProperties=NULL;
        }

        ~PooledDrillClientImpl(){
            for(std::vector<DrillClientImpl*>::iterator it = m_clientConnections.begin(); it != m_clientConnections.end(); ++it){
                delete *it;
            }
            m_clientConnections.clear();
            if(m_pUserProperties!=NULL){ delete m_pUserProperties; m_pUserProperties=NULL;}
            if(m_pError!=NULL){ delete m_pError; m_pError=NULL;}
        }

        //Connect via Zookeeper or directly.
        //Makes an initial connection to a drillbit. successful connect adds the first drillbit to the pool.
        connectionStatus_t connect(const char* connStr);

        // Test whether the client is active. Returns true if any one of the underlying connections is active
        bool Active();

        // Closes all open connections. 
        void Close() ;

        // Returns the last error encountered by any of the underlying executing queries or connections
        DrillClientError* getError();

        // Submits a query to a drillbit. If more than one query is to be sent, we may choose a
        // a different drillbit in the pool. No more than m_maxConcurrentConnections will be allowed.
        // Connections once added to the pool will be removed only when the DrillClient is closed.
        DrillClientQueryResult* SubmitQuery(::exec::shared::QueryType t, const std::string& plan, pfnQueryResultsListener listener, void* listenerCtx);

        //Waits as long as any one drillbit connection has results pending
        void waitForResults();

        //Validates handshake only against the first drillbit connected to.
        connectionStatus_t validateHandshake(DrillUserProperties* props);

        void freeQueryResources(DrillClientQueryResult* pQryResult);

        int getDrillbitCount(){ return m_drillbits.size();};

    private:
        
        std::string m_connectStr; 
        std::string m_lastQuery;
        
        // A list of all the current client connections. We choose a new one for every query. 
        // When picking a drillClientImpl to use, we see how many queries each drillClientImpl
        // is currently executing. If none,  
        std::vector<DrillClientImpl*> m_clientConnections; 
		boost::mutex m_poolMutex; // protect access to the vector
        
        //ZookeeperImpl zook;
        
        // Use this to decide which drillbit to select next from the list of drillbits.
        size_t m_lastConnection;
		boost::mutex m_cMutex;

        // Number of queries executed so far. Can be used to select a new Drillbit from the pool.
        size_t m_queriesExecuted;

        size_t m_maxConcurrentConnections;

        bool m_bIsDirectConnection;

        DrillClientError* m_pError;

        connectionStatus_t handleConnError(connectionStatus_t status, std::string msg);
        // get a connection from the pool or create a new one. Return NULL if none is found
        DrillClientImpl* getOneConnection();

        std::vector<std::string> m_drillbits;

        DrillUserProperties* m_pUserProperties;//Keep a copy of user properties
};

class ZookeeperImpl{
    public:
        ZookeeperImpl();
        ~ZookeeperImpl();
        static ZooLogLevel getZkLogLevel();
        // comma separated host:port pairs, each corresponding to a zk
        // server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002
        DEPRECATED int connectToZookeeper(const char* connectStr, const char* pathToDrill);
        void close();
        static void watcher(zhandle_t *zzh, int type, int state, const char *path, void* context);
        void debugPrint();
        std::string& getError(){return m_err;}
        const exec::DrillbitEndpoint& getEndPoint(){ return m_drillServiceInstance.endpoint();}
        // return unshuffled list of drillbits
        int getAllDrillbits(const char* connectStr, const char* pathToDrill, std::vector<std::string>& drillbits);
        // picks the index drillbit and returns the corresponding endpoint object
        int getEndPoint(std::vector<std::string>& drillbits, size_t index, exec::DrillbitEndpoint& endpoint);
        

    private:
        static char s_drillRoot[];
        static char s_defaultCluster[];
        zhandle_t* m_zh;
        clientid_t m_id;
        int m_state;
        std::string m_err;

        struct String_vector* m_pDrillbits;

        boost::mutex m_cvMutex;
        // Condition variable to signal connection callback has been processed
        boost::condition_variable m_cv;
        bool m_bConnecting;
        exec::DrillServiceInstance m_drillServiceInstance;
        std::string m_rootDir;
};

} // namespace Drill

#endif
