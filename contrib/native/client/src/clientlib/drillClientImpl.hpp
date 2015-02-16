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

#include <stdlib.h>
#include <time.h>
#include <queue>
#include <vector>
#include <boost/asio.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/thread.hpp>
#ifdef _WIN32
#include <zookeeper.h>
#else
#include <zookeeper/zookeeper.h>
#endif

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

class DrillClientQueryResult{
    friend class DrillClientImpl;
    public:
    DrillClientQueryResult(DrillClientImpl * pClient, uint64_t coordId):
        m_pClient(pClient),
        m_coordinationId(coordId),
        m_numBatches(0),
        m_columnDefs(new std::vector<Drill::FieldMetadata*>),
        m_bIsQueryPending(true),
        m_bIsLastChunk(false),
        m_bCancel(false),
        m_bHasSchemaChanged(false),
        m_bHasData(false),
        m_bHasError(false),
        m_queryState(exec::shared::QueryResult_QueryState_PENDING),
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

    private:
    status_t setupColumnDefs(exec::shared::QueryResult* pQueryResult);
    status_t defaultQueryResultsListener(void* ctx, RecordBatch* b, DrillClientError* err);
    // Construct a DrillClientError object, set the appropriate state and signal any listeners, condition variables.
    // Also used when a query is cancelled or when a query completed response is received.
    // Error object is now owned by the DrillClientQueryResult object.
    void signalError(DrillClientError* pErr);
    void clearAndDestroy();


    DrillClientImpl* m_pClient;

    int32_t m_coordinationId;
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

class DrillClientImpl{
    public:
        DrillClientImpl():
            m_coordinationId(1),
            m_handshakeVersion(0),
            m_bIsConnected(false),
            m_pendingRequests(0),
            m_pError(NULL),
            m_pListenerThread(NULL),
            m_socket(m_io_service),
            m_pWork(NULL),
            m_deadlineTimer(m_io_service),
            m_rbuf(NULL),
            m_wbuf(MAX_SOCK_RD_BUFSIZE)
    {
        srand(time(NULL));
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

            clearCancelledEntries();
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
        connectionStatus_t validateHandShake(const char* defaultSchema);

    private:
        friend class DrillClientQueryResult;

        struct compareQueryId{
            bool operator()(const exec::shared::QueryId* q1, const exec::shared::QueryId* q2) const {
                return q1->part1()<q2->part1() || (q1->part1()==q2->part1() && q1->part2() < q2->part2());
            }
        };

        // Direct connection to a drillbit
        // host can be name or ip address, port can be port number or name of service in /etc/services
        connectionStatus_t connect(const char* host, const char* port);
        int32_t getNextCoordinationId(){ return ++m_coordinationId; };
        void parseConnectStr(const char* connectStr, std::string& pathToDrill, std::string& protocol, std::string& hostPortStr);
        // send synchronous messages
        //connectionStatus_t recvSync(InBoundRpcMessage& msg);
        connectionStatus_t sendSync(OutBoundRpcMessage& msg);
        // handshake
        connectionStatus_t recvHandshake();
        void handleHandshake(ByteBuf_t b, const boost::system::error_code& err, std::size_t bytes_transferred );
        void handleHShakeReadTimeout(const boost::system::error_code & err);
        // Query results
        void getNextResult();
        status_t readMsg(
                ByteBuf_t _buf,
                AllocatedBufferPtr* allocatedBuffer,
                InBoundRpcMessage& msg,
                boost::system::error_code& error);
        status_t processQueryResult(AllocatedBufferPtr allocatedBuffer, InBoundRpcMessage& msg);
        status_t processCancelledQueryResult( exec::shared::QueryId& qid, exec::shared::QueryResult* qr);
        status_t processQueryId(AllocatedBufferPtr allocatedBuffer, InBoundRpcMessage& msg );
        status_t processQueryStatusResult( exec::shared::QueryResult* qr,
                DrillClientQueryResult* pDrillClientQueryResult);
        void handleReadTimeout(const boost::system::error_code & err);
        void handleRead(ByteBuf_t _buf, const boost::system::error_code & err, size_t bytes_transferred) ;
        status_t validateMessage(InBoundRpcMessage& msg, exec::shared::QueryResult& qr, std::string& valError);
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
        void clearCancelledEntries();
        void sendAck(InBoundRpcMessage& msg, bool isOk);
        void sendCancel(exec::shared::QueryId* pQueryId);


        static RpcEncoder s_encoder;
        static RpcDecoder s_decoder;

        int32_t m_coordinationId;
        int32_t m_handshakeVersion;
        bool m_bIsConnected;

        // number of outstanding read requests.
        // handleRead will keep asking for more results as long as this number is not zero.
        size_t m_pendingRequests;

        // Error Object. NULL if no error. Set if the error is valid for ALL running queries.
        // All the query result objects will
        // also have the error object set.
        // If the error is query specific, only the query results object will have the error set.
        DrillClientError* m_pError;

        // for boost asio
        boost::thread * m_pListenerThread;
        boost::asio::io_service m_io_service;
        // the work object prevent io_service running out of work
        boost::asio::io_service::work * m_pWork;
        boost::asio::ip::tcp::socket m_socket;
        boost::asio::deadline_timer m_deadlineTimer; // to timeout async queries that never return

        //for synchronous messages, like validate handshake
        ByteBuf_t m_rbuf; // buffer for receiving synchronous messages
        DataBuf m_wbuf; // buffer for sending synchronous message

        // Mutex to protect drill client operations
        boost::mutex m_dcMutex;

        // Map of coordination id to  Query Ids.
        std::map<int, DrillClientQueryResult*> m_queryIds;

        // Map of query id to query result for currently executing queries
        std::map<exec::shared::QueryId*, DrillClientQueryResult*, compareQueryId> m_queryResults;
        //
        // State for every Query id whose queries have result data pending but which
        // have been cancelled and whose resources have been released by the client application.
        // The entry is cleared when the state changes to completed or failed.
        std::set<exec::shared::QueryId*, compareQueryId> m_cancelledQueries;

};

inline bool DrillClientImpl::Active() {
    return this->m_bIsConnected;;
}

inline void DrillClientImpl::Close() {
    //TODO: cancel pending query
    if(this->m_bIsConnected){
        boost::system::error_code ignorederr;
        m_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignorederr);
        m_socket.close();
        m_bIsConnected=false;
    }
}

class ZookeeperImpl{
    public:
        ZookeeperImpl();
        ~ZookeeperImpl();
        static ZooLogLevel getZkLogLevel();
        // comma separated host:port pairs, each corresponding to a zk
        // server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002
        int connectToZookeeper(const char* connectStr, const char* pathToDrill);
        void close();
        static void watcher(zhandle_t *zzh, int type, int state, const char *path, void* context);
        void debugPrint();
        std::string& getError(){return m_err;}
        const exec::DrillbitEndpoint& getEndPoint(){ return m_drillServiceInstance.endpoint();}

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
};

} // namespace Drill

#endif
