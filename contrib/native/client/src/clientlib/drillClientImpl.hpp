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


#ifndef DRILL_CLIENT_ASYNC_H
#define DRILL_CLIENT_ASYNC_H

#include <stdlib.h>
#include <time.h>
#include <queue>
#include <vector>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <zookeeper/zookeeper.h>

#include "drill/common.hpp"
#include "drill/drillClient.hpp"
#include "rpcEncoder.hpp"
#include "rpcDecoder.hpp"
#include "User.pb.h"
#include "UserBitShared.pb.h"

namespace Drill {

class DrillClientImpl;
class InBoundRpcMessage;
class OutBoundRpcMessage;
class RecordBatch;
class RpcEncoder;
class RpcDecoder;
//struct UserServerEndPoint;

class DrillClientQueryResult{
    friend class DrillClientImpl;
    public:
    DrillClientQueryResult(DrillClientImpl * pClient, uint64_t coordId):
        m_pClient(pClient),
        m_coordinationId(coordId),
        m_bIsQueryPending(true),
        m_bIsLastChunk(false),
        m_bCancel(false),
        m_bHasSchemaChanged(false), 
        m_bHasData(false),
        m_bHasError(false),
        m_pError(NULL),
        m_pQueryId(NULL),
        m_pResultsListener(NULL),
        m_pListenerCtx(NULL)
    {};

    ~DrillClientQueryResult(){
        this->clearAndDestroy();
    };

    // get data asynchronously
    void registerListener(pfnQueryResultsListener listener, void* listenerCtx){
        this->m_pResultsListener=listener;
        this->m_pListenerCtx = listenerCtx;
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
    static std::vector<Drill::FieldMetadata*> s_emptyColDefs;

    std::vector<Drill::FieldMetadata*>& getColumnDefs(){ 
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
    DrillClientError* getError(){ return m_pError;}

    private:
    status_t setupColumnDefs(exec::user::QueryResult* pQueryResult);
    status_t defaultQueryResultsListener(void* ctx, RecordBatch* b, DrillClientError* err);
    // Construct a DrillClientError object, set the appropriate state and signal any listeners, condition variables.
    void signalError(DrillClientError* pErr);
    void clearAndDestroy();


    DrillClientImpl* m_pClient;

    int32_t m_coordinationId;

    // Vector of Buffers holding data returned by the server
    // Each data buffer is decoded into a RecordBatch
    std::vector<ByteBuf_t> m_dataBuffers;
    std::queue<RecordBatch*> m_recordBatches;
    std::vector<Drill::FieldMetadata*> m_columnDefs;

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

    DrillClientError* m_pError;

    exec::shared::QueryId* m_pQueryId;

    // Results callback
    pfnQueryResultsListener m_pResultsListener;

    // Listener context
    void * m_pListenerCtx;
};

class DrillClientImpl{
    public:
        DrillClientImpl():
            m_coordinationId(1) ,
            m_bIsConnected(false),
            m_pendingRequests(0), 
            m_pError(NULL), 
            m_pListenerThread(NULL), 
            m_socket(m_io_service), 
            m_rbuf(1024), 
            m_wbuf(1024)
    {
        srand(time(NULL));
        m_coordinationId=rand()%1729+1;
    };

        ~DrillClientImpl(){ 
            //TODO: Cleanup. 
            //Free any record batches or buffers remaining
            //Cancel any pending requests
            //Clear and destroy DrillClientQueryResults vector?

            //Terminate and free the listener thread
            if(this->m_pListenerThread!=NULL){
                this->m_pListenerThread->interrupt();
                delete this->m_pListenerThread;
            }
            if(m_pError!=NULL){
                delete m_pError; m_pError=NULL;
            }
        };

        //Connect via Zookeeper or directly
        connectionStatus_t connect(const char* connStr);
        // test whether the client is active
        bool Active();
        void Close() ;
        DrillClientError* getError(){ return m_pError;}
        DrillClientQueryResult* SubmitQuery(exec::user::QueryType t, const std::string& plan, pfnQueryResultsListener listener, void* listenerCtx);
        void waitForResults();
        bool ValidateHandShake(); // throw expection if not valid


    private:
        friend class DrillClientQueryResult;

        struct compareQueryId{
            bool operator()(const exec::shared::QueryId* q1, const exec::shared::QueryId* q2) const {
                return q1->part1()<q2->part1() || (q1->part1()==q2->part1() && q1->part2() < q2->part2());
            }
        };

        //allocate memory for Record Batches
        ByteBuf_t allocateBuffer(size_t len){
            ByteBuf_t b = (ByteBuf_t)malloc(len); memset(b, 0, len); return b;
        }
        void freeBuffer(ByteBuf_t b){ free(b); }

        // Direct connection to a drillbit
        // host can be name or ip address, port can be port number or name of service in /etc/services
        connectionStatus_t connect(const char* host, const char* port);       
        int32_t getNextCoordinationId(){ return ++m_coordinationId; };
        void parseConnectStr(const char* connectStr, std::string& protocol, std::string& hostPortStr);
        // end and receive synchronous messages
        void recvSync(InBoundRpcMessage& msg);
        void sendSync(OutBoundRpcMessage& msg);

        void getNextResult();
        status_t readMsg(ByteBuf_t _buf, InBoundRpcMessage& msg, boost::system::error_code& error);
        status_t processQueryResult(InBoundRpcMessage& msg);
        status_t processQueryId(InBoundRpcMessage& msg );
        void handleRead(ByteBuf_t _buf, const boost::system::error_code & err, size_t bytes_transferred) ;
        status_t validateMessage(InBoundRpcMessage& msg, exec::user::QueryResult& qr, std::string& valError);
        connectionStatus_t handleConnError(connectionStatus_t status, std::string msg);
        status_t handleQryError(status_t status, std::string msg, DrillClientQueryResult* pQueryResult);
        status_t handleQryError(status_t status, 
                const exec::shared::DrillPBError& e, 
                DrillClientQueryResult* pQueryResult);
        void broadcastError(DrillClientError* pErr);
        void clearMapEntries(DrillClientQueryResult* pQueryResult);
        void sendAck(InBoundRpcMessage& msg);
        void sendCancel(InBoundRpcMessage& msg);


        static RpcEncoder s_encoder;
        static RpcDecoder s_decoder;

        int32_t m_coordinationId;
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
        boost::asio::ip::tcp::socket m_socket;

        //for synchronous messages, like validate handshake
        DataBuf m_rbuf; // buffer for receiving synchronous messages
        DataBuf m_wbuf; // buffer for sending synchronous message

        // Mutex to protect drill client operations 
        boost::mutex m_dcMutex; 

        // Map of coordination id to  Query Ids.
        std::map<int, DrillClientQueryResult*> m_queryIds;

        // Map of query id to query result
        std::map<exec::shared::QueryId*, DrillClientQueryResult*, compareQueryId> m_queryResults;

};

inline bool DrillClientImpl::Active() {
    return this->m_bIsConnected;;
}

inline void DrillClientImpl::Close() {
    //TODO: cancel pending query
    if(this->m_bIsConnected){
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
        int connectToZookeeper(const char* connectStr);
        void close();
        static void watcher(zhandle_t *zzh, int type, int state, const char *path, void* context);
        void debugPrint();
        std::string& getError(){return m_err;}
        const exec::DrillbitEndpoint& getEndPoint(){ return m_drillServiceInstance.endpoint();}

    private:
        static char s_drillRoot[];
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
