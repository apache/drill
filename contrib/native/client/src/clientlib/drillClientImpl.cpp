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


#include "drill/common.hpp"
#include <queue>
#include <string.h>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#ifdef _WIN32
#include <zookeeper.h>
#else
#include <zookeeper/zookeeper.h>
#endif

#include "drill/drillClient.hpp"
#include "drill/recordBatch.hpp"
#include "drillClientImpl.hpp"
#include "errmsgs.hpp"
#include "logger.hpp"
#include "rpcEncoder.hpp"
#include "rpcDecoder.hpp"
#include "rpcMessage.hpp"
#include "utils.hpp"

#include "GeneralRPC.pb.h"
#include "UserBitShared.pb.h"

namespace Drill{

RpcEncoder DrillClientImpl::s_encoder;
RpcDecoder DrillClientImpl::s_decoder;

std::string debugPrintQid(const exec::shared::QueryId& qid){
    return std::string("[")+boost::lexical_cast<std::string>(qid.part1()) +std::string(":") + boost::lexical_cast<std::string>(qid.part2())+std::string("] ");
}

void setSocketTimeout(boost::asio::ip::tcp::socket& socket, int32_t timeout){
#if defined _WIN32
    int32_t timeoutMsecs=timeout*1000;
    setsockopt(socket.native(), SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeoutMsecs, sizeof(timeoutMsecs));
    setsockopt(socket.native(), SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeoutMsecs, sizeof(timeoutMsecs));
#else
    struct timeval tv;
    tv.tv_sec  = timeout;
    tv.tv_usec = 0;
    int e=0;
    e=setsockopt(socket.native(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    e=setsockopt(socket.native(), SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
#endif
}


void DrillClientImpl::parseConnectStr(const char* connectStr,
        std::string& pathToDrill,
        std::string& protocol,
        std::string& hostPortStr){
    char u[MAX_CONNECT_STR+1];
    strncpy(u,connectStr, MAX_CONNECT_STR); u[MAX_CONNECT_STR]=0;
    char* z=strtok(u, "=");
    char* c=strtok(NULL, "/");
    char* p=strtok(NULL, "");

    if(p!=NULL) pathToDrill=std::string("/")+p;
    protocol=z; hostPortStr=c;
    return;
}

connectionStatus_t DrillClientImpl::connect(const char* connStr){
    std::string pathToDrill, protocol, hostPortStr;
    std::string host;
    std::string port;
    if(!this->m_bIsConnected){
        parseConnectStr(connStr, pathToDrill, protocol, hostPortStr);
        if(!strcmp(protocol.c_str(), "zk")){
            ZookeeperImpl zook;
            if(zook.connectToZookeeper(hostPortStr.c_str(), pathToDrill.c_str())!=0){
                return handleConnError(CONN_ZOOKEEPER_ERROR, getMessage(ERR_CONN_ZOOKEEPER, zook.getError().c_str()));
            }
            zook.debugPrint();
            exec::DrillbitEndpoint e=zook.getEndPoint();
            host=boost::lexical_cast<std::string>(e.address());
            port=boost::lexical_cast<std::string>(e.user_port());
            zook.close();
        }else if(!strcmp(protocol.c_str(), "local")){
            char tempStr[MAX_CONNECT_STR+1];
            strncpy(tempStr, hostPortStr.c_str(), MAX_CONNECT_STR); tempStr[MAX_CONNECT_STR]=0;
            host=strtok(tempStr, ":");
            port=strtok(NULL, "");
        }else{
            return handleConnError(CONN_INVALID_INPUT, getMessage(ERR_CONN_UNKPROTO, protocol.c_str()));
        }
        return this->connect(host.c_str(), port.c_str());
    }
    return CONN_SUCCESS;
}

connectionStatus_t DrillClientImpl::connect(const char* host, const char* port){
    using boost::asio::ip::tcp;
    tcp::endpoint endpoint;
    try{
        tcp::resolver resolver(m_io_service);
        tcp::resolver::query query(tcp::v4(), host, port);
        tcp::resolver::iterator iter = resolver.resolve(query);
        tcp::resolver::iterator end;
        while (iter != end){
            endpoint = *iter++;
            DRILL_LOG(LOG_TRACE) << endpoint << std::endl;
        }
        boost::system::error_code ec;
        m_socket.connect(endpoint, ec);
        if(ec){
            return handleConnError(CONN_FAILURE, getMessage(ERR_CONN_FAILURE, host, port, ec.message().c_str()));
        }

    }catch(std::exception e){
        return handleConnError(CONN_FAILURE, getMessage(ERR_CONN_EXCEPT, e.what()));
    }

    //
    // We put some OS dependent code here for timing out a socket. Mostly, this appears to
    // do nothing. Should we leave it in there?
    //
    setSocketTimeout(m_socket, DrillClientConfig::getSocketTimeout());

    return CONN_SUCCESS;
}

connectionStatus_t DrillClientImpl::sendSync(OutBoundRpcMessage& msg){
    DrillClientImpl::s_encoder.Encode(m_wbuf, msg);
    boost::system::error_code ec;
    size_t s=m_socket.write_some(boost::asio::buffer(m_wbuf), ec);
    if(!ec && s!=0){
    return CONN_SUCCESS;
    }else{
        return handleConnError(CONN_FAILURE, getMessage(ERR_CONN_WFAIL, ec.message().c_str()));
    }
}

connectionStatus_t DrillClientImpl::recvHandshake(){
    if(m_rbuf==NULL){
        m_rbuf = Utils::allocateBuffer(MAX_SOCK_RD_BUFSIZE);
    }

    m_deadlineTimer.expires_from_now(boost::posix_time::seconds(DrillClientConfig::getSocketTimeout()));
    m_deadlineTimer.async_wait(boost::bind(
                &DrillClientImpl::handleHShakeReadTimeout,
                this,
                boost::asio::placeholders::error
                ));
    DRILL_LOG(LOG_TRACE) << "Started new handshake wait timer."  << std::endl;

    async_read(
            this->m_socket,
            boost::asio::buffer(m_rbuf, LEN_PREFIX_BUFLEN),
            boost::bind(
                &DrillClientImpl::handleHandshake,
                this,
                m_rbuf,
                boost::asio::placeholders::error,
                boost::asio::placeholders::bytes_transferred)
            );
    DRILL_LOG(LOG_DEBUG) << "Sent handshake read request to server" << std::endl;
    m_io_service.run();
    if(m_rbuf!=NULL){
        Utils::freeBuffer(m_rbuf, MAX_SOCK_RD_BUFSIZE); m_rbuf=NULL;
    }
    return CONN_SUCCESS;
}

void DrillClientImpl::handleHandshake(ByteBuf_t _buf,
        const boost::system::error_code& err,
        size_t bytes_transferred) {
    boost::system::error_code error=err;
    // cancel the timer
    m_deadlineTimer.cancel();
    DRILL_LOG(LOG_TRACE) << "Deadline timer cancelled."  << std::endl;
    if(!error){
        InBoundRpcMessage msg;
        uint32_t length = 0;
        int bytes_read = DrillClientImpl::s_decoder.LengthDecode(m_rbuf, &length);
        if(length>0){
            size_t leftover = LEN_PREFIX_BUFLEN - bytes_read;
            ByteBuf_t b=m_rbuf + LEN_PREFIX_BUFLEN;
            size_t bytesToRead=length - leftover;
            while(1){
                size_t dataBytesRead=m_socket.read_some(
                        boost::asio::buffer(b, bytesToRead),
                        error);
                if(err) break;
                DRILL_LOG(LOG_TRACE) << "Handshake Message: actual bytes read = " << dataBytesRead << std::endl;
                if(dataBytesRead==bytesToRead) break;
                bytesToRead-=dataBytesRead;
                b+=dataBytesRead;
            }
            DrillClientImpl::s_decoder.Decode(m_rbuf+bytes_read, length, msg);
        }else{
            handleConnError(CONN_FAILURE, getMessage(ERR_CONN_RDFAIL, "No handshake"));
        }
        exec::user::BitToUserHandshake b2u;
        b2u.ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
        this->m_handshakeVersion=b2u.rpc_version();

    }else{
        // boost error
        handleConnError(CONN_FAILURE, getMessage(ERR_CONN_RDFAIL, error.message().c_str()));
        return;
    }
    return;
}

void DrillClientImpl::handleHShakeReadTimeout(const boost::system::error_code & err){
    // if err == boost::asio::error::operation_aborted) then the caller cancelled the timer.
    if(!err){
        // Check whether the deadline has passed.
        if (m_deadlineTimer.expires_at() <= boost::asio::deadline_timer::traits_type::now()){
            // The deadline has passed.
            m_deadlineTimer.expires_at(boost::posix_time::pos_infin);
            DRILL_LOG(LOG_TRACE) << "Deadline timer expired."  << std::endl;
            boost::system::error_code ignorederr;
            m_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_send, ignorederr);
            m_socket.close();
        }
    }
    return;
}

bool DrillClientImpl::validateHandShake(const char* defaultSchema){

    DRILL_LOG(LOG_TRACE) << "validateHandShake\n";

    exec::user::UserToBitHandshake u2b;
    u2b.set_channel(exec::shared::USER);
    u2b.set_rpc_version(DRILL_RPC_VERSION);
    u2b.set_support_listening(true);

    if ( defaultSchema != NULL ){
        DRILL_LOG(LOG_TRACE) << "defaultSchema = " << defaultSchema << "\n";
        exec::user::UserProperties* userProperties = u2b.mutable_properties();
        exec::user::Property* connSchema = userProperties->add_properties();
        connSchema->set_key("schema");
        connSchema->set_value(defaultSchema);
    }
    {
        boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
        uint64_t coordId = this->getNextCoordinationId();

        OutBoundRpcMessage out_msg(exec::rpc::REQUEST, exec::user::HANDSHAKE, coordId, &u2b);
        sendSync(out_msg);
    }

    recvHandshake();
    this->m_io_service.reset();
    if(this->m_pError!=NULL){
        return false;
    }
    if(m_handshakeVersion != u2b.rpc_version()) {
        DRILL_LOG(LOG_TRACE) << "Invalid rpc version.  Expected << "
            << DRILL_RPC_VERSION << ", actual "<< m_handshakeVersion << "." << std::endl;
        handleConnError(CONN_HANDSHAKE_FAILED,
                getMessage(ERR_CONN_NOHSHAKE, DRILL_RPC_VERSION, m_handshakeVersion));
        return false;
    }
    return true;
}


FieldDefPtr DrillClientQueryResult::s_emptyColDefs( new (std::vector<Drill::FieldMetadata*>));

DrillClientQueryResult* DrillClientImpl::SubmitQuery(::exec::shared::QueryType t,
        const std::string& plan,
        pfnQueryResultsListener l,
        void* lCtx){
    exec::user::RunQuery query;
    query.set_results_mode(exec::user::STREAM_FULL);
    query.set_type(t);
    query.set_plan(plan);

    uint64_t coordId;
    DrillClientQueryResult* pQuery=NULL;
    {
        boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
        coordId = this->getNextCoordinationId();
        OutBoundRpcMessage out_msg(exec::rpc::REQUEST, exec::user::RUN_QUERY, coordId, &query);
        sendSync(out_msg);

        pQuery = new DrillClientQueryResult(this, coordId);
        pQuery->registerListener(l, lCtx);
        bool sendRequest=false;
        this->m_queryIds[coordId]=pQuery;

        DRILL_LOG(LOG_DEBUG)  << "Submit Query Request. Coordination id = " << coordId << std::endl;

        if(m_pendingRequests++==0){
            sendRequest=true;
        }else{
            DRILL_LOG(LOG_DEBUG) << "Queueing read request to server" << std::endl;
            DRILL_LOG(LOG_DEBUG) << "Number of pending requests = " << m_pendingRequests << std::endl;
        }
        if(sendRequest){
            DRILL_LOG(LOG_DEBUG) << "Sending read request. Number of pending requests = "
                << m_pendingRequests << std::endl;
            getNextResult(); // async wait for results
        }
    }

    //run this in a new thread
    {
        if(this->m_pListenerThread==NULL){
            DRILL_LOG(LOG_DEBUG) << "Starting listener thread." << std::endl;
            this->m_pListenerThread= new boost::thread(boost::bind(&boost::asio::io_service::run,
                        &this->m_io_service));
        }
    }
    return pQuery;
}

void DrillClientImpl::getNextResult(){

    // This call is always made from within a function where the mutex has already been acquired
    //boost::lock_guard<boost::mutex> lock(this->m_dcMutex);

    {
        boost::unique_lock<boost::mutex> memLock(AllocatedBuffer::s_memCVMutex);
        DRILL_LOG(LOG_TRACE) << "Read blocked waiting for memory." << std::endl;
        while(AllocatedBuffer::s_isBufferLimitReached){
            AllocatedBuffer::s_memCV.wait(memLock);
        }
    }
    //use free, not delete to free
    ByteBuf_t readBuf = Utils::allocateBuffer(LEN_PREFIX_BUFLEN);

    m_deadlineTimer.expires_from_now(boost::posix_time::seconds(DrillClientConfig::getSocketTimeout()));
    m_deadlineTimer.async_wait(boost::bind(
                &DrillClientImpl::handleReadTimeout,
                this,
                boost::asio::placeholders::error
                ));
    DRILL_LOG(LOG_TRACE) << "Started new async wait timer."  << std::endl;

    async_read(
            this->m_socket,
            boost::asio::buffer(readBuf, LEN_PREFIX_BUFLEN),
            boost::bind(
                &DrillClientImpl::handleRead,
                this,
                readBuf,
                boost::asio::placeholders::error,
                boost::asio::placeholders::bytes_transferred)
            );
    DRILL_LOG(LOG_DEBUG) << "Sent read request to server" << std::endl;
}

void DrillClientImpl::waitForResults(){
    this->m_pListenerThread->join();
    DRILL_LOG(LOG_DEBUG) << "Listener thread exited." << std::endl;
    delete this->m_pListenerThread; this->m_pListenerThread=NULL;
}

status_t DrillClientImpl::readMsg(ByteBuf_t _buf, 
        AllocatedBufferPtr* allocatedBuffer, 
        InBoundRpcMessage& msg, 
        boost::system::error_code& error){
    size_t leftover=0;
    uint32_t rmsgLen;
    AllocatedBufferPtr currentBuffer;
    *allocatedBuffer=NULL;
    {
        // We need to protect the readLength and read buffer, and the pending requests counter,
        // but we don't have to keep the lock while we decode the rest of the buffer.
        boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
        int bytes_read = DrillClientImpl::s_decoder.LengthDecode(_buf, &rmsgLen);
        DRILL_LOG(LOG_TRACE) << "len bytes = " << bytes_read << std::endl;
        DRILL_LOG(LOG_TRACE) << "rmsgLen = " << rmsgLen << std::endl;

        if(rmsgLen>0){
            leftover = LEN_PREFIX_BUFLEN - bytes_read;
            // Allocate a buffer
            DRILL_LOG(LOG_TRACE) << "Allocated and locked buffer." << std::endl;
            currentBuffer=new AllocatedBuffer(rmsgLen);
            if(currentBuffer==NULL){
                Utils::freeBuffer(_buf, LEN_PREFIX_BUFLEN);
                return handleQryError(QRY_CLIENT_OUTOFMEM, getMessage(ERR_QRY_OUTOFMEM), NULL);
            }
            *allocatedBuffer=currentBuffer;
            if(leftover){
                memcpy(currentBuffer->m_pBuffer, _buf + bytes_read, leftover);
            }
            DRILL_LOG(LOG_TRACE) << "reading data (rmsgLen - leftover) : "
                << (rmsgLen - leftover) << std::endl;
            ByteBuf_t b=currentBuffer->m_pBuffer + leftover;
            size_t bytesToRead=rmsgLen - leftover;
            while(1){
                size_t dataBytesRead=this->m_socket.read_some(
                        boost::asio::buffer(b, bytesToRead),
                        error);
                if(error) break;
                DRILL_LOG(LOG_TRACE) << "Data Message: actual bytes read = " << dataBytesRead << std::endl;
                if(dataBytesRead==bytesToRead) break;
                bytesToRead-=dataBytesRead;
                b+=dataBytesRead;
            }
            if(!error){
                // read data successfully
                DrillClientImpl::s_decoder.Decode(currentBuffer->m_pBuffer, rmsgLen, msg);
                DRILL_LOG(LOG_TRACE) << "Done decoding chunk. Coordination id: " <<msg.m_coord_id<< std::endl;
            }else{
                Utils::freeBuffer(_buf, LEN_PREFIX_BUFLEN);
                return handleQryError(QRY_COMM_ERROR,
                        getMessage(ERR_QRY_COMMERR, error.message().c_str()), NULL);
            }
        }else{
            // got a message with an invalid read length.
            Utils::freeBuffer(_buf, LEN_PREFIX_BUFLEN);
            return handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVREADLEN), NULL);
        }
    }
    Utils::freeBuffer(_buf, LEN_PREFIX_BUFLEN);
    return QRY_SUCCESS;
}

status_t DrillClientImpl::processQueryResult(AllocatedBufferPtr  allocatedBuffer, InBoundRpcMessage& msg ){
    DrillClientQueryResult* pDrillClientQueryResult=NULL;
    status_t ret=QRY_SUCCESS;
    exec::shared::QueryId qid;
    {
        boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
        exec::shared::QueryResult* qr = new exec::shared::QueryResult; //Record Batch will own this object and free it up.

        DRILL_LOG(LOG_DEBUG) << "Processing Query Result " << std::endl;
        qr->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
        DRILL_LOG(LOG_TRACE) << qr->DebugString() << std::endl;

        DRILL_LOG(LOG_DEBUG) << "Searching for Query Id - " << debugPrintQid(qr->query_id()) << std::endl;

        qid.CopyFrom(qr->query_id());
        std::map<exec::shared::QueryId*, DrillClientQueryResult*, compareQueryId>::iterator it;
        it=this->m_queryResults.find(&qid);
        if(it!=this->m_queryResults.end()){
            pDrillClientQueryResult=(*it).second;
        }else{
            assert(0);
            //assert might be compiled away in a release build. So return an error to the app.
            status_t ret= handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_OUTOFORDER), NULL);
            delete qr;
            return ret;
        }
        DRILL_LOG(LOG_DEBUG) << "Drill Client Query Result Query Id - " <<
            debugPrintQid(*pDrillClientQueryResult->m_pQueryId)
            << std::endl;
        //Check QueryResult.queryState. QueryResult could have an error.
        if(qr->query_state() == exec::shared::QueryResult_QueryState_FAILED){
            status_t ret=handleQryError(QRY_FAILURE, qr->error(0), pDrillClientQueryResult);
            delete allocatedBuffer;
            delete qr;
            return ret;
        }
        //Validate the RPC message
        std::string valErr;
        if( (ret=validateMessage(msg, *qr, valErr)) != QRY_SUCCESS){
            delete allocatedBuffer;
            delete qr;
            return handleQryError(ret, getMessage(ERR_QRY_INVRPC, valErr.c_str()), pDrillClientQueryResult);
        }

        //Build Record Batch here
        DRILL_LOG(LOG_TRACE) << qr->DebugString() << std::endl;

        RecordBatch* pRecordBatch= new RecordBatch(qr, allocatedBuffer,  msg.m_dbody);
        pDrillClientQueryResult->m_numBatches++;

        DRILL_LOG(LOG_TRACE) << "Allocated new Record batch." << (void*)pRecordBatch << std::endl;
        pRecordBatch->build();
        DRILL_LOG(LOG_DEBUG) << debugPrintQid(qr->query_id())<<"recordBatch.numRecords "
            << pRecordBatch->getNumRecords()  << std::endl;
        DRILL_LOG(LOG_DEBUG) << debugPrintQid(qr->query_id())<<"recordBatch.numFields "
            << pRecordBatch->getNumFields()  << std::endl;
        DRILL_LOG(LOG_DEBUG) << debugPrintQid(qr->query_id())<<"recordBatch.isLastChunk "
            << pRecordBatch->isLastChunk()  << std::endl;

        ret=pDrillClientQueryResult->setupColumnDefs(qr);
        if(ret==QRY_SUCCESS_WITH_INFO)pRecordBatch->schemaChanged(true);

        pDrillClientQueryResult->m_bIsQueryPending=true;
        pDrillClientQueryResult->m_bIsLastChunk=qr->is_last_chunk();
        pfnQueryResultsListener pResultsListener=pDrillClientQueryResult->m_pResultsListener;
        if(pResultsListener!=NULL){
            ret = pResultsListener(pDrillClientQueryResult, pRecordBatch, NULL);
        }else{
            //Use a default callback that is called when a record batch is received
            ret = pDrillClientQueryResult->defaultQueryResultsListener(pDrillClientQueryResult,
                    pRecordBatch, NULL);
        }
    } // release lock
    if(ret==QRY_FAILURE){
        sendCancel(&qid);
        {
            boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
            m_pendingRequests--;
        }
        pDrillClientQueryResult->m_bIsQueryPending=false;
        DRILL_LOG(LOG_DEBUG) << "Client app cancelled query." << std::endl;
        return ret;
    }
    if(pDrillClientQueryResult->m_bIsLastChunk){
        {
            boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
            m_pendingRequests--;
            DRILL_LOG(LOG_DEBUG) << debugPrintQid(*pDrillClientQueryResult->m_pQueryId)
                <<  "Received last batch. " << std::endl;
            DRILL_LOG(LOG_DEBUG) << debugPrintQid(*pDrillClientQueryResult->m_pQueryId)
                << "Pending requests: " << m_pendingRequests <<"." << std::endl;
        }
        ret=QRY_NO_MORE_DATA;
        sendAck(msg, true);
        return ret;
    }
    sendAck(msg, true);
    return ret;
}

status_t DrillClientImpl::processQueryId(AllocatedBufferPtr allocatedBuffer, InBoundRpcMessage& msg ){
    DrillClientQueryResult* pDrillClientQueryResult=NULL;
    DRILL_LOG(LOG_DEBUG) << "Processing Query Handle with coordination id:" << msg.m_coord_id << std::endl;
    status_t ret=QRY_SUCCESS;

    boost::lock_guard<boost::mutex> lock(m_dcMutex);
    std::map<int,DrillClientQueryResult*>::iterator it;
    it=this->m_queryIds.find(msg.m_coord_id);
    if(it!=this->m_queryIds.end()){
        pDrillClientQueryResult=(*it).second;
        exec::shared::QueryId *qid = new exec::shared::QueryId;
        DRILL_LOG(LOG_TRACE)  << "Received Query Handle" << msg.m_pbody.size() << std::endl;
        qid->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
        DRILL_LOG(LOG_TRACE) << qid->DebugString() << std::endl;
        m_queryResults[qid]=pDrillClientQueryResult;
        //save queryId allocated here so we can free it later
        pDrillClientQueryResult->setQueryId(qid);
    }else{
        delete allocatedBuffer;
        return handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVQUERYID), NULL);
    }
    delete allocatedBuffer;
    return ret;
}

void DrillClientImpl::handleReadTimeout(const boost::system::error_code & err){
    // if err == boost::asio::error::operation_aborted) then the caller cancelled the timer.
    if(!err){
        // Check whether the deadline has passed.
        if (m_deadlineTimer.expires_at() <= boost::asio::deadline_timer::traits_type::now()){
            // The deadline has passed.
            handleQryError(QRY_COMM_ERROR, getMessage(ERR_QRY_TIMOUT), NULL);
            // There is no longer an active deadline. The expiry is set to positive
            // infinity so that the timer never expires until a new deadline is set.
            // Note that at this time, the caller is not in a (async) wait for the timer.
            m_deadlineTimer.expires_at(boost::posix_time::pos_infin);
            DRILL_LOG(LOG_TRACE) << "Deadline timer expired."  << std::endl;
            // Cancel all pending async IOs.
            // The cancel call _MAY_ not work on all platforms. To be a little more reliable we need
            // to have the BOOST_ASIO_ENABLE_CANCELIO macro (as well as the BOOST_ASIO_DISABLE_IOCP macro?)
            // defined. To be really sure, we need to close the socket. Closing the socket is a bit
            // drastic and we will defer that till a later release.
            m_socket.cancel();
        }
    }
    return;
}

void DrillClientImpl::handleRead(ByteBuf_t _buf,
        const boost::system::error_code& err,
        size_t bytes_transferred) {
    boost::system::error_code error=err;
    // cancel the timer
    m_deadlineTimer.cancel();
    DRILL_LOG(LOG_TRACE) << "Deadline timer cancelled."  << std::endl;
    if(!error){
        InBoundRpcMessage msg;

        DRILL_LOG(LOG_TRACE) << "Getting new message" << std::endl;
        AllocatedBufferPtr allocatedBuffer=NULL;

        if(readMsg(_buf, &allocatedBuffer, msg, error)!=QRY_SUCCESS){
            if(m_pendingRequests!=0){
                boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
                getNextResult();
            }
            return;
        }

        if(!error && msg.m_rpc_type==exec::user::QUERY_RESULT){
            if(processQueryResult(allocatedBuffer, msg)!=QRY_SUCCESS){
                if(m_pendingRequests!=0){
                    boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
                    getNextResult();
                }
                return;
            }
        }else if(!error && msg.m_rpc_type==exec::user::QUERY_HANDLE){
            if(processQueryId(allocatedBuffer, msg)!=QRY_SUCCESS){
                if(m_pendingRequests!=0){
                    boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
                    getNextResult();
                }
                return;
            }
        }else{
            boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
            if(error){
                // We have a socket read error, but we do not know which query this is for.
                // Signal ALL pending queries that they should stop waiting.
                DRILL_LOG(LOG_TRACE) << "read error: " << error << std::endl;
                handleQryError(QRY_COMM_ERROR, getMessage(ERR_QRY_COMMERR, error.message().c_str()), NULL);
                return;
            }else{
                //If not QUERY_RESULT, then we think something serious has gone wrong?
                assert(0);
                DRILL_LOG(LOG_TRACE) << "QueryResult returned " << msg.m_rpc_type << std::endl;
                handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVRPCTYPE, msg.m_rpc_type), NULL);
                return;
            }
        }
        {
            boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
            getNextResult();
        }
    }else{
        // boost error
        Utils::freeBuffer(_buf, LEN_PREFIX_BUFLEN);
        boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
        handleQryError(QRY_COMM_ERROR, getMessage(ERR_QRY_COMMERR, error.message().c_str()), NULL);
        return;
    }
    return;
}

status_t DrillClientImpl::validateMessage(InBoundRpcMessage& msg, exec::shared::QueryResult& qr, std::string& valErr){
    if(msg.m_mode == exec::rpc::RESPONSE_FAILURE){
        valErr=getMessage(ERR_QRY_RESPFAIL);
        return QRY_FAILURE;
    }
    if(qr.query_state()== exec::shared::QueryResult_QueryState_UNKNOWN_QUERY){
        valErr=getMessage(ERR_QRY_UNKQRY);
        return QRY_FAILURE;
    }
    if(qr.query_state()== exec::shared::QueryResult_QueryState_CANCELED){
        valErr=getMessage(ERR_QRY_CANCELED);
        return QRY_FAILURE;
    }
    if(qr.def().carries_two_byte_selection_vector() == true){
        valErr=getMessage(ERR_QRY_SELVEC2);
        return QRY_FAILURE;
    }
    return QRY_SUCCESS;
}

connectionStatus_t DrillClientImpl::handleConnError(connectionStatus_t status, std::string msg){
    DrillClientError* pErr = new DrillClientError(status, DrillClientError::CONN_ERROR_START+status, msg);
    m_pendingRequests=0;
    if(m_pError!=NULL){ delete m_pError; m_pError=NULL;}
    m_pError=pErr;
    broadcastError(this->m_pError);
    return status;
}

status_t DrillClientImpl::handleQryError(status_t status, std::string msg, DrillClientQueryResult* pQueryResult){
    DrillClientError* pErr = new DrillClientError(status, DrillClientError::QRY_ERROR_START+status, msg);
    if(m_pError!=NULL){ delete m_pError; m_pError=NULL;}
    m_pError=pErr;
    if(pQueryResult!=NULL){
        m_pendingRequests--;
        pQueryResult->signalError(pErr);
    }else{
        m_pendingRequests=0;
        broadcastError(this->m_pError);
    }
    return status;
}

status_t DrillClientImpl::handleQryError(status_t status,
        const exec::shared::DrillPBError& e,
        DrillClientQueryResult* pQueryResult){
    assert(pQueryResult!=NULL);
    this->m_pError = DrillClientError::getErrorObject(e);
    pQueryResult->signalError(this->m_pError);
    m_pendingRequests--;
    return status;
}

void DrillClientImpl::broadcastError(DrillClientError* pErr){
    if(pErr!=NULL){
        std::map<int, DrillClientQueryResult*>::iterator iter;
        if(!m_queryIds.empty()){
            for(iter = m_queryIds.begin(); iter != m_queryIds.end(); iter++) {
                iter->second->signalError(pErr);
            }
        }
    }
    return;
}

void DrillClientImpl::clearMapEntries(DrillClientQueryResult* pQueryResult){
    std::map<int, DrillClientQueryResult*>::iterator iter;
    boost::lock_guard<boost::mutex> lock(m_dcMutex);
    if(!m_queryIds.empty()){
        for(iter=m_queryIds.begin(); iter!=m_queryIds.end(); iter++) {
            if(pQueryResult==(DrillClientQueryResult*)iter->second){
                m_queryIds.erase(iter->first);
                break;
            }
        }
    }
    if(!m_queryResults.empty()){
        std::map<exec::shared::QueryId*, DrillClientQueryResult*, compareQueryId>::iterator it;
        for(it=m_queryResults.begin(); it!=m_queryResults.end(); it++) {
            if(pQueryResult==(DrillClientQueryResult*)it->second){
                m_queryResults.erase(it->first);
                break;
            }
        }
    }
}

void DrillClientImpl::sendAck(InBoundRpcMessage& msg, bool isOk){
    exec::rpc::Ack ack;
    ack.set_ok(isOk);
    OutBoundRpcMessage ack_msg(exec::rpc::RESPONSE, exec::user::ACK, msg.m_coord_id, &ack);
    boost::lock_guard<boost::mutex> lock(m_dcMutex);
    sendSync(ack_msg);
    DRILL_LOG(LOG_TRACE) << "ACK sent" << std::endl;
}

void DrillClientImpl::sendCancel(exec::shared::QueryId* pQueryId){
    boost::lock_guard<boost::mutex> lock(m_dcMutex);
    uint64_t coordId = this->getNextCoordinationId();
    OutBoundRpcMessage cancel_msg(exec::rpc::REQUEST, exec::user::CANCEL_QUERY, coordId, pQueryId);
    sendSync(cancel_msg);
    DRILL_LOG(LOG_TRACE) << "CANCEL sent" << std::endl;
}

// This COPIES the FieldMetadata definition for the record batch.  ColumnDefs held by this
// class are used by the async callbacks.
status_t DrillClientQueryResult::setupColumnDefs(exec::shared::QueryResult* pQueryResult) {
    bool hasSchemaChanged=false;
    bool isFirstIter=false;
    boost::lock_guard<boost::mutex> schLock(this->m_schemaMutex);

    FieldDefPtr prevSchema=this->m_columnDefs;
    isFirstIter=this->m_numBatches==1?true:false;
    std::map<std::string, Drill::FieldMetadata*> oldSchema;
    if(!m_columnDefs->empty()){
        for(std::vector<Drill::FieldMetadata*>::iterator it = prevSchema->begin(); it != prevSchema->end(); ++it){
            // the key is the field_name + type
            char type[256];
            sprintf(type, ":%d:%d",(*it)->getMinorType(), (*it)->getDataMode() );
            std::string k= (*it)->getName()+type;
            oldSchema[k]=*it;
        }
    }
    m_columnDefs->clear();
    size_t numFields=pQueryResult->def().field_size();
    for(size_t i=0; i<numFields; i++){
        Drill::FieldMetadata* fmd= new Drill::FieldMetadata;
        fmd->set(pQueryResult->def().field(i));
        this->m_columnDefs->push_back(fmd);

        //Look for changes in the vector and trigger a Schema change event if necessary.
        //If vectors are different, then call the schema change listener.
        char type[256];
        sprintf(type, ":%d:%d",fmd->getMinorType(), fmd->getDataMode() );
        std::string k= fmd->getName()+type;
        std::map<std::string, Drill::FieldMetadata*>::iterator iter=oldSchema.find(k);
        if(iter==oldSchema.end()){
            // not found
            hasSchemaChanged=true;
        }else{
            oldSchema.erase(iter);
        }
    }
    if(oldSchema.size()>0){
        hasSchemaChanged=true;
    }

    //free memory allocated for FieldMetadata objects saved in previous columnDefs;
    if(!prevSchema->empty()){
        for(std::vector<Drill::FieldMetadata*>::iterator it = prevSchema->begin(); it != prevSchema->end(); ++it){
            delete *it;
        }
    }
    prevSchema->clear();
    this->m_bHasSchemaChanged=hasSchemaChanged&&!isFirstIter;
    if(this->m_bHasSchemaChanged){
        //invoke schema change Listener
        if(m_pSchemaListener!=NULL){
            m_pSchemaListener(this, m_columnDefs, NULL);
        }
    }
    return this->m_bHasSchemaChanged?QRY_SUCCESS_WITH_INFO:QRY_SUCCESS;
}

status_t DrillClientQueryResult::defaultQueryResultsListener(void* ctx,
        RecordBatch* b,
        DrillClientError* err) {
    //ctx; // unused, we already have the this pointer
    DRILL_LOG(LOG_TRACE) << "Query result listener called" << std::endl;
    //check if the query has been canceled. IF so then return FAILURE. Caller will send cancel to the server.
    if(this->m_bCancel){
        return QRY_FAILURE;
    }
    if (!err) {
        // signal the cond var
        {
            #ifdef DEBUG
            DRILL_LOG(LOG_DEBUG)<<debugPrintQid(b->getQueryResult()->query_id())
                << "Query result listener saved result to queue." << std::endl;
            #endif
            boost::lock_guard<boost::mutex> cvLock(this->m_cvMutex);
            this->m_recordBatches.push(b);
            this->m_bHasData=true;
        }
        m_cv.notify_one();
    }else{
        return QRY_FAILURE;
    }
    return QRY_SUCCESS;
}

RecordBatch*  DrillClientQueryResult::peekNext(){
    RecordBatch* pRecordBatch=NULL;
    //if no more data, return NULL;
    if(!m_bIsQueryPending) return NULL;
    boost::unique_lock<boost::mutex> cvLock(this->m_cvMutex);
    DRILL_LOG(LOG_TRACE) << "Synchronous read waiting for data." << std::endl;
    while(!this->m_bHasData && !m_bHasError) {
        this->m_cv.wait(cvLock);
    }
    // READ but not remove first element from queue
    pRecordBatch = this->m_recordBatches.front();
    return pRecordBatch;
}

RecordBatch*  DrillClientQueryResult::getNext() {
    RecordBatch* pRecordBatch=NULL;
    //if no more data, return NULL;
    if(!m_bIsQueryPending){
        DRILL_LOG(LOG_TRACE) << "Query is done." << std::endl;
        if(!m_recordBatches.empty()){
            DRILL_LOG(LOG_TRACE) << " But there is a Record batch left behind." << std::endl;
        }
        return NULL;
    }

    boost::unique_lock<boost::mutex> cvLock(this->m_cvMutex);
    DRILL_LOG(LOG_TRACE) << "Synchronous read waiting for data." << std::endl;
    while(!this->m_bHasData && !m_bHasError){
        this->m_cv.wait(cvLock);
    }
    // remove first element from queue
    pRecordBatch = this->m_recordBatches.front();
    this->m_recordBatches.pop();
    this->m_bHasData=!this->m_recordBatches.empty();
    // if vector is empty, set m_bHasDataPending to false;
    m_bIsQueryPending=!(this->m_recordBatches.empty()&&m_bIsLastChunk);
    return pRecordBatch;
}

// Blocks until data is available
void DrillClientQueryResult::waitForData() {
    //if no more data, return NULL;
    if(!m_bIsQueryPending) return;
    boost::unique_lock<boost::mutex> cvLock(this->m_cvMutex);
    while(!this->m_bHasData && !m_bHasError) {
        this->m_cv.wait(cvLock);
    }
}

void DrillClientQueryResult::cancel() {
    this->m_bCancel=true;
}

void DrillClientQueryResult::signalError(DrillClientError* pErr){
    // Ignore return values from the listener.
    if(pErr!=NULL){
        m_pError=pErr;
        pfnQueryResultsListener pResultsListener=this->m_pResultsListener;
        if(pResultsListener!=NULL){
            pResultsListener(this, NULL, pErr);
        }else{
            defaultQueryResultsListener(this, NULL, pErr);
        }
        m_bIsQueryPending=false;
        {
            boost::lock_guard<boost::mutex> cvLock(this->m_cvMutex);
            m_bHasData=false;
            m_bHasError=true;
        }
        //Signal the cv in case there is a client waiting for data already.
        m_cv.notify_one();
    }
    return;
}

void DrillClientQueryResult::clearAndDestroy(){
    //free memory allocated for FieldMetadata objects saved in m_columnDefs;
    if(!m_columnDefs->empty()){
        for(std::vector<Drill::FieldMetadata*>::iterator it = m_columnDefs->begin(); it != m_columnDefs->end(); ++it){
            delete *it;
        }
        m_columnDefs->clear();
    }
    //Tell the parent to remove this from it's lists
    m_pClient->clearMapEntries(this);

    //clear query id map entries.
    if(this->m_pQueryId!=NULL){
        delete this->m_pQueryId; this->m_pQueryId=NULL;
    }
    if(!m_recordBatches.empty()){
        // When multiple qwueries execute in parallel we sometimes get an empty record batch back from the servrer _after_
        // the last chunk has been received. We eventually delete it.
        DRILL_LOG(LOG_TRACE) << "Freeing Record batch(es) left behind "<< std::endl;
        RecordBatch* pR=NULL;
        while(!m_recordBatches.empty()){
            pR=m_recordBatches.front();
            m_recordBatches.pop();
            delete pR;
        }
    }
}

char ZookeeperImpl::s_drillRoot[]="/drill/";
char ZookeeperImpl::s_defaultCluster[]="drillbits1";

ZookeeperImpl::ZookeeperImpl(){
    m_pDrillbits=new String_vector;
    srand (time(NULL));
    m_bConnecting=true;
    memset(&m_id, 0, sizeof(m_id));
}

ZookeeperImpl::~ZookeeperImpl(){
    delete m_pDrillbits;
}

ZooLogLevel ZookeeperImpl::getZkLogLevel(){
    //typedef enum {ZOO_LOG_LEVEL_ERROR=1,
    //    ZOO_LOG_LEVEL_WARN=2,
    //    ZOO_LOG_LEVEL_INFO=3,
    //    ZOO_LOG_LEVEL_DEBUG=4
    //} ZooLogLevel;
    switch(DrillClientConfig::getLogLevel()){
        case LOG_TRACE:
        case LOG_DEBUG:
            return ZOO_LOG_LEVEL_DEBUG;
        case LOG_INFO:
            return ZOO_LOG_LEVEL_INFO;
        case LOG_WARNING:
            return ZOO_LOG_LEVEL_WARN;
        case LOG_ERROR:
        case LOG_FATAL:
        default:
            return ZOO_LOG_LEVEL_ERROR;
    }
    return ZOO_LOG_LEVEL_ERROR;
}

int ZookeeperImpl::connectToZookeeper(const char* connectStr, const char* pathToDrill){
    uint32_t waitTime=30000; // 10 seconds
    zoo_set_debug_level(getZkLogLevel());
    zoo_deterministic_conn_order(1); // enable deterministic order
    m_zh = zookeeper_init(connectStr, watcher, waitTime, 0, this, 0);
    if(!m_zh) {
        m_err = getMessage(ERR_CONN_ZKFAIL);
        return CONN_FAILURE;
    }else{
        m_err="";
        //Wait for the completion handler to signal successful connection
        boost::unique_lock<boost::mutex> bufferLock(this->m_cvMutex);
        boost::system_time const timeout=boost::get_system_time()+ boost::posix_time::milliseconds(waitTime);
        while(this->m_bConnecting) {
            if(!this->m_cv.timed_wait(bufferLock, timeout)){
                m_err = getMessage(ERR_CONN_ZKTIMOUT);
                return CONN_FAILURE;
            }
        }
    }
    if(m_state!=ZOO_CONNECTED_STATE){
        return CONN_FAILURE;
    }
    int rc = ZOK;
    char rootDir[MAX_CONNECT_STR+1];
    if(pathToDrill==NULL || strlen(pathToDrill)==0){
        strcpy(rootDir, (char*)s_drillRoot);
        strcat(rootDir, s_defaultCluster);
    }else{
        strncpy(rootDir, pathToDrill, MAX_CONNECT_STR); rootDir[MAX_CONNECT_STR]=0;
    }
    rc=zoo_get_children(m_zh, (char*)rootDir, 0, m_pDrillbits);
    if(rc!=ZOK){
        m_err=getMessage(ERR_CONN_ZKERR, rc);
        zookeeper_close(m_zh);
        return -1;
    }

    //Let's pick a random drillbit.
    if(m_pDrillbits && m_pDrillbits->count >0){
        int r=rand()%(this->m_pDrillbits->count);
        assert(r<this->m_pDrillbits->count);
        char * bit=this->m_pDrillbits->data[r];
        std::string s;
        s=rootDir +  std::string("/") + bit;
        int buffer_len=MAX_CONNECT_STR;
        char buffer[MAX_CONNECT_STR+1];
        struct Stat stat;
        buffer[MAX_CONNECT_STR]=0;
        rc= zoo_get(m_zh, s.c_str(), 0, buffer,  &buffer_len, &stat);
        if(rc!=ZOK){
            m_err=getMessage(ERR_CONN_ZKDBITERR, rc);
            zookeeper_close(m_zh);
            return -1;
        }
        m_drillServiceInstance.ParseFromArray(buffer, buffer_len);
    }else{
        m_err=getMessage(ERR_CONN_ZKNODBIT);
        zookeeper_close(m_zh);
        return -1;
    }
    return 0;
}

void ZookeeperImpl::close(){
    zookeeper_close(m_zh);
}

void ZookeeperImpl::watcher(zhandle_t *zzh, int type, int state, const char *path, void* context) {
    //From cli.c

    /* Be careful using zh here rather than zzh - as this may be mt code
     * the client lib may call the watcher before zookeeper_init returns */

    ZookeeperImpl* self=(ZookeeperImpl*)context;
    self->m_state=state;
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
        } else if (state == ZOO_AUTH_FAILED_STATE) {
            self->m_err= getMessage(ERR_CONN_ZKNOAUTH);
            zookeeper_close(zzh);
            self->m_zh=0;
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            self->m_err= getMessage(ERR_CONN_ZKEXP);
            zookeeper_close(zzh);
            self->m_zh=0;
        }
    }
    // signal the cond var
    {
        if (state == ZOO_CONNECTED_STATE){
            DRILL_LOG(LOG_TRACE) << "Connected to Zookeeper." << std::endl;
        }
        boost::lock_guard<boost::mutex> bufferLock(self->m_cvMutex);
        self->m_bConnecting=false;
    }
    self->m_cv.notify_one();
}

void ZookeeperImpl:: debugPrint(){
    if(m_zh!=NULL && m_state==ZOO_CONNECTED_STATE){
        DRILL_LOG(LOG_TRACE) << m_drillServiceInstance.DebugString() << std::endl;
    }
}

} // namespace Drill
