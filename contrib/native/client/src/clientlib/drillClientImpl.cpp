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
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#ifdef _WIN32
#include <zookeeper.h>
#else
#include <zookeeper/zookeeper.h>
#endif
#include <boost/assign.hpp>

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

static std::map<exec::shared::QueryResult_QueryState, status_t> QUERYSTATE_TO_STATUS_MAP = boost::assign::map_list_of
    (exec::shared::QueryResult_QueryState_STARTING, QRY_PENDING)
    (exec::shared::QueryResult_QueryState_RUNNING, QRY_RUNNING)
    (exec::shared::QueryResult_QueryState_COMPLETED, QRY_COMPLETED)
    (exec::shared::QueryResult_QueryState_CANCELED, QRY_CANCELED)
    (exec::shared::QueryResult_QueryState_FAILED, QRY_FAILED)
    ;

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

connectionStatus_t DrillClientImpl::connect(const char* connStr){
    std::string pathToDrill, protocol, hostPortStr;
    std::string host;
    std::string port;
    if(!this->m_bIsConnected){
        m_connectStr=connStr;
        Utils::parseConnectStr(connStr, pathToDrill, protocol, hostPortStr);
        if(!strcmp(protocol.c_str(), "zk")){
            ZookeeperImpl zook;
            std::vector<std::string> drillbits;
            int err = zook.getAllDrillbits(hostPortStr.c_str(), pathToDrill.c_str(), drillbits);
            if(!err){
                Utils::shuffle(drillbits);
                exec::DrillbitEndpoint endpoint;
                err = zook.getEndPoint(drillbits, drillbits.size()-1, endpoint);// get the last one in the list
                if(!err){
                    host=boost::lexical_cast<std::string>(endpoint.address());
                    port=boost::lexical_cast<std::string>(endpoint.user_port());
                }
            }
            if(err){
                return handleConnError(CONN_ZOOKEEPER_ERROR, getMessage(ERR_CONN_ZOOKEEPER, zook.getError().c_str()));
            }
            zook.close();
            m_bIsDirectConnection=true;  
        }else if(!strcmp(protocol.c_str(), "local")){
            boost::lock_guard<boost::mutex> lock(m_dcMutex);//strtok is not reentrant
            char tempStr[MAX_CONNECT_STR+1];
            strncpy(tempStr, hostPortStr.c_str(), MAX_CONNECT_STR); tempStr[MAX_CONNECT_STR]=0;
            host=strtok(tempStr, ":");
            port=strtok(NULL, "");
            m_bIsDirectConnection=false;  
        }else{
            return handleConnError(CONN_INVALID_INPUT, getMessage(ERR_CONN_UNKPROTO, protocol.c_str()));
        }
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Connecting to endpoint: " << host << ":" << port << std::endl;)
        connectionStatus_t ret = this->connect(host.c_str(), port.c_str());
        return ret;
    }else if(std::strcmp(connStr, m_connectStr.c_str())){ // tring to connect to a different address is not allowed if already connected
        return handleConnError(CONN_ALREADYCONNECTED, getMessage(ERR_CONN_ALREADYCONN));
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
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << endpoint << std::endl;)
        }
        boost::system::error_code ec;
        m_socket.connect(endpoint, ec);
        if(ec){
            return handleConnError(CONN_FAILURE, getMessage(ERR_CONN_FAILURE, host, port, ec.message().c_str()));
        }

    }catch(std::exception e){
        // Handle case when the hostname cannot be resolved. "resolve" is hard-coded in boost asio resolver.resolve
        if (!strcmp(e.what(), "resolve")) {
            return handleConnError(CONN_HOSTNAME_RESOLUTION_ERROR, getMessage(ERR_CONN_EXCEPT, e.what()));
        }
        return handleConnError(CONN_FAILURE, getMessage(ERR_CONN_EXCEPT, e.what()));
    }

    m_bIsConnected=true;
    // set socket keep alive
    boost::asio::socket_base::keep_alive keepAlive(true);
    m_socket.set_option(keepAlive);
	// set no_delay
    boost::asio::ip::tcp::no_delay noDelay(true);
    m_socket.set_option(noDelay);

    std::ostringstream connectedHost;
    connectedHost << "id: " << m_socket.native_handle() << " address: " << host << ":" << port;
    m_connectedHost = connectedHost.str();
    DRILL_MT_LOG(DRILL_LOG(LOG_INFO) << "Connected to endpoint: " << m_connectedHost << std::endl;)
    
    return CONN_SUCCESS;
}

void DrillClientImpl::startHeartbeatTimer(){
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Started new heartbeat timer with "
        << DrillClientConfig::getHeartbeatFrequency() << " seconds." << std::endl;)
    m_heartbeatTimer.expires_from_now(boost::posix_time::seconds(DrillClientConfig::getHeartbeatFrequency()));
    m_heartbeatTimer.async_wait(boost::bind(
                &DrillClientImpl::handleHeartbeatTimeout,
                this,
                boost::asio::placeholders::error
                ));
        startMessageListener(); // start this thread early so we don't have the timer blocked
}

connectionStatus_t DrillClientImpl::sendHeartbeat(){
    connectionStatus_t status=CONN_SUCCESS;
    exec::rpc::Ack ack;
    ack.set_ok(true);
    OutBoundRpcMessage heartbeatMsg(exec::rpc::PING, exec::user::ACK/*can be anything */, 0, &ack);
    boost::lock_guard<boost::mutex> prLock(this->m_prMutex);
    boost::lock_guard<boost::mutex> lock(m_dcMutex);
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Heartbeat sent." << std::endl;)
    status=sendSync(heartbeatMsg);
    status=status==CONN_SUCCESS?status:CONN_DEAD;
    //If the server sends responses to a heartbeat, we need to increment the pending requests counter.
    if(m_pendingRequests++==0){
        getNextResult(); // async wait for results
    }
    return status;
}

void DrillClientImpl::resetHeartbeatTimer(){
    m_heartbeatTimer.cancel();
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Reset Heartbeat timer." << std::endl;)
    startHeartbeatTimer();
}

void DrillClientImpl::handleHeartbeatTimeout(const boost::system::error_code & err){
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl:: Heartbeat timer expired." << std::endl;)
    if(err != boost::asio::error::operation_aborted){
        // Check whether the deadline has passed.
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::Heartbeat Timer -  Expires at: " 
            << to_simple_string(m_heartbeatTimer.expires_at())
            << " and time now is: "
            << to_simple_string(boost::asio::deadline_timer::traits_type::now())
            << std::endl;)
            ;
        if (m_heartbeatTimer.expires_at() <= boost::asio::deadline_timer::traits_type::now()){
            // The deadline has passed.
            m_heartbeatTimer.expires_at(boost::posix_time::pos_infin);
            if(sendHeartbeat()==CONN_SUCCESS){
                startHeartbeatTimer();
            }else{
                // Close connection.
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl:: No heartbeat. Closing connection.";)
                shutdownSocket();
                //broadcast to any executing queries
                handleConnError(CONN_FAILURE, getMessage(ERR_QRY_COMMERR, "Connection to drillbit lost."));
            }
        }
    }
    return;
}

void DrillClientImpl::Close() {
    shutdownSocket();
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

    m_io_service.reset();
    if (DrillClientConfig::getHandshakeTimeout() > 0){
        m_deadlineTimer.expires_from_now(boost::posix_time::seconds(DrillClientConfig::getHandshakeTimeout()));
        m_deadlineTimer.async_wait(boost::bind(
                    &DrillClientImpl::handleHShakeReadTimeout,
                    this,
                    boost::asio::placeholders::error
                    ));
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Started new handshake wait timer with "
                << DrillClientConfig::getHandshakeTimeout() << " seconds." << std::endl;)
    }

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
    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "DrillClientImpl::recvHandshake: async read waiting for server handshake response.\n";)
    m_io_service.run();
    if(m_rbuf!=NULL){
        Utils::freeBuffer(m_rbuf, MAX_SOCK_RD_BUFSIZE); m_rbuf=NULL;
    }
#ifdef WIN32_SHUTDOWN_ON_TIMEOUT
    if (m_pError != NULL) {
        return static_cast<connectionStatus_t>(m_pError->status);
    }
#endif // WIN32_SHUTDOWN_ON_TIMEOUT
    startHeartbeatTimer();

    return CONN_SUCCESS;
}

void DrillClientImpl::handleHandshake(ByteBuf_t _buf,
        const boost::system::error_code& err,
        size_t bytes_transferred) {
    boost::system::error_code error=err;
    // cancel the timer
    m_deadlineTimer.cancel();
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Deadline timer cancelled." << std::endl;)
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
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Handshake Message: actual bytes read = " << dataBytesRead << std::endl;)
                if(dataBytesRead==bytesToRead) break;
                bytesToRead-=dataBytesRead;
                b+=dataBytesRead;
            }
            DrillClientImpl::s_decoder.Decode(m_rbuf+bytes_read, length, msg);
        }else{
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleHandshake: ERR_CONN_RDFAIL. No handshake.\n";)
            handleConnError(CONN_FAILURE, getMessage(ERR_CONN_RDFAIL, "No handshake"));
            return;
        }
        exec::user::BitToUserHandshake b2u;
        b2u.ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
        this->m_handshakeVersion=b2u.rpc_version();
        this->m_handshakeStatus=b2u.status();
        this->m_handshakeErrorId=b2u.errorid();
        this->m_handshakeErrorMsg=b2u.errormessage();

    }else{
        // boost error
        if(error==boost::asio::error::eof){ // Server broke off the connection
            handleConnError(CONN_HANDSHAKE_FAILED,
                getMessage(ERR_CONN_NOHSHAKE, DRILL_RPC_VERSION));
        }else{
            handleConnError(CONN_FAILURE, getMessage(ERR_CONN_RDFAIL, error.message().c_str()));
        }
        return;
    }
    return;
}

void DrillClientImpl::handleHShakeReadTimeout(const boost::system::error_code & err){
    // if err == boost::asio::error::operation_aborted) then the caller cancelled the timer.
    if(err != boost::asio::error::operation_aborted){
        // Check whether the deadline has passed.
        if (m_deadlineTimer.expires_at() <= boost::asio::deadline_timer::traits_type::now()){
            // The deadline has passed.
            m_deadlineTimer.expires_at(boost::posix_time::pos_infin);
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::HandleHShakeReadTimeout: Deadline timer expired; ERR_CONN_HSHAKETIMOUT.\n";)
            handleConnError(CONN_HANDSHAKE_TIMEOUT, getMessage(ERR_CONN_HSHAKETIMOUT));
            m_io_service.stop();
            boost::system::error_code ignorederr;
            m_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignorederr);
        }
    }
    return;
}

connectionStatus_t DrillClientImpl::validateHandshake(DrillUserProperties* properties){

    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "validateHandShake\n";)

    exec::user::UserToBitHandshake u2b;
    u2b.set_channel(exec::shared::USER);
    u2b.set_rpc_version(DRILL_RPC_VERSION);
    u2b.set_support_listening(true);
    u2b.set_support_timeout(true);

    if(properties != NULL && properties->size()>0){
        std::string username;
        std::string err;
        if(!properties->validate(err)){
            DRILL_MT_LOG(DRILL_LOG(LOG_INFO) << "Invalid user input:" << err << std::endl;)
        }
        exec::user::UserProperties* userProperties = u2b.mutable_properties();

        std::map<char,int>::iterator it;
        for(size_t i=0; i<properties->size(); i++){
            std::map<std::string,uint32_t>::const_iterator it=DrillUserProperties::USER_PROPERTIES.find(properties->keyAt(i));
            if(it==DrillUserProperties::USER_PROPERTIES.end()){
                DRILL_MT_LOG(DRILL_LOG(LOG_WARNING) << "Connection property ("<< properties->keyAt(i) 
                    << ") is unknown and is being skipped" << std::endl;)
                continue;
            }
            if(IS_BITSET((*it).second,USERPROP_FLAGS_SERVERPROP)){
                exec::user::Property* connProp = userProperties->add_properties();
                connProp->set_key(properties->keyAt(i));
                connProp->set_value(properties->valueAt(i));
                //Username(but not the password) also needs to be set in UserCredentials
                if(IS_BITSET((*it).second,USERPROP_FLAGS_USERNAME)){
                    exec::shared::UserCredentials* creds = u2b.mutable_credentials();
                    username=properties->valueAt(i);
                    creds->set_user_name(username);
                    //u2b.set_credentials(&creds);
                }
                if(IS_BITSET((*it).second,USERPROP_FLAGS_PASSWORD)){
                    DRILL_MT_LOG(DRILL_LOG(LOG_INFO) <<  properties->keyAt(i) << ": ********** " << std::endl;)
                }else{
                    DRILL_MT_LOG(DRILL_LOG(LOG_INFO) << properties->keyAt(i) << ":" << properties->valueAt(i) << std::endl;)
                }
            }// Server properties
        }
    }

    {
        boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
        uint64_t coordId = this->getNextCoordinationId();

        OutBoundRpcMessage out_msg(exec::rpc::REQUEST, exec::user::HANDSHAKE, coordId, &u2b);
        sendSync(out_msg);
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Sent handshake request message. Coordination id: " << coordId << "\n";)
    }

    connectionStatus_t ret = recvHandshake();
    if(ret!=CONN_SUCCESS){
        return ret;
    }
    if(this->m_handshakeStatus != exec::user::SUCCESS){
        switch(this->m_handshakeStatus){
            case exec::user::RPC_VERSION_MISMATCH:
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Invalid rpc version.  Expected "
                    << DRILL_RPC_VERSION << ", actual "<< m_handshakeVersion << "." << std::endl;)
                return handleConnError(CONN_BAD_RPC_VER,
                        getMessage(ERR_CONN_BAD_RPC_VER, DRILL_RPC_VERSION,
                            m_handshakeVersion,
                            this->m_handshakeErrorId.c_str(),
                            this->m_handshakeErrorMsg.c_str()));
            case exec::user::AUTH_FAILED:
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Authentication failed." << std::endl;)
                return handleConnError(CONN_AUTH_FAILED,
                        getMessage(ERR_CONN_AUTHFAIL,
                            this->m_handshakeErrorId.c_str(),
                            this->m_handshakeErrorMsg.c_str()));
            case exec::user::UNKNOWN_FAILURE:
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Unknown error during handshake." << std::endl;)
                return handleConnError(CONN_HANDSHAKE_FAILED,
                        getMessage(ERR_CONN_UNKNOWN_ERR,
                            this->m_handshakeErrorId.c_str(),
                            this->m_handshakeErrorMsg.c_str()));
            default:
                break;
        }
    }
    // reset io_service after handshake is validated before running queries
    m_io_service.reset();
    return CONN_SUCCESS;
}


FieldDefPtr DrillClientQueryResult::s_emptyColDefs( new (std::vector<Drill::FieldMetadata*>));

void DrillClientImpl::startMessageListener() {
    if(this->m_pListenerThread==NULL){
        // Stopping the io_service from running out-of-work
        if(m_io_service.stopped()){
            DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "DrillClientImpl::startMessageListener: io_service is stopped. Restarting." <<std::endl;)
            m_io_service.reset();
        }
        this->m_pWork = new boost::asio::io_service::work(m_io_service);
        this->m_pListenerThread = new boost::thread(boost::bind(&boost::asio::io_service::run,
                    &this->m_io_service));
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "DrillClientImpl::startMessageListener: Starting listener thread: "
            << this->m_pListenerThread << std::endl;)
    }
}

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
    connectionStatus_t cStatus=CONN_SUCCESS;
    {
        boost::lock_guard<boost::mutex> prLock(this->m_prMutex);
        boost::lock_guard<boost::mutex> dcLock(this->m_dcMutex);
        coordId = this->getNextCoordinationId();
        OutBoundRpcMessage out_msg(exec::rpc::REQUEST, exec::user::RUN_QUERY, coordId, &query);

        // Create the result object and register the listener before we send the query
        // because sometimes the caller is not checking the status of the submitQuery call.
        // This way, the broadcast error call will cause the results listener to be called
        // with a COMM_ERROR status.
        pQuery = new DrillClientQueryResult(this, coordId, plan);
        pQuery->registerListener(l, lCtx);
        this->m_queryIds[coordId]=pQuery;

        connectionStatus_t cStatus=sendSync(out_msg);
        if(cStatus == CONN_SUCCESS){
            bool sendRequest=false;

            DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG)  << "Sent query request. " << "[" << m_connectedHost << "]"  << "Coordination id = " << coordId << std::endl;)
                DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG)  << "Sent query " <<  "Coordination id = " << coordId << " query: " << plan << std::endl;)

                if(m_pendingRequests++==0){
                    sendRequest=true;
                }else{
                    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Queueing query request to server" << std::endl;)
                        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Number of pending requests = " << m_pendingRequests << std::endl;)
                }
            if(sendRequest){
                DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Sending query request. Number of pending requests = "
                        << m_pendingRequests << std::endl;)
                    getNextResult(); // async wait for results
            }
        }

    }
    if(cStatus!=CONN_SUCCESS){
        this->m_queryIds.erase(coordId);
        delete pQuery;
        return NULL;
    }



    //run this in a new thread
    startMessageListener();

    return pQuery;
}

void DrillClientImpl::getNextResult(){

    // This call is always made from within a function where the mutex has already been acquired
    //boost::lock_guard<boost::mutex> lock(this->m_dcMutex);

    {
        boost::unique_lock<boost::mutex> memLock(AllocatedBuffer::s_memCVMutex);
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Read blocked waiting for memory." << std::endl;)
        while(AllocatedBuffer::s_isBufferLimitReached){
            AllocatedBuffer::s_memCV.wait(memLock);
        }
    }
    
    //use free, not delete to free
    ByteBuf_t readBuf = Utils::allocateBuffer(LEN_PREFIX_BUFLEN);
    if (DrillClientConfig::getQueryTimeout() > 0){
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Started new query wait timer with "
                << DrillClientConfig::getQueryTimeout() << " seconds." << std::endl;)
        m_deadlineTimer.expires_from_now(boost::posix_time::seconds(DrillClientConfig::getQueryTimeout()));
        m_deadlineTimer.async_wait(boost::bind(
            &DrillClientImpl::handleReadTimeout,
            this,
            boost::asio::placeholders::error
            ));
    }

    resetHeartbeatTimer();

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
    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "DrillClientImpl::getNextResult: async_read from the server\n";)
}

void DrillClientImpl::waitForResults(){
    // The listener thread never exists because it may be sending/receiving a heartbeat. Before the heartbeat was introduced
    // we could check if the listener thread has exited to tell if the queries are done. We can no longer do so now. We check
    // a condition variable instead
    {
        boost::unique_lock<boost::mutex> cvLock(this->m_dcMutex);
        //if no more data, return NULL;
        while(this->m_pendingRequests>0) {
            this->m_cv.wait(cvLock);
        }
    }
}

status_t DrillClientImpl::readMsg(ByteBuf_t _buf,
        AllocatedBufferPtr* allocatedBuffer,
        InBoundRpcMessage& msg,
        boost::system::error_code& error){

    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::readMsg: Read message from buffer "
        <<  reinterpret_cast<int*>(_buf) << std::endl;)
    size_t leftover=0;
    uint32_t rmsgLen;
    AllocatedBufferPtr currentBuffer;
    *allocatedBuffer=NULL;
    {
        // We need to protect the readLength and read buffer, and the pending requests counter,
        // but we don't have to keep the lock while we decode the rest of the buffer.
        boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
        int bytes_read = DrillClientImpl::s_decoder.LengthDecode(_buf, &rmsgLen);
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "len bytes = " << bytes_read << std::endl;)
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "rmsgLen = " << rmsgLen << std::endl;)

        if(rmsgLen>0){
            leftover = LEN_PREFIX_BUFLEN - bytes_read;
            // Allocate a buffer
            currentBuffer=new AllocatedBuffer(rmsgLen);
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::readMsg: Allocated and locked buffer: [ "
                << currentBuffer << ", size = " << rmsgLen << " ]\n";)
            if(currentBuffer==NULL){
                Utils::freeBuffer(_buf, LEN_PREFIX_BUFLEN);
                return handleQryError(QRY_CLIENT_OUTOFMEM, getMessage(ERR_QRY_OUTOFMEM), NULL);
            }
            *allocatedBuffer=currentBuffer;
            if(leftover){
                memcpy(currentBuffer->m_pBuffer, _buf + bytes_read, leftover);
            }
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "reading data (rmsgLen - leftover) : "
                << (rmsgLen - leftover) << std::endl;)
            ByteBuf_t b=currentBuffer->m_pBuffer + leftover;
            size_t bytesToRead=rmsgLen - leftover;
              
            while(1){
                size_t dataBytesRead=this->m_socket.read_some(
                        boost::asio::buffer(b, bytesToRead),
                        error);
                if(error) break;
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Data Message: actual bytes read = " << dataBytesRead << std::endl;)
                if(dataBytesRead==bytesToRead) break;
                bytesToRead-=dataBytesRead;
                b+=dataBytesRead;
            }
            
            if(!error){
                // read data successfully
                DrillClientImpl::s_decoder.Decode(currentBuffer->m_pBuffer, rmsgLen, msg);
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Done decoding chunk. Coordination id: " <<msg.m_coord_id<< std::endl;)
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
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::readMsg: Free buffer "
        <<  reinterpret_cast<int*>(_buf) << std::endl;)
    Utils::freeBuffer(_buf, LEN_PREFIX_BUFLEN);
    return QRY_SUCCESS;
}

status_t DrillClientImpl::processQueryResult(AllocatedBufferPtr  allocatedBuffer, InBoundRpcMessage& msg ){
    DrillClientQueryResult* pDrillClientQueryResult=NULL;
    status_t ret=QRY_SUCCESS;
    exec::shared::QueryId qid;
    sendAck(msg, true);
    {
        boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
        exec::shared::QueryResult qr;

        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Processing Query Result " << std::endl;)
        qr.ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << qr.DebugString() << std::endl;)
        
        qid.CopyFrom(qr.query_id());
        
        if (qr.has_query_state() &&
                qr.query_state() != exec::shared::QueryResult_QueryState_RUNNING &&
                qr.query_state() != exec::shared::QueryResult_QueryState_STARTING) {
            pDrillClientQueryResult=findQueryResult(qid);
            //Queries that have been cancelled or whose resources are freed before completion 
            //do not have a DrillClientQueryResult object. We need not handle the terminal message 
            //in that case since all it does is to free resources (and they have already been freed)
            if(pDrillClientQueryResult!=NULL){
                //Validate the RPC message
                std::string valErr;
                if( (ret=validateResultMessage(msg, qr, valErr)) != QRY_SUCCESS){
                    delete allocatedBuffer;
                    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processQueryResult: ERR_QRY_INVRPC." << std::endl;)
                    return handleQryError(ret, getMessage(ERR_QRY_INVRPC, valErr.c_str()), pDrillClientQueryResult);
                }
                ret=processQueryStatusResult(&qr, pDrillClientQueryResult);
            }else{
                // We've received the final message for a query that has been cancelled
                // or for which the resources have been freed. We no longer need to listen
                // for more incoming messages for such a query.
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processQueryResult:" << debugPrintQid(qid)<< " completed."<< std::endl;)
                m_pendingRequests--;
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processQueryResult: pending requests is " << m_pendingRequests<< std::endl;)
                ret=QRY_CANCELED;
            }
            delete allocatedBuffer;
            //return ret;
        }else{
            // Normal query results come back with query_state not set.
            // Actually this is not strictly true. The query state is set to
            // 0(i.e. PENDING), but protobuf thinks this means the value is not set.
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processQueryResult: Query State was not set.\n";)
        }
    }
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processQueryResult: " << m_pendingRequests << " requests pending." << std::endl;)
    if(m_pendingRequests==0){
        // signal any waiting client that it can exit because there are no more any query results to arrive.
        // We keep the heartbeat going though.
        m_cv.notify_one();
    }
    return ret;
}

status_t DrillClientImpl::processQueryData(AllocatedBufferPtr  allocatedBuffer, InBoundRpcMessage& msg ){
    DrillClientQueryResult* pDrillClientQueryResult=NULL;
    status_t ret=QRY_SUCCESS;
    exec::shared::QueryId qid;
    // Be a good client and send ack as early as possible.
    // Drillbit pushed the query result to the client, the client should send ack
    // whenever it receives the message
    sendAck(msg, true);
    RecordBatch* pRecordBatch=NULL;
    {
        boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
        exec::shared::QueryData* qr = new exec::shared::QueryData; //Record Batch will own this object and free it up.

        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Processing Query Data " << std::endl;)
        qr->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << qr->DebugString() << std::endl;)

        qid.CopyFrom(qr->query_id());
        if(qid.part1()==0){
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processQueryData: QID=0. Ignore and return QRY_SUCCESS." << std::endl;)
            delete allocatedBuffer;
            return QRY_SUCCESS;
        }

        pDrillClientQueryResult=findQueryResult(qid);
        if(pDrillClientQueryResult==NULL){
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Cleaning up resources allocated for canceled query (" 
                                 << debugPrintQid(qid) << ")." << std::endl;)
            delete qr;
            delete allocatedBuffer;
            return ret;
        }
        
        //Validate the RPC message
        std::string valErr;
        if( (ret=validateDataMessage(msg, *qr, valErr)) != QRY_SUCCESS){
            delete allocatedBuffer;
            delete qr;
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processQueryData: ERR_QRY_INVRPC.\n";)
            pDrillClientQueryResult->setQueryStatus(ret);
            return handleQryError(ret, getMessage(ERR_QRY_INVRPC, valErr.c_str()), pDrillClientQueryResult);
        }

        //Build Record Batch here
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Building record batch for Query Id - " << debugPrintQid(qr->query_id()) << std::endl;)

        pRecordBatch= new RecordBatch(qr, allocatedBuffer,  msg.m_dbody);
        pDrillClientQueryResult->m_numBatches++;

        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Allocated new Record batch." << (void*)pRecordBatch << std::endl;)
        pRecordBatch->build();
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << debugPrintQid(qr->query_id())<<"recordBatch.numRecords "
            << pRecordBatch->getNumRecords()  << std::endl;)
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << debugPrintQid(qr->query_id())<<"recordBatch.numFields "
            << pRecordBatch->getNumFields()  << std::endl;)

        ret=pDrillClientQueryResult->setupColumnDefs(qr);
        if(ret==QRY_SUCCESS_WITH_INFO){
            pRecordBatch->schemaChanged(true);
        }

        pDrillClientQueryResult->setIsQueryPending(true);
        pfnQueryResultsListener pResultsListener=pDrillClientQueryResult->m_pResultsListener;
        if(pDrillClientQueryResult->m_bIsLastChunk){
            DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << debugPrintQid(*pDrillClientQueryResult->m_pQueryId)
                <<  "Received last batch. " << std::endl;)
            ret=QRY_NO_MORE_DATA;
        }
        pDrillClientQueryResult->setQueryStatus(ret);
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
        // Do not decrement pending requests here. We have sent a cancel and we may still receive results that are
        // pushed on the wire before the cancel is processed.
        pDrillClientQueryResult->setIsQueryPending(false);
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Client app cancelled query." << std::endl;)
        pDrillClientQueryResult->setQueryStatus(ret);
        clearMapEntries(pDrillClientQueryResult);
        return ret;
    }
    return ret;
}

status_t DrillClientImpl::processQueryId(AllocatedBufferPtr allocatedBuffer, InBoundRpcMessage& msg ){
    DrillClientQueryResult* pDrillClientQueryResult=NULL;
    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Processing Query Handle with coordination id:" << msg.m_coord_id << std::endl;)
    status_t ret=QRY_SUCCESS;

    boost::lock_guard<boost::mutex> lock(m_dcMutex);
    std::map<int,DrillClientQueryResult*>::iterator it;
    for(it=this->m_queryIds.begin();it!=this->m_queryIds.end();it++){
        std::string qidString = it->second->m_pQueryId!=NULL?debugPrintQid(*it->second->m_pQueryId):std::string("NULL");
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "DrillClientImpl::processQueryId: m_queryIds: coordinationId: " << it->first
        << " QueryId: "<< qidString << std::endl;)
    }
    if(msg.m_coord_id==0){
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processQueryId: m_coord_id=0. Ignore and return QRY_SUCCESS." << std::endl;)
        return QRY_SUCCESS;
    }
    it=this->m_queryIds.find(msg.m_coord_id);
    if(it!=this->m_queryIds.end()){
        pDrillClientQueryResult=(*it).second;
        exec::shared::QueryId *qid = new exec::shared::QueryId;
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE)  << "Received Query Handle " << msg.m_pbody.size() << std::endl;)
        qid->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Query Id - " << debugPrintQid(*qid) << std::endl;)
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

DrillClientQueryResult* DrillClientImpl::findQueryResult(exec::shared::QueryId& qid){
    DrillClientQueryResult* pDrillClientQueryResult=NULL;
    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Searching for Query Id - " << debugPrintQid(qid) << std::endl;)
    std::map<exec::shared::QueryId*, DrillClientQueryResult*, compareQueryId>::iterator it;
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processQueryResult: m_queryResults size: " << m_queryResults.size() << std::endl;)
    if(m_queryResults.size() != 0){
        for(it=m_queryResults.begin(); it!=m_queryResults.end(); it++){
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::findQueryResult: m_QueryResult ids: [" << it->first->part1() << ":"
                << it->first->part2() << "]\n";)
        }
    }
    it=this->m_queryResults.find(&qid);
    if(it!=this->m_queryResults.end()){
        pDrillClientQueryResult=(*it).second;
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Drill Client Query Result Query Id - " <<
            debugPrintQid(*pDrillClientQueryResult->m_pQueryId) << std::endl;)
    }
    return pDrillClientQueryResult;
}

status_t DrillClientImpl::processQueryStatusResult(exec::shared::QueryResult* qr,
        DrillClientQueryResult* pDrillClientQueryResult){
    status_t ret = QUERYSTATE_TO_STATUS_MAP[qr->query_state()];
    if(pDrillClientQueryResult!=NULL){
        pDrillClientQueryResult->setQueryStatus(ret);
        pDrillClientQueryResult->setQueryState(qr->query_state());
    }
    switch(qr->query_state()) {
        case exec::shared::QueryResult_QueryState_FAILED:
            {
                // get the error message from protobuf and handle errors
                ret=handleQryError(ret, qr->error(0), pDrillClientQueryResult);
            }
            break;
            // m_pendingRequests should be decremented when the query is
            // completed
        case exec::shared::QueryResult_QueryState_CANCELED:
            {
                ret=handleTerminatedQryState(ret,
                        getMessage(ERR_QRY_CANCELED),
                        pDrillClientQueryResult);
                m_pendingRequests--;
            }
            break;
        case exec::shared::QueryResult_QueryState_COMPLETED:
            {
                //Not clean to call the handleTerminatedQryState method
                //because it signals an error to the listener.
                //The ODBC driver expects this though and the sync API
                //handles this (luckily).
                ret=handleTerminatedQryState(ret,
                        getMessage(ERR_QRY_COMPLETED),
                        pDrillClientQueryResult);
                m_pendingRequests--;
            }
            break;
        default:
            {
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processQueryStatusResult: Unknown Query State.\n";)
                ret=handleQryError(QRY_INTERNAL_ERROR,
                        getMessage(ERR_QRY_UNKQRYSTATE),
                        pDrillClientQueryResult);
            }
            break;
    }
    return ret;
}

void DrillClientImpl::handleReadTimeout(const boost::system::error_code & err){
    // if err == boost::asio::error::operation_aborted) then the caller cancelled the timer.
    if(err != boost::asio::error::operation_aborted){

        // Check whether the deadline has passed.
        if (m_deadlineTimer.expires_at() <= boost::asio::deadline_timer::traits_type::now()){
            // The deadline has passed.
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleReadTimeout: Deadline timer expired; ERR_QRY_TIMOUT. \n";)
            handleQryError(QRY_TIMEOUT, getMessage(ERR_QRY_TIMOUT), NULL);
            // There is no longer an active deadline. The expiry is set to positive
            // infinity so that the timer never expires until a new deadline is set.
            // Note that at this time, the caller is not in a (async) wait for the timer.
            m_deadlineTimer.expires_at(boost::posix_time::pos_infin);
            // Cancel all pending async IOs.
            // The cancel call _MAY_ not work on all platforms. To be a little more reliable we need
            // to have the BOOST_ASIO_ENABLE_CANCELIO macro (as well as the BOOST_ASIO_DISABLE_IOCP macro?)
            // defined. To be really sure, we need to close the socket. Closing the socket is a bit
            // drastic and we will defer that till a later release.
#ifdef WIN32_SHUTDOWN_ON_TIMEOUT
            boost::system::error_code ignorederr;
            m_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignorederr);
#else // NOT WIN32_SHUTDOWN_ON_TIMEOUT
            m_socket.cancel();
#endif // WIN32_SHUTDOWN_ON_TIMEOUT
        }
    }
    return;
}

void DrillClientImpl::handleRead(ByteBuf_t _buf,
        const boost::system::error_code& err,
        size_t bytes_transferred) {
    boost::system::error_code error=err;
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: Handle Read from buffer "
        <<  reinterpret_cast<int*>(_buf) << std::endl;)
    if(DrillClientConfig::getQueryTimeout() > 0){
        // Cancel the timeout if handleRead is called
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: Cancel deadline timer.\n";)
        m_deadlineTimer.cancel();
    }
    if(!error){
        InBoundRpcMessage msg;
        boost::lock_guard<boost::mutex> lock(this->m_prMutex);

        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Getting new message" << std::endl;)
        AllocatedBufferPtr allocatedBuffer=NULL;

        if(readMsg(_buf, &allocatedBuffer, msg, error)!=QRY_SUCCESS){
            if(m_pendingRequests!=0){
                boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
                getNextResult();
            }
            return;
        }

        if(!error && msg.m_mode==exec::rpc::PONG){ //heartbeat response. Throw it away
            m_pendingRequests--;
            delete allocatedBuffer;
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Received heartbeat from server. " <<  std::endl;)
            if(m_pendingRequests!=0){
                boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
                getNextResult();
            }else{
                boost::unique_lock<boost::mutex> cvLock(this->m_dcMutex);
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "No more results expected from server. " <<  std::endl;)
                m_cv.notify_one();
            }
            return;
        }else if(!error && msg.m_rpc_type==exec::user::QUERY_RESULT){
            status_t s = processQueryResult(allocatedBuffer, msg);
            if(s !=QRY_SUCCESS && s!= QRY_NO_MORE_DATA){
                if(m_pendingRequests!=0){
                    boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
                    getNextResult();
                }
                return;
            }
        }else if(!error && msg.m_rpc_type==exec::user::QUERY_DATA){
            if(processQueryData(allocatedBuffer, msg)!=QRY_SUCCESS){
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
        }else if(!error && msg.m_rpc_type==exec::user::ACK){
            // Cancel requests will result in an ACK sent back.
            // Consume silently
            delete allocatedBuffer;
            if(m_pendingRequests!=0){
                boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
                getNextResult();
            }
            return;
        }else{
            boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
            if(error){
                // We have a socket read error, but we do not know which query this is for.
                // Signal ALL pending queries that they should stop waiting.
                delete allocatedBuffer;
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "read error: " << error << std::endl;)
                handleQryError(QRY_COMM_ERROR, getMessage(ERR_QRY_COMMERR, error.message().c_str()), NULL);
                return;
            }else{
                // If not QUERY_RESULT, then we think something serious has gone wrong?
                // In one case when the client hung, we observed that the server was sending a handshake request to the client
                // We should properly handle these handshake requests/responses
                if(msg.has_rpc_type() && msg.m_rpc_type==exec::user::HANDSHAKE){
                    if(msg.has_mode() && msg.m_mode==exec::rpc::REQUEST){
                        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: Handshake request from server. Send response.\n";)
                        exec::user::UserToBitHandshake u2b;
                        u2b.set_channel(exec::shared::USER);
                        u2b.set_rpc_version(DRILL_RPC_VERSION);
                        u2b.set_support_listening(true);
                        OutBoundRpcMessage out_msg(exec::rpc::RESPONSE, exec::user::HANDSHAKE, msg.m_coord_id, &u2b);
                        sendSync(out_msg);
                        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: Handshake response sent.\n";)
                    }else{
                        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: Handshake response from server. Ignore.\n";)
                    }
                }else{
                    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: ERR_QRY_INVRPCTYPE. "
                        << "QueryResult returned " << msg.m_rpc_type << std::endl;)
                    handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVRPCTYPE, msg.m_rpc_type), NULL);
                }
                delete allocatedBuffer;
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
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: ERR_QRY_COMMERR. "
            "Boost Communication Error: " << error.message() << std::endl;)
        handleQryError(QRY_COMM_ERROR, getMessage(ERR_QRY_COMMERR, error.message().c_str()), NULL);
        return;
    }
    return;
}

status_t DrillClientImpl::validateDataMessage(InBoundRpcMessage& msg, exec::shared::QueryData& qd, std::string& valErr){
    if(msg.m_mode == exec::rpc::RESPONSE_FAILURE){
        valErr=getMessage(ERR_QRY_RESPFAIL);
        return QRY_FAILURE;
    }
    if(qd.def().carries_two_byte_selection_vector() == true){
        valErr=getMessage(ERR_QRY_SELVEC2);
        return QRY_FAILURE;
    }
    return QRY_SUCCESS;
}

status_t DrillClientImpl::validateResultMessage(InBoundRpcMessage& msg, exec::shared::QueryResult& qr, std::string& valErr){
    if(msg.m_mode == exec::rpc::RESPONSE_FAILURE){
        valErr=getMessage(ERR_QRY_RESPFAIL);
        return QRY_FAILURE;
    }
    if(qr.query_state()==exec::shared::QueryResult_QueryState_CANCELED){
        valErr=getMessage(ERR_QRY_CANCELED);
        return QRY_FAILURE;
    }
    return QRY_SUCCESS;
}

connectionStatus_t DrillClientImpl::handleConnError(connectionStatus_t status, std::string msg){
    DrillClientError* pErr = new DrillClientError(status, DrillClientError::CONN_ERROR_START+status, msg);
    m_pendingRequests=0;
    if(!m_queryIds.empty()){
        // set query error only if queries are running
        broadcastError(pErr);
    }else{
        if(m_pError!=NULL){ delete m_pError; m_pError=NULL;}
        m_pError=pErr;
        shutdownSocket();
    }
    return status;
}

status_t DrillClientImpl::handleQryError(status_t status, std::string msg, DrillClientQueryResult* pQueryResult){
    DrillClientError* pErr = new DrillClientError(status, DrillClientError::QRY_ERROR_START+status, msg);
    // set query error only if queries are running
    if(pQueryResult!=NULL){
        m_pendingRequests--;
        pQueryResult->signalError(pErr);
    }else{
        m_pendingRequests=0;
        broadcastError(pErr);
    }
    return status;
}

status_t DrillClientImpl::handleQryError(status_t status,
        const exec::shared::DrillPBError& e,
        DrillClientQueryResult* pQueryResult){
    assert(pQueryResult!=NULL);
    DrillClientError* pErr =  DrillClientError::getErrorObject(e);
    pQueryResult->signalError(pErr);
    m_pendingRequests--;
    return status;
}

void DrillClientImpl::broadcastError(DrillClientError* pErr){
    if(pErr!=NULL){
        std::map<int, DrillClientQueryResult*>::iterator iter;
        if(!m_queryIds.empty()){
            for(iter = m_queryIds.begin(); iter != m_queryIds.end(); iter++) {
                DrillClientError* err=new DrillClientError(pErr->status, pErr->errnum, pErr->msg);
                iter->second->signalError(err);
            }
        }
        delete pErr;
    }
    // We have an error at the connection level. Cancel the heartbeat. 
    // And close the connection 
    m_heartbeatTimer.cancel();
    m_pendingRequests=0;
    m_cv.notify_one();
    shutdownSocket();
    return;
}

// The implementation is similar to handleQryError
status_t DrillClientImpl::handleTerminatedQryState(
        status_t status,
        std::string msg,
        DrillClientQueryResult* pQueryResult){
    assert(pQueryResult!=NULL);
    if(status==QRY_COMPLETED){
        pQueryResult->signalComplete();
    }else{
        // set query error only if queries did not complete successfully
        DrillClientError* pErr = new DrillClientError(status, DrillClientError::QRY_ERROR_START+status, msg);
        pQueryResult->signalError(pErr);
    }
    return status;
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
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "ACK sent" << std::endl;)
}

void DrillClientImpl::sendCancel(exec::shared::QueryId* pQueryId){
    boost::lock_guard<boost::mutex> lock(m_dcMutex);
    uint64_t coordId = this->getNextCoordinationId();
    OutBoundRpcMessage cancel_msg(exec::rpc::REQUEST, exec::user::CANCEL_QUERY, coordId, pQueryId);
    sendSync(cancel_msg);
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "CANCEL sent" << std::endl;)
}

void DrillClientImpl::shutdownSocket(){
    m_io_service.stop();
    boost::system::error_code ignorederr;
    m_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignorederr);
    m_bIsConnected=false;
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Socket shutdown" << std::endl;)
}

// This COPIES the FieldMetadata definition for the record batch.  ColumnDefs held by this
// class are used by the async callbacks.
status_t DrillClientQueryResult::setupColumnDefs(exec::shared::QueryData* pQueryData) {
    bool hasSchemaChanged=false;
    bool isFirstIter=false;
    boost::lock_guard<boost::mutex> schLock(this->m_schemaMutex);

    isFirstIter=this->m_numBatches==1?true:false;
    std::map<std::string, Drill::FieldMetadata*> oldSchema;
    if(!m_columnDefs->empty()){
        for(std::vector<Drill::FieldMetadata*>::iterator it = this->m_columnDefs->begin(); it != this->m_columnDefs->end(); ++it){
            // the key is the field_name + type
            char type[256];
            sprintf(type, ":%d:%d",(*it)->getMinorType(), (*it)->getDataMode() );
            std::string k= (*it)->getName()+type;
            oldSchema[k]=*it;
            delete *it;
        }
    }
    m_columnDefs->clear();
    size_t numFields=pQueryData->def().field_size();
    if (numFields > 0){
        for(size_t i=0; i<numFields; i++){
            Drill::FieldMetadata* fmd= new Drill::FieldMetadata;
            fmd->set(pQueryData->def().field(i));
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
            oldSchema.clear();
        }
    }
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
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Query result listener called" << std::endl;)
    //check if the query has been canceled. IF so then return FAILURE. Caller will send cancel to the server.
    if(this->m_bCancel){
        if(b!=NULL) delete b;
        return QRY_FAILURE;
    }
    if (!err) {
        // signal the cond var
        {
            if(b!=NULL){
#ifdef DEBUG
                DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG)<<debugPrintQid(b->getQueryResult()->query_id())
                    << "Query result listener saved result to queue." << std::endl;)
#endif
                boost::lock_guard<boost::mutex> cvLock(this->m_cvMutex);
                this->m_recordBatches.push(b);
                this->m_bHasData=true;
            }
        }
        m_cv.notify_one();
    }else{
        return QRY_FAILURE;
    }
    return QRY_SUCCESS;
}

RecordBatch*  DrillClientQueryResult::peekNext(){
    RecordBatch* pRecordBatch=NULL;
    boost::unique_lock<boost::mutex> cvLock(this->m_cvMutex);
    //if no more data, return NULL;
    if(!m_bIsQueryPending) return NULL;
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Synchronous read waiting for data." << std::endl;)
    while(!this->m_bHasData && !m_bHasError && m_bIsQueryPending) {
        this->m_cv.wait(cvLock);
    }
    // READ but not remove first element from queue
    pRecordBatch = this->m_recordBatches.front();
    return pRecordBatch;
}

RecordBatch*  DrillClientQueryResult::getNext() {
    RecordBatch* pRecordBatch=NULL;
    boost::unique_lock<boost::mutex> cvLock(this->m_cvMutex);
    //if no more data, return NULL;
    if(!m_bIsQueryPending){
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Query is done." << std::endl;)
        if(!m_recordBatches.empty()){
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << " But there is a Record batch left behind." << std::endl;)
        }
        return NULL;
    }

    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Synchronous read waiting for data." << std::endl;)
    while(!this->m_bHasData && !m_bHasError && m_bIsQueryPending){
        this->m_cv.wait(cvLock);
    }
    // remove first element from queue
    pRecordBatch = this->m_recordBatches.front();
    this->m_recordBatches.pop();
    this->m_bHasData=!this->m_recordBatches.empty();
    // if vector is empty, set m_bHasDataPending to false;
    m_bIsQueryPending=!(this->m_recordBatches.empty()&&m_queryState==exec::shared::QueryResult_QueryState_COMPLETED);
    return pRecordBatch;
}

// Blocks until data is available
void DrillClientQueryResult::waitForData() {
    boost::unique_lock<boost::mutex> cvLock(this->m_cvMutex);
    //if no more data, return NULL;
    if(!m_bIsQueryPending) return;
    while(!this->m_bHasData && !m_bHasError && m_bIsQueryPending) {
        this->m_cv.wait(cvLock);
    }
}

void DrillClientQueryResult::cancel() {
    this->m_bCancel=true;
}

void DrillClientQueryResult::signalError(DrillClientError* pErr){
    // Ignore return values from the listener.
    if(pErr!=NULL){
        if(m_pError!=NULL){
            delete m_pError; m_pError=NULL;
        }
        m_pError=pErr;
        pfnQueryResultsListener pResultsListener=this->m_pResultsListener;
        if(pResultsListener!=NULL){
            pResultsListener(this, NULL, pErr);
        }else{
            defaultQueryResultsListener(this, NULL, pErr);
        }
        {
            boost::lock_guard<boost::mutex> cvLock(this->m_cvMutex);
            m_bIsQueryPending=false;
            m_bHasData=false;
            m_bHasError=true;
        }
        //Signal the cv in case there is a client waiting for data already.
        m_cv.notify_one();
    }
    return;
}

void DrillClientQueryResult::signalComplete(){
    pfnQueryResultsListener pResultsListener=this->m_pResultsListener;
    if(pResultsListener!=NULL){
        pResultsListener(this, NULL, NULL);
    }else{
        defaultQueryResultsListener(this, NULL, NULL);
    }
    {
        boost::lock_guard<boost::mutex> cvLock(this->m_cvMutex);
        m_bIsQueryPending=false;
        m_bIsQueryPending=!(this->m_recordBatches.empty()&&m_queryState==exec::shared::QueryResult_QueryState_COMPLETED);
        m_bHasError=false;
    }
    //Signal the cv in case there is a client waiting for data already.
    m_cv.notify_one();
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
    if(this->m_pQueryId!=NULL){
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Clearing state for Query Id - " << debugPrintQid(*this->m_pQueryId) << std::endl;)
    }
    //Tell the parent to remove this from its lists
    m_pClient->clearMapEntries(this);

    //clear query id map entries.
    if(this->m_pQueryId!=NULL){
        delete this->m_pQueryId; this->m_pQueryId=NULL;
    }
    if(!m_recordBatches.empty()){
        // When multiple qwueries execute in parallel we sometimes get an empty record batch back from the server _after_
        // the last chunk has been received. We eventually delete it.
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Freeing Record batch(es) left behind "<< std::endl;)
        RecordBatch* pR=NULL;
        while(!m_recordBatches.empty()){
            pR=m_recordBatches.front();
            m_recordBatches.pop();
            delete pR;
        }
    }
    if(m_pError!=NULL){
        delete m_pError; m_pError=NULL;
    }
}


connectionStatus_t PooledDrillClientImpl::connect(const char* connStr){
    connectionStatus_t stat = CONN_SUCCESS;
    std::string pathToDrill, protocol, hostPortStr;
    std::string host;
    std::string port;
    m_connectStr=connStr;
    Utils::parseConnectStr(connStr, pathToDrill, protocol, hostPortStr);
    if(!strcmp(protocol.c_str(), "zk")){
        // Get a list of drillbits
        ZookeeperImpl zook;
        std::vector<std::string> drillbits;
        int err = zook.getAllDrillbits(hostPortStr.c_str(), pathToDrill.c_str(), drillbits);
        if(!err){
            Utils::shuffle(drillbits);
            // The original shuffled order is maintained if we shuffle first and then add any missing elements
            Utils::add(m_drillbits, drillbits);
            exec::DrillbitEndpoint e;
            size_t nextIndex=0;
            {
                boost::lock_guard<boost::mutex> cLock(m_cMutex);
                m_lastConnection++;
                nextIndex = (m_lastConnection)%(getDrillbitCount());
            }
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Pooled Connection"
                    << "(" << (void*)this << ")"
                    << ": Current counter is: " 
                    << m_lastConnection << std::endl;)
                err=zook.getEndPoint(m_drillbits, nextIndex, e);
            if(!err){
                host=boost::lexical_cast<std::string>(e.address());
                port=boost::lexical_cast<std::string>(e.user_port());
            }
        }
        if(err){
            return handleConnError(CONN_ZOOKEEPER_ERROR, getMessage(ERR_CONN_ZOOKEEPER, zook.getError().c_str()));
        }
        zook.close();
        m_bIsDirectConnection=false;
    }else if(!strcmp(protocol.c_str(), "local")){
        char tempStr[MAX_CONNECT_STR+1];
        strncpy(tempStr, hostPortStr.c_str(), MAX_CONNECT_STR); tempStr[MAX_CONNECT_STR]=0;
        host=strtok(tempStr, ":");
        port=strtok(NULL, "");
        m_bIsDirectConnection=true;
    }else{
        return handleConnError(CONN_INVALID_INPUT, getMessage(ERR_CONN_UNKPROTO, protocol.c_str()));
    }
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Connecting to endpoint: (Pooled) " << host << ":" << port << std::endl;)
        DrillClientImpl* pDrillClientImpl = new DrillClientImpl();
    stat =  pDrillClientImpl->connect(host.c_str(), port.c_str());
    if(stat == CONN_SUCCESS){
        boost::lock_guard<boost::mutex> lock(m_poolMutex);
        m_clientConnections.push_back(pDrillClientImpl);
    }else{
        DrillClientError* pErr = pDrillClientImpl->getError();
        handleConnError((connectionStatus_t)pErr->status, pErr->msg);
        delete pDrillClientImpl;
    }
    return stat;
}

connectionStatus_t PooledDrillClientImpl::validateHandshake(DrillUserProperties* props){
    // Assume there is one valid connection to at least one drillbit
    connectionStatus_t stat=CONN_FAILURE;
    // Keep a copy of the user properties
    if(props!=NULL){
        m_pUserProperties = new DrillUserProperties;
        for(size_t i=0; i<props->size(); i++){
            m_pUserProperties->setProperty(
                    props->keyAt(i),
                    props->valueAt(i)
                    );
        }
    }
    DrillClientImpl* pDrillClientImpl = getOneConnection();
    if(pDrillClientImpl != NULL){
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Validating handshake: (Pooled) " << pDrillClientImpl->m_connectedHost << std::endl;)
        stat=pDrillClientImpl->validateHandshake(m_pUserProperties);
    }
    else{
        stat =  handleConnError(CONN_NOTCONNECTED, getMessage(ERR_CONN_NOCONN));
    }
    return stat;
}

DrillClientQueryResult* PooledDrillClientImpl::SubmitQuery(::exec::shared::QueryType t, const std::string& plan, pfnQueryResultsListener listener, void* listenerCtx){
    DrillClientQueryResult* pDrillClientQueryResult = NULL;
    DrillClientImpl* pDrillClientImpl = NULL;
    pDrillClientImpl = getOneConnection();
    if(pDrillClientImpl != NULL){
        pDrillClientQueryResult=pDrillClientImpl->SubmitQuery(t,plan,listener,listenerCtx);
        m_queriesExecuted++;
    }
    return pDrillClientQueryResult;
}

void PooledDrillClientImpl::freeQueryResources(DrillClientQueryResult* pQryResult){
    // Nothing to do. If this class ever keeps track of executing queries then it will need 
    // to implement this call to free any query specific resources the pool might have 
    // allocated
    return;
}

bool PooledDrillClientImpl::Active(){
    boost::lock_guard<boost::mutex> lock(m_poolMutex);
    for(std::vector<DrillClientImpl*>::iterator it = m_clientConnections.begin(); it != m_clientConnections.end(); ++it){
        if((*it)->Active()){
            return true;
        }
    }
    return false;
}

void PooledDrillClientImpl::Close() {
    boost::lock_guard<boost::mutex> lock(m_poolMutex);
    for(std::vector<DrillClientImpl*>::iterator it = m_clientConnections.begin(); it != m_clientConnections.end(); ++it){
        (*it)->Close();
        delete *it;
    }
    m_clientConnections.clear();
    if(m_pUserProperties!=NULL){ delete m_pUserProperties; m_pUserProperties=NULL;}
    if(m_pError!=NULL){ delete m_pError; m_pError=NULL;}
    m_lastConnection=-1;
    m_queriesExecuted=0;
}

DrillClientError* PooledDrillClientImpl::getError(){
    std::string errMsg;
    std::string nl="";
    uint32_t stat;
    boost::lock_guard<boost::mutex> lock(m_poolMutex);
    for(std::vector<DrillClientImpl*>::iterator it = m_clientConnections.begin(); it != m_clientConnections.end(); ++it){
        if((*it)->getError() != NULL){
            errMsg+=nl+"Query"/*+(*it)->queryId() +*/":"+(*it)->getError()->msg;
            stat=(*it)->getError()->status;
        }
    }
    if(errMsg.length()>0){
        if(m_pError!=NULL){ delete m_pError; m_pError=NULL; }
        m_pError = new DrillClientError(stat, DrillClientError::QRY_ERROR_START+stat, errMsg);
    }
    return m_pError;
}

//Waits as long as any one drillbit connection has results pending
void PooledDrillClientImpl::waitForResults(){
    boost::lock_guard<boost::mutex> lock(m_poolMutex);
    for(std::vector<DrillClientImpl*>::iterator it = m_clientConnections.begin(); it != m_clientConnections.end(); ++it){
        (*it)->waitForResults();
    }
    return;
}

connectionStatus_t PooledDrillClientImpl::handleConnError(connectionStatus_t status, std::string msg){
    DrillClientError* pErr = new DrillClientError(status, DrillClientError::CONN_ERROR_START+status, msg);
    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Connection Error: (Pooled) " << pErr->msg << std::endl;)
    if(m_pError!=NULL){ delete m_pError; m_pError=NULL;}
    m_pError=pErr;
    return status;
}

DrillClientImpl* PooledDrillClientImpl::getOneConnection(){
    DrillClientImpl* pDrillClientImpl = NULL;
    while(pDrillClientImpl==NULL){
        if(m_queriesExecuted == 0){
            // First query ever sent can use the connection already established to authenticate the user
            boost::lock_guard<boost::mutex> lock(m_poolMutex);
            pDrillClientImpl=m_clientConnections[0];// There should be one connection in the list when the first query is executed
        }else if(m_clientConnections.size() == m_maxConcurrentConnections){
            // Pool is full. Use one of the already established connections
            boost::lock_guard<boost::mutex> lock(m_poolMutex);
            pDrillClientImpl = m_clientConnections[m_queriesExecuted%m_maxConcurrentConnections];
            if(!pDrillClientImpl->Active()){
                Utils::eraseRemove(m_clientConnections, pDrillClientImpl);
                pDrillClientImpl=NULL;
            }
        }else{
            int tries=0;
            connectionStatus_t ret=CONN_SUCCESS;
            while(pDrillClientImpl==NULL && tries++ < 3){
                if((ret=connect(m_connectStr.c_str()))==CONN_SUCCESS){
                    boost::lock_guard<boost::mutex> lock(m_poolMutex);
                    pDrillClientImpl=m_clientConnections.back();
                    ret=pDrillClientImpl->validateHandshake(m_pUserProperties);
                    if(ret!=CONN_SUCCESS){
                        delete pDrillClientImpl; pDrillClientImpl=NULL;
                        m_clientConnections.erase(m_clientConnections.end());
                    }
                }
            } // try a few times
            if(ret!=CONN_SUCCESS){
                break;
            }
        } // need a new connection 
    }// while

    if(pDrillClientImpl==NULL){
        connectionStatus_t status = CONN_NOTCONNECTED;
        handleConnError(status, getMessage(status));
    }
    return pDrillClientImpl;
}

char ZookeeperImpl::s_drillRoot[]="/drill/";
char ZookeeperImpl::s_defaultCluster[]="drillbits1";

ZookeeperImpl::ZookeeperImpl(){
    m_pDrillbits=new String_vector;
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

int ZookeeperImpl::getAllDrillbits(const char* connectStr, const char* pathToDrill, std::vector<std::string>& drillbits){
    uint32_t waitTime=30000; // 10 seconds
    zoo_set_debug_level(getZkLogLevel());
    zoo_deterministic_conn_order(1); // enable deterministic order
    struct String_vector* pDrillbits=NULL;
    m_zh = zookeeper_init(connectStr, watcher, waitTime, 0, this, 0);
    if(!m_zh) {
        m_err = getMessage(ERR_CONN_ZKFAIL);
        zookeeper_close(m_zh);
        return -1;
    }else{
        m_err="";
        //Wait for the completion handler to signal successful connection
        boost::unique_lock<boost::mutex> bufferLock(this->m_cvMutex);
        boost::system_time const timeout=boost::get_system_time()+ boost::posix_time::milliseconds(waitTime);
        while(this->m_bConnecting) {
            if(!this->m_cv.timed_wait(bufferLock, timeout)){
                m_err = getMessage(ERR_CONN_ZKTIMOUT);
                zookeeper_close(m_zh);
                return -1;
            }
        }
    }
    if(m_state!=ZOO_CONNECTED_STATE){
        zookeeper_close(m_zh);
        return -1;
    }
    int rc = ZOK;
    if(pathToDrill==NULL || strlen(pathToDrill)==0){
        m_rootDir=s_drillRoot;
        m_rootDir += s_defaultCluster;
    }else{
        m_rootDir=pathToDrill;
    }

    pDrillbits = new String_vector;
    rc=zoo_get_children(m_zh, m_rootDir.c_str(), 0, pDrillbits);
    if(rc!=ZOK){
        delete pDrillbits;
        m_err=getMessage(ERR_CONN_ZKERR, rc);
        zookeeper_close(m_zh);
        return -1;
    }
    if(pDrillbits && pDrillbits->count > 0){
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Found " << pDrillbits->count << " drillbits in cluster (" 
                << connectStr << "/" << pathToDrill
                << ")." <<std::endl;)
            for(int i=0; i<pDrillbits->count; i++){
                drillbits.push_back(pDrillbits->data[i]);
            }
        for(int i=0; i<drillbits.size(); i++){
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "\t Unshuffled Drillbit id: " << drillbits[i] << std::endl;)
        }
    }
    delete pDrillbits;
    return 0;
}

int ZookeeperImpl::getEndPoint(std::vector<std::string>& drillbits, size_t index, exec::DrillbitEndpoint& endpoint){
    int rc = ZOK;
    exec::DrillServiceInstance drillServiceInstance;
    if( drillbits.size() >0){
        // pick the drillbit at 'index'
        const char * bit=drillbits[index].c_str();
        std::string s;
        s=m_rootDir +  std::string("/") + bit;
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
        exec::DrillServiceInstance drillServiceInstance;
        drillServiceInstance.ParseFromArray(buffer, buffer_len);
        endpoint=drillServiceInstance.endpoint();
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Choosing drillbit <" <<index << ">. Selected " << drillServiceInstance.DebugString() << std::endl;)
    }else{

        m_err=getMessage(ERR_CONN_ZKNODBIT);
        zookeeper_close(m_zh);
        return -1;
    }
    return 0;
}

// Deprecated
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

        std::vector<std::string> randomDrillbits;
        for(int i=0; i<m_pDrillbits->count; i++){
            randomDrillbits.push_back(m_pDrillbits->data[i]);
        }
        //Use the same random shuffle as the Java client instead of picking a drillbit at random.
        //Gives much better randomization when the size of the cluster is small.
        std::random_shuffle(randomDrillbits.begin(), randomDrillbits.end());
        const char * bit=randomDrillbits[0].c_str();
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
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Connected to Zookeeper." << std::endl;)
        }
        boost::lock_guard<boost::mutex> bufferLock(self->m_cvMutex);
        self->m_bConnecting=false;
    }
    self->m_cv.notify_one();
}

void ZookeeperImpl:: debugPrint(){
    if(m_zh!=NULL && m_state==ZOO_CONNECTED_STATE){
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << m_drillServiceInstance.DebugString() << std::endl;)
    }
}

} // namespace Drill
