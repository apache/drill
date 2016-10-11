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
#include <string>
#include <boost/asio.hpp>
#include <boost/assign.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/functional/factory.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>


#include "drill/drillClient.hpp"
#include "drill/fieldmeta.hpp"
#include "drill/recordBatch.hpp"
#include "drillClientImpl.hpp"
#include "collectionsImpl.hpp"
#include "errmsgs.hpp"
#include "logger.hpp"
#include "metadata.hpp"
#include "rpcMessage.hpp"
#include "utils.hpp"
#include "GeneralRPC.pb.h"
#include "UserBitShared.pb.h"
#include "zookeeperClient.hpp"

namespace Drill{

static std::map<exec::shared::QueryResult_QueryState, status_t> QUERYSTATE_TO_STATUS_MAP = boost::assign::map_list_of
    (exec::shared::QueryResult_QueryState_STARTING, QRY_PENDING)
    (exec::shared::QueryResult_QueryState_RUNNING, QRY_RUNNING)
    (exec::shared::QueryResult_QueryState_COMPLETED, QRY_COMPLETED)
    (exec::shared::QueryResult_QueryState_CANCELED, QRY_CANCELED)
    (exec::shared::QueryResult_QueryState_FAILED, QRY_FAILED)
    ;

static std::string debugPrintQid(const exec::shared::QueryId& qid){
    return std::string("[")+boost::lexical_cast<std::string>(qid.part1()) +std::string(":") + boost::lexical_cast<std::string>(qid.part2())+std::string("] ");
}

connectionStatus_t DrillClientImpl::connect(const char* connStr){
    std::string pathToDrill, protocol, hostPortStr;
    std::string host;
    std::string port;

    if (this->m_bIsConnected) {
        if(std::strcmp(connStr, m_connectStr.c_str())){ // trying to connect to a different address is not allowed if already connected
            return handleConnError(CONN_ALREADYCONNECTED, getMessage(ERR_CONN_ALREADYCONN));
        }
        return CONN_SUCCESS;
    }

    m_connectStr=connStr;
    Utils::parseConnectStr(connStr, pathToDrill, protocol, hostPortStr);
    if(protocol == "zk"){
        ZookeeperClient zook(pathToDrill);
        std::vector<std::string> drillbits;
        int err = zook.getAllDrillbits(hostPortStr, drillbits);
        if(!err){
            Utils::shuffle(drillbits);
            exec::DrillbitEndpoint endpoint;
            err = zook.getEndPoint(drillbits[drillbits.size() -1], endpoint);// get the last one in the list
            if(!err){
                host=boost::lexical_cast<std::string>(endpoint.address());
                port=boost::lexical_cast<std::string>(endpoint.user_port());
            }
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Choosing drillbit <" << (drillbits.size() - 1)  << ">. Selected " << endpoint.DebugString() << std::endl;)

        }
        if(err){
            return handleConnError(CONN_ZOOKEEPER_ERROR, getMessage(ERR_CONN_ZOOKEEPER, zook.getError().c_str()));
        }
        zook.close();
        m_bIsDirectConnection=true;
    }else if(protocol == "local"){
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

    }catch(const std::exception & e){
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
    rpc::OutBoundRpcMessage heartbeatMsg(exec::rpc::PING, exec::user::ACK/*can be anything */, 0, &ack);
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


connectionStatus_t DrillClientImpl::sendSync(rpc::OutBoundRpcMessage& msg){
    encode(m_wbuf, msg);
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
        rpc::InBoundRpcMessage msg;
        uint32_t length = 0;
        std::size_t bytes_read = rpc::lengthDecode(m_rbuf, length);
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
            if (!decode(m_rbuf+bytes_read, length, msg)) {
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleHandshake: ERR_CONN_RDFAIL. Cannot decode handshake.\n";)
                handleConnError(CONN_FAILURE, getMessage(ERR_CONN_RDFAIL, "Cannot decode handshake"));
                return;
            }
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
        this->m_serverInfos = b2u.server_infos();

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

    // Adding version info
    exec::user::RpcEndpointInfos* infos = u2b.mutable_client_infos();
    infos->set_name(DRILL_CONNECTOR_NAME);
    infos->set_version(DRILL_VERSION_STRING);
    infos->set_majorversion(DRILL_VERSION_MAJOR);
    infos->set_minorversion(DRILL_VERSION_MINOR);
    infos->set_patchversion(DRILL_VERSION_PATCH);

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

        rpc::OutBoundRpcMessage out_msg(exec::rpc::REQUEST, exec::user::HANDSHAKE, coordId, &u2b);
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

    boost::function<DrillClientQueryResult*(int32_t)> factory = boost::bind(
            boost::factory<DrillClientQueryResult*>(),
            boost::ref(*this),
            _1,
            boost::cref(plan),
            l,
            lCtx);
    return sendMsg(factory, ::exec::user::RUN_QUERY, query);
}

DrillClientPrepareHandle* DrillClientImpl::PrepareQuery(const std::string& plan,
        pfnPreparedStatementListener l,
        void* lCtx){
    exec::user::CreatePreparedStatementReq query;
    query.set_sql_query(plan);

    boost::function<DrillClientPrepareHandle*(int32_t)> factory = boost::bind(
            boost::factory<DrillClientPrepareHandle*>(),
            boost::ref(*this),
            _1,
            boost::cref(plan),
            l,
            lCtx);
    return sendMsg(factory, ::exec::user::CREATE_PREPARED_STATEMENT, query);
}

DrillClientQueryResult* DrillClientImpl::ExecuteQuery(const PreparedStatement& pstmt,
        pfnQueryResultsListener l,
        void* lCtx){
    const DrillClientPrepareHandle& handle = static_cast<const DrillClientPrepareHandle&>(pstmt);

    exec::user::RunQuery query;
    query.set_results_mode(exec::user::STREAM_FULL);
    query.set_type(::exec::shared::PREPARED_STATEMENT);
    query.set_allocated_prepared_statement_handle(new ::exec::user::PreparedStatementHandle(handle.m_preparedStatementHandle));

    boost::function<DrillClientQueryResult*(int32_t)> factory = boost::bind(
            boost::factory<DrillClientQueryResult*>(),
            boost::ref(*this),
            _1,
            boost::cref(handle.m_query),
            l,
            lCtx);
    return sendMsg(factory, ::exec::user::RUN_QUERY, query);
}

DrillClientCatalogResult* DrillClientImpl::getCatalogs(const std::string& catalogPattern,
        Metadata::pfnCatalogMetadataListener listener,
        void* listenerCtx) {
    exec::user::GetCatalogsReq query;
    exec::user::LikeFilter* catalogFilter(query.mutable_catalog_name_filter());
    catalogFilter->set_pattern(catalogPattern);

    boost::function<DrillClientCatalogResult*(int32_t)> factory = boost::bind(
            boost::factory<DrillClientCatalogResult*>(),
            boost::ref(*this),
            _1,
            listener,
            listenerCtx);
    return sendMsg(factory, ::exec::user::GET_CATALOGS, query);
}

DrillClientSchemaResult* DrillClientImpl::getSchemas(const std::string& catalogPattern,
        const std::string& schemaPattern,
        Metadata::pfnSchemaMetadataListener listener,
        void* listenerCtx) {
    exec::user::GetSchemasReq query;
    query.mutable_catalog_name_filter()->set_pattern(catalogPattern);
    query.mutable_schema_name_filter()->set_pattern(schemaPattern);

    boost::function<DrillClientSchemaResult*(int32_t)> factory = boost::bind(
            boost::factory<DrillClientSchemaResult*>(),
            boost::ref(*this),
            _1,
            listener,
            listenerCtx);
    return sendMsg(factory, ::exec::user::GET_SCHEMAS, query);
}

DrillClientTableResult* DrillClientImpl::getTables(const std::string& catalogPattern,
        const std::string& schemaPattern,
        const std::string& tablePattern,
		const std::vector<std::string>* tableTypes,
        Metadata::pfnTableMetadataListener listener,
        void* listenerCtx) {
    exec::user::GetTablesReq query;
    query.mutable_catalog_name_filter()->set_pattern(catalogPattern);
    query.mutable_schema_name_filter()->set_pattern(schemaPattern);
    query.mutable_table_name_filter()->set_pattern(tablePattern);
    if (tableTypes) {
    	std::copy(tableTypes->begin(), tableTypes->end(),
    			google::protobuf::RepeatedFieldBackInserter(query.mutable_table_type_filter()));
    }

    boost::function<DrillClientTableResult*(int32_t)> factory = boost::bind(
            boost::factory<DrillClientTableResult*>(),
            boost::ref(*this),
            _1,
            listener,
            listenerCtx);
    return sendMsg(factory, ::exec::user::GET_TABLES, query);
}

DrillClientColumnResult* DrillClientImpl::getColumns(const std::string& catalogPattern,
        const std::string& schemaPattern,
        const std::string& tablePattern,
        const std::string& columnsPattern,
        Metadata::pfnColumnMetadataListener listener,
        void* listenerCtx) {
    exec::user::GetColumnsReq query;
    query.mutable_catalog_name_filter()->set_pattern(catalogPattern);
    query.mutable_schema_name_filter()->set_pattern(schemaPattern);
    query.mutable_table_name_filter()->set_pattern(tablePattern);
    query.mutable_column_name_filter()->set_pattern(columnsPattern);

    boost::function<DrillClientColumnResult*(int32_t)> factory = boost::bind(
            boost::factory<DrillClientColumnResult*>(),
            boost::ref(*this),
            _1,
            listener,
            listenerCtx);
    return sendMsg(factory, ::exec::user::GET_COLUMNS, query);
}

template<typename Handle>
Handle* DrillClientImpl::sendMsg(boost::function<Handle*(int32_t)> handleFactory, ::exec::user::RpcType type, const ::google::protobuf::Message& message) {
    int32_t coordId;
    Handle* phandle=NULL;
    connectionStatus_t cStatus=CONN_SUCCESS;
    {
        boost::lock_guard<boost::mutex> prLock(this->m_prMutex);
        boost::lock_guard<boost::mutex> dcLock(this->m_dcMutex);
        coordId = this->getNextCoordinationId();
        rpc::OutBoundRpcMessage out_msg(exec::rpc::REQUEST, type, coordId, &message);

        phandle = handleFactory(coordId);
        this->m_queryHandles[coordId]=phandle;

        connectionStatus_t cStatus=sendSync(out_msg);
        if(cStatus == CONN_SUCCESS){
            bool sendRequest=false;

            DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG)  << "Sent " << ::exec::user::RpcType_Name(type) << " request. " << "[" << m_connectedHost << "]"  << "Coordination id = " << coordId << std::endl;)
                DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG)  << "Sent " << ::exec::user::RpcType_Name(type) <<  " Coordination id = " << coordId << " query: " << phandle->getQuery() << std::endl;)

                if(m_pendingRequests++==0){
                    sendRequest=true;
                }else{
                    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Queuing " << ::exec::user::RpcType_Name(type) <<  " request to server" << std::endl;)
                        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Number of pending requests = " << m_pendingRequests << std::endl;)
                }
            if(sendRequest){
                DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Sending " << ::exec::user::RpcType_Name(type) <<  " request. Number of pending requests = "
                        << m_pendingRequests << std::endl;)
                    getNextResult(); // async wait for results
            }
        }

    }
    if(cStatus!=CONN_SUCCESS){
        this->m_queryHandles.erase(coordId);
        delete phandle;
        return NULL;
    }

    //run this in a new thread
    startMessageListener();

    return phandle;
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
        rpc::InBoundRpcMessage& msg){

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
        std::size_t bytes_read = rpc::lengthDecode(_buf, rmsgLen);
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
            boost::system::error_code error;
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
                if (!decode(currentBuffer->m_pBuffer, rmsgLen, msg)) {
                    Utils::freeBuffer(_buf, LEN_PREFIX_BUFLEN);
                    return handleQryError(QRY_COMM_ERROR,
                            getMessage(ERR_QRY_COMMERR, "Cannot decode server message"), NULL);;
                }
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

status_t DrillClientImpl::processQueryResult(AllocatedBufferPtr  allocatedBuffer, const rpc::InBoundRpcMessage& msg ){
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

status_t DrillClientImpl::processQueryData(AllocatedBufferPtr allocatedBuffer, const rpc::InBoundRpcMessage& msg ){
    DrillClientQueryResult* pDrillClientQueryResult=NULL;
    status_t ret=QRY_SUCCESS;
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

        const ::exec::shared::QueryId& qid = qr->query_id();
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

        // check if query has been cancelled
        if (pDrillClientQueryResult->isCancelled()) {
            DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Processing Query cancellation " << std::endl;)
        	delete qr;
        	delete allocatedBuffer;
        	ret =  QRY_CANCEL;
        } else {
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
        	DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Building record batch for Query Id - " << debugPrintQid(qid) << std::endl;)

        	pRecordBatch= new RecordBatch(qr, allocatedBuffer,  msg.m_dbody);
        	pDrillClientQueryResult->m_numBatches++;

        	DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Allocated new Record batch." << (void*)pRecordBatch << std::endl;)
        	pRecordBatch->build();
        	DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << debugPrintQid(qid)<<"recordBatch.numRecords "
        			<< pRecordBatch->getNumRecords()  << std::endl;)
        	DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << debugPrintQid(qid)<<"recordBatch.numFields "
        			<< pRecordBatch->getNumFields()  << std::endl;)

					ret=pDrillClientQueryResult->setupColumnDefs(qr);
        	if(ret==QRY_SUCCESS_WITH_INFO){
        		pRecordBatch->schemaChanged(true);
        	}

        	pDrillClientQueryResult->setIsQueryPending(true);
        	if(pDrillClientQueryResult->m_bIsLastChunk){
        		DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << debugPrintQid(qid)
        				<<  "Received last batch. " << std::endl;)
            		ret=QRY_NO_MORE_DATA;
        	}
        	pDrillClientQueryResult->setQueryStatus(ret);
        	ret = pDrillClientQueryResult->notifyListener(pRecordBatch, NULL);
        }
    } // release lock
    if((ret==QRY_FAILURE || ret==QRY_CANCELED) && pDrillClientQueryResult != NULL){
        return handleQryCancellation(ret, pDrillClientQueryResult);
    }
    return ret;
}

status_t DrillClientImpl::processQueryId(AllocatedBufferPtr allocatedBuffer, const rpc::InBoundRpcMessage& msg ){
    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Processing Query Handle with coordination id:" << msg.m_coord_id << std::endl;)
	DrillClientQueryResult* pDrillClientQueryResult=NULL;
    status_t ret=QRY_SUCCESS;

    // make sure to deallocate buffer
    boost::shared_ptr<AllocatedBuffer> deallocationGuard(allocatedBuffer);
    {
    	boost::lock_guard<boost::mutex> lock(m_dcMutex);

    	if(msg.m_coord_id==0){
    		DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processQueryId: m_coord_id=0. Ignore and return QRY_SUCCESS." << std::endl;)
      		return QRY_SUCCESS;
    	}

    	for(std::map< ::exec::shared::QueryId*, DrillClientQueryResult*>::const_iterator it=this->m_queryResults.begin();it!=this->m_queryResults.end();it++){
    		DrillClientQueryResult* pQueryResult=it->second;
    		std::string qidString = (pQueryResult->m_pQueryId!=NULL)?debugPrintQid(*pQueryResult->m_pQueryId):std::string("NULL");
    		DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "DrillClientImpl::processQueryId: m_queryIds: coordinationId: " << pQueryResult->m_coordinationId
    				<< " QueryId: "<< qidString << std::endl;)
    	}

    	std::map<int, DrillClientQueryHandle*>::const_iterator it;
    	it=this->m_queryHandles.find(msg.m_coord_id);
    	if(it==this->m_queryHandles.end()){
    		return handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVQUERYID), NULL);
    	}
    	pDrillClientQueryResult=dynamic_cast<DrillClientQueryResult*>((*it).second);
    	if (!pDrillClientQueryResult) {
    		return handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVQUERYID), NULL);
    	}

    	// Check for cancellation to notify
    	if (pDrillClientQueryResult->isCancelled()) {
    		ret = QRY_CANCELED;
    	}
    	else {
    		exec::shared::QueryId *qid = new exec::shared::QueryId;
    		DRILL_MT_LOG(DRILL_LOG(LOG_TRACE)  << "Received Query Handle " << msg.m_pbody.size() << std::endl;)
    		qid->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
    		DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Query Id - " << debugPrintQid(*qid) << std::endl;)
    		m_queryResults[qid]=pDrillClientQueryResult;
    		//save queryId allocated here so we can free it later
    		pDrillClientQueryResult->setQueryId(qid);
    	}
    }
    if (ret == QRY_CANCELED && pDrillClientQueryResult != NULL) {
    	return handleQryCancellation(ret, pDrillClientQueryResult);
    }
    return ret;
}

status_t DrillClientImpl::processPreparedStatement(AllocatedBufferPtr allocatedBuffer, const rpc::InBoundRpcMessage& msg ){
    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Processing Prepared Statement with coordination id:" << msg.m_coord_id << std::endl;)
    status_t ret=QRY_SUCCESS;

    // make sure to deallocate buffer
    boost::shared_ptr<AllocatedBuffer> deallocationGuard(allocatedBuffer);
    boost::lock_guard<boost::mutex> lock(m_dcMutex);

    if(msg.m_coord_id==0){
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processPreparedStatement: m_coord_id=0. Ignore and return QRY_SUCCESS." << std::endl;)
        return QRY_SUCCESS;
    }
    std::map<int,DrillClientQueryHandle*>::const_iterator it=this->m_queryHandles.find(msg.m_coord_id);
    if(it!=this->m_queryHandles.end()){
        DrillClientPrepareHandle* pDrillClientPrepareHandle=static_cast<DrillClientPrepareHandle*>((*it).second);
        exec::user::CreatePreparedStatementResp resp;
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE)  << "Received Prepared Statement Handle " << msg.m_pbody.size() << std::endl;)
        if (!resp.ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size())) {
            return handleQryError(QRY_COMM_ERROR, "Cannot decode prepared statement", pDrillClientPrepareHandle);
        }
        if (resp.has_status() && resp.status() != exec::user::OK) {
            return handleQryError(QRY_FAILED, resp.error(), pDrillClientPrepareHandle);
        }
        pDrillClientPrepareHandle->setupPreparedStatement(resp.prepared_statement());
        pDrillClientPrepareHandle->notifyListener(pDrillClientPrepareHandle, NULL);
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Prepared Statement handle - " << resp.prepared_statement().server_handle().DebugString() << std::endl;)
    }else{
        return handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVQUERYID), NULL);
    }
    m_pendingRequests--;
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processPreparedStament: " << m_pendingRequests << " requests pending." << std::endl;)
    if(m_pendingRequests==0){
        // signal any waiting client that it can exit because there are no more any query results to arrive.
        // We keep the heartbeat going though.
        m_cv.notify_one();
    }
    return ret;
}

status_t DrillClientImpl::processCatalogsResult(AllocatedBufferPtr allocatedBuffer, const rpc::InBoundRpcMessage& msg ){
    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Processing GetCatalogsResp with coordination id:" << msg.m_coord_id << std::endl;)
    status_t ret=QRY_SUCCESS;

    // make sure to deallocate buffer
    boost::shared_ptr<AllocatedBuffer> deallocationGuard(allocatedBuffer);
    boost::lock_guard<boost::mutex> lock(m_dcMutex);

    if(msg.m_coord_id==0){
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processCatalogsResult: m_coord_id=0. Ignore and return QRY_SUCCESS." << std::endl;)
        return QRY_SUCCESS;
    }
    std::map<int,DrillClientQueryHandle*>::const_iterator it=this->m_queryHandles.find(msg.m_coord_id);
    if(it!=this->m_queryHandles.end()){
        DrillClientCatalogResult* pHandle=static_cast<DrillClientCatalogResult*>((*it).second);
        exec::user::GetCatalogsResp* resp = new exec::user::GetCatalogsResp;
        pHandle->attachMetadataResult(resp);
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE)  << "Received GetCatalogs result Handle " << msg.m_pbody.size() << std::endl;)
        if (!(resp->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size()))) {
            return handleQryError(QRY_COMM_ERROR, "Cannot decode getcatalogs results", pHandle);
        }
        if (resp->status() != exec::user::OK) {
            return handleQryError(QRY_FAILED, resp->error(), pHandle);
        }

        const ::google::protobuf::RepeatedPtrField< ::exec::user::CatalogMetadata>& catalogs = resp->catalogs();
        pHandle->m_meta.clear();
        pHandle->m_meta.reserve(resp->catalogs_size());

        for(::google::protobuf::RepeatedPtrField< ::exec::user::CatalogMetadata>::const_iterator it = catalogs.begin(); it != catalogs.end(); ++it) {
            meta::DrillCatalogMetadata meta(*it);
            pHandle->m_meta.push_back(meta);
        }
        pHandle->notifyListener(&pHandle->m_meta, NULL);
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "GetCatalogs result -  " << resp->catalogs_size() << " catalog(s)" << std::endl;)
    }else{
        return handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVQUERYID), NULL);
    }
    m_pendingRequests--;
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processCatalogsResult: " << m_pendingRequests << " requests pending." << std::endl;)
    if(m_pendingRequests==0){
        // signal any waiting client that it can exit because there are no more any query results to arrive.
        // We keep the heartbeat going though.
        m_cv.notify_one();
    }
    return ret;
}

status_t DrillClientImpl::processSchemasResult(AllocatedBufferPtr allocatedBuffer, const rpc::InBoundRpcMessage& msg ){
    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Processing GetSchemaResp with coordination id:" << msg.m_coord_id << std::endl;)
    status_t ret=QRY_SUCCESS;

    // make sure to deallocate buffer
    boost::shared_ptr<AllocatedBuffer> deallocationGuard(allocatedBuffer);
    boost::lock_guard<boost::mutex> lock(m_dcMutex);

    if(msg.m_coord_id==0){
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processSchemasResult: m_coord_id=0. Ignore and return QRY_SUCCESS." << std::endl;)
        return QRY_SUCCESS;
    }
    std::map<int,DrillClientQueryHandle*>::const_iterator it=this->m_queryHandles.find(msg.m_coord_id);
    if(it!=this->m_queryHandles.end()){
        DrillClientSchemaResult* pHandle=static_cast<DrillClientSchemaResult*>((*it).second);
        exec::user::GetSchemasResp* resp = new exec::user::GetSchemasResp();
        pHandle->attachMetadataResult(resp);
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE)  << "Received GetSchemasResp result Handle " << msg.m_pbody.size() << std::endl;)
        if (!(resp->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size()))) {
            return handleQryError(QRY_COMM_ERROR, "Cannot decode getschemas results", pHandle);
        }
        if (resp->status() != exec::user::OK) {
            return handleQryError(QRY_FAILED, resp->error(), pHandle);
        }

        const ::google::protobuf::RepeatedPtrField< ::exec::user::SchemaMetadata>& schemas = resp->schemas();
        pHandle->m_meta.clear();
        pHandle->m_meta.reserve(resp->schemas_size());

        for(::google::protobuf::RepeatedPtrField< ::exec::user::SchemaMetadata>::const_iterator it = schemas.begin(); it != schemas.end(); ++it) {
            meta::DrillSchemaMetadata meta(*it);
            pHandle->m_meta.push_back(meta);
        }
        pHandle->notifyListener(&pHandle->m_meta, NULL);
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "GetSchemaResp result - " << resp->schemas_size() << " schema(s)" << std::endl;)
    }else{
        return handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVQUERYID), NULL);
    }
    m_pendingRequests--;
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processSchemasResult: " << m_pendingRequests << " requests pending." << std::endl;)
    if(m_pendingRequests==0){
        // signal any waiting client that it can exit because there are no more any query results to arrive.
        // We keep the heartbeat going though.
        m_cv.notify_one();
    }
    return ret;
}

status_t DrillClientImpl::processTablesResult(AllocatedBufferPtr allocatedBuffer, const rpc::InBoundRpcMessage& msg ){
    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Processing GetTablesResp with coordination id:" << msg.m_coord_id << std::endl;)
    status_t ret=QRY_SUCCESS;

    // make sure to deallocate buffer
    boost::shared_ptr<AllocatedBuffer> deallocationGuard(allocatedBuffer);
    boost::lock_guard<boost::mutex> lock(m_dcMutex);

    if(msg.m_coord_id==0){
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processTablesResult: m_coord_id=0. Ignore and return QRY_SUCCESS." << std::endl;)
        return QRY_SUCCESS;
    }
    std::map<int,DrillClientQueryHandle*>::const_iterator it=this->m_queryHandles.find(msg.m_coord_id);
    if(it!=this->m_queryHandles.end()){
        DrillClientTableResult* pHandle=static_cast<DrillClientTableResult*>((*it).second);
        exec::user::GetTablesResp* resp =  new exec::user::GetTablesResp();
        pHandle->attachMetadataResult(resp);
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE)  << "Received GeTablesResp result Handle " << msg.m_pbody.size() << std::endl;)
        if (!(resp->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size()))) {
            return handleQryError(QRY_COMM_ERROR, "Cannot decode gettables results", pHandle);
        }
        if (resp->status() != exec::user::OK) {
            return handleQryError(QRY_FAILED, resp->error(), pHandle);
        }
        const ::google::protobuf::RepeatedPtrField< ::exec::user::TableMetadata>& tables = resp->tables();
        pHandle->m_meta.clear();
        pHandle->m_meta.reserve(resp->tables_size());

        for(::google::protobuf::RepeatedPtrField< ::exec::user::TableMetadata>::const_iterator it = tables.begin(); it != tables.end(); ++it) {
            meta::DrillTableMetadata meta(*it);
            pHandle->m_meta.push_back(meta);
        }
        pHandle->notifyListener(&pHandle->m_meta, NULL);
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "GetTables result - " << resp->tables_size() << " table(s)" << std::endl;)
    }else{
        return handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVQUERYID), NULL);
    }
    m_pendingRequests--;
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processTablesResult: " << m_pendingRequests << " requests pending." << std::endl;)
    if(m_pendingRequests==0){
        // signal any waiting client that it can exit because there are no more any query results to arrive.
        // We keep the heartbeat going though.
        m_cv.notify_one();
    }
    return ret;
}

status_t DrillClientImpl::processColumnsResult(AllocatedBufferPtr allocatedBuffer, const rpc::InBoundRpcMessage& msg ){
    DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Processing GetColumnsResp with coordination id:" << msg.m_coord_id << std::endl;)
    status_t ret=QRY_SUCCESS;

    // make sure to deallocate buffer
    boost::shared_ptr<AllocatedBuffer> deallocationGuard(allocatedBuffer);
    boost::lock_guard<boost::mutex> lock(m_dcMutex);

    if(msg.m_coord_id==0){
         DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processColumnsResult: m_coord_id=0. Ignore and return QRY_SUCCESS." << std::endl;)
        return QRY_SUCCESS;
    }
    std::map<int,DrillClientQueryHandle*>::const_iterator it=this->m_queryHandles.find(msg.m_coord_id);
    if(it!=this->m_queryHandles.end()){
        DrillClientColumnResult* pHandle=static_cast<DrillClientColumnResult*>((*it).second);
        exec::user::GetColumnsResp* resp = new exec::user::GetColumnsResp();
        pHandle->attachMetadataResult(resp);
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE)  << "Received GetColumnsResp result Handle " << msg.m_pbody.size() << std::endl;)
        if (!(resp->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size()))) {
            return handleQryError(QRY_COMM_ERROR, "Cannot decode getcolumns results", pHandle);
        }
        if (resp->status() != exec::user::OK) {
            return handleQryError(QRY_FAILED, resp->error(), pHandle);
        }
        const ::google::protobuf::RepeatedPtrField< ::exec::user::ColumnMetadata>& columns = resp->columns();
        pHandle->m_meta.clear();
        pHandle->m_meta.reserve(resp->columns_size());

        for(::google::protobuf::RepeatedPtrField< ::exec::user::ColumnMetadata>::const_iterator it = columns.begin(); it != columns.end(); ++it) {
            meta::DrillColumnMetadata meta(*it);
            pHandle->m_meta.push_back(meta);
        }
        pHandle->notifyListener(&pHandle->m_meta, NULL);
        DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "GetColumnsResp result - " << resp->columns_size() << " columns(s)" << std::endl;)
    }else{
        return handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVQUERYID), NULL);
    }
    m_pendingRequests--;
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::processColumnsResult: " << m_pendingRequests << " requests pending." << std::endl;)
    if(m_pendingRequests==0){
        // signal any waiting client that it can exit because there are no more any query results to arrive.
        // We keep the heartbeat going though.
        m_cv.notify_one();
    }
    return ret;
}

DrillClientQueryResult* DrillClientImpl::findQueryResult(const exec::shared::QueryId& qid){
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
    it=this->m_queryResults.find(const_cast<exec::shared::QueryId * const>(&qid));
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
        const boost::system::error_code& error,
        size_t bytes_transferred) {
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: Handle Read from buffer "
        <<  reinterpret_cast<int*>(_buf) << std::endl;)
    if(DrillClientConfig::getQueryTimeout() > 0){
        // Cancel the timeout if handleRead is called
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: Cancel deadline timer.\n";)
        m_deadlineTimer.cancel();
    }
    if (error) {
        // boost error
        Utils::freeBuffer(_buf, LEN_PREFIX_BUFLEN);
        boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: ERR_QRY_COMMERR. "
            "Boost Communication Error: " << error.message() << std::endl;)
        handleQryError(QRY_COMM_ERROR, getMessage(ERR_QRY_COMMERR, error.message().c_str()), NULL);
        return;
    }

    rpc::InBoundRpcMessage msg;
    boost::lock_guard<boost::mutex> lockPR(this->m_prMutex);

    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Getting new message" << std::endl;)
    AllocatedBufferPtr allocatedBuffer=NULL;

    if(readMsg(_buf, &allocatedBuffer, msg)!=QRY_SUCCESS){
        delete allocatedBuffer;
        if(m_pendingRequests!=0){
            boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
            getNextResult();
        }
        return;
    }

    if(msg.m_mode==exec::rpc::PONG) { //heartbeat response. Throw it away
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
    }

    if(msg.m_mode == exec::rpc::RESPONSE) {
        status_t s;
        switch(msg.m_rpc_type) {
        case exec::user::QUERY_HANDLE:
            s = processQueryId(allocatedBuffer, msg);
            break;

        case exec::user::PREPARED_STATEMENT:
            s = processPreparedStatement(allocatedBuffer, msg);
            break;

        case exec::user::CATALOGS:
            s = processCatalogsResult(allocatedBuffer, msg);
            break;

        case exec::user::SCHEMAS:
            s = processSchemasResult(allocatedBuffer, msg);
            break;

        case exec::user::TABLES:
            s = processTablesResult(allocatedBuffer, msg);
            break;

        case exec::user::COLUMNS:
            s = processColumnsResult(allocatedBuffer, msg);
            break;

        case exec::user::HANDSHAKE:
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: Handshake response from server. Ignore.\n";)
            delete allocatedBuffer;
            break;

        case exec::user::ACK:
            // Cancel requests will result in an ACK sent back.
            // Consume silently
            s = QRY_CANCELED;
            delete allocatedBuffer;
            break;

        default:
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: ERR_QRY_INVRPCTYPE. "
                    << "QueryResult returned " << msg.m_rpc_type << std::endl;)
            delete allocatedBuffer;
            handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVRPCTYPE, msg.m_rpc_type), NULL);
        }

        if (m_pendingRequests != 0) {
            boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
            getNextResult();
        }

        return;
    }

    if (msg.has_mode() && msg.m_mode == exec::rpc::REQUEST) {
        status_t s;
        switch(msg.m_rpc_type) {
        case exec::user::QUERY_RESULT:
            s = processQueryResult(allocatedBuffer, msg);
            break;

        case exec::user::QUERY_DATA:
            s = processQueryData(allocatedBuffer, msg);
            break;

        case exec::user::HANDSHAKE:
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: Handshake request from server. Send response.\n";)
            delete allocatedBuffer;
            // In one case when the client hung, we observed that the server was sending a handshake request to the client
            // We should properly handle these handshake requests/responses
            {
                boost::lock_guard<boost::mutex> lockDC(this->m_dcMutex);
                exec::user::UserToBitHandshake u2b;
                u2b.set_channel(exec::shared::USER);
                u2b.set_rpc_version(DRILL_RPC_VERSION);
                u2b.set_support_listening(true);
                rpc::OutBoundRpcMessage out_msg(exec::rpc::RESPONSE, exec::user::HANDSHAKE, msg.m_coord_id, &u2b);
                sendSync(out_msg);
                DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: Handshake response sent.\n";)
            }
            break;

        default:
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: ERR_QRY_INVRPCTYPE. "
                    << "QueryResult returned " << msg.m_rpc_type << std::endl;)
            delete allocatedBuffer;
            handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVRPCTYPE, msg.m_rpc_type), NULL);
        }

        if (m_pendingRequests != 0) {
            boost::lock_guard<boost::mutex> lock(this->m_dcMutex);
            getNextResult();
        }

        return;
    }

    // If not QUERY_RESULT, then we think something serious has gone wrong?
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "DrillClientImpl::handleRead: ERR_QRY_INVRPCTYPE. "
        << "QueryResult returned " << msg.m_rpc_type << " for " << msg.m_mode << std::endl;)
    handleQryError(QRY_INTERNAL_ERROR, getMessage(ERR_QRY_INVRPCTYPE, msg.m_rpc_type), NULL);
    delete allocatedBuffer;

}

status_t DrillClientImpl::validateDataMessage(const rpc::InBoundRpcMessage& msg, const exec::shared::QueryData& qd, std::string& valErr){
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

status_t DrillClientImpl::validateResultMessage(const rpc::InBoundRpcMessage& msg, const exec::shared::QueryResult& qr, std::string& valErr){
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

connectionStatus_t DrillClientImpl::handleConnError(connectionStatus_t status, const std::string& msg){
    DrillClientError* pErr = new DrillClientError(status, DrillClientError::CONN_ERROR_START+status, msg);
    m_pendingRequests=0;
    if(!m_queryHandles.empty()){
        // set query error only if queries are running
        broadcastError(pErr);
    }else{
        if(m_pError!=NULL){ delete m_pError; m_pError=NULL;}
        m_pError=pErr;
        shutdownSocket();
    }
    return status;
}

status_t DrillClientImpl::handleQryError(status_t status, const std::string& msg, DrillClientQueryHandle* pQueryHandle){
    DrillClientError* pErr = new DrillClientError(status, DrillClientError::QRY_ERROR_START+status, msg);
    // set query error only if queries are running
    if(pQueryHandle!=NULL){
        m_pendingRequests--;
        pQueryHandle->signalError(pErr);
    }else{
        m_pendingRequests=0;
        broadcastError(pErr);
    }
    return status;
}

status_t DrillClientImpl::handleQryError(status_t status,
        const exec::shared::DrillPBError& e,
        DrillClientQueryHandle* pQueryHandle){
    assert(pQueryHandle!=NULL);
    DrillClientError* pErr =  DrillClientError::getErrorObject(e);
    pQueryHandle->signalError(pErr);
    m_pendingRequests--;
    return status;
}

status_t DrillClientImpl::handleQryCancellation(status_t status, DrillClientQueryResult* pQueryHandle) {
	sendCancel(&pQueryHandle->getQueryId());
	// Do not decrement pending requests here. We have sent a cancel and we may still receive results that are
	// pushed on the wire before the cancel is processed.
	pQueryHandle->setIsQueryPending(false);
	DRILL_MT_LOG(DRILL_LOG(LOG_DEBUG) << "Client app cancelled query." << std::endl;)
	pQueryHandle->setQueryStatus(status);
	removeQueryResult(pQueryHandle);
	removeQueryHandle(pQueryHandle);
	return status;
}

void DrillClientImpl::broadcastError(DrillClientError* pErr){
    if(pErr!=NULL){
        std::map<int, DrillClientQueryHandle*>::const_iterator iter;
        if(!m_queryHandles.empty()){
            for(iter = m_queryHandles.begin(); iter != m_queryHandles.end(); iter++) {
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
        const std::string& msg,
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

void DrillClientImpl::removeQueryHandle(DrillClientQueryHandle* pQueryHandle){
    boost::lock_guard<boost::mutex> lock(m_dcMutex);
    if(!m_queryHandles.empty()){
        for(std::map<int, DrillClientQueryHandle*>::const_iterator iter=m_queryHandles.begin(); iter!=m_queryHandles.end(); iter++) {
            if(pQueryHandle==(DrillClientQueryHandle*)iter->second){
                m_queryHandles.erase(iter->first);
                break;
            }
        }
    }
}

void DrillClientImpl::removeQueryResult(DrillClientQueryResult* pQueryResult){
    boost::lock_guard<boost::mutex> lock(m_dcMutex);
    if(!m_queryResults.empty()){
        for(std::map<exec::shared::QueryId*, DrillClientQueryResult*, compareQueryId>::const_iterator it=m_queryResults.begin(); it!=m_queryResults.end(); it++) {
            if(pQueryResult==(DrillClientQueryResult*)it->second){
                m_queryResults.erase(it->first);
                break;
            }
        }
    }
}

void DrillClientImpl::sendAck(const rpc::InBoundRpcMessage& msg, bool isOk){
    exec::rpc::Ack ack;
    ack.set_ok(isOk);
    rpc::OutBoundRpcMessage ack_msg(exec::rpc::RESPONSE, exec::user::ACK, msg.m_coord_id, &ack);
    boost::lock_guard<boost::mutex> lock(m_dcMutex);
    sendSync(ack_msg);
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "ACK sent" << std::endl;)
}

void DrillClientImpl::sendCancel(const exec::shared::QueryId* pQueryId){
    boost::lock_guard<boost::mutex> lock(m_dcMutex);
    uint64_t coordId = this->getNextCoordinationId();
    rpc::OutBoundRpcMessage cancel_msg(exec::rpc::REQUEST, exec::user::CANCEL_QUERY, coordId, pQueryId);
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

meta::DrillMetadata* DrillClientImpl::getMetadata() {
    return new meta::DrillMetadata(*this);
}

void DrillClientImpl::freeMetadata(meta::DrillMetadata* metadata) {
    delete metadata;
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
    if(this->isCancelled()){
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
    while(!this->m_bHasData && !this->hasError() && m_bIsQueryPending) {
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
    while(!this->m_bHasData && !this->hasError() && m_bIsQueryPending){
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
    while(!this->m_bHasData && !this->hasError() && m_bIsQueryPending) {
        this->m_cv.wait(cvLock);
    }
}

template<typename Listener, typename Value>
status_t DrillClientBaseHandle<Listener, Value>::notifyListener(Value v, DrillClientError* pErr){
	return m_pApplicationListener(getApplicationContext(), v, pErr);
}

void DrillClientQueryHandle::cancel() {
    this->m_bCancel=true;
}

void DrillClientQueryHandle::signalError(DrillClientError* pErr){
    // Ignore return values from the listener.
    if(pErr!=NULL){
        if(m_pError!=NULL){
            delete m_pError; m_pError=NULL;
        }
        m_pError=pErr;
        // TODO should it be protected by m_cvMutex?
        m_bHasError=true;
    }
    return;
}

template<typename Listener, typename Value>
void DrillClientBaseHandle<Listener, Value>::signalError(DrillClientError* pErr){
    DrillClientQueryHandle::signalError(pErr);
    // Ignore return values from the listener.
    if(pErr!=NULL){
        this->notifyListener(NULL, pErr);
    }
}

status_t DrillClientQueryResult::notifyListener(RecordBatch* batch, DrillClientError* pErr) {
    pfnQueryResultsListener pResultsListener=getApplicationListener();
    if(pResultsListener!=NULL){
        return pResultsListener(this, batch, pErr);
    }else{
        return defaultQueryResultsListener(this, batch, pErr);
    }
}

void DrillClientQueryResult::signalError(DrillClientError* pErr){
    DrillClientQueryHandle::signalError(pErr);
    // Ignore return values from the listener.
    if(pErr!=NULL){
        this->notifyListener(NULL, pErr);
        {
            boost::lock_guard<boost::mutex> cvLock(this->m_cvMutex);
            m_bIsQueryPending=false;
            m_bHasData=false;
        }
        //Signal the cv in case there is a client waiting for data already.
        m_cv.notify_one();
    }
    return;
}

void DrillClientQueryResult::signalComplete(){
    this->notifyListener(NULL, NULL);
    {
        boost::lock_guard<boost::mutex> cvLock(this->m_cvMutex);
        m_bIsQueryPending=!(this->m_recordBatches.empty()&&m_queryState==exec::shared::QueryResult_QueryState_COMPLETED);
        resetError();
    }
    //Signal the cv in case there is a client waiting for data already.
    m_cv.notify_one();
    return;
}

void DrillClientQueryHandle::clearAndDestroy(){
    //Tell the parent to remove this from its lists
    m_client.removeQueryHandle(this);

    if(m_pError!=NULL){
        delete m_pError; m_pError=NULL;
    }
}
void DrillClientQueryResult::clearAndDestroy(){
    DrillClientQueryHandle::clearAndDestroy();
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
    this->client().removeQueryResult(this);

    //clear query id map entries.
    if(this->m_pQueryId!=NULL){
        delete this->m_pQueryId; this->m_pQueryId=NULL;
    }
    if(!m_recordBatches.empty()){
        // When multiple queries execute in parallel we sometimes get an empty record batch back from the server _after_
        // the last chunk has been received. We eventually delete it.
        DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Freeing Record batch(es) left behind "<< std::endl;)
        RecordBatch* pR=NULL;
        while(!m_recordBatches.empty()){
            pR=m_recordBatches.front();
            m_recordBatches.pop();
            delete pR;
        }
    }
}

status_t DrillClientPrepareHandle::setupPreparedStatement(const exec::user::PreparedStatement& pstmt) {
    // Get columns schema information
    const ::google::protobuf::RepeatedPtrField< ::exec::user::ResultColumnMetadata>& columns = pstmt.columns();
    for(::google::protobuf::RepeatedPtrField< ::exec::user::ResultColumnMetadata>::const_iterator it = columns.begin(); it != columns.end(); ++it) {
        FieldMetadata* metadata = new FieldMetadata;
        metadata->set(*it);
        m_columnDefs->push_back(metadata);
    }

    // Copy server handle
    this->m_preparedStatementHandle.CopyFrom(pstmt.server_handle());
    return QRY_SUCCESS;
}

void DrillClientPrepareHandle::clearAndDestroy(){
    DrillClientQueryHandle::clearAndDestroy();
    //free memory allocated for FieldMetadata objects saved in m_columnDefs;
    if(!m_columnDefs->empty()){
        for(std::vector<Drill::FieldMetadata*>::iterator it = m_columnDefs->begin(); it != m_columnDefs->end(); ++it){
            delete *it;
        }
        m_columnDefs->clear();
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
        ZookeeperClient zook(pathToDrill);
        std::vector<std::string> drillbits;
        int err = zook.getAllDrillbits(hostPortStr, drillbits);
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
                err=zook.getEndPoint(m_drillbits[nextIndex], e);
            if(!err){
                host=boost::lexical_cast<std::string>(e.address());
                port=boost::lexical_cast<std::string>(e.user_port());
            }
            DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Choosing drillbit <" << nextIndex  << ">. Selected " << e.DebugString() << std::endl;)
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
        m_pUserProperties = boost::shared_ptr<DrillUserProperties>(new DrillUserProperties);
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
        stat = pDrillClientImpl->validateHandshake(m_pUserProperties.get());
    }
    else{
        stat = handleConnError(CONN_NOTCONNECTED, getMessage(ERR_CONN_NOCONN));
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

DrillClientPrepareHandle* PooledDrillClientImpl::PrepareQuery(const std::string& plan, pfnPreparedStatementListener listener, void* listenerCtx){
    DrillClientPrepareHandle* pDrillClientPrepareHandle = NULL;
    DrillClientImpl* pDrillClientImpl = NULL;
    pDrillClientImpl = getOneConnection();
    if(pDrillClientImpl != NULL){
        pDrillClientPrepareHandle=pDrillClientImpl->PrepareQuery(plan,listener,listenerCtx);
        m_queriesExecuted++;
    }
    return pDrillClientPrepareHandle;
}

DrillClientQueryResult* PooledDrillClientImpl::ExecuteQuery(const PreparedStatement& pstmt, pfnQueryResultsListener listener, void* listenerCtx){
    DrillClientQueryResult* pDrillClientQueryResult = NULL;
    DrillClientImpl* pDrillClientImpl = NULL;
    pDrillClientImpl = getOneConnection();
    if(pDrillClientImpl != NULL){
        pDrillClientQueryResult=pDrillClientImpl->ExecuteQuery(pstmt, listener, listenerCtx);
        m_queriesExecuted++;
    }
    return pDrillClientQueryResult;
}

void PooledDrillClientImpl::freeQueryResources(DrillClientQueryHandle* pQryHandle){
    // If this class ever keeps track of executing queries then it will need
    // to implement this call to free any query specific resources the pool might have
    // allocated

    pQryHandle->client().freeQueryResources(pQryHandle);
}

meta::DrillMetadata* PooledDrillClientImpl::getMetadata() {
    meta::DrillMetadata* metadata = NULL;
    DrillClientImpl* pDrillClientImpl = getOneConnection();
    if (pDrillClientImpl != NULL) {
        metadata = pDrillClientImpl->getMetadata();
    }
    return metadata;
}

void PooledDrillClientImpl::freeMetadata(meta::DrillMetadata* metadata) {
    metadata->client().freeMetadata(metadata);
}

bool PooledDrillClientImpl::Active(){
    boost::lock_guard<boost::mutex> lock(m_poolMutex);
    for(std::vector<DrillClientImpl*>::const_iterator it = m_clientConnections.begin(); it != m_clientConnections.end(); ++it){
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
    m_pUserProperties.reset();
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
                    ret=pDrillClientImpl->validateHandshake(m_pUserProperties.get());
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
        handleConnError(status, getMessage(ERR_CONN_NOCONN));
    }
    return pDrillClientImpl;
}

} // namespace Drill
