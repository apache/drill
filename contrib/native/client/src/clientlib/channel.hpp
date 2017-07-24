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

#ifndef CHANNEL_HPP
#define CHANNEL_HPP

#include "drill/common.hpp"
#include "drill/drillClient.hpp"
#include "streamSocket.hpp"

namespace Drill {

class UserProperties;

    class ConnectionEndpoint{
        public:
            ConnectionEndpoint(const char* connStr);
            ConnectionEndpoint(const char* host, const char* port);
            ~ConnectionEndpoint();

            //parse the connection string and set up the host and port to connect to
            connectionStatus_t getDrillbitEndpoint();

            const std::string& getProtocol() const {return m_protocol;}
            const std::string& getHost() const {return m_host;}
            const std::string& getPort() const {return m_port;}
            DrillClientError* getError(){ return m_pError;};

        private:
            void parseConnectString();
            bool isDirectConnection();
            bool isZookeeperConnection();
            connectionStatus_t getDrillbitEndpointFromZk();
            connectionStatus_t handleError(connectionStatus_t status, std::string msg);

            std::string m_connectString;
            std::string m_pathToDrill;
            std::string m_protocol; 
            std::string m_hostPortStr;
            std::string m_host;
            std::string m_port;

            DrillClientError* m_pError;

    };

    class ChannelContext{
        public:
            ChannelContext(DrillUserProperties* props):m_properties(props){};
            virtual ~ChannelContext(){};
            const DrillUserProperties* getUserProperties() const { return m_properties;}
        protected:
            DrillUserProperties* m_properties;
    };

    class SSLChannelContext: public ChannelContext{
        public:
            static boost::asio::ssl::context::method getTlsVersion(const std::string & version){
                if (version == "tlsv12") {
                    return boost::asio::ssl::context::tlsv12;
                } else if (version == "tlsv11") {
                    return boost::asio::ssl::context::tlsv11;
                } else if (version == "tlsv1") {
                    return boost::asio::ssl::context::tlsv1;
                } else {
                    return boost::asio::ssl::context::tlsv12;
                }
            }

        SSLChannelContext(DrillUserProperties *props,
                          boost::asio::ssl::context::method tlsVersion,
                          boost::asio::ssl::verify_mode verifyMode) :
                ChannelContext(props),
                m_SSLContext(tlsVersion) {
                m_SSLContext.set_default_verify_paths();
                m_SSLContext.set_options(
                        boost::asio::ssl::context::default_workarounds
                        | boost::asio::ssl::context::no_sslv2
                        | boost::asio::ssl::context::single_dh_use
                        );
                m_SSLContext.set_verify_mode(verifyMode);
            };
            ~SSLChannelContext(){};
            boost::asio::ssl::context& getSslContext(){ return m_SSLContext;}
        private:
            boost::asio::ssl::context m_SSLContext;
    };

    typedef ChannelContext ChannelContext_t; 
    typedef SSLChannelContext SSLChannelContext_t; 

    /***
     * The Channel class encapsulates a connection to a drillbit. Based on 
     * the connection string and the options, the connection will be either 
     * a simple socket or a socket using an ssl stream. The class also encapsulates
     * connecting to a drillbit directly or thru zookeeper.
     * The channel class owns the socket and the io_service that the applications
     * will use to communicate with the server.
     ***/
    class Channel{
        friend class ChannelFactory;
        public: 
            Channel(boost::asio::io_service& ioService, const char* connStr);
            Channel(boost::asio::io_service& ioService, const char* host, const char* port);
            virtual ~Channel();
            virtual connectionStatus_t init()=0;
            connectionStatus_t connect();
            bool isConnected(){ return m_state == CHANNEL_CONNECTED;}
            template <typename SettableSocketOption> void setOption(SettableSocketOption& option);
            DrillClientError* getError(){ return m_pError;}
            void close(){ 
                if(m_state==CHANNEL_INITIALIZED||m_state==CHANNEL_CONNECTED){
                    m_pSocket->protocolClose();
                    m_state=CHANNEL_CLOSED;
                }
            } // Not OK to use the channel after this call. 

            boost::asio::io_service& getIOService(){
                return m_ioService;
            }

            // returns a reference to the underlying socket 
            // This access should really be removed and encapsulated in calls that 
            // manage async_send and async_recv 
            // Until then we will let DrillClientImpl have direct access
            streamSocket_t& getInnerSocket(){
                return m_pSocket->getInnerSocket();
            }
            
            AsioStreamSocket& getSocketStream(){
                return *m_pSocket;
            }

            ConnectionEndpoint* getEndpoint(){return m_pEndpoint;}

        protected:
            connectionStatus_t handleError(connectionStatus_t status, std::string msg);

            boost::asio::io_service& m_ioService;
            boost::asio::io_service m_ioServiceFallback; // used if m_ioService is not provided
            AsioStreamSocket* m_pSocket;
            ConnectionEndpoint *m_pEndpoint;
            ChannelContext_t *m_pContext;

        private:
            typedef enum channelState{ 
                CHANNEL_UNINITIALIZED=1, 
                CHANNEL_INITIALIZED, 
                CHANNEL_CONNECTED, 
                CHANNEL_CLOSED       
            } channelState_t;
            
            connectionStatus_t connectInternal();
            connectionStatus_t protocolHandshake(bool useSystemConfig){
                connectionStatus_t status = CONN_SUCCESS;
                try{
                    m_pSocket->protocolHandshake(useSystemConfig);
                } catch (boost::system::system_error e) {
                    status = handleError(CONN_HANDSHAKE_FAILED, e.what());
                }
                return status;
            }

            channelState_t m_state;
            DrillClientError* m_pError;
    };

    class SocketChannel: public Channel{
        public:
            SocketChannel(boost::asio::io_service& ioService, const char* connStr)
                :Channel(ioService, connStr){
            }
            SocketChannel(boost::asio::io_service& ioService, const char* host, const char* port)
                :Channel(ioService, host, port){
            }
            connectionStatus_t init();
    };

    class SSLStreamChannel: public Channel{
        public:
            SSLStreamChannel(boost::asio::io_service& ioService, const char* connStr)
                :Channel(ioService, connStr){
            }
            SSLStreamChannel(boost::asio::io_service& ioService, const char* host, const char* port)
                :Channel(ioService, host, port){
            }
            connectionStatus_t init();
    };

    class ChannelFactory{
        public:
            static Channel* getChannel(channelType_t t,
                                       boost::asio::io_service& ioService,
                                       const char* connStr, DrillUserProperties* props);
            static Channel* getChannel(channelType_t t,
                                       boost::asio::io_service& ioService,
                                       const char* host,
                                       const char* port,
                                       DrillUserProperties* props);
        private:
            static ChannelContext_t* getChannelContext(channelType_t t, DrillUserProperties* props);
    };


} // namespace Drill

#endif // CHANNEL_HPP

