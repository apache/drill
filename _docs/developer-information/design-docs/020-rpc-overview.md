---
title: "RPC Overview"
date: 2018-02-09 00:16:00 UTC
parent: "Design Docs"
---
Drill leverages the Netty 4 project as an RPC underlayment. From there, we
built a simple protobuf based communication layer optimized to minimize the
requirement for on heap data transformations. Both client and server utilize
the CompleteRpcMessage protobuf envelope to communicate requests, responses
and errors. The communication model is that each endpoint sends a stream of
CompleteRpcMessages to its peer. The CompleteRpcMessage is prefixed by a
protobuf encoded length.

CompleteRpcMessage is broken into three key components: RpcHeader, Protobuf
Body (bytes), RawBody (bytes).

RpcHeader has the following fields:

Drillbits communicate through the BitCom intermediary. BitCom manages...

##Drill Channel Pipeline with Handlers
The Drill RPC layer is built on [Netty](http://netty.io/index.html "Netty project"), an asynchronous network application framework that makes it easy to develop network-related components (such as, client and server) in an application. Within Netty, each connection is represented as a *Channel* that consists of its own *Pipeline*. The Pipeline is created when a Channel is created.  A Channel Pipeline consists of one or more inbound and/or outbound ChannelHandlers, which act on I/O events sent and/or received by an application. (For reference, see [Interface Channel](https://netty.io/4.0/api/io/netty/channel/Channel.html "Interface Channel"), [Interface ChannelPipeline](https://netty.io/4.0/api/io/netty/channel/ChannelPipeline.html "Interface ChannelPipeline"), and [Interface ChannelHandler](https://netty.io/4.0/api/io/netty/channel/ChannelHandler.html "Interface ChannelHandler").)

In the Drill ecosystem, UserClient (on the client side) and UserServer (on the server side) represent the wrapper for each connection between client and server, respectively.  They define the Pipeline and various handlers added for the communication path. 

For encryption support, both UserClient and UserServer require modification since new handlers will be added for encryption and decryption if privacy is negotiated as part of the handshake. 

###Encryption, Decryption, and ChunkCreation Handlers
In addition to an Encryption/Decryption handler, a ChunkCreation handler on the sender side and LengthFieldBasedFrameDecoder on the receiver side should be added. 

The ChunkCreation handler helps divide the Outgoing RPC message into smaller chunk units for encryption, whereas the LengthFieldBasedFrameDecoder helps to accumulate all  bytes of an encrypted payload on the receiver side before calling the decrypt module on it. 

The following diagram shows the existing handlers as well as the new handlers added in the Drill Channel Pipeline for client-to-drillbit encryption support in Drill 1.11. 

![drillpipeline]({{site.baseurl}}/docs/img/drill-channel-pipeline-with-handlers.png)  







