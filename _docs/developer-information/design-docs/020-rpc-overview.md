---
title: "RPC Overview"
date: 
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

