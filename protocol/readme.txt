This project contains the protobuf definition files used by Drill.

The java sources are generated into src/main/java and checked in.

To regenerate the sources after making changes to .proto files
---------------------------------------------------------------
1. Ensure that the protobuf 'protoc' tool (version 2.5 or newer) is
in your PATH (you may need to download and build it first). You can 
download it from http://code.google.com/p/protobuf/downloads/list.

2. Run "mvn process-sources -P proto-compile".

3. Check in the new/updated files.