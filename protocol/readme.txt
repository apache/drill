This project contains the protobuf definition files used by Drill.

The java sources are generated into src/main/java and checked in.

To regenerate the sources after making changes to .proto files
---------------------------------------------------------------
1. Ensure that the protobuf 'protoc' tool (version 3.6.1 or newer (but 3.x series)) is
in your PATH (you may need to download and build it first). You can 
download it from http://code.google.com/p/protobuf/downloads/list.

Note: The Maven file has a dependence on exactly 3.6.1. Find it here:
https://github.com/protocolbuffers/protobuf/releases/tag/v3.6.1

    Note: If generating sources on MAC follow below instructions:

              a) Download and install "brew"
                 Command: /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

              b) Download and install "protoc"
                 Command: brew install protobuf361  --- installs protobuf for version 3.6.1
                          brew install protobuf     --- installs latest protobuf version

              c) Check the version of "protoc"
                 Command: protoc --version

              d) Follow steps 2 and 3 below

2. In protocol dir, run "mvn process-sources -P proto-compile" or "mvn clean install -P proto-compile".

3. Check in the new/updated files.

---------------------------------------------------------------
If changes are made to the DrillClient's protobuf, you would need to regenerate the sources for the C++ client as well.
Steps for regenerating the sources are available https://github.com/apache/drill/blob/master/contrib/native/client/

You can use any of the following platforms specified in the above location to regenerate the protobuf sources:
readme.linux	: Regenerating on Linux
readme.macos	: Regenerating on MacOS
readme.win.txt	: Regenerating on Windows
