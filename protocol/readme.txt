This project contains the protobuf definition files used by Drill.

The java sources are generated into src/main/java and checked in.

To regenerate the sources after making changes to .proto files
---------------------------------------------------------------
1. Ensure that the protobuf 'protoc' tool (version 2.5 or newer (but 2.x series)) is
in your PATH (you may need to download and build it first). You can 
download it from http://code.google.com/p/protobuf/downloads/list.

    Note: If generating sources on MAC follow below instructions:

              a) Download and install "brew"
                 Command: /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

              b) Download and install "protoc"
                 Command: brew install protobuf250  --- installs protobuf for version 2.5.0
                          brew install protobuf     --- installs latest protobuf version

              c) Check the version of "protoc"
                 Command: protoc --version

              d) Follow steps 2 and 3 below

2. In protocol dir, run "mvn process-sources -P proto-compile" or "mvn clean install -P proto-compile".

3. Check in the new/updated files.