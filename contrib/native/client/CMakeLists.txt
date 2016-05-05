#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
cmake_minimum_required(VERSION 2.6)

project(drillclient)

message("Project Dir = ${PROJECT_SOURCE_DIR}")
message("Source Dir = ${CMAKE_SOURCE_DIR} ")

cmake_policy(SET CMP0043 NEW)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmakeModules/")

# Get the latest git commit properties of the working branch
execute_process(
    COMMAND git log -1 --format="\\nCommit: %H \\nDescription: %s \\nAuthor: %aN Date: %ai"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_COMMIT_PROP
    OUTPUT_STRIP_TRAILING_WHITESPACE
    )
STRING(REPLACE . " " GIT_COMMIT_PROP "${GIT_COMMIT_PROP}")
STRING(REPLACE \" "" GIT_COMMIT_PROP "${GIT_COMMIT_PROP}")
set(GIT_COMMIT_PROP "\"${GIT_COMMIT_PROP}\"")
add_definitions("-DGIT_COMMIT_PROP=${GIT_COMMIT_PROP}")



# Find Boost
if(MSVC)
    set(Boost_USE_STATIC_LIBS ON)
    set(Boost_USE_MULTITHREADED ON)
    set(Boost_USE_STATIC_RUNTIME OFF)
else()
    set(Boost_USE_STATIC_LIBS OFF)
    set(Boost_USE_MULTITHREADED ON)
    set(Boost_USE_STATIC_RUNTIME OFF)
endif()

find_package(Boost 1.53.0 REQUIRED COMPONENTS regex system date_time chrono thread random)
include_directories(${Boost_INCLUDE_DIRS})

if(CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_EXE_LINKER_FLAGS "-lrt -lpthread")
    set(CMAKE_CXX_FLAGS "-fPIC")
endif()
if(MSVC)
    set(CMAKE_CXX_FLAGS "/EHsc")
endif()

if(MSVC)
    # ask boost to not to auto link any libraries
    add_definitions(-DBOOST_ALL_NO_LIB)
    # use static libs with zookeeper
    add_definitions(-DUSE_STATIC_LIB)
else()
    add_definitions(-DBOOST_ALL_DYN_LINK)
endif()

# Find Protobufs
find_package(Protobuf REQUIRED )
include_directories(${PROTOBUF_INCLUDE_DIR})

#Find Zookeeper
find_package(Zookeeper  REQUIRED )


# Generated sources
configure_file(
    ${CMAKE_SOURCE_DIR}/src/clientlib/env.h.in
    ${CMAKE_BINARY_DIR}/generated/env.h
    )

include_directories(${CMAKE_BINARY_DIR}/generated)


#
#   TARGETS
#

# Preprocess to fix protobuf .proto definitions
add_subdirectory("${CMAKE_SOURCE_DIR}/src/protobuf")
# protobuf includes are required by clientlib
include_directories(${ProtoHeadersDir})
include_directories(${ProtoIncludesDir})

# Y2038 library
add_subdirectory("${CMAKE_SOURCE_DIR}/src/clientlib/y2038")

# Build the Client Library as a shared library
add_subdirectory("${CMAKE_SOURCE_DIR}/src/clientlib")
include_directories(${CMAKE_SOURCE_DIR}/src/include ${Zookeeper_INCLUDE_DIRS})

# add a DEBUG preprocessor macro
set_property(
    DIRECTORY
    PROPERTY COMPILE_DEFINITIONS_DEBUG DEBUG DEBUG=1 THREADED
    )

# Link directory
link_directories(/usr/local/lib)

add_executable(querySubmitter example/querySubmitter.cpp )

target_link_libraries(querySubmitter ${Boost_LIBRARIES} ${PROTOBUF_LIBRARY} drillClient protomsgs y2038)
