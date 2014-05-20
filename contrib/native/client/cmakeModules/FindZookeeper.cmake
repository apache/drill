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

# - Try to find Zookeeper
# Defines
#  Zookeeper_FOUND - System has Zookeeper
#  Zookeeper_INCLUDE_DIRS - The Zookeeper include directories
#  Zookeeper_LIBRARIES - The libraries needed to use Zookeeper
#  Zookeeper_DEFINITIONS - Compiler switches required for using LibZookeeper

#find_package(PkgConfig)
#pkg_check_modules(PC_LIBXML QUIET libxml-2.0)
#set(Zookeeper_DEFINITIONS ${PC_LIBXML_CFLAGS_OTHER})

find_path(Zookeeper_INCLUDE_DIR zookeeper/zookeeper.h /usr/local/include)

set(Zookeeper_LIB_PATHS /usr/local/lib /opt/local/lib)
find_library(Zookeeper_LIBRARY NAMES zookeeper_mt PATHS ${Zookeeper_LIB_PATHS})

set(Zookeeper_LIBRARIES ${Zookeeper_LIBRARY} )
set(Zookeeper_INCLUDE_DIRS ${Zookeeper_INCLUDE_DIR} )

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set Zookeeper_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(Zookeeper  DEFAULT_MSG
    Zookeeper_LIBRARY Zookeeper_INCLUDE_DIR)

mark_as_advanced(Zookeeper_INCLUDE_DIR Zookeeper_LIBRARY )
