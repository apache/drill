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

#ifndef __LOGGER_H
#define __LOGGER_H

#include <sstream>
#include <ostream>
#include <fstream>
#include <string>
#include <stdio.h>

#include "drill/common.hpp"

namespace Drill{

class Logger{
    public:
        Logger(){}
        ~Logger(){ }

        static void init(const char* path);
        static void close();
        static std::ostream& log(logLevel_t level);
        static std::string levelAsString(logLevel_t level) {
            static const char* const levelNames[] = {
                "TRACE",
                "DEBUG",
                "INFO",
                "WARNING",
                "ERROR",
                "FATAL"
            };
            return levelNames[level];
        }

        // The logging level
        static logLevel_t s_level;
        static std::ostream* s_pOutStream;

    private:
        //static std::ostream* s_pOutStream;
        static std::ofstream* s_pOutFileStream;
        static char* s_filepath;

}; // Logger

std::string getTime();
std::string getTid();

#define DRILL_LOG(level) \
    if (Logger::s_pOutStream==NULL || level < Drill::Logger::s_level); \
    else Drill::Logger::log(level)       \

} // namespace Drill

#endif
