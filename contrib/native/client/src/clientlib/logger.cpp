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

#include "boost/date_time/posix_time/posix_time.hpp"
#include "boost/thread.hpp"

#include "logger.hpp"

namespace Drill{

std::string getTime(){
    return to_simple_string(boost::posix_time::second_clock::local_time());
}

std::string getTid(){
    return boost::lexical_cast<std::string>(boost::this_thread::get_id());
}

logLevel_t Logger::s_level=LOG_ERROR;
std::ostream* Logger::s_pOutStream=NULL;
std::ofstream* Logger::s_pOutFileStream=NULL;
char* Logger::s_filepath=NULL;

void Logger::init(const char* path){
    if(path!=NULL) {
        s_pOutFileStream = new std::ofstream;
        s_pOutFileStream->open(path, std::ofstream::out);
        if(!s_pOutFileStream->is_open()){
            std::cerr << "Logfile could not be opened. Logging to stdout" << std::endl;
        }
    }
    s_pOutStream=(s_pOutFileStream!=NULL && s_pOutFileStream->is_open())?s_pOutFileStream:&std::cout;
}

void Logger::close(){
    if(s_pOutFileStream !=NULL){
        if(s_pOutFileStream->is_open()){
            s_pOutFileStream->close();
        }
        delete s_pOutFileStream; s_pOutFileStream=NULL;
    }
}

std::ostream& Logger::log(logLevel_t level){
    *s_pOutStream << getTime();
    *s_pOutStream << " : "<<levelAsString(level);
    *s_pOutStream << " : "<<getTid();
    *s_pOutStream << " : ";
    return *s_pOutStream;
}


} // namespace Drill

