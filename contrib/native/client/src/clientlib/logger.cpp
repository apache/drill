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
#include "utils.hpp"
#include "logger.hpp"

namespace Drill{

/*
* Creates a single instance of the logger the first time this is called
*/
/*  static */ boost::mutex g_logMutex;
Logger& getLogger() {
    boost::lock_guard<boost::mutex> logLock(g_logMutex);
    static Logger* logger = new Logger();
    return *logger;
}

std::string getTime(){
    return to_simple_string(boost::posix_time::second_clock::local_time());
}

std::string getTid(){
    return boost::lexical_cast<std::string>(boost::this_thread::get_id());
}

void Logger::init(const char* path){

    static bool initialized = false;
    boost::lock_guard<boost::mutex> logLock(m_logMutex);
    if (!initialized && path != NULL && m_filepath.empty()) {
        std::string fullname = path;
        size_t lastindex = fullname.find_last_of(".");
        std::string filename;
        if (lastindex != std::string::npos){
            filename = fullname.substr(0, lastindex)
                + "-"
                + std::to_string(Utils::s_randomNumber())
                + fullname.substr(lastindex, fullname.length());
        }
        else{
            filename = fullname.substr(0, fullname.length())
                + "-"
                + std::to_string(Utils::s_randomNumber())
                + ".log";
        }
        //m_filepath=path;
        m_filepath = filename.c_str();
        m_pOutFileStream = new std::ofstream;
        m_pOutFileStream->open(m_filepath, std::ofstream::out | std::ofstream::app);
        if (!m_pOutFileStream->is_open()){
            std::cerr << "Logfile could not be opened. Logging to stdout" << std::endl;
            m_filepath.erase();
            delete m_pOutFileStream;
        }
        initialized = true;

        m_pOutStream = (m_pOutFileStream != NULL && m_pOutFileStream->is_open()) ? m_pOutFileStream : &std::cout;
#if defined _WIN32 || defined _WIN64

        TCHAR szFile[MAX_PATH];
        GetModuleFileName(NULL, szFile, MAX_PATH);
#endif
        *m_pOutStream
            << " DRILL CLIENT LIBRARY " << std::endl
#if defined _WIN32 || defined _WIN64
            << " Loaded by process : " << szFile << std::endl
            << " Current Process Id is: " << ::GetCurrentProcessId() << std::endl
#endif
            << " Initialized Logging to file (" << path << "). "
            << std::endl;
    }
}

void Logger::close(){
    //boost::lock_guard<boost::mutex> logLock(Drill::Logger::m_logMutex); 
    boost::lock_guard<boost::mutex> logLock(m_logMutex);
    if (m_pOutFileStream != NULL){
        if (m_pOutFileStream->is_open()){
            m_pOutFileStream->close();
        }
        delete m_pOutFileStream; m_pOutFileStream = NULL;
    }
}

// The log call itself cannot be thread safe. Use the DRILL_MT_LOG macro to make 
// this thread safe
std::ostream& Logger::log(logLevel_t level){
    *m_pOutStream << getTime();
    *m_pOutStream << " : " << levelAsString(level);
    *m_pOutStream << " : " << getTid();
    *m_pOutStream << " : ";
    return *m_pOutStream;
}


} // namespace Drill

