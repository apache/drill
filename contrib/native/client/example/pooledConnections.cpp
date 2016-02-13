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

#include <fstream>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <boost/thread.hpp>
#include "drill/drillc.hpp"

int nOptions=5;

struct Option{
    char name[32];
    char desc[128];
    bool required;
}qsOptions[]= {
    {"query", "Query strings, separated by semicolons", true},
    {"connectStr", "Connect string", true},
    {"logLevel", "Logging level [trace|debug|info|warn|error|fatal]", false},
    {"numConnections", "Number of simultaneous connections", true},
    {"numIterations", "Number of iterations to run. Each query is sent to each connection this many times", true}
};

std::map<std::string, std::string> qsOptionValues;

const char* exceptionInject="alter session set `drill.exec.testing.controls` = '{ \"injections\" : [{ \"type\":\"exception\",\"siteClass\":\"org.apache.drill.exec.work.fragment.FragmentExecutor\",\"desc\":\"fragment-execution\",\"nSkip\":0,\"nFire\":1,\"exceptionClass\":\"java.lang.OutOfMemoryError\"}]}'";

Drill::status_t SchemaListener(void* ctx, Drill::FieldDefPtr fields, Drill::DrillClientError* err){
    if(!err){
        printf("SCHEMA CHANGE DETECTED:\n");
        for(size_t i=0; i<fields->size(); i++){
            std::string name= fields->at(i)->getName();
            printf("%s\t", name.c_str());
        }
        printf("\n");
        return Drill::QRY_SUCCESS ;
    }else{
        std::cerr<< "ERROR: " << err->msg << std::endl;
        return Drill::QRY_FAILURE;
    }
}

boost::mutex listenerMutex;
Drill::status_t QueryResultsListener(void* ctx, Drill::RecordBatch* b, Drill::DrillClientError* err){
    boost::lock_guard<boost::mutex> listenerLock(listenerMutex);
    if(!err){
        if(b!=NULL){
            b->print(std::cout, 0); // print all rows
            std::cout << "DATA RECEIVED ..." << std::endl;
            delete b; // we're done with this batch, we can delete it
            return Drill::QRY_FAILURE;
        }else{
            std::cout << "Query Complete." << std::endl;
            return Drill::QRY_SUCCESS;
		}
    }else{
        assert(b==NULL);
        switch(err->status) {
            case Drill::QRY_COMPLETED:
            case Drill::QRY_CANCELED:
                std::cerr<< "INFO: " << err->msg << std::endl;
                return Drill::QRY_SUCCESS;
            default:
                std::cerr<< "ERROR: " << err->msg << std::endl;
                return Drill::QRY_FAILURE;
        }
    }
}

void printUsage(){
    std::cerr<<"Usage: pooledConnections ";
    for(int j=0; j<nOptions ;j++){
        std::cerr<< " "<< qsOptions[j].name <<"="  << "[" <<qsOptions[j].desc <<"]" ;
    }
    std::cerr<<std::endl;
}

int parseArgs(int argc, char* argv[]){
    bool error=false;
    for(int i=1; i<argc; i++){
        char*a =argv[i];
        char* o=strtok(a, "=");
        char*v=strtok(NULL, "");

        bool found=false;
        for(int j=0; j<nOptions ;j++){
            if(!strcmp(qsOptions[j].name, o)){
                found=true; break;
            }
        }
        if(!found){
            std::cerr<< "Unknown option:"<< o <<". Ignoring" << std::endl;
            continue;
        }

        if(v==NULL){
            std::cerr<< ""<< qsOptions[i].name << " [" <<qsOptions[i].desc <<"] " << "requires a parameter."  << std::endl;
            error=true;
        }
        qsOptionValues[o]=v;
    }

    for(int j=0; j<nOptions ;j++){
        if(qsOptions[j].required ){
            if(qsOptionValues.find(qsOptions[j].name) == qsOptionValues.end()){
                std::cerr<< ""<< qsOptions[j].name << " [" <<qsOptions[j].desc <<"] " << "is required." << std::endl;
                error=true;
            }
        }
    }
    if(error){
        printUsage();
        exit(1);
    }
    return 0;
}

void parseUrl(std::string& url, std::string& protocol, std::string& host, std::string& port){
    char u[1024];
    strcpy(u,url.c_str());
    char* z=strtok(u, "=");
    char* h=strtok(NULL, ":");
    char* p=strtok(NULL, ":");
    protocol=z; host=h; port=p;
}

std::vector<std::string> &splitString(const std::string& s, char delim, std::vector<std::string>& elems){
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)){
        elems.push_back(item);
    }
    return elems;
}

int readQueries(const std::string& queryList, std::vector<std::string>& queries){
    splitString(queryList, ';', queries);
    return 0;
}

Drill::logLevel_t getLogLevel(const char *s){
    if(s!=NULL){
        if(!strcmp(s, "trace")) return Drill::LOG_TRACE;
        if(!strcmp(s, "debug")) return Drill::LOG_DEBUG;
        if(!strcmp(s, "info")) return Drill::LOG_INFO;
        if(!strcmp(s, "warn")) return Drill::LOG_WARNING;
        if(!strcmp(s, "error")) return Drill::LOG_ERROR;
        if(!strcmp(s, "fatal")) return Drill::LOG_FATAL;
    }
    return Drill::LOG_ERROR;
}

int main(int argc, char* argv[]) {
    try {

        parseArgs(argc, argv);

        std::vector<std::string*> queries;

        std::string connectStr=qsOptionValues["connectStr"];
        std::string queryList=qsOptionValues["query"];
        std::string logLevel=qsOptionValues["logLevel"];
        std::string numConnections=qsOptionValues["numConnections"];
        std::string numIterations=qsOptionValues["numIterations"];
        int nConnections=std::atoi(numConnections.c_str());
        int nIterations=std::atoi(numIterations.c_str());

        Drill::logLevel_t l=getLogLevel(logLevel.c_str());

        std::vector<std::string> queryInputs;
        readQueries(queryList, queryInputs);

        std::vector<std::string>::iterator queryInpIter;

        //std::vector<Drill::RecordIterator*> recordIterators;
        //std::vector<Drill::RecordIterator*>::iterator recordIterIter;

        std::vector<Drill::QueryHandle_t*> queryHandles;
        std::vector<Drill::QueryHandle_t*>::iterator queryHandleIter;

		std::vector<Drill::DrillClient*> clients;
        std::vector<Drill::DrillClient*>::iterator clientsIter;
        
        
#if defined _WIN32 || defined _WIN64
		const char* logpathPrefix = "C:\\Users\\Administrator\\Documents\\temp\\drillclient";
#else
		const char* logpathPrefix = "/var/log/drill/drillclient";
#endif
		// To log to file
        Drill::DrillClient::initLogging(logpathPrefix, l);
        // To log to stderr
        //Drill::DrillClient::initLogging(NULL, l);

        //Drill::DrillClientConfig::setBufferLimit(2*1024*1024); // 2MB. Allows us to hold at least two record batches.
        int nQueries=queryInputs.size();
        Drill::DrillClientConfig::setBufferLimit(nQueries*2*1024*1024); // 2MB per query. Allows us to hold at least two record batches.

        Drill::DrillUserProperties props;

        for(size_t i=0; i<nConnections; i++){
            Drill::DrillClient* pClient = new Drill::DrillClient();
            clients.push_back(pClient);
            if (pClient->connect(connectStr.c_str(), &props) != Drill::CONN_SUCCESS){
                std::cerr << "Failed to connect with error: " << pClient->getError() << " (Using:" << connectStr << ")" << std::endl;
                return -1;
            }
            std::cout<< "Connected!\n" << std::endl;
            for(int j=0; j<nIterations; j++){
                for(queryInpIter = queryInputs.begin(); queryInpIter != queryInputs.end(); queryInpIter++) {
                    Drill::QueryHandle_t* qryHandle = new Drill::QueryHandle_t;
                    pClient->submitQuery(Drill::SQL, *queryInpIter, QueryResultsListener, NULL, qryHandle);
                    pClient->registerSchemaChangeListener(qryHandle, SchemaListener);
                    queryHandles.push_back(qryHandle);
                    std::cout << "Submitting Query: ("<< j <<")"<< *queryInpIter << std::endl;
                }
            }
        }
        // Wait until all the queries are completed
        for(clientsIter = clients.begin(); clientsIter != clients.end(); clientsIter++) {
            (*clientsIter)->waitForResults();
        }

        // Use any of the client objects to clear out the memory resources for the query.
        
        for(queryHandleIter = queryHandles.begin(); queryHandleIter != queryHandles.end(); queryHandleIter++) {
            Drill::DrillClient* pClient=*clients.begin();
            pClient->freeQueryResources(*queryHandleIter);
        }
        queryHandles.clear();


        // We should have a full pool by now
        // Let's try to inject a series of exceptions that kill the drillbit
        // We need to try this many times before the query will go to a drillbit that has died.
        //
        for(clientsIter = clients.begin(); clientsIter != clients.end(); clientsIter++) {
            Drill::DrillClient* pClient=*clientsIter;
            Drill::QueryHandle_t* qryHandle = new Drill::QueryHandle_t;
            pClient->submitQuery(Drill::SQL, exceptionInject, QueryResultsListener, NULL, qryHandle);
            pClient->registerSchemaChangeListener(qryHandle, SchemaListener);
            queryHandles.push_back(qryHandle);
            std::cout << "Submitting Query: " << exceptionInject << std::endl;
        }
        
        // Wait until all the alter session queries are completed
        for(clientsIter = clients.begin(); clientsIter != clients.end(); clientsIter++) {
            (*clientsIter)->waitForResults();
        }

        for(clientsIter = clients.begin(); clientsIter != clients.end(); clientsIter++) {
            Drill::DrillClient* pClient=*clientsIter;
            Drill::QueryHandle_t* qryHandle = new Drill::QueryHandle_t;
            for(queryInpIter = queryInputs.begin(); queryInpIter != queryInputs.end(); queryInpIter++) {
                pClient->submitQuery(Drill::SQL, *queryInpIter, QueryResultsListener, NULL, qryHandle);
                pClient->registerSchemaChangeListener(qryHandle, SchemaListener);
                queryHandles.push_back(qryHandle);
                std::cout << "Submitting Query: " << *queryInpIter << std::endl;
            }
        }
        // Wait until all the queries are completed
        for(clientsIter = clients.begin(); clientsIter != clients.end(); clientsIter++) {
            (*clientsIter)->waitForResults();
        }

        // Use any of the client objects to clear out the memory resources for the query.
        for(queryHandleIter = queryHandles.begin(); queryHandleIter != queryHandles.end(); queryHandleIter++) {
            Drill::DrillClient* pClient=*clients.begin();
            pClient->freeQueryResources(*queryHandleIter);
        }
        queryHandles.clear();
        // Clean up all the connections
        for(clientsIter = clients.begin(); clientsIter != clients.end(); clientsIter++) {
            delete *clientsIter;
        }
        clients.clear();
                
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}

