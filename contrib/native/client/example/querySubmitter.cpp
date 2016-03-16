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

int nOptions=13;

struct Option{
    char name[32];
    char desc[128];
    bool required;
}qsOptions[]= {
    {"plan", "Plan files separated by semicolons", false},
    {"query", "Query strings, separated by semicolons", false},
    {"type", "Query type [physical|logical|sql]", true},
    {"connectStr", "Connect string", true},
    {"schema", "Default schema", false},
    {"api", "API type [sync|async]", true},
    {"logLevel", "Logging level [trace|debug|info|warn|error|fatal]", false},
    {"testCancel", "Cancel the query afterthe first record batch.", false},
    {"syncSend", "Send query only after previous result is received", false},
    {"hshakeTimeout", "Handshake timeout (second).", false},
    {"queryTimeout", "Query timeout (second).", false},
    {"user", "Username", false},
    {"password", "Password", false}
};

std::map<std::string, std::string> qsOptionValues;

bool bTestCancel=false;
bool bSyncSend=false;


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
    // Invariant:
    // (received an record batch and err is NULL)
    // or
    // (received query state message passed by `err` and b is NULL)
    boost::lock_guard<boost::mutex> listenerLock(listenerMutex);
    if(!err){
        if(b!=NULL){
            b->print(std::cout, 0); // print all rows
            std::cout << "DATA RECEIVED ..." << std::endl;
            delete b; // we're done with this batch, we can delete it
            if(bTestCancel){
                return Drill::QRY_FAILURE;
            }else{
                return Drill::QRY_SUCCESS ;
            }
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

void print(const Drill::FieldMetadata* pFieldMetadata, void* buf, size_t sz){
    common::MinorType type = pFieldMetadata->getMinorType();
    common::DataMode mode = pFieldMetadata->getDataMode();
    unsigned char printBuffer[10240];
    memset(printBuffer, 0, sizeof(printBuffer));
    switch (type) {
        case common::BIGINT:
            switch (mode) {
                case common::DM_REQUIRED:
                    sprintf((char*)printBuffer, "%lld", *(uint64_t*)buf);
                case common::DM_OPTIONAL:
                    break;
                case common::DM_REPEATED:
                    break;
            }
            break;
        case common::VARBINARY:
            switch (mode) {
                case common::DM_REQUIRED:
                    memcpy(printBuffer, buf, sz);
                case common::DM_OPTIONAL:
                    break;
                case common::DM_REPEATED:
                    break;
            }
            break;
        case common::VARCHAR:
            switch (mode) {
                case common::DM_REQUIRED:
                    memcpy(printBuffer, buf, sz);
                case common::DM_OPTIONAL:
                    break;
                case common::DM_REPEATED:
                    break;
            }
            break;
        default:
            //memcpy(printBuffer, buf, sz);
            sprintf((char*)printBuffer, "NIY");
            break;
    }
    printf("%s\t", (char*)printBuffer);
    return;
}

void printUsage(){
    std::cerr<<"Usage: querySubmitter ";
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

int readPlans(const std::string& planList, std::vector<std::string>& plans){
    std::vector<std::string> planFiles;
    std::vector<std::string>::iterator iter;
    splitString(planList, ';', planFiles);
    for(iter = planFiles.begin(); iter != planFiles.end(); iter++) {
        std::ifstream f((*iter).c_str());
        std::string plan((std::istreambuf_iterator<char>(f)), (std::istreambuf_iterator<char>()));
        std::cout << "plan:" << plan << std::endl;
        plans.push_back(plan);
    }
    return 0;
}

int readQueries(const std::string& queryList, std::vector<std::string>& queries){
    splitString(queryList, ';', queries);
    return 0;
}

bool validate(const std::string& type, const std::string& query, const std::string& plan){
    if(query.empty() && plan.empty()){
        std::cerr<< "Either query or plan must be specified"<<std::endl;
        return false;    }
        if(type=="physical" || type == "logical" ){
            if(plan.empty()){
                std::cerr<< "A logical or physical  plan must be specified"<<std::endl;
                return false;
            }
        }else
            if(type=="sql"){
                if(query.empty()){
                    std::cerr<< "A drill SQL query must be specified"<<std::endl;
                    return false;
                }
            }else{
                std::cerr<< "Unknown query type: "<< type << std::endl;
                return false;
            }
        return true;
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
        std::string schema=qsOptionValues["schema"];
        std::string queryList=qsOptionValues["query"];
        std::string planList=qsOptionValues["plan"];
        std::string api=qsOptionValues["api"];
        std::string type_str=qsOptionValues["type"];
        std::string logLevel=qsOptionValues["logLevel"];
        std::string testCancel=qsOptionValues["testCancel"];
        std::string syncSend=qsOptionValues["syncSend"];
        std::string hshakeTimeout=qsOptionValues["hshakeTimeout"];
        std::string queryTimeout=qsOptionValues["queryTimeout"];
        std::string user=qsOptionValues["user"];
        std::string password=qsOptionValues["password"];

        Drill::QueryType type;

        if(!validate(type_str, queryList, planList)){
            exit(1);
        }

        Drill::logLevel_t l=getLogLevel(logLevel.c_str());

        std::vector<std::string> queryInputs;
        if(type_str=="sql" ){
            readQueries(queryList, queryInputs);
            type=Drill::SQL;
        }else if(type_str=="physical" ){
            readPlans(planList, queryInputs);
            type=Drill::PHYSICAL;
        }else if(type_str == "logical"){
            readPlans(planList, queryInputs);
            type=Drill::LOGICAL;
        }else{
            readQueries(queryList, queryInputs);
            type=Drill::SQL;
        }

        bTestCancel = !strcmp(testCancel.c_str(), "true")?true:false;
        bSyncSend = !strcmp(syncSend.c_str(), "true")?true:false;

        std::vector<std::string>::iterator queryInpIter;

        std::vector<Drill::RecordIterator*> recordIterators;
        std::vector<Drill::RecordIterator*>::iterator recordIterIter;

        std::vector<Drill::QueryHandle_t*> queryHandles;
        std::vector<Drill::QueryHandle_t*>::iterator queryHandleIter;

        Drill::DrillClient client;
#if defined _WIN32 || defined _WIN64
        TCHAR tempPath[MAX_PATH];
        GetTempPath(MAX_PATH, tempPath);
		char logpathPrefix[MAX_PATH + 128];
		strcpy(logpathPrefix,tempPath);
		strcat(logpathPrefix, "\\drillclient");
#else
		char* logpathPrefix = "/var/log/drill/drillclient";
#endif
		// To log to file
        Drill::DrillClient::initLogging(logpathPrefix, l);
        // To log to stderr
        //Drill::DrillClient::initLogging(NULL, l);

        int nQueries=queryInputs.size();
        Drill::DrillClientConfig::setBufferLimit(nQueries*2*1024*1024); // 2MB per query. The size of a record batch may vary, but is unlikely to exceed the 256 MB which is the default. 

        if(!hshakeTimeout.empty()){
            Drill::DrillClientConfig::setHandshakeTimeout(atoi(hshakeTimeout.c_str()));
        }
        if (!queryTimeout.empty()){
            Drill::DrillClientConfig::setQueryTimeout(atoi(queryTimeout.c_str()));
        }

        Drill::DrillUserProperties props;
        if(schema.length()>0){
            props.setProperty(USERPROP_SCHEMA, schema);
        }
        if(user.length()>0){
            props.setProperty(USERPROP_USERNAME, user);
        }
        if(password.length()>0){
            props.setProperty(USERPROP_PASSWORD, password);
        }

        props.setProperty("someRandomProperty", "someRandomValue");

        if(client.connect(connectStr.c_str(), &props)!=Drill::CONN_SUCCESS){
            std::cerr<< "Failed to connect with error: "<< client.getError() << " (Using:"<<connectStr<<")"<<std::endl;
            return -1;
        }
        std::cout<< "Connected!\n" << std::endl;
        if(api=="sync"){
            Drill::DrillClientError* err=NULL;
            Drill::status_t ret;
            int nQueries=0;
            for(queryInpIter = queryInputs.begin(); queryInpIter != queryInputs.end(); queryInpIter++) {
                Drill::RecordIterator* pRecIter = client.submitQuery(type, *queryInpIter, err);
                if(pRecIter!=NULL){
                    recordIterators.push_back(pRecIter);
                    nQueries++;
                }
            }
            Drill::DrillClientConfig::setBufferLimit(nQueries*2*1024*1024); // 2MB per query. Allows us to hold at least two record batches.
            size_t row=0;
            for(recordIterIter = recordIterators.begin(); recordIterIter != recordIterators.end(); recordIterIter++) {
                // get fields.
                row=0;
                Drill::RecordIterator* pRecIter=*recordIterIter;
                Drill::FieldDefPtr fields= pRecIter->getColDefs();
                while((ret=pRecIter->next()), (ret==Drill::QRY_SUCCESS || ret==Drill::QRY_SUCCESS_WITH_INFO) && !pRecIter->hasError()){
                    fields = pRecIter->getColDefs();
                    row++;
                    if( (ret==Drill::QRY_SUCCESS_WITH_INFO  && pRecIter->hasSchemaChanged() )|| ( row%100==1)){
                        for(size_t i=0; i<fields->size(); i++){
                            std::string name= fields->at(i)->getName();
                            printf("%s\t", name.c_str());
                        }
                        printf("\n");
                    }
                    printf("ROW: %ld\t", row);
                    for(size_t i=0; i<fields->size(); i++){
                        void* pBuf; size_t sz;
                        pRecIter->getCol(i, &pBuf, &sz);
                        print(fields->at(i), pBuf, sz);
                    }
                    printf("\n");
                    if(bTestCancel && row%100==1){
                        pRecIter->cancel();
                        printf("Application cancelled the query.\n");
                    }
                }
                if(ret!=Drill::QRY_NO_MORE_DATA && ret!=Drill::QRY_CANCEL){
                    std::cerr<< pRecIter->getError() << std::endl;
                }
                client.freeQueryIterator(&pRecIter);
            }
            client.waitForResults();
        }else{
            if(bSyncSend){
                for(queryInpIter = queryInputs.begin(); queryInpIter != queryInputs.end(); queryInpIter++) {
                    Drill::QueryHandle_t* qryHandle = new Drill::QueryHandle_t;
                    client.submitQuery(type, *queryInpIter, QueryResultsListener, NULL, qryHandle);
                    client.registerSchemaChangeListener(qryHandle, SchemaListener);
                    
                    client.waitForResults();

                    client.freeQueryResources(qryHandle);
                    delete qryHandle;
                }

            }else{
                for(queryInpIter = queryInputs.begin(); queryInpIter != queryInputs.end(); queryInpIter++) {
                    Drill::QueryHandle_t* qryHandle = new Drill::QueryHandle_t;
                    client.submitQuery(type, *queryInpIter, QueryResultsListener, NULL, qryHandle);
                    client.registerSchemaChangeListener(qryHandle, SchemaListener);
                    queryHandles.push_back(qryHandle);
                }
                client.waitForResults();
                for(queryHandleIter = queryHandles.begin(); queryHandleIter != queryHandles.end(); queryHandleIter++) {
                    client.freeQueryResources(*queryHandleIter);
                    delete *queryHandleIter;
                }
            }
        }
        client.close();
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}
