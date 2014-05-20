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
#include <boost/asio.hpp>
#include "drill/drillc.hpp"

Drill::status_t QueryResultsListener(void* ctx, Drill::RecordBatch* b, Drill::DrillClientError* err){
    if(!err){
        b->print(std::cout, 0); // print all rows
        delete b; // we're done with this batch, we can delete it
        return Drill::QRY_SUCCESS ;
    }else{
        std::cerr<< "ERROR: " << err->msg << std::endl;
        return Drill::QRY_FAILURE;
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

int nOptions=5;

struct Option{
    char name[32];
    char desc[128];
    bool required;
}qsOptions[]= { 
    {"plan", "Plan files separated by semicolons", false},
    {"query", "Query strings, separated by semicolons", false},
    {"type", "Query type [physical|logical|sql]", true},
    {"connectStr", "Connect string", true},
    {"api", "API type [sync|async]", true}
};

std::map<std::string, std::string> qsOptionValues;

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

int main(int argc, char* argv[]) {
    try {

        parseArgs(argc, argv);

        std::vector<std::string*> queries;

        std::string connectStr=qsOptionValues["connectStr"];
        std::string queryList=qsOptionValues["query"];
        std::string planList=qsOptionValues["plan"];
        std::string api=qsOptionValues["api"];
        std::string type_str=qsOptionValues["type"];

        exec::user::QueryType type;

        if(!validate(type_str, queryList, planList)){
            exit(1);
        }

        std::vector<std::string> queryInputs;
        if(type_str=="sql" ){
            readQueries(queryList, queryInputs);
            type=exec::user::SQL;
        }else if(type_str=="physical" ){
            readPlans(planList, queryInputs);
            type=exec::user::PHYSICAL;
        }else if(type_str == "logical"){
            readPlans(planList, queryInputs);
            type=exec::user::LOGICAL;
        }else{
            readQueries(queryList, queryInputs);
            type=exec::user::SQL;
        }

        std::vector<std::string>::iterator queryInpIter;

        std::vector<Drill::RecordIterator*> recordIterators;
        std::vector<Drill::RecordIterator*>::iterator recordIterIter;

        std::vector<Drill::QueryHandle_t*> queryHandles;
        std::vector<Drill::QueryHandle_t*>::iterator queryHandleIter;

        Drill::DrillClient client;
        // To log to file
        //DrillClient::initLogging("/var/log/drill/", LOG_INFO);
        // To log to stderr
        Drill::DrillClient::initLogging(NULL, Drill::LOG_INFO);

        if(client.connect(connectStr.c_str())!=Drill::CONN_SUCCESS){
            std::cerr<< "Failed to connect with error: "<< client.getError() << " (Using:"<<connectStr<<")"<<std::endl;
            return -1;
        }
        std::cout<< "Connected!\n" << std::endl;

        if(api=="sync"){
            Drill::DrillClientError* err=NULL;
            Drill::status_t ret;
            for(queryInpIter = queryInputs.begin(); queryInpIter != queryInputs.end(); queryInpIter++) {
                Drill::RecordIterator* pRecIter = client.submitQuery(type, *queryInpIter, err);
                if(pRecIter!=NULL){
                    recordIterators.push_back(pRecIter);
                }
            }
            size_t row=0;
            for(recordIterIter = recordIterators.begin(); recordIterIter != recordIterators.end(); recordIterIter++) {
                // get fields.
                row=0;
                Drill::RecordIterator* pRecIter=*recordIterIter;
                std::vector<Drill::FieldMetadata*>& fields = pRecIter->getColDefs();
                while((ret=pRecIter->next())==Drill::QRY_SUCCESS){
                    row++;
                    if(row%4095==0){
                        for(size_t i=0; i<fields.size(); i++){
                            std::string name= fields[i]->getName();
                            printf("%s\t", name.c_str());
                        }
                        printf("\n");
                    }
                    printf("ROW: %ld\t", row);
                    for(size_t i=0; i<fields.size(); i++){
                        void* pBuf; size_t sz;
                        pRecIter->getCol(i, &pBuf, &sz);
                        print(fields[i], pBuf, sz);
                    }
                    printf("\n");
                }
                if(ret!=Drill::QRY_NO_MORE_DATA){
                    std::cerr<< pRecIter->getError();
                }
                client.freeQueryIterator(&pRecIter);
            }
        }else{
            for(queryInpIter = queryInputs.begin(); queryInpIter != queryInputs.end(); queryInpIter++) {
                Drill::QueryHandle_t* qryHandle = new Drill::QueryHandle_t;
                client.submitQuery(type, *queryInpIter, QueryResultsListener, NULL, qryHandle);
                queryHandles.push_back(qryHandle);
            }
            client.waitForResults();
            for(queryHandleIter = queryHandles.begin(); queryHandleIter != queryHandles.end(); queryHandleIter++) {
                client.freeQueryResources(*queryHandleIter);
            }
        }
        client.close();
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}
