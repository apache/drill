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


#include "drill/common.hpp"
#include "drill/drillClient.hpp"
#include "drill/recordBatch.hpp"
#include "drillClientImpl.hpp"
#include "errmsgs.hpp"
#include "logger.hpp"

#include "Types.pb.h"

namespace Drill{

DrillClientError* DrillClientError::getErrorObject(const exec::shared::DrillPBError& e){
    std::string s=Drill::getMessage(ERR_QRY_FAILURE, e.message().c_str());
    DrillClientError* err=NULL;
    err=new DrillClientError(QRY_FAILURE, QRY_ERROR_START+QRY_FAILURE, s);
    return err;
}

DrillClientInitializer::DrillClientInitializer(){
    GOOGLE_PROTOBUF_VERIFY_VERSION;
}

DrillClientInitializer::~DrillClientInitializer(){
    google::protobuf::ShutdownProtobufLibrary();
}

logLevel_t DrillClientConfig::s_logLevel=LOG_ERROR;
uint64_t DrillClientConfig::s_bufferLimit=MAX_MEM_ALLOC_SIZE;
int32_t DrillClientConfig::s_socketTimeout=180;
boost::mutex DrillClientConfig::s_mutex;

DrillClientConfig::DrillClientConfig(){
    initLogging(NULL);
}

DrillClientConfig::~DrillClientConfig(){
    Logger::close();
}

void DrillClientConfig::initLogging(const char* path){
    Logger::init(path);
}

void DrillClientConfig::setLogLevel(logLevel_t l){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    s_logLevel=l;
    Logger::s_level=l;
    //boost::log::core::get()->set_filter(boost::log::trivial::severity >= s_logLevel);
}

void DrillClientConfig::setBufferLimit(uint64_t l){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    s_bufferLimit=l;
}

uint64_t DrillClientConfig::getBufferLimit(){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    return s_bufferLimit;
}

void DrillClientConfig::setSocketTimeout(int32_t t){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    s_socketTimeout=t;
}

int32_t DrillClientConfig::getSocketTimeout(){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    return s_socketTimeout;
}

logLevel_t DrillClientConfig::getLogLevel(){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    return s_logLevel;
}

RecordIterator::~RecordIterator(){
    if(m_pColDefs!=NULL){
        for(std::vector<Drill::FieldMetadata*>::iterator it=m_pColDefs->begin();
                it!=m_pColDefs->end();
                ++it){
            delete *it;
        }
    }
    delete this->m_pQueryResult;
    this->m_pQueryResult=NULL;
    if(this->m_pCurrentRecordBatch!=NULL){
        DRILL_LOG(LOG_TRACE) << "Deleted last Record batch " << (void*) m_pCurrentRecordBatch << std::endl;
        delete this->m_pCurrentRecordBatch; this->m_pCurrentRecordBatch=NULL;
    }
}

FieldDefPtr RecordIterator::getColDefs(){
    if(m_pQueryResult->hasError()){
        return DrillClientQueryResult::s_emptyColDefs;
    }
    //NOTE: if query is cancelled, return whatever you have. Client applications job to deal with it.
    if(this->m_pColDefs==NULL || this->hasSchemaChanged()){
        if(this->m_pCurrentRecordBatch==NULL){
            this->m_pQueryResult->waitForData();
            if(m_pQueryResult->hasError()){
                return DrillClientQueryResult::s_emptyColDefs;
            }
        }
        if(this->hasSchemaChanged()){
            if(m_pColDefs!=NULL){
                for(std::vector<Drill::FieldMetadata*>::iterator it=m_pColDefs->begin();
                        it!=m_pColDefs->end();
                        ++it){
                    delete *it;
                }
                m_pColDefs->clear();
                //delete m_pColDefs; m_pColDefs=NULL;
            }
        }
        FieldDefPtr pColDefs(  new std::vector<Drill::FieldMetadata*>);
        {   //lock after we come out of the  wait.
            boost::lock_guard<boost::mutex> bufferLock(this->m_recordBatchMutex);
            boost::shared_ptr< std::vector<Drill::FieldMetadata*> >  currentColDefs=DrillClientQueryResult::s_emptyColDefs;
            if(this->m_pCurrentRecordBatch!=NULL){
                currentColDefs=this->m_pCurrentRecordBatch->getColumnDefs();
            }else{
                // This is reached only when the first results have been received but
                // the getNext call has not been made to retrieve the record batch
                RecordBatch* pR=this->m_pQueryResult->peekNext();
                if(pR!=NULL){
                    currentColDefs=pR->getColumnDefs();
                }
            }
            for(std::vector<Drill::FieldMetadata*>::iterator it=currentColDefs->begin(); it!=currentColDefs->end(); ++it){
                Drill::FieldMetadata* fmd= new Drill::FieldMetadata;
                fmd->copy(*(*it));//Yup, that's 2 stars
                pColDefs->push_back(fmd);
            }
        }
        this->m_pColDefs = pColDefs;
    }
    return this->m_pColDefs;
}

status_t RecordIterator::next(){
    status_t ret=QRY_SUCCESS;
    this->m_currentRecord++;

    if(!this->m_pQueryResult->isCancelled()){
        if(this->m_pCurrentRecordBatch==NULL || this->m_currentRecord==this->m_pCurrentRecordBatch->getNumRecords()){
            boost::lock_guard<boost::mutex> bufferLock(this->m_recordBatchMutex);
            if(this->m_pCurrentRecordBatch !=NULL){
                DRILL_LOG(LOG_TRACE) << "Deleted old Record batch " << (void*) m_pCurrentRecordBatch << std::endl;
                delete this->m_pCurrentRecordBatch; //free the previous record batch
                this->m_pCurrentRecordBatch=NULL;
            }
            this->m_currentRecord=0;
            this->m_pQueryResult->waitForData();
            if(m_pQueryResult->hasError()){
                return m_pQueryResult->getErrorStatus();
            }
            this->m_pCurrentRecordBatch=this->m_pQueryResult->getNext();
            if(this->m_pCurrentRecordBatch != NULL){
                DRILL_LOG(LOG_TRACE) << "Fetched new Record batch " << std::endl;
            }else{
                DRILL_LOG(LOG_TRACE) << "No new Record batch found " << std::endl;
            }
            if(this->m_pCurrentRecordBatch==NULL || this->m_pCurrentRecordBatch->getNumRecords()==0){
                DRILL_LOG(LOG_TRACE) << "No more data." << std::endl;
                ret = QRY_NO_MORE_DATA;
            }else if(this->m_pCurrentRecordBatch->hasSchemaChanged()){
                ret=QRY_SUCCESS_WITH_INFO;
            }
        }
    }else{
        ret=QRY_CANCEL;
    }
    return ret;
}

/* Gets the ith column in the current record. */
status_t RecordIterator::getCol(size_t i, void** b, size_t* sz){
    //TODO: check fields out of bounds without calling getColDefs
    //if(i>=getColDefs().size()) return QRY_OUT_OF_BOUNDS;
    //return raw byte buffer
    if(!this->m_pQueryResult->isCancelled()){
        const ValueVectorBase* pVector=this->m_pCurrentRecordBatch->getFields()[i]->getVector();
        if(!pVector->isNull(this->m_currentRecord)){
            *b=pVector->getRaw(this->m_currentRecord);
            *sz=pVector->getSize(this->m_currentRecord);
        }else{
            *b=NULL;
            *sz=0;

        }
        return QRY_SUCCESS;
    }else{
        return QRY_CANCEL;
    }
}

/* true if ith column in the current record is NULL. */
bool RecordIterator::isNull(size_t i){
    if(!this->m_pQueryResult->isCancelled()){
        const ValueVectorBase* pVector=this->m_pCurrentRecordBatch->getFields()[i]->getVector();
        return pVector->isNull(this->m_currentRecord);
    }else{
        return false;
    }
}

status_t RecordIterator::cancel(){
    this->m_pQueryResult->cancel();
    return QRY_CANCEL;
}

bool RecordIterator::hasSchemaChanged(){
    return m_currentRecord==0 && m_pCurrentRecordBatch!=NULL && m_pCurrentRecordBatch->hasSchemaChanged();
}

void RecordIterator::registerSchemaChangeListener(pfnSchemaListener l){
    assert(m_pQueryResult!=NULL);
    this->m_pQueryResult->registerSchemaChangeListener(l);
}

const std::string& RecordIterator::getError(){
    return m_pQueryResult->getError()->msg;
}

DrillClientInitializer DrillClient::s_init;

DrillClientConfig DrillClient::s_config;

void DrillClient::initLogging(const char* path, logLevel_t l){
    if(path!=NULL) s_config.initLogging(path);
    s_config.setLogLevel(l);
}

DrillClient::DrillClient(){
    this->m_pImpl=new DrillClientImpl;
}

DrillClient::~DrillClient(){
    delete this->m_pImpl;
}

connectionStatus_t DrillClient::connect(const char* connectStr, const char* defaultSchema){
    connectionStatus_t ret=CONN_SUCCESS;
    ret=this->m_pImpl->connect(connectStr);

    if(ret==CONN_SUCCESS)
        ret=this->m_pImpl->validateHandShake(defaultSchema)?CONN_SUCCESS:CONN_HANDSHAKE_FAILED;
    return ret;

}

bool DrillClient::isActive(){
    return this->m_pImpl->Active();
}

void DrillClient::close() {
    this->m_pImpl->Close();
}

status_t DrillClient::submitQuery(Drill::QueryType t, const std::string& plan, pfnQueryResultsListener listener, void* listenerCtx, QueryHandle_t* qHandle){

    ::exec::shared::QueryType castedType = static_cast<::exec::shared::QueryType> (t);
    DrillClientQueryResult* pResult=this->m_pImpl->SubmitQuery(castedType, plan, listener, listenerCtx);
    *qHandle=(QueryHandle_t)pResult;
    return QRY_SUCCESS;
}

RecordIterator* DrillClient::submitQuery(Drill::QueryType t, const std::string& plan, DrillClientError* err){
    RecordIterator* pIter=NULL;
    ::exec::shared::QueryType castedType = static_cast<::exec::shared::QueryType> (t);
    DrillClientQueryResult* pResult=this->m_pImpl->SubmitQuery(castedType, plan, NULL, NULL);
    if(pResult){
        pIter=new RecordIterator(pResult);
    }
    return pIter;
}

std::string& DrillClient::getError(){
    return m_pImpl->getError()->msg;
}


void DrillClient::waitForResults(){
    this->m_pImpl->waitForResults();
}

void DrillClient::registerSchemaChangeListener(QueryHandle_t* handle, pfnSchemaListener l){
    if(handle!=NULL){
        ((DrillClientQueryResult*)(*handle))->registerSchemaChangeListener(l);
    }
}

void DrillClient::freeQueryResources(QueryHandle_t* handle){
    delete (DrillClientQueryResult*)(*handle);
    *handle=NULL;
}

} // namespace Drill
