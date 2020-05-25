//
// Created by ophir on 19/05/2020.
//
#include "MapReduceFramework.h"
#include <pthread.h>
#include <cstdio>
#include <atomic>


typedef std::vector<std::pair<K2*,V2*>> threadVector;
typedef std::pair<threadVector*, pthread_mutex_t*> threadVectorAndMutex;


/**
     * startMapReduceFramework – This function starts running the MapReduce algorithm (with
    several threads) and returns a JobHandle.

    client – The implementation of ​ MapReduceClient class, in other words the task that the
    framework should run.
    inputVec – a vector of type std::vector<std::pair<K1*, V1*>>, the input elements
    outputVec – a vector of type std::vector<std::pair<K3*, V3*>>, to which the output
    elements will be added before returning. You can assume that outputVec is empty.
    multiThreadLevel – the number of worker threads to be used for running the algorithm.
    You can assume this argument is valid (greater or equal to 2).
 * @param client
 * @param inputVec
 * @param outputVec
 * @param multiThreadLevel
 * @return
 */
typedef struct {

    // is shuffle phase over?
    bool doneJob;

    //The mapreduceclient
    const MapReduceClient* client;

    // how many threads have finished map phase
    std::atomic<int>* mapBarrier;

    // how many threads have finished map phase
    std::atomic<int>* processedKeys;

    // is shuffle phase over?
    pthread_cond_t* shuffleDone;

    // is shuffle phase over?
    pthread_cond_t* doneCv;

    // is shuffle phase over?
    pthread_mutex_t* shuffleDoneMutex;

    // is shuffle phase over?
    pthread_mutex_t* outVecMutex;

    // is shuffle phase over?
    pthread_mutex_t* percMutex;

    // is shuffle phase over?
    pthread_mutex_t* doneMutex;

    // how many pairs are available for shuffle to process
    std::atomic<int>* pairCount;

    // how many pairs are available for shuffle to process
    std::atomic<unsigned long>* workCounter;

    // map of key = k2*, value = V2*
    IntermediateMap* inter;

    // map of key = k2*, value = V2*
    std::vector<K2*>* interKeys;

    //output vec
    OutputVec* out;

    //jobstate
    JobState* jobState;

} JobContext;

typedef struct{

    // the input vector
    const InputVec* inputVec;

    //common job map threads need
    JobContext* job;

    // personal output vec for mapthread
    threadVectorAndMutex* tvAndMutex;

    // thread id
    int threadID;


} mapThread;

typedef struct{

    // all personal output thread vectors
    std::vector<std::pair<threadVector*, pthread_mutex_t*>>* threadVectors;

    // how many threads do we have
    int multiThreadLevel;

    // shuffle thread id
    int threadID;

    //common job map threads need
    JobContext* data;
} shuffleThread;

void reduceWrapper(void* context){
    auto* tJob = (JobContext*) context;
    unsigned long oldValue = (*tJob->workCounter)++;
    unsigned long interKeysSize = tJob->interKeys->size();
    while(oldValue < interKeysSize)
    {
        K2* key = tJob->interKeys->at(oldValue);
        tJob->client->reduce(key, tJob->inter->at(key), context);
        pthread_mutex_lock(tJob->percMutex);
        tJob->jobState->percentage = (PERCENTAGE * ((float) ++(*tJob->processedKeys)/
                tJob->interKeys->size()));
        pthread_mutex_unlock(tJob->percMutex);
        oldValue = (*tJob->workCounter)++;
    }
    pthread_mutex_lock(tJob->doneMutex);
    if(tJob->jobState->percentage == PERCENTAGE){
        tJob->doneJob = true;
        pthread_cond_broadcast(tJob->doneCv);
    }
    pthread_mutex_unlock(tJob->doneMutex);
    //todo terminate?
}

void* mapNreduce(void *context){
    auto *tData = (mapThread*) context;
    unsigned long old_value = (*tData->job->workCounter)++;
    unsigned long vecsize = tData->inputVec->size();
    while(old_value < vecsize) {
        InputPair a = tData->inputVec->at(old_value);
        JobContext* c = tData->job;
        const MapReduceClient* b = c->client;
        b->map(a.first, a.second, context);
        pthread_mutex_lock(tData->job->percMutex);
        tData->job->jobState->percentage = (PERCENTAGE * ((float) ++(*tData->job->processedKeys)/
                                                          tData->inputVec->size()));
        pthread_mutex_unlock(tData->job->percMutex);
        old_value = (*tData->job->workCounter)++;
    }
    (*tData->job->mapBarrier)++;
    //waitForJob(tData->job->shuffleDone); // todo find shuffle jobhandle
    if(pthread_mutex_lock(tData->job->shuffleDoneMutex) != 0){
        //todo
        //exit()
    }
    pthread_cond_wait(tData->job->shuffleDone, tData->job->shuffleDoneMutex);
    pthread_mutex_unlock(tData->job->shuffleDoneMutex);
    reduceWrapper(tData->job);
    //    tData->job->client->reduce(tData->job->inputVec->at(old_value).first, tData->job->inputVec->at(old_value).second, context);
    // const K1* key, const V1* value, void* context
    return context;
}

JobHandle shuffle(void* context){
    auto* sData = (shuffleThread*) context;
    int mapThreadCount = sData->multiThreadLevel - 1;
    bool done = false;
    float shuffleCount = 0;
    float totalJob = 0;
    while(!done)
    {
        for(int i = 0; i < sData->multiThreadLevel - 1; ++i)
        {
            threadVector* curVec = sData->threadVectors->at(i).first;
            pthread_mutex_t* mutex = sData->threadVectors->at(i).second;
            pthread_mutex_lock(mutex);
            IntermediateMap* intermediateMap = sData->data->inter;
            while(!curVec->empty())
            {
                IntermediatePair pair  = curVec->at(0);
                curVec->erase(curVec->begin());
                if(intermediateMap->find(pair.first) == intermediateMap->end())
                {
                    intermediateMap->insert(std::make_pair(pair.first,std::vector<V2*>()));
                    //intermediateMap->at(pair.first) = std::vector<V2*>();
                    sData->data->interKeys->push_back(pair.first);
                }
                intermediateMap->at(pair.first).push_back(pair.second);
                --(*sData->data->pairCount);
                shuffleCount++;
                if(sData->data->jobState->stage == SHUFFLE_STAGE)
                {
                    sData->data->jobState->percentage = PERCENTAGE * (shuffleCount / totalJob);
                }
            }
            pthread_mutex_unlock(mutex);
        }
        if(*sData->data->mapBarrier == mapThreadCount)
        {
            sData->data->jobState->stage = SHUFFLE_STAGE;
            totalJob = shuffleCount + *sData->data->pairCount;
            sData->data->jobState->percentage = PERCENTAGE * (shuffleCount / totalJob);
            if (*sData->data->pairCount == 0)
            {
                done = true;
            }
        }
    }
    for(auto & threadVector : *sData->threadVectors)
    {
        pthread_mutex_destroy(threadVector.second);
    }
    if(pthread_mutex_lock(sData->data->shuffleDoneMutex) != 0){
        //todo
        //exit()
    }
    (*sData->data->workCounter) = 0;
    (*sData->data->processedKeys) = 0;
    sData->data->jobState->stage = REDUCE_STAGE;
    sData->data->jobState->percentage = 0.0;
    pthread_cond_broadcast(sData->data->shuffleDone);
    pthread_mutex_unlock(sData->data->shuffleDoneMutex);
    reduceWrapper(sData->data);
    return nullptr;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    //todo make sure multiThreadLevel >= 2
    auto* threads = new pthread_t[multiThreadLevel];
    auto* threadVectors = new std::vector<threadVectorAndMutex>;
    auto* data = new JobContext;
    data->inter = new IntermediateMap;
    data->client = &client;
    data->mapBarrier = new std::atomic<int>(0);
    data->pairCount = new std::atomic<int>(0);
    data->processedKeys = new std::atomic<int>(0);
    data->workCounter = new std::atomic<unsigned long>(0);
    data->interKeys = new std::vector<K2*>();
    data->jobState = new JobState;
    data->jobState->stage = UNDEFINED_STAGE;
    data->jobState->percentage = 0.0;

    pthread_cond_t shuffleDone = PTHREAD_COND_INITIALIZER;
    pthread_cond_t doneCv = PTHREAD_COND_INITIALIZER;

    pthread_mutex_t shuffDone = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t percMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t outVecMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t doneMutex = PTHREAD_MUTEX_INITIALIZER;

    data->percMutex = &percMutex;
    data->shuffleDone = &shuffleDone;
    data->doneCv = &doneCv;
    data->doneJob = false;
    data->shuffleDoneMutex = &shuffDone;
    data->outVecMutex = &outVecMutex;
    data->doneMutex = &doneMutex;

    data->out = &outputVec;
    data->jobState->stage = MAP_STAGE;
    for(int i = 0; i < multiThreadLevel - 1; ++i){
        auto* myData = new mapThread;
        myData->threadID = i;
        myData->inputVec = &inputVec;
        pthread_mutex_t vecMutex = PTHREAD_MUTEX_INITIALIZER;
        myData->tvAndMutex = new threadVectorAndMutex();
        myData->tvAndMutex->first = new threadVector();
        myData->tvAndMutex->second = &vecMutex;
        threadVectors->push_back(*myData->tvAndMutex);
        myData->job = data;
        pthread_create(threads + i, nullptr, mapNreduce, myData);
    }
    auto* shuffi = new shuffleThread;
    shuffi->threadVectors = threadVectors;
    shuffi->threadID = multiThreadLevel - 1;
    shuffi->multiThreadLevel = multiThreadLevel;
    shuffi->data = data;
    pthread_create(threads + (multiThreadLevel - 1), nullptr, shuffle, shuffi);
    return data;/// todo Jobhandle
}

void waitForJob(JobHandle job){
    auto* context = (JobContext*) job;
    pthread_mutex_lock(context->doneMutex);
    if(!context->doneJob){
        pthread_cond_wait(context->doneCv, context->doneMutex);
    }
    pthread_mutex_unlock(context->doneMutex);
}

void getJobState(JobHandle job, JobState* state){
    auto* context = (JobContext*) job;
    state->stage =  context->jobState->stage; // todo what?
    state->percentage =  context->jobState->percentage;
}

void closeJobHandle(JobHandle job){
    waitForJob(job);
    auto* context = (JobContext*) job;
    pthread_mutex_destroy(context->doneMutex);
    pthread_mutex_destroy(context->percMutex);
    pthread_mutex_destroy(context->outVecMutex);
    pthread_mutex_destroy(context->shuffleDoneMutex);
    pthread_cond_destroy(context->doneCv);
    pthread_cond_destroy(context->shuffleDone);
    delete context->pairCount;
    delete context->processedKeys;
    delete context->mapBarrier;
    delete context->workCounter;
    delete context->jobState;
}

void emit2 (K2* key, V2* value, void* context){
    auto tData = (mapThread*) context;
    pthread_mutex_lock(tData->tvAndMutex->second);
    tData->tvAndMutex->first->push_back(std::make_pair(key,value));
    pthread_mutex_unlock(tData->tvAndMutex->second);
    ++(*tData->job->pairCount);
}

void emit3 (K3* key, V3* value, void* context){
    auto* tData = (JobContext*) context;
    pthread_mutex_lock(tData->outVecMutex);
    tData->out->push_back(std::make_pair(key, value));
    pthread_mutex_unlock(tData->outVecMutex);
}