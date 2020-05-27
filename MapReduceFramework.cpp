//
// Created by ophir on 19/05/2020.
//
#include "MapReduceFramework.h"
#include <pthread.h>
#include "Barrier.h"
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
    //std::atomic<int>* mapBarrier;
    Barrier* mapBarrier;

    pthread_t* threads;

    // how many threads have finished map phase
    std::atomic<int>* processedKeys;

    // is shuffle phase over?
    pthread_mutex_t* outVecMutex;

    // is shuffle phase over?
    pthread_mutex_t* jobStateMutex;

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

    // how many threads do we have
    int multiThreadLevel;

} JobContext;

typedef struct{

    // the input vector
    const InputVec* inputVec;

    //common job map threads need
    JobContext* job;

    // personal output vec for mapthread
    threadVector* tv;

    pthread_mutex_t* tvMutex;



} mapThread;

typedef struct{

    // all personal output thread vectors
    std::vector<threadVector*>* threadVectors;

    // all personal output thread vectors
    std::vector<pthread_mutex_t*>* threadMutexes;

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
        pthread_mutex_lock(tJob->jobStateMutex);
        tJob->jobState->percentage = (PERCENTAGE * ((float) ++(*tJob->processedKeys)/interKeysSize));
        pthread_mutex_unlock(tJob->jobStateMutex);
        oldValue = (*tJob->workCounter)++;
    }
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
        pthread_mutex_lock(tData->job->jobStateMutex);
        tData->job->jobState->percentage = (PERCENTAGE * ((float) ++(*tData->job->processedKeys)/
                                                          tData->inputVec->size()));
        pthread_mutex_unlock(tData->job->jobStateMutex);
        old_value = (*tData->job->workCounter)++;
    }
    tData->job->mapBarrier->barrier();
    JobContext* job = tData->job;
    delete tData;
    reduceWrapper(job);
    return context;
}

JobHandle shuffle(void* context){
    auto* sData = (shuffleThread*) context;
    int mapThreadCount = sData->data->multiThreadLevel - 1;
    bool done = false;
    float shuffleCount = 0;
    float totalJob = 0;
    IntermediateMap* intermediateMap = sData->data->inter;
    while(!done)
    {
        for(int i = 0; i < mapThreadCount; ++i)
        {
            threadVector* curVec = sData->threadVectors->at(i);
            pthread_mutex_t* mutex = sData->threadMutexes->at(i);
            pthread_mutex_lock(mutex);
            while(!curVec->empty())
            {
                IntermediatePair pair  = curVec->at(0);
                curVec->erase(curVec->begin());
                if(intermediateMap->find(pair.first) == intermediateMap->end())
                {
                    intermediateMap->insert(std::make_pair(pair.first,std::vector<V2*>()));
                    sData->data->interKeys->push_back(pair.first);
                }
                intermediateMap->at(pair.first).push_back(pair.second);
                --(*sData->data->pairCount);
                shuffleCount++;
            }
            pthread_mutex_unlock(mutex);
            pthread_mutex_lock(sData->data->jobStateMutex);
            if(sData->data->jobState->stage == SHUFFLE_STAGE)
            {
                sData->data->jobState->percentage = PERCENTAGE * (shuffleCount / totalJob);
            }
            pthread_mutex_unlock(sData->data->jobStateMutex);
        }
        int count;
        sData->data->mapBarrier->getCount(&count);
        if(count == mapThreadCount)
        {
            pthread_mutex_lock(sData->data->jobStateMutex);
            sData->data->jobState->stage = SHUFFLE_STAGE;
            totalJob = shuffleCount + *sData->data->pairCount;
            sData->data->jobState->percentage = PERCENTAGE * (shuffleCount / totalJob);
            pthread_mutex_unlock(sData->data->jobStateMutex);
            if (*sData->data->pairCount == 0)
            {
                done = true;
            }
        }
    }
    for(int i=0; i < mapThreadCount; ++i){
        delete sData->threadVectors->at(i);
        pthread_mutex_destroy(sData->threadMutexes->at(i));
        free(sData->threadMutexes->at(i));
    }
//    for(auto & threadVector : *sData->threadVectors)
//    {
//        pthread_mutex_destroy(threadVector);
//        free(threadVector.second);
//        delete threadVector.first;
//    }
//    std::vector<threadVectorAndMutex>().swap(*sData->threadVectors);
    delete sData->threadVectors;
    delete sData->threadMutexes;
    (*sData->data->workCounter) = 0;
    (*sData->data->processedKeys) = 0;
    pthread_mutex_lock(sData->data->jobStateMutex);
    sData->data->jobState->stage = REDUCE_STAGE;
    sData->data->jobState->percentage = 0.0;
    pthread_mutex_unlock(sData->data->jobStateMutex);

    sData->data->mapBarrier->barrier();
    JobContext* job = sData->data;
    delete sData;
    reduceWrapper(job);
    return nullptr;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    auto* threads = new pthread_t[multiThreadLevel];

    auto* threadVectors = new std::vector<threadVector*>;
    auto threadMutexes = new std::vector<pthread_mutex_t*>;

    auto* data = new JobContext;
    data->jobState = new JobState;
    data->out = &outputVec;

    data->threads = threads;

    data->jobState->stage = UNDEFINED_STAGE;
    data->jobState->percentage = 0.0; // only thread running

    data->inter = new IntermediateMap;
    data->interKeys = new std::vector<K2*>();

    data->client = &client;
    //data->mapBarrier = new std::atomic<int>(0);
    data->mapBarrier = new Barrier(multiThreadLevel);

    data->pairCount = new std::atomic<int>(0);
    data->processedKeys = new std::atomic<int>(0);
    data->workCounter = new std::atomic<unsigned long>(0);


    data->multiThreadLevel = multiThreadLevel;

    auto percMutex = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(percMutex, nullptr);
    data->jobStateMutex = percMutex;

    auto outVecMutex = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(outVecMutex, nullptr);
    data->outVecMutex = outVecMutex;

    auto doneMutex = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(doneMutex, nullptr);
    data->doneMutex = doneMutex;

    data->doneJob = false;

    data->jobState->stage = MAP_STAGE;

    for(int i = 0; i < multiThreadLevel - 1; ++i){
        auto* myData = new mapThread;
        myData->inputVec = &inputVec;
        auto vecMutex = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(vecMutex, nullptr);
        myData->tv = new threadVector();
        myData->tvMutex = vecMutex;
        threadVectors->push_back(myData->tv);
        threadMutexes->push_back(myData->tvMutex);
        myData->job = data;
        pthread_create(threads + i, nullptr, mapNreduce, myData);
    }
    auto shuffleContext = new shuffleThread;
    shuffleContext->threadVectors = threadVectors;
    shuffleContext->threadMutexes = threadMutexes;
    shuffleContext->data = data;
    pthread_create(threads + (multiThreadLevel - 1), nullptr, shuffle, shuffleContext);
    return data;
}

void waitForJob(JobHandle job){
    auto* context = (JobContext*) job;
    for(int i=0;i < context->multiThreadLevel; ++i){
        pthread_join(context->threads[i], nullptr);
    }

}

void getJobState(JobHandle job, JobState* state){
    auto context = (JobContext*) job;
    pthread_mutex_lock(context->jobStateMutex);
    state->stage =  context->jobState->stage;
    state->percentage =  context->jobState->percentage;
    pthread_mutex_unlock(context->jobStateMutex);
}

void closeJobHandle(JobHandle job){
    waitForJob(job);

    auto* context = (JobContext*) job;
    pthread_mutex_destroy(context->doneMutex);
    free(context->doneMutex);
    pthread_mutex_destroy(context->jobStateMutex);
    free(context->jobStateMutex);
    pthread_mutex_destroy(context->outVecMutex);
    free(context->outVecMutex);
    delete context->inter;
    delete context->interKeys;
    delete context->pairCount;
    delete context->processedKeys;
    delete context->workCounter;
    delete context->jobState;
    delete context->mapBarrier;
    delete[] context->threads;
    delete context;
}

void emit2 (K2* key, V2* value, void* context){
    auto tData = (mapThread*) context;
    pthread_mutex_lock(tData->tvMutex);
    tData->tv->emplace_back(key,value);
    pthread_mutex_unlock(tData->tvMutex);
    ++(*tData->job->pairCount);
}

void emit3 (K3* key, V3* value, void* context){
    JobContext* tData;
    tData = (JobContext*) context;
    pthread_mutex_lock(tData->outVecMutex);
    tData->out->emplace_back(key, value);
    pthread_mutex_unlock(tData->outVecMutex);
}