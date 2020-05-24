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

    //The mapreduceclient
    const MapReduceClient* client;

    // how many threads have finished map phase
    std::atomic<int>* mapBarrier;

    // is shuffle phase over?
    pthread_cond_t* shuffleDone;

    // is shuffle phase over?
    pthread_mutex_t* shuffleDoneMutex;

    // is shuffle phase over?
    pthread_mutex_t* outVecMutex;

    // how many pairs are available for shuffle to process
    std::atomic<int>* pairCount;

    // how many pairs are available for shuffle to process
    std::atomic<int>* reduceCounter;

    // map of key = k2*, value = V2*
    IntermediateMap* inter;

    // map of key = k2*, value = V2*
    std::vector<K2*> interKeys;

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
    threadVectorAndMutex tvAndMutex;

    // thread id
    int threadID;

    // check progress on input vector
    std::atomic<int>* inputCounter;

} mapThread;

typedef struct{

    // all personal output thread vectors
    std::vector<std::pair<threadVector*, pthread_mutex_t*>> threadVectors;

    // how many threads do we have
    int multiThreadLevel;

    // shuffle thread id
    int threadID;

    //common job map threads need
    JobContext* data;
} shuffleThread;

void reduceWrapper(void* context){
    auto* tJob = (JobContext*) context;
    int oldValue = (*tJob->reduceCounter)++;
    while(oldValue < tJob->interKeys.size())
    {
        K2* key = tJob->interKeys.at(oldValue);
        tJob->client->reduce(key, tJob->inter->at(key), context);
    }
}

void* mapNreduce(void *context){
    auto *tData = (mapThread*)context;
    int old_value = (*tData->inputCounter)++;
    while(old_value < tData->inputVec->size()) {
        tData->job->client->map(tData->inputVec->at(old_value).first, tData->inputVec->at(old_value).second, context);
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
    auto sData = (shuffleThread*) context;
    int mapThreadCount = sData->multiThreadLevel - 1;
    bool done = false;
    while(!done)
    {
        for(int i = 0; i < sData->multiThreadLevel; ++i)
        {
            threadVector* curVec = sData->threadVectors.at(i).first;
            pthread_mutex_t* mutex = sData->threadVectors.at(i).second;
            pthread_mutex_lock(mutex);
            IntermediateMap* intermediateMap = sData->data->inter;
            while(!curVec->empty())
            {
                IntermediatePair pair  = curVec->at(0);
                curVec->erase(curVec->begin());
                if(intermediateMap->find(pair.first) == intermediateMap->end())
                {
                    intermediateMap->at(pair.first) = std::vector<V2*>();
                    sData->data->interKeys.push_back(pair.first);
                }
                intermediateMap->at(pair.first).push_back(pair.second);
                --(*sData->data->pairCount);
            }
            pthread_mutex_unlock(mutex);
        }
        if(*sData->data->mapBarrier == mapThreadCount)
        {
            sData->data->jobState->stage = SHUFFLE_STAGE;
            if (*sData->data->pairCount == 0)
            {
                done = true;
            }
        }
    }

    if(pthread_mutex_lock(sData->data->shuffleDoneMutex) != 0){
        //todo
        //exit()
    }
    pthread_cond_broadcast(sData->data->shuffleDone);
    pthread_mutex_unlock(sData->data->shuffleDoneMutex);
    reduceWrapper(sData->data);
    return nullptr;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    pthread_t threads[multiThreadLevel];   // todo -1?
    std::vector<threadVectorAndMutex> threadVectors;
    std::atomic<int> inputCounter(0);
    std::atomic<int> mapBarrier(0);
    std::atomic<int> pairCount(0);
    std::atomic<int> reduceCounter(0);
    std::atomic<int> processedK1(0);
    auto* data = new JobContext;
    auto* jobState = new JobState;
    jobState->stage = UNDEFINED_STAGE;
    jobState->percentage = 0.0;
    IntermediateMap a;
    data->inter = &a;
    data->client = &client;
    data->mapBarrier = &mapBarrier;
    data->pairCount = &pairCount;
    data->reduceCounter = &reduceCounter;
    data->interKeys = std::vector<K2*>();
    data->jobState = jobState;
    pthread_cond_t shuffleDone = PTHREAD_COND_INITIALIZER;
    pthread_mutex_t shuffDone = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t outVecMutex = PTHREAD_MUTEX_INITIALIZER;
    data->shuffleDone = &shuffleDone;
    data->shuffleDoneMutex = &shuffDone;
    data->outVecMutex = &outVecMutex;
    data->out = &outputVec;
    jobState->stage = MAP_STAGE;
    for(int i = 0; i < multiThreadLevel - 1; ++i){
        mapThread myData;
        myData.threadID = i;
        threadVector myVec;
        myData.inputCounter = &inputCounter;
        myData.inputVec = &inputVec;
        pthread_mutex_t vecMutex = PTHREAD_MUTEX_INITIALIZER;
        myData.tvAndMutex = std::make_pair(&myVec, &vecMutex);
        threadVectors.push_back(myData.tvAndMutex);
        myData.job = data;
        pthread_create(threads + i, nullptr, mapNreduce, &myData);
    }
    shuffleThread shuffi;
    shuffi.threadVectors = threadVectors;
    shuffi.threadID = multiThreadLevel - 1;
    shuffi.multiThreadLevel = multiThreadLevel;
    shuffi.data = data;
    pthread_create(threads + (multiThreadLevel - 1), nullptr, shuffle, &shuffi);
    return nullptr;/// todo Jobhandle
}

void waitForJob(JobHandle job){
    auto* cvAndMutex = (std::pair<pthread_cond_t*, pthread_mutex_t*>*) job; // pointer to pair
    pthread_mutex_lock(cvAndMutex->second);
    pthread_cond_wait(cvAndMutex->first, cvAndMutex->second);
    //work
    //unlock
}

void getJobState(JobHandle job, JobState* state){

}

void closeJobHandle(JobHandle job){
    //unlock

}

void emit2 (K2* key, V2* value, void* context){
    auto tData = (mapThread*) context;
    pthread_mutex_lock(tData->tvAndMutex.second);
    tData->tvAndMutex.first->push_back(std::make_pair(key,value));
    pthread_mutex_unlock(tData->tvAndMutex.second);
    ++(*tData->job->pairCount);
//    tData->job->jobState->percentage =
}

void emit3 (K3* key, V3* value, void* context){
    auto* tData = (JobContext*) context;
    pthread_mutex_lock(tData->outVecMutex);
    tData->out->push_back(std::make_pair(key, value));
    pthread_mutex_unlock(tData->outVecMutex);
}