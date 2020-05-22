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
typedef struct threadData{

    // the input vector
    const InputVec* inputVec;///

    //The mapreduceclient
    const MapReduceClient* client;

    // check progress on input vector
    std::atomic<int>* inputCounter;///

    // how many threads have finished map phase
    std::atomic<int>* mapBarrier;

    // is shuffle phase over?
    pthread_cond_t* shuffleDone;

    // signal shuffle new pairs are available
    pthread_cond_t* shuffleResume;

    // how many pairs are available for shuffle to process
    std::atomic<int>* pairCount;
}threadData;

typedef struct mapThread{

    //common data map threads need
    threadData* data;

    // personal output vec for mapthread
    threadVectorAndMutex tvAndMutex;

    // thread id
    int threadID;
} mapThread;

typedef struct shuffleThread{

    // all personal output thread vectors
    std::vector<std::pair<threadVector*, pthread_mutex_t*>> threadVectors;////

    // how many pairs are available for shuffle
    std::atomic<int>* pairCount;

    // the client
    const MapReduceClient* client;

    // how many map threads done
    std::atomic<int>* mapBarrier;

    // sig shuffle to resume
    pthread_cond_t* shuffleResume;

    // shuffle phase over
    pthread_cond_t* shuffleDone;

    // how many threads do we have
    int multiThreadLevel;

    // shuffle thread id
    int threadID;
} shuffleThread;

void* threadLife(void *context){
    auto *tData = (mapThread*)context;
    int old_value = (*tData->data->inputCounter)++;
    while(old_value < tData->data->inputVec->size()) {
        tData->data->client->map(tData->data->inputVec->at(old_value).first, tData->data->inputVec->at(old_value).second, context);
    }
    (*tData->data->mapBarrier)++;
    waitForJob(tData->data->shuffleDone); // todo find shuffle jobhandle
    //    tData->data->client->reduce(tData->data->inputVec->at(old_value).first, tData->data->inputVec->at(old_value).second, context);
    // const K1* key, const V1* value, void* context
    return nullptr;
}

JobHandle shuffle(void* context){
    auto sData = (shuffleThread*) context;
    auto* intermediateMap = new std::map< K2*, std::vector<V2*>*, K2PointerComp>;
    int mapThreadCount = sData->multiThreadLevel - 1;
    bool done = false;
    while(!done)
    {
        for(int i = 0; i < sData->multiThreadLevel; ++i)
        {
            threadVector* curVec = sData->threadVectors.at(i).first;
            pthread_mutex_t* mutex = sData->threadVectors.at(i).second;
            pthread_mutex_lock(mutex);
            while(!curVec->empty())
            {
                IntermediatePair pair  = curVec->at(0);
                curVec->erase(curVec->begin());
                if(intermediateMap->find(pair.first) == intermediateMap->end())
                {
                    intermediateMap->at(pair.first) = new std::vector<V2*>;
                }
                intermediateMap->at(pair.first)->push_back(pair.second);
            }
            pthread_mutex_unlock(mutex);
        }
        if(*sData->pairCount == 0)
        {
            if(*sData->mapBarrier < mapThreadCount)
            {
                // what if here last signal happens
                pthread_cond_wait(sData->shuffleResume, nullptr);
            }
            else if (*sData->pairCount == 0) // make sure
            {
                done = true;
            }
        }
    }
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
    auto *data = new threadData;
    data->client = &client;
    data->inputCounter = &inputCounter;
    data->mapBarrier = &mapBarrier;
    data->pairCount = &pairCount;
    data->inputVec = &inputVec;
    pthread_cond_t *shuffleDone = nullptr;// = PTHREAD_COND_INITIALIZER;
    pthread_cond_t* shuffleResume = nullptr;// = PTHREAD_COND_INITIALIZER;
    pthread_cond_init(shuffleDone, nullptr);
    pthread_cond_init(shuffleResume, nullptr);
    data->shuffleDone = shuffleDone;
    data->shuffleResume = shuffleResume;
    for(int i = 0; i < multiThreadLevel - 1; ++i){
        mapThread myData;
        myData.threadID = i;
        threadVector myVec;
        pthread_mutex_t* vecMutex;
        myData.tvAndMutex = std::make_pair(&myVec, vecMutex);
        threadVectors.push_back(myData.tvAndMutex);
        myData.data = data;
        pthread_create(threads + i, nullptr, threadLife, &myData);
    }
    shuffleThread shuffi;
    shuffi.client = &client;
    shuffi.threadVectors = threadVectors;
    shuffi.mapBarrier = &mapBarrier;
    shuffi.threadID = multiThreadLevel - 1;
    shuffi.multiThreadLevel = multiThreadLevel;
    shuffi.pairCount = &pairCount;
    shuffi.shuffleResume = shuffleResume;
    pthread_create(threads + (multiThreadLevel - 1), nullptr, shuffle, &shuffi);
}

void waitForJob(JobHandle job){
    auto* cvAndMutex = (std::pair<pthread_cond_t*, pthread_mutex_t*>*) job;
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
    (*tData->data->pairCount)++;
    pthread_cond_signal(tData->data->shuffleResume);
}

void emit3 (K3* key, V3* value, void* context){

}