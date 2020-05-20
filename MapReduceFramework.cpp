//
// Created by ophir on 19/05/2020.
//
#include "MapReduceFramework.h"
#include <pthread.h>
#include <cstdio>
#include <atomic>


typedef std::vector<std::pair<K2*,V2*>> threadVector;

void emit2 (K2* key, V2* value, void* context){

}

void emit3 (K3* key, V3* value, void* context){

}

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

    const InputVec* inputVec;
    const MapReduceClient* client;
    std::atomic<int>* inputCounter;
    std::atomic<int>* mapBarrier;
    pthread_cond_t* shuffleDone;
    pthread_cond_t* shuffleResume;
}threadData;

typedef struct mapThread{
    threadData* data;
    threadVector* myVec;
    int threadID;
} mapThread;

typedef struct shuffleThread{
    std::vector<threadVector*> threadVectors;
    const MapReduceClient* client;
    std::atomic<int>* mapBarrier;
    int threadID;

} shuffleThread;

void* threadLife(void *context){
    auto *tData = (mapThread*)context;
    int old_value = (*tData->data->inputCounter)++;
    while(old_value < tData->data->inputVec->size()) {
        tData->data->client->map(tData->data->inputVec->at(old_value).first, tData->data->inputVec->at(old_value).second, context);
        pthread_cond_signal(tData->data->shuffleResume);
    }
    (*tData->data->mapBarrier)++;
    waitForJob(tData->data->shuffleDone); // todo find shuffle jobhandle

//    tData->data->client->reduce(tData->data->inputVec->at(old_value).first, tData->data->inputVec->at(old_value).second, context);

    // const K1* key, const V1* value, void* context
}

JobHandle shuffle(void* context){
// shuffle
    std::map< K2*, std::vector<V2*>> intermediateMap;

}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    pthread_t threads[multiThreadLevel];   // todo -1?
    std::vector<threadVector*> threadVectors;
    std::atomic<int> inputCounter(0);
    std::atomic<int> mapBarrier(0);
    threadData *data;
    data->client = &client;
    data->inputCounter = &inputCounter;
    data->mapBarrier = &mapBarrier;
    data->inputVec = &inputVec;
    pthread_cond_t* shuffleDone;
    pthread_cond_t* shuffleResume;
    data->shuffleDone = shuffleDone;
    data->shuffleResume = shuffleResume;
    for(int i = 0; i < multiThreadLevel - 1; ++i){
        mapThread myData;
        myData.threadID = i;
        threadVector myVec;
        myData.myVec = &myVec;
        threadVectors.push_back(&myVec);
        myData.data = data;
        pthread_create(threads + i, nullptr, threadLife, &myData);
    }
    shuffleThread shuffi;
    shuffi.client = &client;
    shuffi.threadVectors = threadVectors;
    shuffi.mapBarrier = &mapBarrier;
    shuffi.threadID = multiThreadLevel - 1;
    pthread_create(threads + (multiThreadLevel - 1), nullptr, shuffle, &myData);
}

void waitForJob(JobHandle job){
    pthread_cond_wait((pthread_cond_t*) job, nullptr);
}
void getJobState(JobHandle job, JobState* state){

}
void closeJobHandle(JobHandle job){

}
