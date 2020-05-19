//
// Created by ophir on 19/05/2020.
//
#include "MapReduceFramework.h"
#include <pthread.h>


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
    K1* key;
    V1* Value;
    MapReduceClient* client;
} threadData;

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    pthread_t threads[multiThreadLevel];   // todo -1?
    for(int i = 0; i < multiThreadLevel; ++i){
        if(!inputVec.empty()){
            K1 *key = inputVec.at(0).first;
            V1 *value = inputVec.at(0).second;
            inputVec.erase(inputVec.begin());
            pthread_create(threads + i, NULL, threadLife, {key, value, client}); // can add more data than client
        }
    }

}

void threadLife(const K1* key, const V1* value, threadData context){
    context.client->context
}

void waitForJob(JobHandle job){

}
void getJobState(JobHandle job, JobState* state){

}
void closeJobHandle(JobHandle job){

}
