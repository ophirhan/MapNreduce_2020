//
// Created by ophir on 19/05/2020.
//
#include "MapReduceFramework.h"
#include <pthread.h>
#include "Barrier.h"
#include <cstdio>
#include <atomic>


typedef std::vector<std::pair<K2*,V2*>> threadVector;


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

    // The mapReduceClient
    const MapReduceClient* client;

    // how many threads have finished map phase
    Barrier* mapBarrier;

    // Array of threads
    pthread_t* threads;

    // counter of processed keys in curent phase.
    std::atomic<int>* processedKeys;

    // pointer to the output vector mutex
    pthread_mutex_t* outVecMutex;

    // Mutex of jobstate
    pthread_mutex_t* jobStateMutex;

    // is shuffle phase over?
    pthread_mutex_t* doneMutex;

    // how many pairs are available for shuffle to process
    std::atomic<int>* pairCount;

    // how many pairs are available to map or reduce.
    std::atomic<unsigned long>* workCounter;

    // map of key = k2*, value = V2*
    IntermediateMap* inter;

    // vector of unique keys to processes
    std::vector<K2*>* interKeys;

    //output vec
    OutputVec* outputVec;

    //jobstate
    JobState* jobState;

    // how many threads do we have
    int multiThreadLevel;

    // Has someone called wait
    std::atomic_bool* someoneWaiting;

} JobContext;

typedef struct{

    // the input vector
    const InputVec* inputVec;

    //common job map threads need
    JobContext* job;

    // personal output vec for mapthread
    threadVector* tv;

    // mutex of threadVector
    pthread_mutex_t* tvMutex;



} mapThread;

typedef struct{

    // all personal output thread vectors
    std::vector<threadVector*>* threadVectors;

    // all personal output thread vectors mutexes
    std::vector<pthread_mutex_t*>* threadMutexes;

    //common job struct
    JobContext* data;
} shuffleThread;

void finishShuffle(const shuffleThread *sData, int mapThreadCount);

float processVector(const shuffleThread *sData, float shuffleCount, float totalJob,
                    IntermediateMap *intermediateMap, int treadNum);

float setShuffle(const shuffleThread *sData, float shuffleCount, float totalJob);

JobContext *
initJobContext(const MapReduceClient &client, OutputVec &outputVec, int multiThreadLevel,
               pthread_t *threads);

mapThread *initMapThreadData(const InputVec &inputVec, JobContext *data);

shuffleThread *
initShuffleStruct(std::vector<threadVector *> *threadVectors, std::vector<pthread_mutex_t *> *threadMutexes,
                  JobContext *data);

/**
 * This function is responsible to find next available key to reduce and call reduce upon it.
 * @param context void* (JobContext*) pointer to struct containing needed resources.
 */
void reduceWrapper(void* context){
    auto* tJob = (JobContext*) context;
    unsigned long oldValue = (*tJob->workCounter)++;
    unsigned long interKeysSize = tJob->interKeys->size();
    while(oldValue < interKeysSize)
    {
        K2* key = tJob->interKeys->at(oldValue);
        tJob->client->reduce(key, tJob->inter->at(key), context);
        if(pthread_mutex_lock(tJob->jobStateMutex) != 0){
            fprintf(stderr, "system error: error on pthread_mutex_lock\n");
            exit(1);
        }
        tJob->jobState->percentage = (PERCENTAGE * ((float) ++(*tJob->processedKeys)/interKeysSize));
        if(pthread_mutex_unlock(tJob->jobStateMutex) != 0){
            fprintf(stderr, "system error: error on pthread_mutex_unlock\n");
            exit(1);
        }
        oldValue = (*tJob->workCounter)++;
    }
}

/**
 * This function is responsible to find next available key to map and call map upon it.
 * after no more keys are available to map, waits for barrier and sends the thread to reduce phase.
 * @param context context void* (JobContext*) pointer to struct containing needed resources.
 * @return JobContext
 */
void* mapNreduce(void *context){
    auto *tData = (mapThread*) context;
    unsigned long old_value = (*tData->job->workCounter)++;
    unsigned long vecsize = tData->inputVec->size();
    while(old_value < vecsize) {
        InputPair a = tData->inputVec->at(old_value);
        JobContext* c = tData->job;
        const MapReduceClient* b = c->client;
        b->map(a.first, a.second, context);
        if(pthread_mutex_lock(tData->job->jobStateMutex) != 0){
            fprintf(stderr, "system error: error on pthread_mutex_lock\n");
            exit(1);
        }
        tData->job->jobState->percentage = (PERCENTAGE * ((float) ++(*tData->job->processedKeys)/
                                                          tData->inputVec->size()));
        if(pthread_mutex_unlock(tData->job->jobStateMutex) != 0){
            fprintf(stderr, "system error: error on pthread_mutex_unlock\n");
            exit(1);
        }
        old_value = (*tData->job->workCounter)++;
    }
    tData->job->mapBarrier->barrier();
    JobContext* job = tData->job;
    delete tData;
    reduceWrapper(job);
    return context;
}

/**
 * Iterates through all private thread vectors and sorts their content into an intermediate map.
 * after that
 * @param context
 * @return
 */
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
            shuffleCount = processVector(sData, shuffleCount, totalJob, intermediateMap, i);
        }
        int count;
        sData->data->mapBarrier->getCount(&count);
        if(count == mapThreadCount)
        {
            totalJob = setShuffle(sData, shuffleCount, totalJob);
            if (*sData->data->pairCount == 0)
            {
                done = true;
            }
        }
    }

    finishShuffle(sData, mapThreadCount);

    sData->data->mapBarrier->barrier();
    JobContext* job = sData->data;
    delete sData;
    reduceWrapper(job);
    return nullptr;
}

/**
 * Sets the stage to shuffle and updates the percentage accordingly.
 * @param sData struct with all resources.
 * @param shuffleCount count of keys that shuffle sorted
 * @param totalJob total count of k2 keys shuffle has to process
 * @return totalJob
 */
float setShuffle(const shuffleThread *sData, float shuffleCount, float totalJob)
{
    if(pthread_mutex_lock(sData->data->jobStateMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_lock\n");
        exit(1);
    }
    sData->data->jobState->stage = SHUFFLE_STAGE;
    totalJob = shuffleCount + *sData->data->pairCount;
    sData->data->jobState->percentage = PERCENTAGE * (shuffleCount / totalJob);
    if(pthread_mutex_unlock(sData->data->jobStateMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_unlock\n");
        exit(1);
    }
    return totalJob;
}

/**
 * sort the private vector into intermediate vector until no more keys are available.
 * @param sData struct with common resources.
 * @param shuffleCount total intermediate pairs keys that were sorted
 * @param totalJob total pairs to process
 * @param intermediateMap the output vec of shuffle
 * @param treadNum current thread and vector num
 * @return total pairs proccesed by shuffle.
 */
float processVector(const shuffleThread *sData, float shuffleCount, float totalJob,
                    IntermediateMap *intermediateMap, int treadNum)
{
    threadVector* curVec = sData->threadVectors->at(treadNum);
    pthread_mutex_t* curMutex = sData->threadMutexes->at(treadNum);
    if(pthread_mutex_lock(curMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_lock\n");
        exit(1);
    }
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
    if(pthread_mutex_unlock(curMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_unlock\n");
        exit(1);
    }

    if(pthread_mutex_lock(sData->data->jobStateMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_lock\n");
        exit(1);
    }
    if(sData->data->jobState->stage == SHUFFLE_STAGE)
    {
        sData->data->jobState->percentage = PERCENTAGE * (shuffleCount / totalJob);
    }
    if(pthread_mutex_unlock(sData->data->jobStateMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_unlock\n");
        exit(1);
    }
    return shuffleCount;
}

/**
 * finish shuffle set stage to reduce and init relevant structures.
 * @param sData struct containg common resources
 * @param mapThreadCount number of thread in map phase.
 */
void finishShuffle(const shuffleThread *sData, int mapThreadCount)
{
    for(int i=0; i < mapThreadCount; ++i){
        delete sData->threadVectors->at(i);
        if(pthread_mutex_destroy(sData->threadMutexes->at(i)) != 0){
            fprintf(stderr, "system error: error on pthread_mutex_destroy\n");
            exit(1);
        }
        free(sData->threadMutexes->at(i));
    }
    delete sData->threadVectors;
    delete sData->threadMutexes;

    (*sData->data->workCounter) = 0;
    (*sData->data->processedKeys) = 0;
    if(pthread_mutex_lock(sData->data->jobStateMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_lock\n");
        exit(1);
    }
    sData->data->jobState->stage = REDUCE_STAGE;
    sData->data->jobState->percentage = 0.0;
    if(pthread_mutex_unlock(sData->data->jobStateMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_unlock\n");
        exit(1);
    }
}

/**
 * This function starts running the MapReduce algorithm (with several threads)
 * and returns a JobHandle
 * @param client containing map and reduce implemntations
 * @param inputVec Given inputVec
 * @param outputVec pointer to the vector to output results to
 * @param multiThreadLevel desired number of threads to create for the job.
 * @return JobHandle containing jobstate.
 */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    auto* threads = new pthread_t[multiThreadLevel];

    auto* threadVectors = new std::vector<threadVector*>;
    auto threadMutexes = new std::vector<pthread_mutex_t*>;

    JobContext *data = initJobContext(client, outputVec, multiThreadLevel, threads);


    for(int i = 0; i < multiThreadLevel - 1; ++i){
        mapThread *myData = initMapThreadData(inputVec, data);
        threadVectors->push_back(myData->tv);
        threadMutexes->push_back(myData->tvMutex);
        if(pthread_create(threads + i, nullptr, mapNreduce, myData) != 0){
            fprintf(stderr, "system error: error on pthread_create\n");
            exit(1);
        }
    }
    shuffleThread *shuffleContext = initShuffleStruct(threadVectors, threadMutexes, data);
    if(pthread_create(threads + (multiThreadLevel - 1),
                      nullptr, shuffle, shuffleContext) != 0){
        fprintf(stderr, "system error: error on pthread_create\n");
        exit(1);
    }
    return data;
}
/**
 * This function initializes the shuffle thread
 * @param threadVectors - the private work vector of each map thread
 * @param threadMutexes - the mutexes of the work vectors
 * @param data  - the job context of the current job
 * @return all the data needed for the shuffle to commence
 */
shuffleThread *
initShuffleStruct(std::vector<threadVector *> *threadVectors, std::vector<pthread_mutex_t *> *threadMutexes,
                  JobContext *data) {
    auto shuffleContext = new shuffleThread;
    shuffleContext->threadVectors = threadVectors;
    shuffleContext->threadMutexes = threadMutexes;
    shuffleContext->data = data;
    return shuffleContext;
}
/**
 * This function initialize a mapping thread
 * @param inputVec - the input vector of data from the user
 * @param data - the job context of the current job
 * @return data for a single mapping thread
 */
mapThread *initMapThreadData(const InputVec &inputVec, JobContext *data)
{
    auto* myData = new mapThread;
    myData->inputVec = &inputVec;
    auto vecMutex = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
    if(vecMutex == nullptr){
        fprintf(stderr, "system error: error on malloc\n");
        exit(1);
    }
    if(pthread_mutex_init(vecMutex, nullptr) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_init\n");
        exit(1);
    }
    myData->tv = new threadVector();
    myData->tvMutex = vecMutex;
    myData->job = data;
    return myData;
}
/**
 *
 * This function initialize all the information needed for the requested job.
 * @param client - the current user of the program
 * @param outputVec - the output requested in a vector
 * @param multiThreadLevel - the maximum number of working threads
 * @param threads  - an array of threads to create
 * @return The information needed for the jon, held in a single struct.
 */
JobContext *
initJobContext(const MapReduceClient &client, OutputVec &outputVec, int multiThreadLevel,
               pthread_t *threads)
{
    auto* data = new JobContext;
    data->jobState = new JobState;
    data->outputVec = &outputVec;

    data->threads = threads;

    data->jobState->stage = UNDEFINED_STAGE;
    data->jobState->percentage = 0.0; // only thread running

    data->inter = new IntermediateMap;
    data->interKeys = new std::vector<K2*>();

    data->client = &client;
    data->mapBarrier = new Barrier(multiThreadLevel);

    data->pairCount = new std::atomic<int>(0);
    data->processedKeys = new std::atomic<int>(0);
    data->workCounter = new std::atomic<unsigned long>(0);
    data->someoneWaiting = new std::atomic_bool(false);


    data->multiThreadLevel = multiThreadLevel;

    auto percMutex = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
    if(percMutex == nullptr){
        fprintf(stderr, "system error: error on malloc\n");
        exit(1);
    }
    if(pthread_mutex_init(percMutex, nullptr) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_init\n");
        exit(1);
    }
    data->jobStateMutex = percMutex;

    auto outVecMutex = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
    if(outVecMutex == nullptr){
        fprintf(stderr, "system error: error on malloc\n");
        exit(1);
    }
    if(pthread_mutex_init(outVecMutex, nullptr) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_init\n");
        exit(1);
    }
    data->outVecMutex = outVecMutex;

    auto doneMutex = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
    if(doneMutex == nullptr){
        fprintf(stderr, "system error: error on malloc\n");
        exit(1);
    }
    if(pthread_mutex_init(doneMutex, nullptr) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_init\n");
        exit(1);
    }
    data->doneMutex = doneMutex;

    data->jobState->stage = MAP_STAGE;
    return data;
}
/**
 * This function waits for a job to finish.
 * @param job - which the function waits for
 */
void waitForJob(JobHandle job){
    auto context = (JobContext*) job;
    if(*context->someoneWaiting){
        return;
    }
    *context->someoneWaiting= true;
    for(int i=0;i < context->multiThreadLevel; ++i){
        if(pthread_join(context->threads[i], nullptr) != 0){
            fprintf(stderr, "system error: error on pthread_join\n");
            exit(1);
        }
    }

}
/**
 * THis function gets the current job state of the program
 * @param job  - the context of the function
 * @param state -  the struct to fill
 */
void getJobState(JobHandle job, JobState* state){
    auto context = (JobContext*) job;
    if(pthread_mutex_lock(context->jobStateMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_lock\n");
        exit(1);
    }
    state->stage =  context->jobState->stage;
    state->percentage =  context->jobState->percentage;
    if(pthread_mutex_unlock(context->jobStateMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_unlock\n");
        exit(1);
    }
}
/**
 * This function closes all the resources that the program used
 * @param job  - the context of the function
 */
void closeJobHandle(JobHandle job){
    waitForJob(job);

    auto* context = (JobContext*) job;
    if(pthread_mutex_destroy(context->doneMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_destroy\n");
        exit(1);
    }
    free(context->doneMutex);
    if(pthread_mutex_destroy(context->jobStateMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_destroy\n");
        exit(1);
    }
    free(context->jobStateMutex);
    if(pthread_mutex_destroy(context->outVecMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_destroy\n");
        exit(1);
    }
    free(context->outVecMutex);
    delete context->inter;
    delete context->interKeys;
    delete context->pairCount;
    delete context->processedKeys;
    delete context->workCounter;
    delete context->jobState;
    delete context->mapBarrier;
    delete context->someoneWaiting;
    delete[] context->threads;
    delete context;
}
/**
 * This function produces a (K2*,V2*) pair.
 * @param key recieved from the user
 * @param value received from the user
 * @param context - the context of the function
 */
void emit2 (K2* key, V2* value, void* context){
    auto tData = (mapThread*) context;
    if(pthread_mutex_lock(tData->tvMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_lock\n");
        exit(1);
    }
    tData->tv->emplace_back(key,value);
    if(pthread_mutex_unlock(tData->tvMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_unlock\n");
        exit(1);
    }
    ++(*tData->job->pairCount);
}

/**
 * This function produces a (K3*,V3*) pair.
 * @param key received from the user
 * @param value received from the user
 * @param context - the context of the function
 */
void emit3 (K3* key, V3* value, void* context){
    JobContext* tData;
    tData = (JobContext*) context;
    if(pthread_mutex_lock(tData->outVecMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_lock\n");
        exit(1);
    }
    tData->outputVec->emplace_back(key, value);
    if(pthread_mutex_unlock(tData->outVecMutex) != 0){
        fprintf(stderr, "system error: error on pthread_mutex_unlock\n");
        exit(1);
    }
}
