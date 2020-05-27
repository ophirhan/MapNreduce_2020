#ifndef BARRIER_H
#define BARRIER_H
#include <pthread.h>

// a multiple use barrier

class Barrier {
public:
	Barrier(int numThreads);
	void getCount(int*);
	~Barrier();
	void barrier();

private:
	pthread_mutex_t mutex;
	pthread_cond_t cv;
	int count;
	int numThreads;
};

#endif //BARRIER_H
