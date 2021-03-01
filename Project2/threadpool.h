#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <stdlib.h>

typedef struct thread_pool thread_pool_t;

// Creates and returns threadpool.
thread_pool_t *thread_pool_init(size_t pool_size);

// Finishes all threadpool tasks then destroys threadpool.
void thread_pool_shutdown(thread_pool_t *pool);

// Adds a new task to threadpool.
void thread_pool_add_task(thread_pool_t *pool, void (*function)(void *), void *arg, size_t arg_size);

#endif
