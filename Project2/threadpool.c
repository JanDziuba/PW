#include "threadpool.h"
#include "err.h"

#include <pthread.h>
#include <stddef.h>
#include <stdbool.h>

#include "queue.h"
#include "dynarray.h"

typedef struct runnable {
    void (*function)(void *);
    void *arg;
    size_t arg_size;
} runnable_t;

typedef pthread_t *pthread_dynamic_arr_t;
typedef queue *runnable_queue_t;

struct thread_pool {
    pthread_mutex_t thread_pool_mutex;

    pthread_dynamic_arr_t threads;
    pthread_cond_t
        thread_pool_cond; // Condition variable worker threads wait on.

    runnable_queue_t tasks;

    bool shutdown; // Flag set after thread_pool_shutdown.
};

// Function run by threadpool thread.
// Takes tasks from queue and runs them.
static void *thread_pool_thread(void *thread_pool) {
    thread_pool_t *pool = (thread_pool_t *) thread_pool;

    while (true) {
        if (pthread_mutex_lock(&(pool->thread_pool_mutex))) {
            fatal(__FILE__, __LINE__);
        }

        while (getSize(pool->tasks) == 0 && !pool->shutdown) {
            pthread_cond_wait(&(pool->thread_pool_cond),
                              &(pool->thread_pool_mutex));
        }

        if (pool->shutdown && getSize(pool->tasks) == 0) {
            if (pthread_mutex_unlock(&(pool->thread_pool_mutex))) {
                fatal(__FILE__, __LINE__);
            }
            break;
        }

        runnable_t task;
        dequeue(pool->tasks, &task);

        if (pthread_mutex_unlock(&(pool->thread_pool_mutex))) {
            fatal(__FILE__, __LINE__);
        }

        (*(task.function))(task.arg);
        free(task.arg);
    }

    return 0;
}

thread_pool_t *thread_pool_init(size_t num_threads) {
    thread_pool_t *pool = (thread_pool_t *) malloc(sizeof(thread_pool_t));
    if (pool == NULL) {
        fatal(__FILE__, __LINE__);
    }

    if (pthread_mutex_init(&(pool->thread_pool_mutex), NULL)) {
        fatal(__FILE__, __LINE__);
    }

    pool->threads = dynarray_create(pthread_t);

    while (dynarray_length(pool->threads) < num_threads) {
        pthread_t temp_thread;
        dynarray_push(pool->threads, temp_thread);
    }

    if (pthread_cond_init(&(pool->thread_pool_cond), NULL)) {
        fatal(__FILE__, __LINE__);
    }

    pool->tasks = createQueue(sizeof(runnable_t));

    pool->shutdown = false;

    for (size_t i = 0; i < dynarray_length(pool->threads); i++) {
        if (pthread_create(&pool->threads[i],
                           NULL,
                           thread_pool_thread,
                           (void *) pool)) {
            fatal(__FILE__, __LINE__);
        }
    }

    return pool;
}

void thread_pool_shutdown(thread_pool_t *pool) {
    if (pool == NULL) {
        fatal(__FILE__, __LINE__);
    }

    if (pthread_mutex_lock(&(pool->thread_pool_mutex))) {
        fatal(__FILE__, __LINE__);
    }

    pool->shutdown = true;

    if (pthread_cond_broadcast(&(pool->thread_pool_cond))) {
        fatal(__FILE__, __LINE__);
    }

    if (pthread_mutex_unlock(&(pool->thread_pool_mutex))) {
        fatal(__FILE__, __LINE__);
    }

    for (size_t i = 0; i < dynarray_length(pool->threads); i++) {
        (pthread_join((pool->threads[i]), NULL));
    }
    pthread_mutex_destroy(&(pool->thread_pool_mutex));
    dynarray_destroy(pool->threads);
    pthread_cond_destroy(&(pool->thread_pool_cond));
    destroyQueue(pool->tasks);
    free(pool);
}

void thread_pool_add_task(thread_pool_t *pool,
                          void (*function)(void *),
                          void *arg,
                          size_t arg_size) {
    if (pool == NULL) {
        fatal(__FILE__, __LINE__);
    }

    if (pthread_mutex_lock(&(pool->thread_pool_mutex))) {
        fatal(__FILE__, __LINE__);
    }

    if (pool->shutdown) {
        fatal(__FILE__, __LINE__);
    }

    runnable_t runnable;
    runnable.function = function;
    runnable.arg = (void *) malloc(arg_size);
    if (runnable.arg == NULL) {
        fatal(__FILE__, __LINE__);
    }
    memcpy(runnable.arg, arg, arg_size);

    enqueue(pool->tasks, &runnable);

    if (pthread_cond_broadcast(&(pool->thread_pool_cond))) {
        fatal(__FILE__, __LINE__);
    }

    if (pthread_mutex_unlock(&(pool->thread_pool_mutex))) {
        fatal(__FILE__, __LINE__);
    }
}
