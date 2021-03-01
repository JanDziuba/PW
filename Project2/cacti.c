#include "cacti.h"
#include "queue.h"
#include "threadpool.h"
#include "dynarray.h"
#include "err.h"

#include <pthread.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>

typedef queue *message_queue_t;

typedef struct actor {
    actor_id_t id;
    role_t *role_ptr;
    void *state;
    message_queue_t message_queue;
    bool alive;
    bool free;
    pthread_cond_t *free_cond_ptr;
} actor_t;

typedef actor_t *actor_dynamic_arr_t;

static _Thread_local actor_id_t actor_id_thread_local;
static thread_pool_t *thread_pool_ptr_g;
static actor_dynamic_arr_t actor_array_g;
static int active_actor_number_g = 0;
static int all_actor_number_g = 0;
static pthread_cond_t all_dead_cond_g;
static pthread_mutex_t mutex_g;

actor_id_t actor_id_self() {
    return actor_id_thread_local;
}

static actor_t create_new_actor(role_t *const role_ptr) {
    if (pthread_mutex_lock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    actor_t actor;
    actor.id = dynarray_length(actor_array_g);
    actor.role_ptr = role_ptr;
    actor.state = NULL;
    actor.message_queue = createQueue(sizeof(message_t));
    actor.alive = true;
    actor.free = true;
    actor.free_cond_ptr = (pthread_cond_t *) malloc(sizeof(pthread_cond_t));
    if (pthread_cond_init(actor.free_cond_ptr, NULL)) {
        fatal(__FILE__, __LINE__);
    }

    dynarray_push(actor_array_g, actor);

    all_actor_number_g++;
    if (all_actor_number_g > CAST_LIMIT) {
        fatal(__FILE__, __LINE__);
    }
    active_actor_number_g++;

    if (pthread_mutex_unlock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    return actor;
}

static void process_spawn(const message_t *message_ptr) {
    role_t *role_ptr = (role_t *) message_ptr->data;
    actor_t actor = create_new_actor(role_ptr);

    message_t hello_msg;
    hello_msg.message_type = MSG_HELLO;
    actor_id_t self_id = actor_id_self();
    hello_msg.nbytes = sizeof(self_id);
    hello_msg.data = (void *) self_id;

    if (send_message(actor.id, hello_msg) != 0) {
        fatal(__FILE__, __LINE__);
    }
}

static void process_godie() {
    if (pthread_mutex_lock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    actor_id_t actor_id = actor_id_self();
    actor_t *actor_ptr = &actor_array_g[actor_id];
    actor_ptr->alive = false;

    if (pthread_mutex_unlock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }
}

static void process_message(const message_t *message_ptr,
                            const role_t *role_ptr,
                            void **state_ptr) {
    if (message_ptr->message_type == MSG_SPAWN) {
        process_spawn(message_ptr);
    } else if (message_ptr->message_type == MSG_GODIE) {
        process_godie();
    } else {
        act_t *func_ptr = &role_ptr->prompts[message_ptr->message_type];
        (*func_ptr)(state_ptr, message_ptr->nbytes, message_ptr->data);
    }
}

static void thread_pool_task(void *arg) {
    actor_id_t actor_id = *(actor_id_t *) arg;
    actor_id_thread_local = actor_id;

    if (pthread_mutex_lock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    // get actor lock
    while (!actor_array_g[actor_id].free) {
        pthread_cond_wait(actor_array_g[actor_id].free_cond_ptr, &mutex_g);
    }
    actor_array_g[actor_id].free = false;

    message_t message;
    dequeue(actor_array_g[actor_id].message_queue, &message);

    const role_t *role_ptr = actor_array_g[actor_id].role_ptr;
    void **state_ptr = &actor_array_g[actor_id].state;

    if (pthread_mutex_unlock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    process_message(&message, role_ptr, state_ptr);

    if (pthread_mutex_lock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    if (getSize(actor_array_g[actor_id].message_queue) == 0
        && !actor_array_g[actor_id].alive) {
        active_actor_number_g--;
        if (active_actor_number_g == 0) {
            if (pthread_cond_broadcast(&all_dead_cond_g)) {
                fatal(__FILE__, __LINE__);
            }
        }
    }

    // release actor lock
    actor_array_g[actor_id].free = true;
    if (pthread_cond_broadcast(actor_array_g[actor_id].free_cond_ptr)) {
        fatal(__FILE__, __LINE__);
    }

    if (pthread_mutex_unlock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }
}

int actor_system_create(actor_id_t *actor_id, role_t *const role_ptr) {
    if (CAST_LIMIT < 1) {
        return -1;
    }

    if (pthread_mutex_init(&mutex_g, NULL)) {
        fatal(__FILE__, __LINE__);
    }

    if (pthread_cond_init(&all_dead_cond_g, NULL)) {
        fatal(__FILE__, __LINE__);
    }

    thread_pool_ptr_g = thread_pool_init(POOL_SIZE);
    actor_array_g = dynarray_create(actor_t);
    actor_t first_actor = create_new_actor(role_ptr);
    *actor_id = first_actor.id;

    message_t hello_msg;
    hello_msg.message_type = MSG_HELLO;
    hello_msg.nbytes = 0;
    hello_msg.data = NULL;

    if (send_message(first_actor.id, hello_msg) != 0) {
        fatal(__FILE__, __LINE__);
    }

    return 0;
}

static void free_actors() {
    for (size_t i = 0; i < dynarray_length(actor_array_g); ++i) {
        actor_t *actor_ptr = &actor_array_g[i];
        if (pthread_cond_destroy(actor_ptr->free_cond_ptr)) {
            fatal(__FILE__, __LINE__);
        }
        free(actor_ptr->free_cond_ptr);
        destroyQueue(actor_ptr->message_queue);
    }

    dynarray_destroy(actor_array_g);
}

void actor_system_join(actor_id_t actor_id) {
    if (all_actor_number_g <= actor_id) {
        return;
    }

    if (pthread_mutex_lock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    while (active_actor_number_g > 0) {
        pthread_cond_wait(&all_dead_cond_g, &mutex_g);
    }

    if (pthread_mutex_unlock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    thread_pool_shutdown(thread_pool_ptr_g);
    free_actors();
    all_actor_number_g = 0;
    if (pthread_mutex_destroy(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    if (pthread_cond_destroy(&all_dead_cond_g)) {
        fatal(__FILE__, __LINE__);
    }
}

int send_message(actor_id_t actor_id, message_t message) {
    if (pthread_mutex_lock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    if (actor_id < 0 || dynarray_length(actor_array_g) <= (size_t) actor_id) {
        if (pthread_mutex_unlock(&mutex_g)) {
            fatal(__FILE__, __LINE__);
        }
        return -2;
    }

    actor_t *actor_ptr = &actor_array_g[actor_id];

    if (!actor_ptr->alive) {
        if (pthread_mutex_unlock(&mutex_g)) {
            fatal(__FILE__, __LINE__);
        }
        return -1;
    }

    size_t queue_size = getSize(actor_ptr->message_queue);

    if (queue_size >= ACTOR_QUEUE_LIMIT) {
        if (pthread_mutex_unlock(&mutex_g)) {
            fatal(__FILE__, __LINE__);
        }
        return -3;
    }

    enqueue(actor_ptr->message_queue, &message);
    thread_pool_add_task(thread_pool_ptr_g,
                         &thread_pool_task,
                         &actor_id,
                         sizeof(actor_id));

    if (pthread_mutex_unlock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    return 0;
}
