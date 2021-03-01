#include <stdbool.h>
#include <pthread.h>
#include <errno.h>
#include <stdio.h>

#include "cacti.h"
#include "dynarray.h"
#include "err.h"

#define ignore (void)

#define MSG_HELLO_ANSWER (message_type_t)1
#define MSG_FACTORIAL (message_type_t)2
#define MSG_RECURSIVE_GODIE (message_type_t)3

typedef struct factorial {
    unsigned long long k_factorial;
    unsigned long k;
    unsigned long n;
} factorial_t;

typedef struct actor_state {
    bool has_father;
    bool has_son;
    actor_id_t father_id;
    actor_id_t son_id;
    factorial_t factorial;
} actor_state_t;

typedef actor_state_t *actor_state_dynamic_arr_t;

void first_process_hello(void **stateptr, size_t nbytes, void *data);
void process_hello(void **stateptr, size_t nbytes, void *data);
void process_hello_answer(void **stateptr, size_t nbytes, void *data);
void process_factorial(void **stateptr, size_t nbytes, void *data);
void process_recursive_godie(void **stateptr, size_t nbytes, void *data);

static pthread_mutex_t mutex_g;
// actor state is an index to this array;
static actor_state_dynamic_arr_t actor_state_array_g;
static unsigned long n_g;
static act_t first_prompts_g[] = {
    first_process_hello,
    process_hello_answer,
    process_factorial,
    process_recursive_godie
};
static role_t first_role_g = {
    .nprompts = 4,
    .prompts = first_prompts_g
};
static act_t normal_prompts_g[] = {
    process_hello,
    process_hello_answer,
    process_factorial,
    process_recursive_godie
};
static role_t normal_role_g = {
    .nprompts = 4,
    .prompts = normal_prompts_g
};

message_t make_spawn_msg(role_t *role_ptr) {
    message_t message;
    message.message_type = MSG_SPAWN;
    message.data = (void *) role_ptr;
    message.nbytes = sizeof(role_t);
    return message;
}

message_t make_hello_answer_msg(actor_id_t id) {
    message_t message;
    message.message_type = MSG_HELLO_ANSWER;
    message.data = (void *) id;
    return message;
}

message_t make_factorial_msg(const factorial_t *factorial_ptr) {
    message_t message;
    message.message_type = MSG_FACTORIAL;
    size_t factorial_size = sizeof(factorial_t);

    message.data = (void *) malloc(factorial_size);
    memcpy(message.data, factorial_ptr, factorial_size);

    message.nbytes = factorial_size;
    return message;
}

message_t make_recursive_godie_msg() {
    message_t message;
    message.message_type = MSG_RECURSIVE_GODIE;
    message.data = NULL;
    message.nbytes = 0;
    return message;
}

void first_process_hello(void **stateptr, size_t nbytes, void *data) {
    ignore nbytes;
    ignore data;
    actor_state_t actor_state;
    actor_state.has_father = false;
    actor_state.has_son = false;
    actor_state.factorial.k = 0;
    actor_state.factorial.k_factorial = 1;

    if (pthread_mutex_lock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }
    actor_state.factorial.n = n_g;

    size_t state_id = dynarray_length(actor_state_array_g);
    dynarray_push(actor_state_array_g, actor_state);
    message_t message = make_spawn_msg(&normal_role_g);
    if (pthread_mutex_unlock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    *stateptr = (void *) state_id;
    send_message(actor_id_self(), message);
}

void process_hello(void **stateptr, size_t nbytes, void *data) {
    ignore nbytes;
    actor_state_t actor_state;
    actor_state.has_father = true;
    actor_state.father_id = (actor_id_t) data;
    actor_state.has_son = false;

    if (pthread_mutex_lock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }
    size_t state_id = dynarray_length(actor_state_array_g);
    dynarray_push(actor_state_array_g, actor_state);
    if (pthread_mutex_unlock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    *stateptr = (void *) state_id;
    message_t message = make_hello_answer_msg(actor_id_self());
    send_message(actor_state.father_id, message);
}

void process_hello_answer(void **stateptr, size_t nbytes, void *data) {
    ignore nbytes;

    size_t state_id = (size_t) *stateptr;

    if (pthread_mutex_lock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }
    actor_state_array_g[state_id].has_son = true;
    actor_id_t son_id = (actor_id_t) data;
    actor_state_array_g[state_id].son_id = son_id;
    message_t
        message = make_factorial_msg(&actor_state_array_g[state_id].factorial);
    if (pthread_mutex_unlock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }
    send_message(son_id, message);
}

void process_factorial(void **stateptr, size_t nbytes, void *data) {
    ignore nbytes;

    size_t state_id = (size_t) *stateptr;

    if (pthread_mutex_lock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }
    factorial_t *factorial_ptr = (factorial_t *) data;
    factorial_t factorial = *factorial_ptr;
    if (factorial.k == factorial.n) {
        printf("%llu", factorial.k_factorial);
        message_t message = make_recursive_godie_msg();
        if (pthread_mutex_unlock(&mutex_g)) {
            fatal(__FILE__, __LINE__);
        }
        send_message(actor_id_self(), message);
    } else {
        factorial.k++;
        factorial.k_factorial *= factorial.k;
        actor_state_array_g[state_id].factorial.k_factorial =
            factorial.k_factorial;
        actor_state_array_g[state_id].factorial.k = factorial.k;
        actor_state_array_g[state_id].factorial.n = factorial.n;
        message_t message = make_spawn_msg(&normal_role_g);
        if (pthread_mutex_unlock(&mutex_g)) {
            fatal(__FILE__, __LINE__);
        }
        send_message(actor_id_self(), message);
    }

    free(factorial_ptr);
}

void process_recursive_godie(void **stateptr, size_t nbytes, void *data) {
    ignore nbytes;
    ignore data;

    size_t state_id = (size_t) *stateptr;

    if (pthread_mutex_lock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }
    if (actor_state_array_g[state_id].has_father) {
        actor_id_t father_id = actor_state_array_g[state_id].father_id;
        message_t recursive_godie_msg = make_recursive_godie_msg();
        send_message(father_id, recursive_godie_msg);
    }
    if (pthread_mutex_unlock(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }

    message_t godie_msg;
    godie_msg.message_type = MSG_GODIE;
    send_message(actor_id_self(), godie_msg);
}

unsigned long parse_unsigned_long(const char *str) {
    errno = 0;
    char *temp;

    if (*str == '-') {
        fatal(__FILE__, __LINE__);
    }

    unsigned long val = strtoul(str, &temp, 0);

    if (temp == str || *temp != '\0' || errno != 0) {
        fatal(__FILE__, __LINE__);
    }
    return val;
}

void global_init() {
    if (pthread_mutex_init(&mutex_g, NULL)) {
        fatal(__FILE__, __LINE__);
    }
    actor_state_array_g = dynarray_create(actor_state_t);
    char word[256];
    fgets(word, sizeof(word), stdin);
    size_t word_length = strlen(word);
    if(word[word_length - 1] == '\n') {
        word[word_length - 1] = '\0';
    }
    n_g = parse_unsigned_long(word);
}

void global_destroy() {
    if (pthread_mutex_destroy(&mutex_g)) {
        fatal(__FILE__, __LINE__);
    }
    dynarray_destroy(actor_state_array_g);
}

int main() {
    global_init();

    actor_id_t first_actor_id;
    if (actor_system_create(&first_actor_id, &first_role_g)) {
        fatal(__FILE__, __LINE__);
    }

    actor_system_join(first_actor_id);

    global_destroy();
    return 0;
}

