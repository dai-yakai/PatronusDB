#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>

#define INSERT(item, head)do{               \
    (item)->next = (head);                  \
    (item)->prev = NULL;                    \
    (head)->prev = (item);                  \
    (head) = (item);                        \
}while(0);                                  \

#define DELETE(item, head)do{               \
    if ((item)->next != NULL)   (item)->next->prev = (item)->prev;       \
    if ((item)->prev != NULL)   (item)->prev->next = (item)->next;       \
    if ((item) == (head))   (head) = (item)->next;                       \
    (item)->next = NULL;                    \
    (item)->prev = NULL;                    \
}while(0);

struct n_task;
struct n_worker;
typedef struct n_manager threadPool_t;

static void* task_func(void* arg);
int createThreadPool(threadPool_t* pool, int workerNum);
int destroyPool(threadPool_t* pool);
int pushTask(threadPool_t* pool, struct n_task* task);