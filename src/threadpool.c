#include "threadpool.h"

struct n_task{
    void (*task_fun)(void*);
    void* data;

    struct n_task* prev;
    struct n_task* next;
};

struct n_worker{
    pthread_t pid;
    threadPool_t* pool;

    int state;

    struct n_worker* prev;
    struct n_worker* next;
};

typedef struct n_manager{
    struct n_task* tasks;
    struct n_worker* workers;

    pthread_mutex_t mutex;
    pthread_cond_t cond;

} threadPool_t;


static void* task_func(void* arg){
    struct n_worker* worker = (struct n_worker*)arg;

    // 循环从任务队列中取任务，执行任务
    while(1){
        pthread_mutex_lock(&worker->pool->mutex);
        while(worker->pool->tasks == NULL && worker->state == 0){
            pthread_cond_wait(&worker->pool->cond, &worker->pool->mutex);
        }
        if (worker->state == 1){
            pthread_mutex_unlock(&worker->pool->mutex);
            break;
        }

        struct n_task* task = worker->pool->tasks;
        DELETE(task, worker->pool->tasks);

        pthread_mutex_unlock(&worker->pool->mutex);
        // 执行任务
        task->task_fun(task->data);

        free(task->data);
        free(task);
    }

    printf("----thread exit----\n");


    return NULL;
}

int createThreadPool(threadPool_t* pool, int workerNum){
    if (pool == NULL){
        printf("parameter pool is null\n");
        return -1;
    }
    if (workerNum < 1){
        workerNum = 1;
    }

    memset(pool, 0, sizeof(threadPool_t));

    // 初始化pool中的各个成员变量
    pthread_mutex_t blankMutex = PTHREAD_MUTEX_INITIALIZER;
    memcpy(&pool->mutex, &blankMutex, sizeof(pthread_mutex_t));

    pthread_cond_t blankCond = PTHREAD_COND_INITIALIZER;
    memcpy(&pool->cond, &blankCond, sizeof(pthread_cond_t));

    // 创建线程队列
    int i = 0;
    for (i = 0; i < workerNum; i++){
        struct n_worker* worker = (struct n_worker*)malloc(sizeof(struct n_worker));
        if (worker == NULL){
            printf("worker malloc error\n");
            return -2;
        }
        memset(worker, 0, sizeof(struct n_worker));

        pthread_t pid;
        worker->state = 0;
        worker->pool = pool;
        pthread_create(&pid, NULL, task_func, (void*)worker);
        worker->pid = pid;  // 必须放在后面

        if (pool->workers == NULL){
            pool->workers = worker;
        }else{
            INSERT(worker, pool->workers);
        }

    }

    return 0;
}

int destroyPool(threadPool_t* pool){
    if (pool == NULL){
        printf("parameter pool NULL\n");
        return -1;
    }

    // 1. 让所有线程退出
    // A. 让所有线程的退出标志位置为1
    struct n_worker* worker = pool->workers;
    for (worker = pool->workers; worker != NULL; worker = worker->next){
        worker->state = 1;
    }
    // B. 唤醒所有在cond_wait()的线程
    pthread_mutex_lock(&pool->mutex);
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);

    // 2. 等待所有线程退出后再释放资源
    // 必须等所有线程退出后，才能释放资源
    for (struct n_worker* w = pool->workers; w != NULL; w = w->next) {
        if (pthread_join(w->pid, NULL) != 0){
            perror("pthread_join error");
            exit(-1);
        }
    }

    // 3. 释放worker
    worker = pool->workers;
    while(worker != NULL){
        struct n_worker* worker_next = worker->next;
        free(worker);
        worker = worker_next;
    }

    // 4. 释放task
    struct n_task* task = pool->tasks;
    while(task != NULL){
        struct n_task* task_next = task->next;
        free(task->data);
        free(task);
        task = task_next;
    }

    return 0;
}

int pushTask(threadPool_t* pool, struct n_task* task){
    if (pool == NULL || task == NULL){
        printf("parameter error\n");
        return -1;
    }

    pthread_mutex_lock(&pool->mutex);

    if (pool->tasks == NULL){
        pool->tasks = task;
    }else{
        INSERT(task, pool->tasks);
    }
    pthread_cond_signal(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);

    return 0;
}

#define WORKER_NUM 20
#define TASK_NUM 10

#if 0
void test(void* arg){
    int idx = *(int*)arg;
    printf("data: %d\n", idx);
}



int main(){
    printf("hello world\n");
    threadPool_t pool;
    createThreadPool(&pool, WORKER_NUM);

    int i;
    for (i = 0; i < TASK_NUM; i++){

        struct n_task* task = (struct n_task*)malloc(sizeof(struct n_task));
        if (task == NULL) {
            perror("malloc error");
            exit(-1);
        }
        memset(task, 0, sizeof(struct n_task));

        task->task_fun = test;
        task->data = malloc(sizeof(int));
        if (task->data == NULL){
            perror("malloc error");
            exit(-1);
        }
        *(int*)task->data = i;

        pushTask(&pool, task);


    }

    sleep(3);
    destroyPool(&pool);
    sleep(1);
    printf("main end\n");

    return 0;
}
#endif