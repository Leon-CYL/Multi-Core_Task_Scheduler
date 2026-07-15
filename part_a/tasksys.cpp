#include "tasksys.h"
#include <thread>
#include <vector>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    std::vector<std::thread> threads;
    for (int i = 0; i < num_total_tasks; i++) {
        threads.emplace_back(&IRunnable::runTask, runnable, i, num_total_tasks);
    }

    for (int i = 0; i < num_total_tasks; i++) {
        threads[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    this->shut_down_ = false;
    this->has_task_ = false;
    this->active_claimers_ = 0;
    int active_threads = (num_threads > 8) ? 8 : num_threads;
    this->num_threads_ = (active_threads > 1) ? active_threads - 1 : 1;
    for (int i = 0; i < this->num_threads_; i++) {
        thread_pool.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    this->has_task_ = false;
    this->shut_down_ = true;

    for (int i = 0; i < this->num_threads_; i++) {
        thread_pool[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    mutex_.lock();
    this->next_task_id_.store(0, std::memory_order_relaxed);
    this->completed_tasks_.store(0, std::memory_order_relaxed);
    this->runnable_ = runnable;
    this->total_tasks_ = num_total_tasks;
    this->has_task_.store(true, std::memory_order_release);
    mutex_.unlock();

    while (this->completed_tasks_.load(std::memory_order_acquire) < num_total_tasks) {
        int id = this->next_task_id_.fetch_add(1, std::memory_order_relaxed);
        if (id < num_total_tasks) {
            runnable->runTask(id, num_total_tasks);
            this->completed_tasks_.fetch_add(1, std::memory_order_release);
        } else {
            while (this->completed_tasks_.load(std::memory_order_acquire) < num_total_tasks) {
                // Spin
            }
            break;
        }
    }

    this->has_task_.store(false, std::memory_order_release);
    while (this->active_claimers_.load(std::memory_order_acquire) > 0) {
        // Spin
    }
}

void TaskSystemParallelThreadPoolSpinning::workerLoop() {
    bool shut_down = this->shut_down_.load();

    while(!shut_down) {
        if (this->has_task_.load(std::memory_order_acquire)) {
            this->active_claimers_.fetch_add(1, std::memory_order_acq_rel);

            if (this->has_task_.load(std::memory_order_acquire)) {
                int total_tasks = this->total_tasks_;
                IRunnable* runnable = this->runnable_;
                int id = total_tasks;

                if (this->next_task_id_.load(std::memory_order_relaxed) < total_tasks) {
                    id = this->next_task_id_.fetch_add(1, std::memory_order_relaxed);
                }

                if (id < total_tasks) {
                    runnable->runTask(id, total_tasks);
                    this->completed_tasks_.fetch_add(1, std::memory_order_release);
                }
            }

            this->active_claimers_.fetch_sub(1, std::memory_order_acq_rel);
        }

        shut_down = this->shut_down_.load();

    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
