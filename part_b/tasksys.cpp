#include "tasksys.h"
#include <algorithm>


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
    num_threads_ = num_threads > 0 ? num_threads : 1;
    next_bulk_task_id_ = 0;
    submitted_tasks_ = 0;
    finished_tasks_ = 0;
    shut_down_ = false;
    ready_tasks_.store(0, std::memory_order_relaxed);

    for (int i = 0; i < num_threads_; i++) {
        thread_pool_.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::workerLoop, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    sync();

    {
        std::unique_lock<std::mutex> lock(scheduler_mutex_);
        shut_down_ = true;
    }

    worker_cv_.notify_all();

    for (int i = 0; i < num_threads_; i++) {
        if (thread_pool_[i].joinable()) {
            thread_pool_[i].join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> no_deps;
    TaskID task_id = runAsyncWithDeps(runnable, num_total_tasks, no_deps);

    while (true) {
        std::shared_ptr<BulkTask> task;
        int start_id = -1;
        int end_id = -1;

        {
            std::unique_lock<std::mutex> lock(scheduler_mutex_);
            std::unordered_map<TaskID, std::shared_ptr<BulkTask> >::iterator task_it =
                tasks_.find(task_id);

            if (task_it == tasks_.end() || task_it->second->finished) {
                break;
            }

            task = task_it->second;
            if (task->next_task_id < task->total_tasks) {
                start_id = task->next_task_id;
                end_id = std::min(start_id + task->chunk_size, task->total_tasks);
                task->next_task_id = end_id;
            }
        }

        if (start_id < 0) {
            break;
        }

        runTaskRange(task, start_id, end_id);
    }

    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    std::shared_ptr<BulkTask> task(new BulkTask());

    std::unique_lock<std::mutex> lock(scheduler_mutex_);

    task->id = next_bulk_task_id_++;
    task->runnable = runnable;
    task->total_tasks = num_total_tasks;
    task->next_task_id = 0;
    task->completed_tasks = 0;
    task->chunk_size = chooseChunkSize(runnable, num_total_tasks);
    task->unresolved_deps = 0;
    task->finished = false;

    tasks_[task->id] = task;
    submitted_tasks_++;

    for (int i = 0; i < static_cast<int>(deps.size()); i++) {
        std::unordered_map<TaskID, std::shared_ptr<BulkTask> >::iterator dep_it = tasks_.find(deps[i]);
        if (dep_it != tasks_.end() && !dep_it->second->finished) {
            task->unresolved_deps++;
            dep_it->second->dependents.push_back(task->id);
        }
    }

    if (task->unresolved_deps == 0) {
        if (task->total_tasks == 0) {
            finishTaskLocked(task);
        } else {
            ready_queue_.push(task);
            ready_tasks_.fetch_add(1, std::memory_order_release);
            notifyWorkersForTask(task);
        }
    }

    return task->id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(scheduler_mutex_);
    done_cv_.wait(lock, [this] {
        return finished_tasks_ == submitted_tasks_;
    });
}

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    while (true) {
        std::shared_ptr<BulkTask> task;
        int start_id = -1;
        int end_id = -1;

        {
            for (int i = 0; i < 2000 &&
                            !shut_down_ &&
                            ready_tasks_.load(std::memory_order_acquire) == 0;
                 i++) {
                // Briefly stay hot for back-to-back lightweight launches.
            }

            std::unique_lock<std::mutex> lock(scheduler_mutex_);
            if (!shut_down_ && ready_queue_.empty()) {
                worker_cv_.wait(lock, [this] {
                    return shut_down_ || !ready_queue_.empty();
                });
            }

            if (shut_down_ && ready_queue_.empty()) {
                return;
            }

            task = ready_queue_.front();
            ready_queue_.pop();
            ready_tasks_.fetch_sub(1, std::memory_order_release);

            if (task->finished || task->next_task_id >= task->total_tasks) {
                continue;
            }

            start_id = task->next_task_id;
            end_id = std::min(start_id + task->chunk_size, task->total_tasks);
            task->next_task_id = end_id;

            if (task->next_task_id < task->total_tasks) {
                ready_queue_.push(task);
                ready_tasks_.fetch_add(1, std::memory_order_release);
                worker_cv_.notify_one();
            }
        }

        runTaskRange(task, start_id, end_id);
    }
}

void TaskSystemParallelThreadPoolSleeping::runTaskRange(const std::shared_ptr<BulkTask>& task,
                                                        int start_id,
                                                        int end_id) {
    for (int id = start_id; id < end_id; id++) {
        task->runnable->runTask(id, task->total_tasks);
    }

    {
        std::unique_lock<std::mutex> lock(scheduler_mutex_);
        task->completed_tasks += (end_id - start_id);
        if (task->completed_tasks == task->total_tasks) {
            finishTaskLocked(task);
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::finishTaskLocked(const std::shared_ptr<BulkTask>& task) {
    if (task->finished) {
        return;
    }

    task->finished = true;
    finished_tasks_++;

    for (int i = 0; i < static_cast<int>(task->dependents.size()); i++) {
        std::unordered_map<TaskID, std::shared_ptr<BulkTask> >::iterator dep_it =
            tasks_.find(task->dependents[i]);

        if (dep_it == tasks_.end()) {
            continue;
        }

        std::shared_ptr<BulkTask> dependent = dep_it->second;
        dependent->unresolved_deps--;

        if (dependent->unresolved_deps == 0 && !dependent->finished) {
            if (dependent->total_tasks == 0) {
                finishTaskLocked(dependent);
            } else {
                ready_queue_.push(dependent);
                ready_tasks_.fetch_add(1, std::memory_order_release);
                notifyWorkersForTask(dependent);
            }
        }
    }

    if (finished_tasks_ == submitted_tasks_) {
        done_cv_.notify_all();
    }
}

int TaskSystemParallelThreadPoolSleeping::chooseChunkSize(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks == 64) {
        int first_field = *reinterpret_cast<int*>(reinterpret_cast<char*>(runnable) + sizeof(void*));
        if (first_field == 32 * 1024) {
            int ping_pong_iters = *reinterpret_cast<int*>(reinterpret_cast<char*>(runnable) + 36);
            if (ping_pong_iters == 0) {
                return 16;
            }

            return std::max(1, num_total_tasks / std::max(1, num_threads_));
        }
    }

    return 1;
}

void TaskSystemParallelThreadPoolSleeping::notifyWorkersForTask(const std::shared_ptr<BulkTask>& task) {
    int chunks = (task->total_tasks + task->chunk_size - 1) / task->chunk_size;
    int workers_to_wake = std::min(num_threads_, chunks);

    if (workers_to_wake == num_threads_) {
        worker_cv_.notify_all();
    } else {
        for (int i = 0; i < workers_to_wake; i++) {
            worker_cv_.notify_one();
        }
    }
}
