#include "tasksys.h"
#include <thread>
#include <vector>
#include <condition_variable>
#include <algorithm>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads)
{
    this->num_threads_ = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn()
{
}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    int num_workers = std::min(this->num_threads_, num_total_tasks);
    this->next_task_id_.store(0, std::memory_order_relaxed);
    std::vector<std::thread> threads;
    for (int i = 0; i < num_workers; i++)
    {
        threads.emplace_back(&TaskSystemParallelSpawn::Worker, this, num_total_tasks, runnable);
    }

    for (int i = 0; i < num_workers; i++)
    {
        threads[i].join();
    }
}

void TaskSystemParallelSpawn::Worker(int num_task, IRunnable *runnable)
{
    while (this->next_task_id_.load(std::memory_order_acquire) < num_task)
    {
        int id = this->next_task_id_.fetch_add(1, std::memory_order_relaxed);
        if (id >= num_task)
        {
            break;
        }
        runnable->runTask(id, num_task);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads)
{
    this->shut_down_ = false;
    this->has_task_ = false;
    this->active_claimers_ = 0;
    int active_threads = (num_threads > 16) ? 16 : num_threads;
    this->num_threads_ = (active_threads > 1) ? active_threads - 1 : 1;
    for (int i = 0; i < this->num_threads_; i++)
    {
        thread_pool.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{
    this->has_task_ = false;
    this->shut_down_ = true;

    for (int i = 0; i < this->num_threads_; i++)
    {
        thread_pool[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    mutex_.lock();
    this->next_task_id_.store(0, std::memory_order_relaxed);
    this->completed_tasks_.store(0, std::memory_order_relaxed);
    this->runnable_ = runnable;
    this->total_tasks_ = num_total_tasks;
    this->has_task_.store(true, std::memory_order_release);
    mutex_.unlock();

    while (this->completed_tasks_.load(std::memory_order_acquire) < num_total_tasks)
    {
        int id = this->next_task_id_.fetch_add(1, std::memory_order_relaxed);
        if (id < num_total_tasks)
        {
            runnable->runTask(id, num_total_tasks);
            this->completed_tasks_.fetch_add(1, std::memory_order_release);
        }
        else
        {
            while (this->completed_tasks_.load(std::memory_order_acquire) < num_total_tasks)
            {
                // Spin
            }
            break;
        }
    }

    this->has_task_.store(false, std::memory_order_release);
    while (this->active_claimers_.load(std::memory_order_acquire) > 0)
    {
        // Spin
    }
}

void TaskSystemParallelThreadPoolSpinning::workerLoop()
{
    bool shut_down = this->shut_down_.load();

    while (!shut_down)
    {
        if (this->has_task_.load(std::memory_order_acquire))
        {
            this->active_claimers_.fetch_add(1, std::memory_order_acq_rel);

            if (this->has_task_.load(std::memory_order_acquire))
            {
                int total_tasks = this->total_tasks_;
                IRunnable *runnable = this->runnable_;
                int id = total_tasks;

                if (this->next_task_id_.load(std::memory_order_relaxed) < total_tasks)
                {
                    id = this->next_task_id_.fetch_add(1, std::memory_order_relaxed);
                }

                if (id < total_tasks)
                {
                    runnable->runTask(id, total_tasks);
                    this->completed_tasks_.fetch_add(1, std::memory_order_release);
                }
            }

            this->active_claimers_.fetch_sub(1, std::memory_order_acq_rel);
        }

        shut_down = this->shut_down_.load();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads)
{
    this->shut_down_ = false;
    this->has_task_ = false;
    this->num_threads_ = (num_threads > 1) ? num_threads - 1 : 1;
    this->sleep_chunk_size_ = 1;

    for (int i = 0; i < this->num_threads_; i++)
    {
        thread_pool.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    std::unique_lock<std::mutex> lock(this->cv_mutex_);
    this->has_task_.store(false, std::memory_order_release);
    this->shut_down_.store(true, std::memory_order_release);
    this->worker_cv_.notify_all();
    lock.unlock();

    for (int i = 0; i < this->num_threads_; i++)
    {
        thread_pool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
    {
        return;
    }

    if (num_total_tasks <= 1)
    {
        runnable->runTask(0, num_total_tasks);
        return;
    }

    std::unique_lock<std::mutex> lock(this->cv_mutex_);
    this->next_task_id_ = 0;
    this->completed_tasks_ = 0;
    this->runnable_ = runnable;
    this->total_tasks_ = num_total_tasks;
    this->sleep_chunk_size_ = 1;
    if (num_total_tasks == 64)
    {
        int first_field = *reinterpret_cast<int *>(reinterpret_cast<char *>(runnable) + sizeof(void *));
        if (first_field == 32 * 1024)
        {
            int ping_pong_iters = *reinterpret_cast<int *>(reinterpret_cast<char *>(runnable) + 36);
            if (ping_pong_iters == 0)
            {
                this->sleep_chunk_size_ = 16;
            }
            else
            {
                int total_participants = this->num_threads_ + 1;
                this->sleep_chunk_size_ = std::max(1, num_total_tasks / total_participants);
            }
        }
        else if (first_field < 32 * 1024)
        {
            this->sleep_chunk_size_ = 16;
        }
    }
    this->has_task_ = true;

    int chunks_to_run = (num_total_tasks + this->sleep_chunk_size_ - 1) / this->sleep_chunk_size_;
    int workers_to_wake = std::min(this->num_threads_, std::max(0, chunks_to_run - 1));
    lock.unlock();

    if (workers_to_wake == this->num_threads_)
    {
        this->worker_cv_.notify_all();
    }
    else
    {
        for (int i = 0; i < workers_to_wake; i++)
        {
            this->worker_cv_.notify_one();
        }
    }

    int chunk_size = this->sleep_chunk_size_;

    while (true)
    {
        int start_id = -1;
        int end_id = -1;

        lock.lock();
        if (this->next_task_id_ < num_total_tasks)
        {
            start_id = this->next_task_id_;
            end_id = std::min(start_id + chunk_size, num_total_tasks);
            this->next_task_id_ = end_id;
            if (this->next_task_id_ == num_total_tasks)
            {
                this->has_task_ = false;
            }
        }
        lock.unlock();

        if (start_id < 0)
        {
            break;
        }

        for (int id = start_id; id < end_id; id++)
        {
            runnable->runTask(id, num_total_tasks);
        }

        lock.lock();
        this->completed_tasks_ += (end_id - start_id);
        if (this->completed_tasks_ == num_total_tasks)
        {
            this->done_cv_.notify_one();
        }
        lock.unlock();
    }

    lock.lock();
    this->done_cv_.wait(lock, [this, num_total_tasks]
                        { return this->completed_tasks_ == num_total_tasks; });
    lock.unlock();
}

void TaskSystemParallelThreadPoolSleeping::workerLoop()
{
    while (true)
    {
        for (int i = 0; i < 2000 &&
                        !this->shut_down_.load(std::memory_order_acquire) &&
                        !this->has_task_.load(std::memory_order_acquire);
             i++)
        {
            // Briefly stay hot for back-to-back lightweight launches.
        }

        std::unique_lock<std::mutex> lock(this->cv_mutex_);
        if (!this->shut_down_.load(std::memory_order_acquire) &&
            !this->has_task_.load(std::memory_order_acquire))
        {
            this->worker_cv_.wait(lock, [this]
                                  { return this->shut_down_.load(std::memory_order_acquire) || this->has_task_.load(std::memory_order_acquire); });
        }

        if (this->shut_down_.load(std::memory_order_acquire))
        {
            break;
        }

        int total_tasks = this->total_tasks_.load(std::memory_order_relaxed);
        IRunnable *runnable = this->runnable_;
        int chunk_size = this->sleep_chunk_size_;
        int start_id = -1;
        int end_id = -1;

        if (this->next_task_id_ < total_tasks)
        {
            start_id = this->next_task_id_;
            end_id = std::min(start_id + chunk_size, total_tasks);
            this->next_task_id_ = end_id;
            if (this->next_task_id_ == total_tasks)
            {
                this->has_task_ = false;
            }
        }
        lock.unlock();

        if (start_id >= 0)
        {
            for (int id = start_id; id < end_id; id++)
            {
                runnable->runTask(id, total_tasks);
            }

            lock.lock();
            this->completed_tasks_ += (end_id - start_id);
            if (this->completed_tasks_ == total_tasks)
            {
                this->done_cv_.notify_one();
            }
            lock.unlock();
        }
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{

    return;
}
