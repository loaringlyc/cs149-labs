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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads), num_total_tasks_(0), running_(true), num_finished_(0), task_idx_(0) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for(int i = 0; i < num_threads_; ++i) {
        workers_.emplace_back([this] {
            while(running_){
                std::function<void()> func;
                TaskID parent_tid = -1; // 被依赖项，需要先做完parent再做child
                bool pop_bulk = false;
                {
                    std::unique_lock<std::mutex> lk(ready_mtx_);
                    consumer_.wait(lk, [=]{return !running_ || !ready_queue_.empty();});
                    if(!ready_queue_.empty()) {
                        parent_tid = ready_queue_.front();
                        std::shared_ptr<Bulk> curr_bulk;
                        {
                            std::lock_guard<std::mutex> lk(map_mtx_);
                            curr_bulk = id2task_[parent_tid];  // 得到ptr的拷贝
                            int curr_func_id = curr_bulk->next_idx_++;
                            func = [=] { curr_bulk->runnable_->runTask(curr_func_id, num_total_tasks_); };
                        }
                        if(curr_bulk->next_idx_ == num_total_tasks_) { // 需要处理的都没有了
                            ready_queue_.pop();
                            pop_bulk = true;
                        }
                    }
                }
                if(func) {
                    func();
                    num_finished_++;
                    sync_cv_.notify_one();
                    if(pop_bulk) { 
                        std::lock_guard<std::mutex> lk(deps_mtx_);
                        // deps_中搜索依赖项，进行删除
                        for (auto it = deps_.begin(); it != deps_.end(); ) { 
                            TaskID child_tid = it->first; 
                            std::vector<TaskID>& temp_deps = it->second; // 元素引用
                            auto id_it = std::find(temp_deps.begin(), temp_deps.end(), parent_tid);
                            if(id_it != temp_deps.end()) {
                                temp_deps.erase(id_it);
                            } 
                            // std::cout << ready_queue_.front() << std::endl;
                            if(temp_deps.empty()) { // 删除deps_的空依赖
                                it = deps_.erase(it);
                                { // 如果有空余的，加入ready_queue_
                                    std::lock_guard<std::mutex> lk_inner(ready_mtx_);
                                    ready_queue_.push(child_tid);
                                }
                                consumer_.notify_all(); // 一个Task会有多个func
                            } else {
                                ++it;
                            }
                        }
                    }

                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    running_ = false;
    consumer_.notify_all();
    for (auto& t: workers_) t.join();
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
    num_total_tasks_ = num_total_tasks; // 一个bulk的task个数

    int this_tid = task_idx_++;
    if(!deps.empty()){
        std::lock_guard<std::mutex> lk(deps_mtx_);
        deps_[this_tid] = deps;  // 做一个拷贝
    }else{
        // std::lock_guard<std::mutex> lk(nodep_mtx_); //没必要因为test调用时候不是多线程的
        nodep_tasks_.emplace_back(this_tid);
    }
    std::function<void()> func = [=]{
        runnable->runTask(this_tid, num_total_tasks);
    };
    {
        std::lock_guard<std::mutex> lk(map_mtx_);
        std::shared_ptr<Bulk> bulk = std::make_shared<Bulk>(this_tid, runnable);
        id2task_[this_tid] = bulk;
    }

    // for (const auto& pair : deps_) {
    //     std::cout << "Key: " << pair.first << " -> [";
    //     for (size_t i = 0; i < pair.second.size(); ++i) {
    //         std::cout << pair.second[i];
    //         if (i + 1 < pair.second.size()) std::cout << ", ";
    //     }
    //     std::cout << "]" << std::endl;
    // }

    // for (auto it = nodep_tasks_.begin(); it != nodep_tasks_.end(); ++it) 
    //     std::cout << "nodep: " << *it << std::endl;

    return this_tid;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    {
        std::lock_guard<std::mutex> lk(ready_mtx_);
        for(size_t i = 0; i < nodep_tasks_.size(); ++i)
            ready_queue_.emplace(nodep_tasks_[i]);
    }
    consumer_.notify_all();
    {
        std::unique_lock<std::mutex> lk(ready_mtx_);
        sync_cv_.wait(lk, [=] { return num_finished_ >= task_idx_; });
    }
    return;
}
