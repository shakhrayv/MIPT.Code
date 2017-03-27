//
//  ThreadPool.h
//  Parallel Programming
//
//  Created by Vlad on 27/03/2017.
//  Copyright © 2017 Codelovin. All rights reserved.
//

#ifndef ThreadPool_h
#define ThreadPool_h

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <utility>
#include <vector>

template <class T, class Container = std::deque<T>>
class BlockingQueue {
public:
    
    explicit BlockingQueue(const size_t& capacity): capacity(capacity), off(false) {}
    
    void Put(T&& element) {
        std::unique_lock<std::mutex> lock(mutex);
        
        producer_cv.wait(lock, [this](){return box.size()!=capacity || off; });
        if (off)
            throw std::bad_exception();
        box.push_back(std::move(element));
        consumer_cv.notify_one();
    }
    
    bool Get(T& result) {
        std::unique_lock<std::mutex> lock(mutex);
        
        consumer_cv.wait(lock, [this](){return box.size() || off; });
        if (off && !box.size())
            return false;
        
        result = std::move(box.front());
        box.pop_front();
        producer_cv.notify_one();
        return true;
    }
    
    void Shutdown() {
        off.store(true);
        consumer_cv.notify_all();
        producer_cv.notify_all();
    }
    
private:
    std::size_t capacity;
    std::atomic_bool off;
    
    Container box;
    
    std::mutex mutex;
    std::condition_variable producer_cv;
    std::condition_variable consumer_cv;
};


template <class T>
class ThreadPool {
public:
    ThreadPool(): ThreadPool(default_num_workers()) {}
    
    explicit ThreadPool(const size_t num_threads): capacity(num_threads), off(false), workers(num_threads), tasks(num_threads) {
        for (auto it = workers.begin(); it != workers.end(); it++)
            *it = std::thread([this](){
                std::packaged_task<T()> task;
                while (tasks.Get(task))
                    task();
            });
    }
    
    std::future<T> Submit(std::function<T()> task) {
        std::packaged_task<T()> current_task(task);
        auto result = current_task.get_future();
        tasks.Put(std::move(current_task));
        return result;
    }
    
    void Shutdown() {
        off.store(true);
        tasks.Shutdown();
        for (auto it = workers.begin(); it != workers.end(); it++)
            it->join();
    }
    
    ~ThreadPool() {
        if (!off)
            Shutdown();
    }
    
private:
    
    std::size_t capacity;
    
    std::atomic_bool off;
    std::vector<std::thread> workers;
    
    BlockingQueue<std::packaged_task<T()>> tasks;
    
    std::size_t default_num_workers() {
        std::size_t cores = std::thread::hardware_concurrency();
        return cores ? cores : 2;
    }
    
};

#endif /* ThreadPool_h */
