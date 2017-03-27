//
//  BlockingQueue.h
//  Parallel Programming
//
//  Created by Vlad on 27/03/2017.
//  Copyright © 2017 Codelovin. All rights reserved.
//

#ifndef BlockingQueue_h
#define BlockingQueue_h

#include <atomic>
#include <condition_variable>
#include <stdexcept>
#include <iostream>
#include <mutex>
#include <queue>
#include <utility>

template <class T, class Container = std::deque<T>>
class BlockingQueue {
public:
    
    explicit BlockingQueue(const size_t& capacity): capacity(capacity), off(false) {}
    
    void Put(T&& element) {
        std::unique_lock<std::mutex> lock(mutex);
        
        producer_cv.wait(lock, [this](){return box.size()!=capacity || off; });
        if (off)
            std::bad_exception("system shutdown");
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

#endif /* BlockingQueue_h */
