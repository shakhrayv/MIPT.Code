//
//  StripedHashSet.h
//  Parallel Programming
//
//  Created by Vlad on 04/04/2017.
//  Copyright © 2017 Codelovin. All rights reserved.
//

#ifndef StripedHashSet_h
#define StripedHashSet_h

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <forward_list>
#include <functional>
#include <vector>

class ReadWriteLock {
public:
    ReadWriteLock() {
        writers_.store(0);
        readers_.store(0);
        write_state.store(false);
    }
    
    void ReadLock() {
        std::unique_lock<std::mutex> lock(mutex_);
        read_cv_.wait(lock, [this](){ return !writers_.load(); });
        readers_.fetch_add(1);
    }
    
    void ReadUnlock() {
        std::unique_lock<std::mutex> lock(mutex_);
        readers_.fetch_sub(1);
        if (readers_.load() == 0)
            write_cv_.notify_one();
    }
    
    void WriteLock() {
        std::unique_lock<std::mutex> lock(mutex_);
        writers_.fetch_add(1);
        write_cv_.wait(lock, [this](){ return readers_.load() == 0 && !write_state; });
        write_state.store(true);
    }
    
    void WriteUnlock() {
        std::unique_lock<std::mutex> lock(mutex_);
        writers_.fetch_sub(1);
        write_state.store(false);
        write_cv_.notify_one();
        read_cv_.notify_all();
    }
    
private:
    std::atomic<size_t> writers_;
    std::atomic<size_t> readers_;
    std::atomic<bool> write_state;
    
    std::condition_variable read_cv_;
    std::condition_variable write_cv_;
    
    std::mutex mutex_;
};

template <typename T, class Hash = std::hash<T>>
class StripedHashSet {
public:
    explicit StripedHashSet(const size_t concurrency_level,
                            const size_t growth_factor = 2,
                            const double load_factor = 1.25):
    growth_factor_(growth_factor),
    max_load_factor_(load_factor),
    num_stripes_(concurrency_level),
    locks_(concurrency_level) {
        
        num_elements_.store(0);
        hash_table_.resize(concurrency_level);
    }
    
    bool Insert(const T& element) {
        const size_t hash_value = hash(element);
        
        std::size_t stripe_index = GetStripeIndex(hash_value);
        locks_[stripe_index].WriteLock();
        
        std::size_t bucket_index = GetBucketIndex(hash_value);
        auto it = std::find(hash_table_[bucket_index].begin(), hash_table_[bucket_index].end(), element);
        
        if (it != hash_table_[bucket_index].end()) {
            locks_[stripe_index].WriteUnlock();
            return false;
        }
        if (GetLoadFactor() > max_load_factor_) {
            locks_[stripe_index].WriteUnlock();
            Rehash();
            return Insert(element);
        } else {
            hash_table_[bucket_index].push_front(element);
            num_elements_.fetch_add(1);
            locks_[stripe_index].WriteUnlock();
            return true;
        }
    }
    
    bool Remove(const T& element) {
        const size_t hash_value = hash(element);
        
        std::size_t stripe_index = GetStripeIndex(hash_value);
        locks_[stripe_index].WriteLock();
        
        std::size_t bucket_index = GetBucketIndex(hash_value);
        auto it = std::find(hash_table_[bucket_index].begin(), hash_table_[bucket_index].end(), element);
        
        if (it == hash_table_[bucket_index].end()) {
            locks_[stripe_index].WriteUnlock();
            return false;
        }
        
        hash_table_[bucket_index].remove(*it);
        num_elements_.fetch_sub(1);
        locks_[stripe_index].WriteUnlock();
        
        return true;
    }
    
    bool Contains(const T& element) {
        const size_t hash_value = hash(element);
        
        std::size_t stripe_index = GetStripeIndex(hash_value);
        locks_[stripe_index].ReadLock();
        
        std::size_t bucket_index = GetBucketIndex(hash_value);
        bool found = std::find(hash_table_[bucket_index].begin(), hash_table_[bucket_index].end(), element) != hash_table_[bucket_index].end();
        locks_[stripe_index].ReadUnlock();
        
        return found;
    }
    
    size_t Size() {
        return num_elements_.load();
    }
    
private:
    
    void Rehash() {
        
        for (std::size_t i = 0; i < num_stripes_; i++) {
            locks_[i].WriteLock();
        }
        
        if (GetLoadFactor() <= max_load_factor_) {
            for (std::size_t i = 0; i < num_stripes_; i++) {
                locks_[i].WriteUnlock();
            }
            return;
        }
        
        const size_t new_size = hash_table_.size() * growth_factor_;
        std::vector<std::forward_list<T>> temp(new_size);
        for (auto const &bucket : hash_table_) {
            for (auto const &item : bucket) {
                const size_t hash_value = hash(item);
                const size_t bucket_index = hash_value % new_size;
                temp[bucket_index].push_front(std::move(item));
            }
        }
        hash_table_ = std::move(temp);
        
        for (std::size_t i = 0; i < num_stripes_; i++) {
            locks_[i].WriteUnlock();
        }
    }
    
    std::size_t GetLoadFactor() const {
        return static_cast<double>(num_elements_.load()) / hash_table_.size();
    }
    
    std::size_t GetBucketIndex(const std::size_t element_hash_value) const {
        return element_hash_value % hash_table_.size();
    }
    
    std::size_t GetStripeIndex(const std::size_t element_hash_value) const {
        return element_hash_value % num_stripes_;
    }
    
    std::size_t growth_factor_;
    std::size_t max_load_factor_;
    
    std::atomic<size_t> num_elements_;
    const std::size_t num_stripes_;
    
    std::vector<ReadWriteLock> locks_;
    std::vector<std::forward_list<T>> hash_table_;
    
    Hash hash;
};

template <typename T> using ConcurrentSet = StripedHashSet<T>;

#endif /* StripedHashSet_h */
