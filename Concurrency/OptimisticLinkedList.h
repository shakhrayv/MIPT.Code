#pragma once

#include "arena_allocator.h"

#include <atomic>
#include <limits>

#undef DEBUG
///////////////////////////////////////////////////////////////////////

template <typename T>
struct ElementTraits {
    static T Min() {
        return std::numeric_limits<T>::min();
    }
    static T Max() {
        return std::numeric_limits<T>::max();
    }
};

///////////////////////////////////////////////////////////////////////

class SpinLock {
 public:
    explicit SpinLock() {
        idle.store(true);
    }

    void Lock() {
        while (!idle.exchange(false)) {}
    }

    void Unlock() {
        idle.exchange(true);
    }

    // adapters for BasicLockable concept

    void lock() {
        Lock();
    }

    void unlock() {
        Unlock();
    }
private:
    std::atomic<bool> idle;
};

///////////////////////////////////////////////////////////////////////

template <typename T>
class OptimisticLinkedSet {
 private:
    struct Node {
        const T element_;
        std::atomic<Node*> next_;
        SpinLock lock_{};
        std::atomic<bool> marked_{false};

        Node(const T& element, Node* next = nullptr)
            : element_(element),
              next_(next) {
        }
    };

    struct Edge {
        Node* pred_;
        Node* curr_;

        Edge(Node* pred, Node* curr)
            : pred_(pred),
              curr_(curr) {
        }
        
        void Lock() {
            pred_->lock_.Lock();
            curr_->lock_.Lock();
        }
        
        void Unlock() {
            curr_->lock_.Unlock();
            pred_->lock_.Unlock();
        }
    };

 public:
    explicit OptimisticLinkedSet(ArenaAllocator& allocator)
        : allocator_(allocator), elements_(0) {
        CreateEmptyList();
    }

    bool Insert(const T& element) {
        while (true) {
            Edge search_result = Locate(element);
            search_result.Lock();
            
            // Validating the edge, in case it is corrupted
            if (!Validate(search_result)) {
                search_result.Unlock();
                continue;
            }
            
            // Checking if the element with the same value is already present
            // Then the insertion should fail
            if (search_result.curr_->element_ == element) {
                search_result.Unlock();
                return false;
            }
            
            // Proceeding with adding the element
            elements_.fetch_add(1);
            Node* new_node = allocator_.New<Node>(element, search_result.curr_);
            search_result.pred_->next_.store(new_node);
            search_result.Unlock();
            return true;
        }
    }

    bool Remove(const T& element) {
        while (true) {
            Edge search_result = Locate(element);
            search_result.Lock();
            
            // Validating the edge, in case it is corrupted
            if (!Validate(search_result)) {
                search_result.Unlock();
                continue;
            }
            
            // Checking if the element with the same value is not present
            // Then the removal should fail
            if (search_result.curr_->element_ != element) {
                search_result.Unlock();
                return false;
            }
            
            // (Element removal)
            // The predecessor will now point to the current's successor
            elements_.fetch_sub(1);
            search_result.pred_->next_.store(search_result.curr_->next_.load());
            search_result.Unlock();
            return true;
        }
    }

    bool Contains(const T& element) const {
        Edge search_result = Locate(element);
        Node* candidate = search_result.curr_;
        return !candidate->marked_.load() && candidate->element_ == element;
    }

    size_t Size() const {
        return elements_.load();
    }

 private:
    void CreateEmptyList() {
        head_ = allocator_.New<Node>(ElementTraits<T>::Min());
        head_->next_ = allocator_.New<Node>(ElementTraits<T>::Max());
    }

    Edge Locate(const T& element) const {
        Node* pred = head_;
        Node* curr = head_->next_;
        while (curr->element_ < element) {
            pred = curr;
            curr = curr->next_;
        }
        return Edge({pred, curr});
    }

    bool Validate(const Edge& edge) const {
        // The validation fails if and only if at least one of the nodes is removed
        // or if the predecessor does not point to the successor
        return !edge.pred_->marked_.load() &&
                !edge.curr_->marked_.load() &&
                edge.pred_->next_.load() == edge.curr_;
    }

 private:
    ArenaAllocator& allocator_;
    Node* head_{nullptr};
    
    std::atomic<size_t> elements_;
};

template <typename T> using ConcurrentSet = OptimisticLinkedSet<T>;

///////////////////////////////////////////////////////////////////////

