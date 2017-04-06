#pragma once

#include <atomic>
#include <memory>
#include <thread>

///////////////////////////////////////////////////////////////////////

class ArenaAllocator {
public:
    explicit ArenaAllocator(const size_t capacity = 4 * 1024 * 1024)
        : arena_(new unsigned char[capacity])
    {}

    ArenaAllocator(const ArenaAllocator& /* that */) = delete;
    ArenaAllocator(ArenaAllocator&& /* that */) = delete;

    ~ArenaAllocator() {
        delete[] arena_;
    }

    template <typename TObject>
    void* Allocate(const size_t alignment = alignof(TObject)) {
        size_t block_size = sizeof(TObject) + alignment;
        void* addr = arena_ + next_offset_.fetch_add(block_size);
        return std::align(alignment, sizeof(TObject), addr, block_size);
    }

    template <typename TObject, typename... Args>
    TObject* New(Args&&... args) {
        void* addr = Allocate<TObject>();
        new (addr) TObject(std::forward<Args>(args)...);
        return static_cast<TObject*>(addr);
    }

    size_t SpaceUsed() const {
        return next_offset_.load();
    }

private:
    unsigned char* arena_;
    std::atomic<size_t> next_offset_{0};
};

///////////////////////////////////////////////////////////////////////
