#pragma once
#include <utility>


template<typename T>
inline void free_and_null(T*& ptr) noexcept {
    if (auto p = std::exchange(ptr, nullptr); p) {
        delete p;
    }
}
