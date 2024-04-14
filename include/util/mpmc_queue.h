/*------------------------------------------------------------------------------*
 * Architecture & Implementation of DBMS                                        *
 *------------------------------------------------------------------------------*
 * Copyright 2022 Databases and Information Systems Group TU Dortmund           *
 * Visit us at                                                                  *
 *             http://dbis.cs.tu-dortmund.de/cms/en/home/                       *
 *                                                                              *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS      *
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL      *
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR         *
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,        *
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR        *
 * OTHER DEALINGS IN THE SOFTWARE.                                              *
 *                                                                              *
 * Authors:                                                                     *
 *          Maximilian Berens   <maximilian.berens@tu-dortmund.de>              *
 *          Roland Kühn         <roland.kuehn@cs.tu-dortmund.de>                *
 *          Jan Mühlig          <jan.muehlig@tu-dortmund.de>                    *
 *------------------------------------------------------------------------------*
 */

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <utility>

namespace beedb::util
{
/**
 * Author: Jan Mühlig <jan.muelig@tu-dortmund.de>
 * Inspired by http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
 */
template <typename T> class BoundMPMCQueue
{
  public:
    BoundMPMCQueue(const std::uint16_t capacity) noexcept : _capacity(capacity)
    {
        _storage = new (std::aligned_alloc(64, sizeof(std::pair<std::atomic_uint64_t, T>) * capacity))
            std::pair<std::atomic_uint64_t, T>[capacity];
        std::memset(static_cast<void *>(_storage), 0, sizeof(std::pair<std::atomic_uint64_t, T>) * capacity);
        for (auto i = 0u; i < capacity; ++i)
        {
            std::get<0>(_storage[i]).store(i, std::memory_order_relaxed);
        }
    }
    ~BoundMPMCQueue() noexcept
    {
        delete[] _storage;
    }
    BoundMPMCQueue(const BoundMPMCQueue<T> &) = delete;
    BoundMPMCQueue(BoundMPMCQueue<T> &&) = delete;
    BoundMPMCQueue<T> &operator=(const BoundMPMCQueue<T> &) = delete;
    BoundMPMCQueue<T> &operator=(BoundMPMCQueue<T> &&) = delete;
    void push_back(const T &item) noexcept
    {
        while (try_push_back(item) == false)
            ;
    }
    T pop_front() noexcept
    {
        T item;
        while (try_pop_front(item) == false)
            ;
        return item;
    }
    T pop_front_or(const T &default_value) noexcept
    {
        T item;
        if (try_pop_front(item))
        {
            return item;
        }
        else
        {
            return default_value;
        }
    }
    bool try_push_back(const T &item) noexcept
    {
        auto pos = _head.load(std::memory_order_relaxed);
        std::uint64_t slot;
        for (;;)
        {
            slot = pos % _capacity;
            const auto sequence = std::get<0>(_storage[slot]).load(std::memory_order_acquire);
            const auto difference = std::int64_t(sequence) - std::int64_t(pos);
            if (difference == 0)
            {
                if (_head.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                {
                    break;
                }
            }
            else if (difference < 0)
            {
                return false;
            }
            else
            {
                pos = _head.load(std::memory_order_relaxed);
            }
        }
        std::get<1>(_storage[slot]) = item;
        std::get<0>(_storage[slot]).store(pos + 1, std::memory_order_release);
        return true;
    }
    bool try_pop_front(T &return_item) noexcept
    {
        auto pos = _tail.load(std::memory_order_relaxed);
        std::uint64_t slot;
        for (;;)
        {
            slot = pos % _capacity;
            const auto sequence = std::get<0>(_storage[slot]).load(std::memory_order_acquire);
            const auto difference = std::int64_t(sequence) - std::int64_t(pos + 1);
            if (difference == 0)
            {
                if (_tail.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                {
                    break;
                }
            }
            else if (difference < 0)
            {
                return false;
            }
            else
            {
                pos = _tail.load(std::memory_order_relaxed);
            }
        }
        return_item = std::get<1>(_storage[slot]);
        std::get<0>(_storage[slot]).store(pos + _capacity, std::memory_order_release);
        return true;
    }

  private:
    const std::uint32_t _capacity;
    std::pair<std::atomic_uint64_t, T> *_storage;
    alignas(64) std::atomic_uint64_t _head{0u};
    alignas(64) std::atomic_uint64_t _tail{0u};
};
} // namespace beedb::util
