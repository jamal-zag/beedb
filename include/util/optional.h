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
#include <array>
#include <cstddef>
#include <cstdint>
#include <new>

namespace beedb::util
{
/**
 * Implementation of an optional data container.
 * Inspired by std::optional (https://en.cppreference.com/w/cpp/utility/optional).
 * In contrast to std::optional, the value is moved on assignment.
 */
template <typename T> class optional
{
  public:
    optional() = default;

    ~optional() noexcept
    {
        if (_is_empty == false)
        {
            pointer()->~T();
        }
    }

    explicit optional(const T &t) : _is_empty(false)
    {
        new (_storage.data()) T(t);
    }

    explicit optional(const T &&) = delete;

    explicit optional(T &t) : _is_empty(false)
    {
        new (_storage.data()) T(t);
    }

    explicit optional(T &&t) : _is_empty(false)
    {
        new (_storage.data()) T(std::move(t));
    }

    optional(const optional<T> &o) : _is_empty(o._is_empty)
    {
        if (o._is_empty == false)
        {
            new (_storage.data()) T(o.value());
        }
    }

    optional(const optional<T> &&) = delete;

    optional(optional<T> &o) : _is_empty(o._is_empty)
    {
        if (o._is_empty == false)
        {
            new (_storage.data()) T(o.value());
        }
    }

    optional(optional<T> &&o) noexcept : _is_empty(o._is_empty)
    {
        if (o._is_empty == false)
        {
            new (_storage.data()) T(std::move(o.value()));
        }
        o._is_empty = true;
    }

    optional<T> &operator=(const T &t)
    {
        if (_is_empty == false)
        {
            pointer()->~T();
        }
        _is_empty = false;
        new (_storage.data()) T(t);
        return *this;
    }

    optional<T> &operator=(T &t)
    {
        if (_is_empty == false)
        {
            pointer()->~T();
        }
        _is_empty = false;
        new (_storage.data()) T(t);
        return *this;
    }

    optional<T> &operator=(T &&t)
    {
        if (_is_empty == false)
        {
            pointer()->~T();
        }
        _is_empty = false;
        new (_storage.data()) T(std::move(t));
        return *this;
    }

    optional<T> &operator=(const optional<T> &o)
    {
        if (_is_empty == false)
        {
            pointer()->~T();
        }
        _is_empty = o._is_empty;
        if (o._is_empty == false)
        {
            new (_storage.data()) T(o.value());
        }
        return *this;
    }

    optional<T> &operator=(optional<T> &&o) noexcept
    {
        if (_is_empty == false)
        {
            pointer()->~T();
        }
        _is_empty = o._is_empty;
        if (o._is_empty == false)
        {
            new (_storage.data()) T(std::move(o.value()));
            o._is_empty = true;
        }
        return *this;
    }

    [[nodiscard]] bool has_value() const noexcept
    {
        return _is_empty == false;
    }
    T &value() noexcept
    {
        return *pointer();
    }
    const T &value() const noexcept
    {
        return *pointer();
    }

    void clear() noexcept
    {
        if (_is_empty == false)
        {
            pointer()->~T();
        }
        _is_empty = true;
    }

    T *operator->() noexcept
    {
        return pointer();
    }
    operator T *() noexcept
    {
        return pointer();
    }
    operator T &() noexcept
    {
        return value();
    }
    operator const T &() const noexcept
    {
        return value();
    }
    bool operator==(const bool b) const noexcept
    {
        return has_value() == b;
    }
    bool operator!=(const bool b) const noexcept
    {
        return has_value() != b;
    }
    bool operator==(const std::nullptr_t) const noexcept
    {
        return has_value() == false;
    }
    bool operator!=(const std::nullptr_t) const noexcept
    {
        return has_value() == true;
    }
    operator bool() const noexcept
    {
        return has_value();
    }

  private:
    std::array<std::byte, sizeof(T)> _storage;
    bool _is_empty = true;

    T *pointer() noexcept
    {
        return reinterpret_cast<T *>(_storage.data());
    }
};

template <typename T> optional<T> make_optional(T &&t)
{
    return optional{std::forward(t)};
}
} // namespace beedb::util