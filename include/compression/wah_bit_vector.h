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

#include <cassert>
#include <cstdint>

namespace beedb::compression
{
class WAHBitVector
{
  public:
    WAHBitVector() : _is_literal_word(0), _content(0)
    {
    }
    ~WAHBitVector() = default;

    inline bool is_fill() const
    {
        return _is_literal_word == 0;
    }
    inline bool is_literal() const
    {
        return _is_literal_word;
    }
    inline void is_fill(bool is_fill)
    {
        _is_literal_word = !is_fill;
    }

    inline bool fill_bit() const
    {
        return _content >> 30;
    }
    inline void fill_bit(const bool fill_bit)
    {
        _content = (fill_bit << 30) | count();
    }

    inline std::uint32_t count() const
    {
        return _content & 0x3FFFFFFF;
    }

    WAHBitVector &operator++()
    {
        _content = (count() + 1) | (fill_bit() << 30);
        return *this;
    }

    WAHBitVector operator++(int)
    {
        WAHBitVector copy(*this);
        ++(*this);
        return copy;
    }

    inline void set(const std::size_t index, const bool bit)
    {
        assert(index >= 0 && index <= 31);
        _content ^= (-bit ^ _content) & (1UL << index);
    }

    inline bool get(const std::size_t index) const
    {
        assert(index >= 0 && index <= 31);
        return (_content >> index) & 1U;
    }

    inline void clear()
    {
        _content = 0;
    }

  private:
    std::uint32_t _is_literal_word : 1, _content : 31;
} __attribute__((packed));
} // namespace beedb::compression
