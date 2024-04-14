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

#include <table/tuple.h>
#include <vector>

namespace beedb::execution
{
/**
 * Buffers tuples; needed by blocking operators.
 */
class TupleBuffer
{
  public:
    TupleBuffer() = default;
    ~TupleBuffer() = default;

    void add(table::Tuple &&tuple)
    {
        _buffer.push_back(std::move(tuple));
    }

    void add(std::vector<table::Tuple> &&tuples)
    {
        std::move(tuples.begin(), tuples.end(), std::back_inserter(_buffer));
    }

    [[nodiscard]] bool empty() const
    {
        return _buffer.empty() || _head > _buffer.size() - 1;
    }

    [[nodiscard]] table::Tuple &&pop()
    {
        return std::move(_buffer[_head++]);
    }

    void clear()
    {
        _buffer.clear();
        _head = 0u;
    }

  private:
    std::vector<table::Tuple> _buffer;
    std::size_t _head = 0u;
};
} // namespace beedb::execution
