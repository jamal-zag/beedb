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
#include "execution_callback.h"
#include <cstddef>
#include <string>
#include <table/schema.h>
#include <table/tuple.h>
#include <vector>

namespace beedb::io
{

/**
 * Results of queries are serialized as follows:
 *  - First 2 bytes: Number of columns
 *  - For every column:
 *      - the type (table::Type)
 *      - the offset (std::size_t)
 *      - the column order index (std::uint16_t)
 *      - the null-terminated name of the column
 *  - After the schema, the bytes of the tuples will serialized.
 */
class QueryResultSerializer
{
  public:
    QueryResultSerializer();
    ~QueryResultSerializer();

    void serialize(const table::Schema &schema);
    void serialize(const table::Tuple &tuple);

    [[nodiscard]] std::byte *data()
    {
        return _data;
    }
    [[nodiscard]] std::size_t size() const
    {
        return _size;
    }

    [[nodiscard]] bool empty() const
    {
        return _size == 0u;
    }

  private:
    std::byte *_data = nullptr;
    std::size_t _size = 0u;
    std::size_t _capacity = 0u;

    void append(const std::byte *data, const std::uint16_t size);
    void prepend(const std::byte *data, const std::uint16_t size);

    void allocate(const std::size_t capacity);
};
} // namespace beedb::io