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
#include "schema.h"
#include "tuple.h"
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace beedb::table
{
/**
 * Stores data in memory.
 * Some times, the data can not stay in the buffer from the BufferManager
 * and has to be copied to "safe" memory that will not be replaced.
 */
class MemoryTable
{
  public:
    explicit MemoryTable(const Schema &schema)
        : _schema(schema), _row_size(schema.row_size() + sizeof(concurrency::Metadata))
    {
        allocate(512u);
    }

    MemoryTable(MemoryTable &&other) noexcept
        : _schema(std::move(other._schema)), _row_size(other._row_size), _capacity(other._capacity), _data(other._data)
    {
        other._data = nullptr;

        _tuples.reserve(other._tuples.size());
        for (auto &&tuple : other._tuples)
        {
            _tuples.emplace_back(Tuple{_schema, std::move(tuple)});
        }
    }

    MemoryTable(const MemoryTable &) = delete;

    ~MemoryTable()
    {
        std::free(_data);
    }

    /**
     * Copy data from disk (or page) to the memory.
     *
     * @param tuple Tuple to be copied to memory.
     * @return Index within the memory table.
     */
    std::size_t add(const Tuple &tuple)
    {
        // Reallocate if needed.
        if (_tuples.size() == _capacity)
        {
            allocate(_capacity << 1);
        }

        // At this location, we will copy the tuples data.
        auto *data = _data + _tuples.size() * _row_size;
        std::memcpy(data, tuple.metadata(), sizeof(concurrency::Metadata));
        std::memcpy(data + sizeof(concurrency::Metadata), tuple.data(), _schema.row_size());

        _tuples.emplace_back(Tuple{_schema,
                                   {storage::Page::MEMORY_TABLE_PAGE_ID, 0u},
                                   reinterpret_cast<concurrency::Metadata *>(data),
                                   data + sizeof(concurrency::Metadata)});
        return _tuples.size() - 1;
    }

    /**
     * @return Constant set of all tuples stored in the memory table.
     */
    [[nodiscard]] const std::vector<Tuple> &tuples() const
    {
        return _tuples;
    }

    /**
     * @return Set of all tuples stored in the memory table.
     */
    std::vector<Tuple> &tuples()
    {
        return _tuples;
    }

    /**
     * @return Schema of this table.
     */
    [[nodiscard]] const Schema &schema() const
    {
        return _schema;
    }

    /**
     * @return True, when the table has no tuples stored.
     */
    [[nodiscard]] bool empty() const
    {
        return _tuples.empty();
    }

    /**
     * @return Number of stored tuples.
     */
    [[nodiscard]] std::size_t size() const
    {
        return _tuples.size();
    }

    /**
     * Grants access to a specific tuple.
     * @param index Index of the tuple.
     * @return Tuple.
     */
    [[nodiscard]] const Tuple &at(const std::size_t index) const
    {
        return _tuples[index];
    }

    const Tuple &operator[](const std::size_t index) const
    {
        return _tuples[index];
    }

    [[nodiscard]] auto begin() const
    {
        return _tuples.begin();
    }
    [[nodiscard]] auto end() const
    {
        return _tuples.end();
    }

  private:
    Schema _schema;
    const std::uint32_t _row_size;
    std::uint64_t _capacity = 0u;
    std::byte *_data = nullptr;
    std::vector<Tuple> _tuples;

    void allocate(const std::uint64_t capacity)
    {
        // Resize capacity.
        const auto old_capacity = _capacity;
        _capacity = capacity;

        // Resize "real" data.
        auto *old_data = _data;
        _data = static_cast<std::byte *>(std::aligned_alloc(64u, capacity * _row_size));

        // Copy old data to new location.
        if (old_data != nullptr)
        {
            std::memmove(_data, old_data, old_capacity * _row_size);
            std::free(old_data);
        }

        // Tell tuples their new location.
        for (auto i = 0u; i < _tuples.size(); ++i)
        {
            _tuples[i].metadata(reinterpret_cast<concurrency::Metadata *>(_data + i * _row_size));
            _tuples[i].data(_data + i * _row_size + sizeof(concurrency::Metadata));
        }
    }
};
} // namespace beedb::table
