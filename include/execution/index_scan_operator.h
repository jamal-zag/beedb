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

#include "operator_interface.h"
#include "tuple_buffer.h"
#include <buffer/manager.h>
#include <index/index_interface.h>
#include <memory>
#include <queue>
#include <storage/page.h>
#include <table/table.h>
#include <table/table_disk_manager.h>
#include <table/tuple.h>
#include <unordered_set>
#include <vector>

namespace beedb::execution
{
/**
 * Key (range) that have to be looked up in the.
 * May be a range or a single key.
 */
class KeyRange
{
  public:
    explicit KeyRange(const std::int64_t single_key) : _from(single_key), _to(std::numeric_limits<std::int64_t>::max())
    {
    }

    KeyRange(const std::int64_t from, const std::int64_t to) : _from(from), _to(to)
    {
    }

    ~KeyRange() = default;

    [[nodiscard]] bool is_single_key() const
    {
        return _to == std::numeric_limits<std::int64_t>::max();
    }
    [[nodiscard]] std::int64_t single_key() const
    {
        return _from;
    }
    [[nodiscard]] std::int64_t from() const
    {
        return _from;
    }
    [[nodiscard]] std::int64_t to() const
    {
        return _to;
    }

    bool operator<(const KeyRange &other) const
    {
        return _from < other._from;
    }

    bool operator==(const KeyRange &other) const
    {
        return _from == other._from && _to == other._to;
    }

  private:
    const std::int64_t _from;
    const std::int64_t _to;
};
} // namespace beedb::execution

namespace std
{
template <> struct hash<beedb::execution::KeyRange>
{
  public:
    std::size_t operator()(const beedb::execution::KeyRange &range) const
    {
        return std::hash<std::int64_t>()(range.from()) ^ std::hash<std::int64_t>()(range.to());
    }
};
} // namespace std

namespace beedb::execution
{

/**
 * Takes an index and keys to be looked up in the index
 * and scans only over pages found in the index instead
 * of scanning all pages from the table.
 */
class IndexScanOperator final : public OperatorInterface
{
  public:
    IndexScanOperator(concurrency::Transaction *transaction, const std::uint32_t scan_page_limit,
                      const table::Schema &schema, buffer::Manager &buffer_manager,
                      table::TableDiskManager &table_disk_manager, std::unordered_set<KeyRange> &&key_ranges,
                      std::shared_ptr<index::IndexInterface> index);
    ~IndexScanOperator() override = default;

    void open() override;
    util::optional<table::Tuple> next() override;
    void close() override;

    [[nodiscard]] const table::Schema &schema() const override
    {
        return _schema;
    }

  private:
    const std::uint32_t _scan_page_limit;
    const table::Schema _schema;
    buffer::Manager &_buffer_manager;
    table::TableDiskManager &_table_disk_manager;
    std::unordered_set<KeyRange> _key_ranges;
    std::shared_ptr<index::IndexInterface> _index;

    std::queue<storage::Page::id_t> _pages_to_scan;
    std::vector<storage::Page::id_t> _pinned_pages;

    TupleBuffer _buffer;
};
} // namespace beedb::execution
