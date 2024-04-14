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

#include "tuple_buffer.h"
#include "unary_operator.h"
#include <buffer/manager.h>
#include <storage/page.h>
#include <table/table.h>
#include <table/table_disk_manager.h>
#include <table/tuple.h>
#include <vector>

namespace beedb::execution
{
/**
 * Scans all pages of a given table and returns all tuples.
 */
class SequentialScanOperator final : public UnaryOperator
{
  public:
    SequentialScanOperator(concurrency::Transaction *transaction, std::uint32_t scan_page_limit, table::Schema &&schema,
                           buffer::Manager &buffer_manager, table::TableDiskManager &table_disk_manager,
                           const table::Table &table);
    ~SequentialScanOperator() override = default;

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
    const table::Table &_table;

    storage::Page::id_t _next_page_id_to_scan = storage::Page::INVALID_PAGE_ID;
    std::vector<storage::Page::id_t> _pinned_pages;

    TupleBuffer _buffer;
};
} // namespace beedb::execution
