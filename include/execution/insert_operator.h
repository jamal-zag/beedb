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

#include "unary_operator.h"
#include <buffer/manager.h>
#include <set>
#include <statistic/system_statistics.h>
#include <table/table.h>
#include <table/table_disk_manager.h>
#include <table/tuple.h>

namespace beedb::execution
{
/**
 * Inserts all tuples provided by the children operator.
 * The child may be a tuple buffer or a subquery.
 */
class InsertOperator final : public UnaryOperator
{
  public:
    InsertOperator(concurrency::Transaction *transaction, buffer::Manager &buffer_manager,
                   table::TableDiskManager &table_disk_manager, statistic::SystemStatistics &statistics,
                   table::Table &table);
    ~InsertOperator() override = default;

    void open() override;
    util::optional<table::Tuple> next() override;
    void close() override;

    [[nodiscard]] const table::Schema &schema() const override
    {
        return _schema;
    }

    [[nodiscard]] bool yields_data() const override
    {
        return false;
    }

  private:
    const table::Schema _schema;
    buffer::Manager &_buffer_manager;
    table::TableDiskManager &_table_disk_manager;
    statistic::SystemStatistics &_statistics;
    table::Table &_table;
    storage::Page::id_t _last_pinned_page = storage::Page::INVALID_PAGE_ID;
};
} // namespace beedb::execution
