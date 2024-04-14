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
#include <cstdint>
#include <table/table_disk_manager.h>
#include <table/value.h>
#include <utility>
#include <vector>

namespace beedb::execution
{
class DeleteOperator : public UnaryOperator
{
  public:
    DeleteOperator(concurrency::Transaction *transaction, table::Table &table,
                   table::TableDiskManager &table_disk_manager, buffer::Manager &buffer_manager)
        : UnaryOperator(transaction), _table(table), _table_disk_manager(table_disk_manager),
          _buffer_manager(buffer_manager)
    {
    }
    ~DeleteOperator() override = default;

    void open() override;
    util::optional<table::Tuple> next() override;
    void close() override;

    [[nodiscard]] const table::Schema &schema() const override
    {
        return _table.schema();
    }

    [[nodiscard]] bool yields_data() const override
    {
        return false;
    }

  private:
    table::Table &_table;
    table::TableDiskManager _table_disk_manager;
    buffer::Manager &_buffer_manager;
};
} // namespace beedb::execution