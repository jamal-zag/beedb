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
#include <database.h>

namespace beedb::boot
{
class TableExecutionCallback final : public io::ExecutionCallback
{
  public:
    TableExecutionCallback(Database &database, concurrency::Transaction *transaction,
                           std::atomic_uint32_t &next_table_id, std::atomic_uint32_t &next_column_id,
                           std::atomic_uint32_t &next_index_id, std::unordered_map<std::string, table::Table *> &tables)
        : _database(database), _transaction(transaction), _next_table_id(next_table_id),
          _next_column_id(next_column_id), _next_index_id(next_index_id), _tables(tables)
    {
    }

    void on_schema(const table::Schema &) override
    {
    }
    void on_tuple(const table::Tuple &tuple) override;

    void on_plan(const std::unique_ptr<plan::logical::NodeInterface> &) override
    {
    }

  private:
    Database &_database;
    concurrency::Transaction *_transaction;
    std::atomic_uint32_t &_next_table_id;
    std::atomic_uint32_t &_next_column_id;
    std::atomic_uint32_t &_next_index_id;
    std::unordered_map<std::string, table::Table *> &_tables;
};

class ColumnExecutionCallback final : public io::ExecutionCallback
{
  public:
    ColumnExecutionCallback(Database &database, concurrency::Transaction *transaction,
                            std::atomic_uint32_t &next_column_id, std::atomic_uint32_t &next_index_id,
                            table::Schema &schema)
        : _database(database), _transaction(transaction), _next_column_id(next_column_id),
          _next_index_id(next_index_id), _schema(schema)
    {
    }
    ~ColumnExecutionCallback() override = default;

    void on_schema(const table::Schema &) override
    {
    }
    void on_tuple(const table::Tuple &tuple) override;

    void on_plan(const std::unique_ptr<plan::logical::NodeInterface> &) override
    {
    }

  private:
    Database &_database;
    concurrency::Transaction *_transaction;
    std::atomic_uint32_t &_next_column_id;
    std::atomic_uint32_t &_next_index_id;
    table::Schema &_schema;
};

class IndexExecutionCallback final : public io::ExecutionCallback
{
  public:
    IndexExecutionCallback(std::atomic_uint32_t &next_index_id,
                           std::vector<std::shared_ptr<index::IndexInterface>> &indices)
        : _next_index_id(next_index_id), _indices(indices)
    {
    }
    ~IndexExecutionCallback() override = default;

    void on_schema(const table::Schema &) override
    {
    }
    void on_tuple(const table::Tuple &tuple) override;

    void on_plan(const std::unique_ptr<plan::logical::NodeInterface> &) override
    {
    }

  private:
    std::atomic_uint32_t &_next_index_id;
    std::vector<std::shared_ptr<index::IndexInterface>> &_indices;
};

class StatisticExecutionCallback final : public io::ExecutionCallback
{
  public:
    explicit StatisticExecutionCallback(statistic::SystemStatistics &system_statistics)
        : _system_statistics(system_statistics)
    {
    }
    ~StatisticExecutionCallback() override = default;

    void on_schema(const table::Schema &) override
    {
    }
    void on_tuple(const table::Tuple &tuple) override;

    void on_plan(const std::unique_ptr<plan::logical::NodeInterface> &) override
    {
    }

  private:
    statistic::SystemStatistics &_system_statistics;
};
} // namespace beedb::boot