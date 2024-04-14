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

#include <boot/execution_callback.h>
#include <buffer/clock_strategy.h>
#include <buffer/lfu_strategy.h>
#include <buffer/lru_k_strategy.h>
#include <buffer/lru_strategy.h>
#include <buffer/random_strategy.h>
#include <buffer/replacement_strategy.h>
#include <cassert>
#include <config.h>
#include <database.h>
#include <index/index_factory.h>
#include <io/executor.h>
#include <plan/physical/builder.h>
#include <sstream>
#include <storage/metadata_page.h>
#include <table/column.h>

using namespace beedb;

Database::Database(Config &config, const std::string &file_name)
    : _config(config), _storage_manager(file_name),
      _buffer_manager(static_cast<std::size_t>(config[Config::k_BufferFrames]), _storage_manager),
      _table_disk_manager(_buffer_manager), _transaction_manager(_buffer_manager)
{
    // Initialize BufferManagerStrategy.
    const auto count_frames = static_cast<std::size_t>(config[Config::k_BufferFrames]);
    auto replacement_strategy = std::unique_ptr<buffer::ReplacementStrategy>{};
    const auto configured_replacement_strategy =
        static_cast<Config::BufferReplacementStrategy>(config[Config::k_BufferReplacementStrategy]);
    switch (configured_replacement_strategy)
    {
    case Config::Random:
        replacement_strategy = std::make_unique<buffer::RandomStrategy>(count_frames);
        break;
    case Config::LFU:
        replacement_strategy = std::make_unique<buffer::LFUStrategy>(count_frames);
        break;
    case Config::LRU:
        replacement_strategy = std::make_unique<buffer::LRUStrategy>(count_frames);
        break;
    case Config::LRU_K:
        replacement_strategy =
            std::make_unique<buffer::LRUKStrategy>(count_frames, static_cast<std::size_t>(config[Config::k_LRU_K]));
        break;
    case Config::Clock:
        replacement_strategy = std::make_unique<buffer::ClockStrategy>(count_frames);
        break;
    }

    this->_buffer_manager.replacement_strategy(std::move(replacement_strategy));
}

Database::~Database()
{
    // Update statistics
    for (auto [_, table] : this->_tables)
    {
        if (table->is_virtual() == false)
        {
            this->persist_table_statistics(table, this->_statistics.table_statistics().cardinality(*table));
        }
    }

    // Write metadata
    auto *metadata_page = reinterpret_cast<storage::MetadataPage *>(this->_buffer_manager.pin(SystemPageIds::Metadata));
    metadata_page->next_transaction_timestamp(this->_transaction_manager.next_timestamp());
    this->_buffer_manager.unpin(metadata_page, true);

    // Delete tables AFTER all statistics are persisted.
    // Otherwise, it is possible to delete the statistics table
    // before all updates are done.
    for (auto [_, table] : this->_tables)
    {
        delete table;
    }
}

void Database::boot()
{
    // Initialize tables with fixed schema and allocate pages for the data,
    // if the file is empty.
    this->initialize_database(this->_storage_manager.count_pages() == 0u);

    // Initialize metadata.
    auto *metadata_page = reinterpret_cast<storage::MetadataPage *>(this->_buffer_manager.pin(SystemPageIds::Metadata));
    const auto next_transaction_timestamp = metadata_page->next_transaction_timestamp();
    this->_buffer_manager.unpin(metadata_page, false);

    // Set from metadata.
    this->_transaction_manager.next_timestamp(next_transaction_timestamp);

    // Read all tables, columns and indices and build tables from the results.
    // After this, we filled Database::_tables map which points from name to physical table.
    auto *boot_transaction = this->transaction_manager().new_transaction();
    auto table_callback = boot::TableExecutionCallback{
        *this, boot_transaction, this->_next_table_id, this->_next_column_id, this->_next_index_id, this->_tables};
    auto tables_executor = io::Executor{*this, boot_transaction};
    tables_executor.execute(io::Query{"select * from system_tables;"}, table_callback);

    // Read all table statistics.
    auto statistic_callback = boot::StatisticExecutionCallback{this->_statistics};
    auto statistics_executor = io::Executor{*this};
    statistics_executor.execute(io::Query{"select * from system_table_statistics"}, statistic_callback);

    // Build and fill all indices.
    auto build_index_executor = io::Executor{*this, boot_transaction};
    for (auto &[table_name, table] : this->_tables)
    {
        for (auto i = 0u; i < table->schema().size(); i++)
        {
            const auto &column = table->schema().column(i);
            const auto &term = table->schema().terms()[i];
            for (auto &index : column.indices())
            {
                auto plan = plan::physical::Builder::build_index_plan(*this, boot_transaction, table_name,
                                                                      term.get<expression::Attribute>().column_name(),
                                                                      index->name());
                build_index_executor.execute(plan);
            }
        }
    }

    this->_transaction_manager.commit(*boot_transaction);
}

void Database::initialize_database(bool create_schema)
{
    if (create_schema)
    {
        // Allocate page for metadata.
        auto *metadata_page =
            reinterpret_cast<storage::MetadataPage *>(this->_buffer_manager.allocate<storage::MetadataPage>());
        assert(metadata_page->id() == SystemPageIds::Metadata);
        metadata_page->next_transaction_timestamp(2u);
        this->_buffer_manager.unpin(metadata_page, true);

        // Allocate page for tables.
        auto *tables_page = this->_buffer_manager.allocate<storage::RecordPage>();
        assert(tables_page->id() == SystemPageIds::Tables);
        this->_buffer_manager.unpin(tables_page, false);

        // Allocate page for columns.
        auto *columns_page = this->_buffer_manager.allocate<storage::RecordPage>();
        assert(columns_page->id() == SystemPageIds::Columns);
        this->_buffer_manager.unpin(columns_page, true);

        // Allocate page for indices.
        auto *indices_page = this->_buffer_manager.allocate<storage::RecordPage>();
        assert(indices_page->id() == SystemPageIds::Indices);
        this->_buffer_manager.unpin(indices_page, true);

        // Allocate page for indices.
        auto *table_statistics_page = this->_buffer_manager.allocate<storage::RecordPage>();
        assert(table_statistics_page->id() == SystemPageIds::Statistics);
        this->_buffer_manager.unpin(table_statistics_page, true);
    }

    auto tables_name = std::string{"system_tables"};
    auto tables_schema = table::Schema{tables_name};
    tables_schema.add(table::Column{table::Type::INT, false},
                      expression::Term::make_attribute(std::string{tables_schema.table_name()}, "id"));
    tables_schema.add(table::Column{{table::Type::CHAR, 48}, false},
                      expression::Term::make_attribute(tables_schema.table_name(), "name"));
    tables_schema.add(table::Column{table::Type::INT, false},
                      expression::Term::make_attribute(tables_schema.table_name(), "page"));
    tables_schema.add(table::Column{table::Type::LONG, false},
                      expression::Term::make_attribute(tables_schema.table_name(), "time_travel_page"));
    auto *tables_table =
        new table::Table(-1, SystemPageIds::Tables, storage::Page::INVALID_PAGE_ID, std::move(tables_schema));

    auto columns_name = std::string{"system_columns"};
    auto columns_schema = table::Schema{columns_name};
    columns_schema.add(table::Column{table::Type::INT, false},
                       expression::Term::make_attribute(columns_schema.table_name(), "id"));
    columns_schema.add(table::Column{table::Type::INT, false},
                       expression::Term::make_attribute(columns_schema.table_name(), "table_id"));
    columns_schema.add(table::Column{table::Type::INT, false},
                       expression::Term::make_attribute(columns_schema.table_name(), "type_id"));
    columns_schema.add(table::Column{table::Type::INT, false},
                       expression::Term::make_attribute(columns_schema.table_name(), "length"));
    columns_schema.add(table::Column{{table::Type::CHAR, 48}, false},
                       expression::Term::make_attribute(columns_schema.table_name(), "name"));
    columns_schema.add(table::Column{table::Type::INT, false},
                       expression::Term::make_attribute(columns_schema.table_name(), "is_nullable"));
    columns_schema.add(table::Column{table::Type::INT, false},
                       expression::Term::make_attribute(columns_schema.table_name(), "is_unique"));
    columns_schema.add(table::Column{table::Type::INT, false},
                       expression::Term::make_attribute(columns_schema.table_name(), "is_primary_key"));
    auto *columns_table =
        new table::Table(-1, SystemPageIds::Columns, storage::Page::INVALID_PAGE_ID, std::move(columns_schema));

    auto indices_name = std::string{"system_indices"};
    auto indices_schema = table::Schema{indices_name};
    indices_schema.add(table::Column{table::Type::INT, false},
                       expression::Term::make_attribute(indices_schema.table_name(), "id"));
    indices_schema.add(table::Column{table::Type::INT, false},
                       expression::Term::make_attribute(indices_schema.table_name(), "column_id"));
    indices_schema.add(table::Column{table::Type::INT, false},
                       expression::Term::make_attribute(indices_schema.table_name(), "type_id"));
    indices_schema.add(table::Column{{table::Type::CHAR, 48}, false},
                       expression::Term::make_attribute(indices_schema.table_name(), "name"));
    indices_schema.add(table::Column{table::Type::INT, false},
                       expression::Term::make_attribute(indices_schema.table_name(), "is_unique"));
    auto *indices_table =
        new table::Table(-1, SystemPageIds::Indices, storage::Page::INVALID_PAGE_ID, std::move(indices_schema));

    std::string table_statistics_name("system_table_statistics");
    table::Schema table_statistics_schema{table_statistics_name};
    table_statistics_schema.add(table::Column{table::Type::INT, false},
                                expression::Term::make_attribute(table_statistics_schema.table_name(), "table_id"));
    table_statistics_schema.add(table::Column{table::Type::LONG, false},
                                expression::Term::make_attribute(table_statistics_schema.table_name(), "cardinality"));
    auto *table_statistics_table = new table::Table(-1, SystemPageIds::Statistics, storage::Page::INVALID_PAGE_ID,
                                                    std::move(table_statistics_schema));

    this->_tables[tables_table->name()] = tables_table;
    this->_tables[columns_table->name()] = columns_table;
    this->_tables[indices_table->name()] = indices_table;
    this->_tables[table_statistics_table->name()] = table_statistics_table;
}

void Database::create_table(concurrency::Transaction *transaction, const table::Schema &schema)
{
    auto *tables_table = this->_tables["system_tables"];
    auto *columns_table = this->_tables["system_columns"];
    auto *table_statistics_table = this->_tables["system_table_statistics"];

    // Persist table.
    auto table_tuple = table::Tuple{tables_table->schema(), tables_table->schema().row_size()};
    auto table_id = table::Table::id_t(this->_next_table_id++);
    auto page = this->_buffer_manager.allocate<storage::RecordPage>();
    auto page_id = std::int32_t(page->id());
    auto time_travel_page_id = std::int64_t(storage::Page::INVALID_PAGE_ID);
    this->_buffer_manager.unpin(page, true);
    table_tuple.set(0, table_id);
    table_tuple.set(1, schema.table_name());
    table_tuple.set(2, page_id);
    table_tuple.set(3, time_travel_page_id);
    const auto table_rid = this->_table_disk_manager.add_row(transaction, *tables_table, std::move(table_tuple));
    transaction->add_to_write_set(concurrency::WriteSetItem{
        tables_table->id(), table_rid,
        static_cast<storage::Page::offset_t>(tables_table->schema().row_size() + sizeof(concurrency::Metadata))});

    // Persist columns
    for (auto i = 0u; i < schema.size(); ++i)
    {
        const auto &column = schema.column(i);
        const auto &term = schema.terms()[i];
        table::Tuple column_tuple(columns_table->schema(), columns_table->schema().row_size());
        column_tuple.set(0, std::int32_t(this->_next_column_id++));
        column_tuple.set(1, table_id);
        column_tuple.set(2, std::int32_t(static_cast<table::Type::Id>(column.type())));
        column_tuple.set(3, std::int32_t(column.type().dynamic_length()));
        column_tuple.set(4, term.get<expression::Attribute>().column_name());
        column_tuple.set(5, std::int32_t(column.is_nullable()));
        column_tuple.set(6, std::int32_t(0));
        column_tuple.set(7, std::int32_t(0));
        const auto column_rid = this->_table_disk_manager.add_row(transaction, *columns_table, std::move(column_tuple));
        transaction->add_to_write_set(concurrency::WriteSetItem{
            columns_table->id(), column_rid,
            static_cast<storage::Page::offset_t>(columns_table->schema().row_size() + sizeof(concurrency::Metadata))});
    }

    table::Tuple table_statistics_tuple(table_statistics_table->schema(), table_statistics_table->schema().row_size());
    table_statistics_tuple.set(0, table_id);
    table_statistics_tuple.set(1, std::int64_t{0});
    const auto statistics_rid =
        this->_table_disk_manager.add_row(transaction, *table_statistics_table, std::move(table_statistics_tuple));
    transaction->add_to_write_set(
        concurrency::WriteSetItem{table_statistics_table->id(), statistics_rid,
                                  static_cast<storage::Page::offset_t>(table_statistics_table->schema().row_size() +
                                                                       sizeof(concurrency::Metadata))});

    {
        std::unique_lock _{this->_tables_latch};
        this->_tables[schema.table_name()] =
            new table::Table(table_id, page_id, storage::Page::INVALID_PAGE_ID, schema);
    }
}

void Database::create_index(concurrency::Transaction *transaction, const table::Column &column, index::Type type,
                            const std::string &name, bool is_unique)
{
    auto *indices_table = this->_tables["system_indices"];
    auto tuple = table::Tuple{indices_table->schema(), indices_table->schema().row_size()};
    tuple.set(0, std::int32_t(this->_next_index_id++));
    tuple.set(1, std::int32_t(column.id()));
    tuple.set(2, std::int32_t(type));
    tuple.set(3, name);
    tuple.set(4, std::int32_t(is_unique));

    const auto record_identifier = this->_table_disk_manager.add_row(transaction, *indices_table, std::move(tuple));
    transaction->add_to_write_set(concurrency::WriteSetItem{
        indices_table->id(), record_identifier,
        static_cast<storage::Page::offset_t>(indices_table->schema().row_size() + sizeof(concurrency::Metadata))});
}

void Database::persist_table_statistics(beedb::table::Table *table, std::uint64_t cardinality)
{
    auto executor = io::Executor{*this};
    executor.execute({"update system_table_statistics set cardinality = " + std::to_string(std::int64_t(cardinality)) +
                      " where table_id = " + std::to_string(table->id()) + ";"});
}
