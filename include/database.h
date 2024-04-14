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
#include <atomic>
#include <buffer/manager.h>
#include <concurrency/transaction_manager.h>
#include <config.h>
#include <cstdint>
#include <functional>
#include <index/type.h>
#include <io/execution_callback.h>
#include <shared_mutex>
#include <statistic/system_statistics.h>
#include <storage/manager.h>
#include <storage/page.h>
#include <string>
#include <table/table.h>
#include <table/table_disk_manager.h>
#include <table/tuple.h>
#include <unordered_map>
#include <utility>

namespace beedb
{
class Database
{
  public:
    Database(Config &config, const std::string &file_name);
    ~Database();

    /**
     * Boots the database management system.
     * During the boot, all persisted tables, their schemas and indices
     * will be loaded to memory.
     * All indices will be filled with data from disk.
     */
    void boot();

    /**
     * @return Instance of the TableDiskManager.
     */
    [[nodiscard]] table::TableDiskManager &table_disk_manager()
    {
        return _table_disk_manager;
    }

    /**
     * @return Instance of the BufferManager.
     */
    [[nodiscard]] buffer::Manager &buffer_manager()
    {
        return _buffer_manager;
    }

    /**
     * @return Instance of the transaction manager.
     */
    [[nodiscard]] concurrency::TransactionManager &transaction_manager()
    {
        return _transaction_manager;
    }

    /**
     * @return Immutable instance of the config.
     */
    [[nodiscard]] const Config &config() const
    {
        return _config;
    }

    /**
     * @return Mutable instance of the config.
     */
    [[nodiscard]] Config &config()
    {
        return _config;
    }

    [[nodiscard]] statistic::SystemStatistics &system_statistics()
    {
        return _statistics;
    }

    /**
     * Checks whether a table exists.
     *
     * @param name Name of the table.
     * @return True, if the table exists.
     */
    [[nodiscard]] bool table_exists(const std::string &name)
    {
        std::shared_lock _{_tables_latch};
        return _tables.find(name) != _tables.end();
    }

    /**
     * Returns a pointer to the requested table.
     *
     * @param name Name of the requested table.
     * @return Pointer to the table.
     */
    [[nodiscard]] table::Table *table(const std::string &name)
    {
        std::shared_lock _{_tables_latch};

        if (table_exists(name))
        {
            return _tables[name];
        }

        return nullptr;
    }

    table::Table *operator[](const std::string &table_name)
    {
        std::shared_lock _{_tables_latch};

        return table(table_name);
    }

    /**
     * Creates a table with a given schema.
     * The table will be persisted and available after creation.
     *
     * @param schema Schema for the table.
     */
    void create_table(concurrency::Transaction *transaction, const table::Schema &schema);

    /**
     * Creates an index for a specific column.
     * The index will be persisted, filled, and available after creation.
     *
     * @param column Column to be indexed.
     * @param type Type of the index.
     * @param name Name of the index.
     * @param is_unique True, when the index is a unique index.
     */
    void create_index(concurrency::Transaction *transaction, const table::Column &column, index::Type type,
                      const std::string &name, bool is_unique);

  private:
    enum SystemPageIds : storage::Page::id_t
    {
        Metadata = 0,
        Tables = 1,
        Columns = 2,
        Indices = 3,
        Statistics = 4,
    };

    Config &_config;
    storage::Manager _storage_manager;
    buffer::Manager _buffer_manager;
    table::TableDiskManager _table_disk_manager;
    concurrency::TransactionManager _transaction_manager;

    std::unordered_map<std::string, table::Table *> _tables;
    std::shared_mutex _tables_latch;

    std::atomic_uint32_t _next_table_id = 1;
    std::atomic_uint32_t _next_column_id = 1;
    std::atomic_uint32_t _next_index_id = 1;

    statistic::SystemStatistics _statistics;

    /**
     * Initializes the database. When the database is empty,
     * we will create a new database schema containing all meta tables.
     *
     * @param create_schema True, when a database schema should be created.
     */
    void initialize_database(bool create_schema);

    /**
     * Persists the table statistics.
     *
     * @param table Table
     * @param cardinality Cardinality
     */
    void persist_table_statistics(table::Table *table, std::uint64_t cardinality);
};

} // namespace beedb
