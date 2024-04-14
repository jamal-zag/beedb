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
#include "metadata.h"
#include "timestamp.h"
#include <execution/predicate_matcher.h>
#include <functional>
#include <optional>
#include <storage/record_page.h>
#include <table/table.h>
#include <unordered_set>
#include <vector>

namespace beedb::concurrency
{
/**
 * Isolation levels with more (serializable) or
 * less (read uncommitted) isolation.
 */
enum IsolationLevel : std::uint8_t
{
    Serializable = 0,
    // RepeatableRead = 1,
    // ReadCommitted = 2,
    // ReadUncommitted = 3
};

/**
 * A read set item contains two pointers (in form of record identifiers):
 *  - A pointer to the "original" record placed in the table space
 *  - A pointer to the read version (which may be the same as the "original").
 * The first one is needed to compare read set items with the write set items
 * of other transactions.
 */
class ReadSetItem
{
  public:
    ReadSetItem(const storage::RecordIdentifier in_place_record_identifier,
                const storage::RecordIdentifier read_record_identifier)
        : _in_place_record_identifier(in_place_record_identifier), _read_record_identifier(read_record_identifier)
    {
    }
    ReadSetItem(ReadSetItem &&) = default;
    ~ReadSetItem() = default;

    /**
     * @return Pointer to the record in the table space.
     */
    [[nodiscard]] storage::RecordIdentifier in_place_record_identifier() const
    {
        return _read_record_identifier;
    }

    /**
     * @return Pointer to the really read record (which may
     *         be in table or time travel space).
     */
    [[nodiscard]] storage::RecordIdentifier read_record_identifier() const
    {
        return _read_record_identifier;
    }

  private:
    // Pointer to the record in the table space.
    storage::RecordIdentifier _in_place_record_identifier;

    // Pointer to the really read record.
    storage::RecordIdentifier _read_record_identifier;
};

/**
 * The scan set indicates that a transaction read a set
 * of tuples (select * from ... or aggregation for example).
 * A scan can miss a record that is inserted/updated/deleted
 * by a concurrent transaction. Therefore the scan has to be
 * validated at commit time.
 */
class ScanSetItem
{
  public:
    ScanSetItem() = default;

    explicit ScanSetItem(std::unique_ptr<execution::PredicateMatcherInterface> &&predicate)
        : _predicate(std::move(predicate))
    {
    }

    explicit ScanSetItem(const table::Table &table) : _table(std::make_optional(std::ref(table)))
    {
    }

    ScanSetItem(ScanSetItem &&) = default;

    ~ScanSetItem() = default;

    /**
     * @return Table that was scanned.
     */
    [[nodiscard]] std::optional<std::reference_wrapper<const table::Table>> table() const
    {
        return _table;
    }

    /**
     * Update table that was scanned.
     * @param table Table
     */
    void table(const table::Table &table)
    {
        _table = std::make_optional(std::ref(table));
    }

    /**
     * @return Predicate that was used for scanning.
     */
    [[nodiscard]] const std::unique_ptr<execution::PredicateMatcherInterface> &predicate() const
    {
        return _predicate;
    }

    /**
     * Update predicate that was used for scanning.
     * @param predicate Predicate that was used for scanning.
     */
    void predicate(std::unique_ptr<execution::PredicateMatcherInterface> &&predicate)
    {
        _predicate = std::move(predicate);
    }

  private:
    // Table, needed for re-execution.
    std::optional<std::reference_wrapper<const table::Table>> _table = std::nullopt;

    // Predicates, needed for re-execution.
    std::unique_ptr<execution::PredicateMatcherInterface> _predicate{nullptr};
};

/**
 * A write set item contains two pointers (in form of record identifiers):
 *  - A pointer to the "original" record placed in the table space
 *  - A pointer to the updated/deleted version.
 * Also the type of modification (inserted/updated/removed) is stored in
 * the write set.
 */
class WriteSetItem
{
  public:
    enum ModificationType
    {
        Inserted,
        Updated,
        Deleted
    };

    WriteSetItem(const table::Table::id_t table_id, const storage::RecordIdentifier in_place_record_identifier,
                 const storage::RecordIdentifier old_version_record_identifier,
                 const ModificationType modification_type, const storage::Page::offset_t size_written)
        : _table_id(table_id), _in_place_record_identifier(in_place_record_identifier),
          _old_version_record_identifier(old_version_record_identifier), _type(modification_type),
          _written_size(size_written)
    {
    }

    WriteSetItem(const table::Table::id_t table_id, const storage::RecordIdentifier record_identifier,
                 const storage::Page::offset_t size_written)
        : WriteSetItem(table_id, record_identifier, record_identifier, ModificationType::Inserted, size_written)
    {
    }

    WriteSetItem(WriteSetItem &&) = default;
    WriteSetItem(const WriteSetItem &) = default;

    ~WriteSetItem() = default;

    /**
     * @return The id of the table this write belongs to.
     */
    [[nodiscard]] table::Table::id_t table_id() const
    {
        return _table_id;
    }

    /**
     * @return Pointer to the record that was really written.
     */
    [[nodiscard]] storage::RecordIdentifier in_place_record_identifier() const
    {
        return _in_place_record_identifier;
    }

    /**
     * @return Pointer to the record that was overwritten (for updates
     *         and deletes).
     */
    [[nodiscard]] storage::RecordIdentifier old_version_record_identifier() const
    {
        return _old_version_record_identifier;
    }

    bool operator==(const ModificationType type) const
    {
        return _type == type;
    }

    /**
     * @return Number of bytes written.
     */
    [[nodiscard]] storage::Page::offset_t written_size() const
    {
        return _written_size;
    }

  private:
    // Id of the table, this write belongs to.
    table::Table::id_t _table_id;

    // The record identifier of the record that was updated in place.
    storage::RecordIdentifier _in_place_record_identifier;

    // The record identifier of the record that was versioned.
    storage::RecordIdentifier _old_version_record_identifier;

    // Type of the update.
    ModificationType _type;

    // Size of the written data (inclusively metadata).
    storage::Page::offset_t _written_size;
};

/**
 * The transaction stores the begin and commit timestamp of the transaction,
 * as well as the read and write set.
 */
class Transaction
{
  public:
    explicit Transaction(const IsolationLevel isolation_level, const timestamp begin)
        : _isolation_level(isolation_level), _begin_timestamp(begin), _commit_timestamp(timestamp::make_infinity())
    {
    }

    ~Transaction() = default;

    /**
     * @return Isolation level of this transaction.
     */
    [[maybe_unused]] [[nodiscard]] IsolationLevel isolation_level() const
    {
        return _isolation_level;
    }

    /**
     * Update the commit timestamp of this transaction.
     * @param commit_timestamp Timestamp this transaction commits.
     */
    void commit_timestamp(const timestamp commit_timestamp)
    {
        _commit_timestamp = commit_timestamp;
    }

    /**
     * @return Timestamp this transaction was started.
     */
    [[nodiscard]] timestamp begin_timestamp() const
    {
        return _begin_timestamp;
    }

    /**
     * @return Timestamp this transaction was committed.
     */
    [[nodiscard]] timestamp commit_timestamp() const
    {
        return _commit_timestamp;
    }

    /**
     * Adds an item to the read set.
     * @param read_set_item Item that was read by this transaction.
     */
    void add_to_read_set(ReadSetItem &&read_set_item)
    {
        _read_set.emplace_back(std::move(read_set_item));
    }

    /**
     * Adds an item to the write set.
     * @param write_set_item Item that was written by this transaction.
     */
    void add_to_write_set(WriteSetItem &&write_set_item)
    {
        _write_set.emplace_back(std::move(write_set_item));
    }

    /**
     * Adds an item to the scan set.
     * @param scan_set_item Item that was scanned by this transaction.
     */
    void add_to_scan_set(ScanSetItem *scan_set_item)
    {
        _scan_set.emplace_back(scan_set_item);
    }

    /**
     * @return Read set of this transaction.
     */
    [[nodiscard]] const std::vector<ReadSetItem> &read_set() const
    {
        return _read_set;
    }

    /**
     * @return Write set of this transaction.
     */
    [[nodiscard]] const std::vector<WriteSetItem> &write_set() const
    {
        return _write_set;
    }

    /**
     * @return Scan set of this transaction.
     */
    [[nodiscard]] const std::vector<ScanSetItem *> &scan_set() const
    {
        return _scan_set;
    }

  private:
    // Isolation level of the transaction.
    const IsolationLevel _isolation_level;

    // Timestamp this transaction was created.
    const timestamp _begin_timestamp;

    // Timestamp this transaction committed.
    timestamp _commit_timestamp;

    // Items that were read by this transaction.
    std::vector<ReadSetItem> _read_set;

    // Items that were written by this transaction.
    std::vector<WriteSetItem> _write_set;

    // Items that were scanned by this transaction.
    std::vector<ScanSetItem *> _scan_set;
};
} // namespace beedb::concurrency