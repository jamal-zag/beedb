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

#include <concurrency/transaction_manager.h>
#include <unordered_set>

using namespace beedb::concurrency;

TransactionManager::TransactionManager(buffer::Manager &buffer_manager) : _buffer_manager(buffer_manager)
{
}

TransactionManager::~TransactionManager()
{
    for (auto [_, transaction] : this->_commit_history)
    {
        delete transaction;
    }
}

Transaction *TransactionManager::new_transaction(const IsolationLevel isolation_level)
{
    return new Transaction(isolation_level, timestamp(this->_next_timestamp.fetch_add(1u), false));
}

bool TransactionManager::commit(Transaction &transaction)
{
    const auto commit_time = this->_next_timestamp.fetch_add(1u);
    transaction.commit_timestamp(timestamp{commit_time, true});

    if (this->validate(transaction))
    {
        // Enter write phase.
        for (const auto &write_set_item : transaction.write_set())
        {
            if (write_set_item == WriteSetItem::Inserted)
            {
                const auto record_identifier = write_set_item.in_place_record_identifier();
                auto *page =
                    reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(record_identifier.page_id()));
                const auto &slot = page->slot(record_identifier.slot());
                auto *metadata = reinterpret_cast<Metadata *>((*page)[slot.start()]);
                metadata->begin_timestamp(transaction.commit_timestamp());
                this->_buffer_manager.unpin(page, true);
            }
            else if (write_set_item == WriteSetItem::Updated)
            {
                const auto record_identifier = write_set_item.in_place_record_identifier();
                const auto outdated_record_identifier = write_set_item.old_version_record_identifier();

                // Commit updated record
                {
                    auto *page =
                        reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(record_identifier.page_id()));
                    const auto &slot = page->slot(record_identifier.slot());
                    auto *metadata = reinterpret_cast<Metadata *>((*page)[slot.start()]);
                    metadata->begin_timestamp(transaction.commit_timestamp());
                    this->_buffer_manager.unpin(page, true);
                }

                // Commit old record
                {
                    auto *page = reinterpret_cast<storage::RecordPage *>(
                        this->_buffer_manager.pin(outdated_record_identifier.page_id()));
                    const auto &slot = page->slot(record_identifier.slot());
                    auto *metadata = reinterpret_cast<Metadata *>((*page)[slot.start()]);
                    metadata->end_timestamp(transaction.commit_timestamp());
                    this->_buffer_manager.unpin(page, true);
                }
            }
            else if (write_set_item == WriteSetItem::Deleted)
            {
                const auto record_identifier = write_set_item.in_place_record_identifier();
                auto *page =
                    reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(record_identifier.page_id()));
                const auto &slot = page->slot(record_identifier.slot());
                auto *metadata = reinterpret_cast<Metadata *>((*page)[slot.start()]);
                metadata->end_timestamp(transaction.commit_timestamp());
                this->_buffer_manager.unpin(page, true);
            }
        }

        // Record commit history.
        {
            std::unique_lock _{this->_commit_history_latch};
            this->_commit_history.insert({commit_time, &transaction});
        }

        return true;
    }
    else
    {
        this->abort(transaction);
        return false;
    }
}

void TransactionManager::abort(Transaction &transaction)
{
    for (const auto &write_set_item : transaction.write_set())
    {
        if (write_set_item == WriteSetItem::Inserted)
        {
            const auto record_identifier = write_set_item.in_place_record_identifier();
            auto *page =
                reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(record_identifier.page_id()));
            auto &slot = page->slot(record_identifier.slot());
            slot.is_free(true);
            this->_buffer_manager.unpin(page, true);
        }
        else if (write_set_item == WriteSetItem::Updated)
        {
            auto *time_travel_page = reinterpret_cast<storage::RecordPage *>(
                this->_buffer_manager.pin(write_set_item.old_version_record_identifier().page_id()));
            auto *in_place_page = reinterpret_cast<storage::RecordPage *>(
                this->_buffer_manager.pin(write_set_item.in_place_record_identifier().page_id()));

            auto &time_travel_slot = time_travel_page->slot(write_set_item.old_version_record_identifier().slot());
            const auto &in_place_slot = in_place_page->slot(write_set_item.in_place_record_identifier().slot());

            // Overwrite in place record.
            auto metadata = Metadata{*reinterpret_cast<Metadata *>((*time_travel_page)[time_travel_slot.start()])};
            metadata.end_timestamp(timestamp::make_infinity());
            in_place_page->write(write_set_item.in_place_record_identifier().slot(), &metadata,
                                 (*time_travel_page)[time_travel_slot.start() + sizeof(Metadata)],
                                 write_set_item.written_size());
            this->_buffer_manager.unpin(in_place_page, true);

            // Free slot in time travel space.
            time_travel_slot.is_free(true);
            this->_buffer_manager.unpin(time_travel_page, true);
        }
        else if (write_set_item == WriteSetItem::Deleted)
        {
            const auto record_identifier = write_set_item.in_place_record_identifier();
            auto *page =
                reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(record_identifier.page_id()));
            const auto &slot = page->slot(record_identifier.slot());
            auto *metadata = reinterpret_cast<Metadata *>((*page)[slot.start()]);
            metadata->end_timestamp(timestamp::make_infinity());
            this->_buffer_manager.unpin(page, true);
        }
    }
}

bool TransactionManager::validate(Transaction &transaction)
{
    const auto concurrent_Transactions = this->committed_transactions(transaction.begin_timestamp().time() + 1,
                                                                      transaction.commit_timestamp().time() - 1);

    if (concurrent_Transactions.empty())
    {
        return true;
    }

    if (TransactionManager::validate_write_skew_anomalies(transaction, concurrent_Transactions) == false)
    {
        return false;
    }

    if (this->validate_scan_set(transaction, concurrent_Transactions) == false)
    {
        return false;
    }

    return true;
}

bool TransactionManager::validate_write_skew_anomalies(Transaction &transaction,
                                                       const std::vector<Transaction *> &concurrent_transactions)
{
    std::unordered_set<storage::RecordIdentifier> read_records;
    read_records.reserve(transaction.read_set().size());
    for (const auto &read_set_item : transaction.read_set())
    {
        read_records.insert(read_set_item.in_place_record_identifier());
    }

    for (auto *concurrent_transaction : concurrent_transactions)
    {
        for (const auto &write_set_item : concurrent_transaction->write_set())
        {
            if (read_records.find(write_set_item.in_place_record_identifier()) != read_records.end())
            {
                return false;
            }
        }
    }

    return true;
}

bool TransactionManager::validate_scan_set(Transaction &transaction,
                                           const std::vector<Transaction *> &concurrent_transactions)
{
    std::unordered_map<table::Table::id_t, std::vector<WriteSetItem>> possible_conflict_map;
    for (auto *concurrent_transaction : concurrent_transactions)
    {
        for (const auto &write_set_item : concurrent_transaction->write_set())
        {
            possible_conflict_map[write_set_item.table_id()].push_back(write_set_item);
        }
    }

    for (auto *scan_set_item : transaction.scan_set())
    {
        if (scan_set_item->table().has_value())
        {
            const auto table_id = scan_set_item->table().value().get().id();
            if (possible_conflict_map.find(table_id) != possible_conflict_map.end())
            {
                const auto &possible_conflicts = possible_conflict_map[table_id];
                if (this->validate_scan_set_item(scan_set_item, possible_conflicts) == false)
                {
                    return false;
                }
            }
        }
    }

    return true;
}

bool TransactionManager::validate_scan_set_item(ScanSetItem *scan_set_item, const std::vector<WriteSetItem> &write_set)
{
    for (const auto &write_set_item : write_set)
    {
        const auto record_identifier = write_set_item.in_place_record_identifier();
        auto *page = reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(record_identifier.page_id()));

        // Build tuple
        const auto &slot = page->slot(record_identifier.slot());
        auto *metadata = reinterpret_cast<Metadata *>((*page)[slot.start()]);
        auto *data = (*page)[slot.start() + sizeof(Metadata)];
        auto tuple = table::Tuple(scan_set_item->table().value().get().schema(), record_identifier, metadata, data);

        const auto matches = scan_set_item->predicate() == nullptr || scan_set_item->predicate()->matches(tuple);
        this->_buffer_manager.unpin(page, false);
        if (matches)
        {
            return false;
        }
    }

    return true;
}

std::vector<Transaction *> TransactionManager::committed_transactions(const timestamp::timestamp_t begin,
                                                                      const timestamp::timestamp_t end)
{
    std::vector<Transaction *> transactions;

    if (begin < end)
    {
        transactions.reserve(end - begin);
        std::shared_lock _{this->_commit_history_latch};
        for (auto i = begin; i <= end; ++i)
        {
            if (this->_commit_history.find(i) != this->_commit_history.end())
            {
                transactions.emplace_back(this->_commit_history[i]);
            }
        }
    }

    return transactions;
}