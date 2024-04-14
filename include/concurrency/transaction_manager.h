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
#include "transaction.h"
#include <array>
#include <atomic>
#include <buffer/manager.h>
#include <shared_mutex>
#include <unordered_map>

namespace beedb::concurrency
{
/**
 * The TransactionManager creates new transactions and performs
 * commit and abort of those transactions.
 */
class TransactionManager
{
  public:
    explicit TransactionManager(buffer::Manager &buffer_manager);
    ~TransactionManager();

    /**
     * Creates a new transaction with the given isolation level.
     *
     * @param isolation_level Isolation level.
     * @return New transaction.
     */
    Transaction *new_transaction(const IsolationLevel isolation_level = IsolationLevel::Serializable);

    /**
     * Aborts the given transaction and reverts all
     * writes done by this transaction.
     * @param transaction Transaction to abort.
     */
    void abort(Transaction &transaction);

    /**
     * Commits a transaction. Before writing the data written by the transaction
     * to the global state, the transaction is validated.
     * @param transaction Transaction to commit.
     * @return True, when the commit was successful. Otherwise, the transaction
     *         will be aborted automatically.
     */
    bool commit(Transaction &transaction);

    /**
     * Tests if a given timestamp range is visible for the given transaction.
     * @param transaction Transaction to test.
     * @param begin_timestamp Start timestamp.
     * @param end_timestamp End timestamp.
     * @return True, when the transaction is allowed to see the data living in the given range.
     */
    static bool is_visible(const Transaction &transaction, const timestamp begin_timestamp,
                           const timestamp end_timestamp);

    /**
     * Tests if a given metadata is visible for the given transaction.
     * @param transaction Transaction to test.
     * @param metadata Metadata of a record.
     * @return True, when the transaction is allowed to see the data living in the given range.
     */
    static bool is_visible(const Transaction &transaction, Metadata *const metadata)
    {
        return is_visible(transaction, metadata->begin_timestamp(), metadata->end_timestamp());
    }

    /**
     * @return The next timestamp for transactions.
     */
    timestamp::timestamp_t next_timestamp() const
    {
        return _next_timestamp.load();
    }

    /**
     * Updates the next timestamp for transactions.
     * @param timestamp Next timestamp for the next transaction.
     */
    void next_timestamp(const timestamp::timestamp_t timestamp)
    {
        _next_timestamp.store(timestamp);
    }

  private:
    // Buffer manager to read/write to pages.
    buffer::Manager &_buffer_manager;

    // Timestamp for the next transaction.
    std::atomic<timestamp::timestamp_t> _next_timestamp{2u};

    // Map of Timestamp => Transaction of only committed transactions.
    std::unordered_map<timestamp::timestamp_t, Transaction *> _commit_history;

    // Latch for the history map.
    std::shared_mutex _commit_history_latch;

    /**
     * Validates a transaction to commit.
     * @param transaction Transaction to commit.
     * @return True, when the transaction is valid.
     */
    bool validate(Transaction &transaction);

    /**
     * Validates write skew anomalies. A write skew anomaly can occur,
     * when a transaction T reads a record A and writes a record B,
     * but a concurrent transaction T' writes to A between T reading A and
     * writing B.
     * What the DBMS can not see is how A affects B in T, therefore we have
     * to abort the transaction.
     * @param transaction Transaction to validate.
     * @param concurrent_transactions List of transactions that committed between
     *        the transactions started and committed.
     * @return True, when the transaction is valid.
     */
    static bool validate_write_skew_anomalies(Transaction &transaction,
                                              const std::vector<Transaction *> &concurrent_transactions);

    /**
     * Validates the scan set of a transaction. A transaction T
     * may scan a table while a concurrent transaction inserts/deletes/updates
     * a record that would be part of the scan. Therefore, the scan
     * has to be (re)checked to validate that the scan would be the same
     * at commit time.
     * @param transaction Transaction to validate.
     * @param concurrent_transactions List of concurrent transactions.
     * @return True, when the transaction is valid.
     */
    bool validate_scan_set(Transaction &transaction, const std::vector<Transaction *> &concurrent_transactions);

    /**
     * Validates a single scan of a transaction against the write sets of concurrent transactions.
     * @param scan_set_item Single scan set.
     * @param write_set List of write sets of concurrent transactions.
     * @return True, when the scan set is valid.
     */
    bool validate_scan_set_item(ScanSetItem *scan_set_item, const std::vector<WriteSetItem> &write_set);

    /**
     * Calculates the transactions that have committed between begin and end.
     * @param begin Begin.
     * @param end End.
     * @return List of transactions that committed in the given time range.
     */
    std::vector<Transaction *> committed_transactions(const timestamp::timestamp_t begin,
                                                      const timestamp::timestamp_t end);
};
} // namespace beedb::concurrency