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
#include "execution_callback.h"
#include <chrono>
#include <concurrency/transaction.h>
#include <concurrency/transaction_callback.h>
#include <database.h>
#include <plan/physical/plan.h>
#include <string>

namespace beedb::io
{
/**
 * @brief The Query struct contains the query-string and some parameters
 * that influence the execution of a query, e.g. printing the logical plan
 */
struct Query
{
    enum ExplainLevel
    {
        None,
        Plan,
        Graph
    };

    const std::string query_string;
    ExplainLevel explain = None;
};

/**
 * Wrapper for time and performance statistics, measured
 * while query execution.
 */
class ExecutionResult
{
  public:
    explicit ExecutionResult(std::string &&error) : _is_successful(false), _error(std::move(error))
    {
    }

    ExecutionResult(const std::uint64_t count_tuples, const std::chrono::milliseconds build_time,
                    const std::chrono::milliseconds execution_time, const std::size_t evicted_pages)
        : _is_successful(true), _count_tuples(count_tuples), _build_ms(build_time), _execution_ms(execution_time),
          _evicted_pages(evicted_pages)
    {
    }

    ~ExecutionResult() = default;

    [[nodiscard]] std::chrono::milliseconds build_time() const
    {
        return _build_ms;
    }

    [[nodiscard]] std::chrono::milliseconds execution_time() const
    {
        return _execution_ms;
    }

    [[nodiscard]] std::size_t evicted_pages() const
    {
        return _evicted_pages;
    }

    [[nodiscard]] bool is_successful() const
    {
        return _is_successful;
    }

    [[nodiscard]] const std::string &error() const
    {
        return _error;
    }

    [[nodiscard]] std::uint64_t count_tuples() const
    {
        return _count_tuples;
    }

  private:
    const bool _is_successful = false;
    const std::string _error;
    const std::uint64_t _count_tuples = 0u;
    const std::chrono::milliseconds _build_ms{};
    const std::chrono::milliseconds _execution_ms{};
    const std::size_t _evicted_pages = 0u;
};

/**
 * Executes queries and query plans.
 */
class Executor
{
  public:
    explicit Executor(Database &database, concurrency::Transaction *transaction = nullptr)
        : _database(database), _transaction(transaction)
    {
    }

    virtual ~Executor() = default;

    ExecutionResult execute(const Query &query, ExecutionCallback &execution_callback,
                            concurrency::TransactionCallback &transaction_callback);

    ExecutionResult execute(const Query &query, concurrency::TransactionCallback &transaction_callback)
    {
        SilentExecutionCallback execution_callback;
        return execute(query, execution_callback, transaction_callback);
    }

    ExecutionResult execute(const Query &query, ExecutionCallback &execution_callback)
    {
        concurrency::SilentTransactionCallback silent_transaction_callback;
        return execute(query, execution_callback, silent_transaction_callback);
    }

    ExecutionResult execute(const Query &query)
    {
        SilentExecutionCallback silent_execution_callback;
        concurrency::SilentTransactionCallback silent_transaction_callback;
        return execute(query, silent_execution_callback, silent_transaction_callback);
    }

    ExecutionResult execute(plan::physical::Plan &plan, ExecutionCallback &execution_callback);

    ExecutionResult execute(plan::physical::Plan &plan)
    {
        SilentExecutionCallback execution_callback;
        return execute(plan, execution_callback);
    }

  protected:
    Database &_database;
    concurrency::Transaction *_transaction;
};
} // namespace beedb::io
