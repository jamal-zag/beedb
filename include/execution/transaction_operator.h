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
#include "operator_interface.h"
#include <concurrency/transaction.h>
#include <concurrency/transaction_callback.h>
#include <concurrency/transaction_manager.h>

namespace beedb::execution
{
class BeginTransactionOperator : public OperatorInterface
{
  public:
    BeginTransactionOperator(concurrency::TransactionManager &transaction_manager,
                             concurrency::TransactionCallback &transaction_callback)
        : OperatorInterface(nullptr), _transaction_manager(transaction_manager),
          _transaction_callback(transaction_callback)
    {
    }
    ~BeginTransactionOperator() override = default;

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
    const table::Schema _schema = {};
    concurrency::TransactionManager &_transaction_manager;
    concurrency::TransactionCallback &_transaction_callback;
};

class AbortTransactionOperator : public OperatorInterface
{
  public:
    AbortTransactionOperator(concurrency::TransactionManager &transaction_manager,
                             concurrency::Transaction *transaction,
                             concurrency::TransactionCallback &transaction_callback)
        : OperatorInterface(transaction), _transaction_manager(transaction_manager),
          _transaction_callback(transaction_callback)
    {
    }
    ~AbortTransactionOperator() override = default;

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
    const table::Schema _schema = {};
    concurrency::TransactionManager &_transaction_manager;
    concurrency::TransactionCallback &_transaction_callback;
};

class CommitTransactionOperator : public OperatorInterface
{
  public:
    CommitTransactionOperator(concurrency::TransactionManager &transaction_manager,
                              concurrency::Transaction *transaction,
                              concurrency::TransactionCallback &transaction_callback)
        : OperatorInterface(transaction), _transaction_manager(transaction_manager),
          _transaction_callback(transaction_callback)
    {
    }
    ~CommitTransactionOperator() override = default;

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
    const table::Schema _schema = {};
    concurrency::TransactionManager &_transaction_manager;
    concurrency::TransactionCallback &_transaction_callback;
};
} // namespace beedb::execution