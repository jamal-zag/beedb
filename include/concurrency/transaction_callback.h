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
#include "transaction.h"
#include <functional>

namespace beedb::concurrency
{
/**
 * The transaction callback will be called when a transaction begins
 * and when the transaction was aborted or committed.
 */
class TransactionCallback
{
  public:
    TransactionCallback() = default;
    virtual ~TransactionCallback() = default;

    /**
     * Will be called when a transaction starts.
     * @param transaction Transaction that started.
     */
    virtual void on_begin(Transaction *transaction) = 0;

    /**
     * Will be called when a transaction ends. This may be
     * a successful or not successful commit or abort.
     * @param transaction Transaction that ends.
     * @param successful True, when successful committed.
     */
    virtual void on_end(Transaction *transaction, const bool successful) = 0;
};

class SilentTransactionCallback : public TransactionCallback
{
  public:
    SilentTransactionCallback() = default;
    ~SilentTransactionCallback() override = default;

    void on_begin(Transaction *) override
    {
    }

    void on_end(Transaction *, const bool) override
    {
    }
};

/**
 * This implementation of a transaction callback takes two lambdas
 * that will be invoked on start and end of a transaction.
 */
class FunctionTransactionCallback : public TransactionCallback
{
  public:
    FunctionTransactionCallback(std::function<void(Transaction *)> &&begin_callback,
                                std::function<void(Transaction *, const bool)> &&end_callback)
        : _begin_transaction_callback(std::move(begin_callback)), _end_transaction_callback(std::move(end_callback))
    {
    }

    ~FunctionTransactionCallback() override = default;

    void on_begin(Transaction *transaction) override
    {
        _begin_transaction_callback(transaction);
    }

    void on_end(Transaction *transaction, const bool successful) override
    {
        _end_transaction_callback(transaction, successful);
    }

  private:
    std::function<void(Transaction *)> _begin_transaction_callback;
    std::function<void(Transaction *, const bool)> _end_transaction_callback;
};
} // namespace beedb::concurrency