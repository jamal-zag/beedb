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
#include <concurrency/transaction.h>
#include <table/schema.h>
#include <table/tuple.h>
#include <util/optional.h>

namespace beedb::execution
{
/**
 * Interface for all physical execution operators.
 * The interface is volcano-style (using open, next, and close).
 */
class OperatorInterface
{
  public:
    explicit OperatorInterface(concurrency::Transaction *transaction) : _transaction(transaction)
    {
    }
    virtual ~OperatorInterface() = default;

    virtual void open() = 0;
    virtual util::optional<table::Tuple> next() = 0;
    virtual void close() = 0;

    [[nodiscard]] virtual bool yields_data() const
    {
        return true;
    }

    [[nodiscard]] virtual const table::Schema &schema() const = 0;

  protected:
    [[nodiscard]] concurrency::Transaction *transaction() const
    {
        return _transaction;
    }

  private:
    concurrency::Transaction *_transaction;
};
} // namespace beedb::execution