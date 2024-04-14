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

#include <exception/concurrency_exception.h>
#include <execution/transaction_operator.h>

using namespace beedb::execution;

void BeginTransactionOperator::open()
{
}

beedb::util::optional<beedb::table::Tuple> BeginTransactionOperator::next()
{
    auto *transaction = this->_transaction_manager.new_transaction();
    this->_transaction_callback.on_begin(transaction);
    return {};
}

void BeginTransactionOperator::close()
{
}

void AbortTransactionOperator::open()
{
}

beedb::util::optional<beedb::table::Tuple> AbortTransactionOperator::next()
{
    this->_transaction_manager.abort(*this->transaction());
    this->_transaction_callback.on_end(this->transaction(), false);
    return {};
}

void AbortTransactionOperator::close()
{
}

void CommitTransactionOperator::open()
{
}

beedb::util::optional<beedb::table::Tuple> CommitTransactionOperator::next()
{
    const auto success = this->_transaction_manager.commit(*this->transaction());
    this->_transaction_callback.on_end(this->transaction(), success);
    if (success == false)
    {
        throw exception::AbortTransactionException();
    }
    return {};
}

void CommitTransactionOperator::close()
{
}