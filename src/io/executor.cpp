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

#include <chrono>
#include <exception/concurrency_exception.h>
#include <exception/exception.h>
#include <io/executor.h>
#include <iostream>
#include <parser/sql_parser.h>
#include <plan/logical/builder.h>
#include <plan/optimizer/optimizer.h>
#include <plan/physical/builder.h>
#include <util/clock.h>

using namespace beedb::io;

ExecutionResult Executor::execute(const Query &query, ExecutionCallback &execution_callback,
                                  concurrency::TransactionCallback &transaction_callback)
{
    std::chrono::milliseconds planning_time{}, execution_time{};

    try
    {
        bool is_single_transaction = false;

        ////////////////////////////////////////////////////////////////////////
        /// \brief parser this object offers all components of the posed query,
        /// to construct a (logical) plan.
        const auto ast = parser::SQLParser{}.parse(std::string{query.query_string});

        ////////////////////////////////////////////////////////////////////////
        /// \brief Start a single transaction around this query if no
        /// transaction was started and this query is no (begin|commit|abort)
        /// transaction query.
        if (this->_transaction == nullptr && typeid(*ast) != typeid(parser::TransactionStatement))
        {
            is_single_transaction = true;
            this->_transaction = this->_database.transaction_manager().new_transaction();
            transaction_callback.on_begin(this->_transaction);
        }

        util::Clock planning_clock{}; // we do not measure parsing time
        const auto before_query_evicted_frames = this->_database.buffer_manager().evicted_frames();

        ////////////////////////////////////////////////////////////////////////
        /// \brief logical_plan holds the canonical plan, created from the query.
        /// This plan has no optimizations at all.
        auto logical_plan = beedb::plan::logical::Builder::build(this->_database, ast);

        /// In case this is a SELECT query and optimization is enabled...
        if (typeid(*ast) == typeid(parser::SelectQuery) &&
            !(this->_database.config()[Config::k_OptimizationDisableOptimization]))
        {
            ////////////////////////////////////////////////////////////////////////
            /// \brief optimizer holds a reference to the canonical plan and can create a new, optimized plan
            /// \brief optimized_plan might hold a "more efficient" plan, could be empty though
            auto optimizer = plan::logical::CompleteOptimizer{this->_database};
            logical_plan = optimizer.optimize(std::move(logical_plan));
        }

        auto count_tuples = 0u;
        if (query.explain == Query::ExplainLevel::Plan)
        {
            planning_time = planning_clock.end();
            execution_callback.on_plan(logical_plan);
        }
        else
        {
            ////////////////////////////////////////////////////////////////////////
            /// create the physical plan from the logical one:
            auto plan = beedb::plan::physical::Builder::build(this->_database, this->_transaction, transaction_callback,
                                                              true, logical_plan);
            planning_time = planning_clock.end();

            util::Clock execution_clock{};
            count_tuples = plan.execute(execution_callback);
            execution_time = execution_clock.end();
        }

        ////////////////////////////////////////////////////////////////////////
        /// \brief If we started the transaction only for this query, commit
        ///        the transaction directly.
        if (is_single_transaction)
        {
            const auto committed = this->_database.transaction_manager().commit(*this->_transaction);
            transaction_callback.on_end(this->_transaction, committed);

            if (committed == false)
            {
                throw beedb::exception::AbortTransactionException();
            }
        }

        const auto final_evicted_frames =
            this->_database.buffer_manager().evicted_frames() - before_query_evicted_frames;

        return ExecutionResult{count_tuples, planning_time, execution_time, final_evicted_frames};
    }
    catch (beedb::exception::AbortTransactionException &e)
    {
        this->_database.transaction_manager().abort(*this->_transaction);
        return ExecutionResult{std::string(e.what())};
    }
    catch (std::runtime_error &e)
    {
        return ExecutionResult{std::string(e.what())};
    }
    catch (beedb::exception::DatabaseException &e)
    {
        return ExecutionResult{std::string(e.what())};
    }
}

ExecutionResult Executor::execute(plan::physical::Plan &plan, ExecutionCallback &execution_callback)
{
    std::chrono::milliseconds execution_time{};
    try
    {
        util::Clock execution_clock{};
        const auto count_tuples = plan.execute(execution_callback);
        execution_time = execution_clock.end();

        return {count_tuples, std::chrono::milliseconds{0}, execution_time, 0u};
    }
    catch (std::runtime_error &e)
    {
        return ExecutionResult{std::string(e.what())};
    }
    catch (beedb::exception::DatabaseException &e)
    {
        return ExecutionResult{std::string(e.what())};
    }
}
