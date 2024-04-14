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

#include "binary_operator.h"
#include "predicate_matcher.h"
#include <string>
#include <table/memory_table.h>
#include <table/schema.h>
#include <table/tuple.h>
#include <table/value.h>
#include <utility>
#include <vector>

namespace beedb::execution
{
/**
 * Joins two sources by outer-looping over the left
 * and inner looping over the right children tuples.
 */
class NestedLoopsJoinOperator final : public BinaryOperator
{
  public:
    NestedLoopsJoinOperator(concurrency::Transaction *transaction, table::Schema &&schema,
                            std::unique_ptr<PredicateMatcherInterface> &&predicate_matcher);
    ~NestedLoopsJoinOperator() override = default;

    void open() override;
    util::optional<table::Tuple> next() override;
    void close() override;

    [[nodiscard]] const table::Schema &schema() const override
    {
        return _schema;
    }

  private:
    const table::Schema _schema;
    std::unique_ptr<PredicateMatcherInterface> _predicate_matcher;
    util::optional<table::Tuple> _next_left_tuple;

    [[nodiscard]] bool matches(const table::Tuple &left, const table::Tuple &right)
    {
        return this->_predicate_matcher->matches(left, right);
    }
};
} // namespace beedb::execution
