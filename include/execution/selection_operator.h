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

#include "predicate_matcher.h"
#include "unary_operator.h"
#include <cstdint>
#include <memory>
#include <string>
#include <table/column.h>
#include <table/schema.h>
#include <table/value.h>

namespace beedb::execution
{
/**
 * Selects tuples matching a given predicate.
 * Some tuples may be filtered.
 */
class SelectionOperator final : public UnaryOperator
{
  public:
    SelectionOperator(concurrency::Transaction *transaction, const table::Schema &schema,
                      std::unique_ptr<PredicateMatcherInterface> &&predicate_matcher);

    ~SelectionOperator() override = default;

    void open() override;

    util::optional<table::Tuple> next() override;

    void close() override;

    [[nodiscard]] const table::Schema &schema() const override
    {
        return _schema;
    }

  private:
    const table::Schema &_schema;
    std::unique_ptr<PredicateMatcherInterface> _predicate_matcher;

    [[nodiscard]] bool matches(const table::Tuple &tuple)
    {
        return this->_predicate_matcher->matches(tuple);
    }
};
} // namespace beedb::execution
