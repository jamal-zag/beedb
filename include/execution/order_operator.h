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

#include "unary_operator.h"
#include <cstdint>
#include <memory>
#include <table/memory_table.h>
#include <table/schema.h>
#include <utility>
#include <vector>

namespace beedb::execution
{
/**
 * Comparator for comparing two tuples during
 * sort.
 */
class TupleComparator
{
  public:
    explicit TupleComparator(const std::vector<std::pair<std::uint32_t, bool>> &indices) : _indices(indices)
    {
    }
    ~TupleComparator() = default;

    bool operator()(const table::Tuple &left, const table::Tuple &right) const
    {
        for (auto &[index, is_ascending] : _indices)
        {
            const auto value_left = left.get(index);
            const auto value_right = right.get(index);
            if (is_ascending)
            {
                if (value_left < value_right)
                {
                    return true;
                }
                else if (value_left > value_right)
                {
                    return false;
                }
            }
            else
            {
                if (value_right < value_left)
                {
                    return true;
                }
                else if (value_right > value_left)
                {
                    return false;
                }
            }
        }

        return false;
    }

  private:
    const std::vector<std::pair<std::uint32_t, bool>> &_indices;
};

/**
 * Sorts the result provided by a child using quicksort.
 */
class OrderOperator final : public UnaryOperator
{
  public:
    OrderOperator(concurrency::Transaction *transaction, const table::Schema &schema,
                  std::vector<std::pair<std::uint32_t, bool>> &&order_columns);
    ~OrderOperator() override = default;

    void open() override;

    util::optional<table::Tuple> next() override;

    void close() override;

    [[nodiscard]] const table::Schema &schema() const override
    {
        return _schema;
    }

  private:
    const table::Schema &_schema;
    const std::vector<std::pair<std::uint32_t, bool>> _order_columns;
    std::unique_ptr<table::MemoryTable> _result_table;
    std::size_t _stack_index = 0u;
};
} // namespace beedb::execution
