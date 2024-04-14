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

#include "node_interface.h"

namespace beedb::plan::logical
{
class AggregationNode final : public UnaryNode
{
  public:
    AggregationNode(std::vector<std::unique_ptr<expression::Operation>> &&aggregations,
                    std::vector<expression::Term> &&group_by)
        : UnaryNode("Aggregation"), _aggregation_expressions(std::move(aggregations)),
          _group_expressions(std::move(group_by))
    {
    }
    ~AggregationNode() override = default;

    [[nodiscard]] const std::vector<std::unique_ptr<expression::Operation>> &aggregation_expressions() const
    {
        return _aggregation_expressions;
    }
    [[nodiscard]] const std::vector<expression::Term> &group_expressions() const
    {
        return _group_expressions;
    }

    const Schema &check_and_emit_schema(TableMap &tables) override
    {
        _schema.clear();

        child()->check_and_emit_schema(tables);

        for (auto &group : _group_expressions)
        {
            tables.check_and_replace_table(group.get<expression::Attribute>());
        }

        for (const auto &aggregation : _aggregation_expressions)
        {
            if (aggregation->result().has_value())
            {
                _schema.push_back(aggregation->result().value());
            }
        }

        std::copy(_group_expressions.begin(), _group_expressions.end(), std::back_inserter(_schema));
        return _schema;
    }

    [[nodiscard]] const Schema &schema() const override
    {
        return _schema;
    }

  private:
    std::vector<std::unique_ptr<expression::Operation>> _aggregation_expressions;
    std::vector<expression::Term> _group_expressions;
    Schema _schema;
};
} // namespace beedb::plan::logical