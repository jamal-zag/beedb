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
class ArithmeticNode final : public UnaryNode
{
  public:
    explicit ArithmeticNode(std::unique_ptr<expression::Operation> &&expression)
        : UnaryNode("Arithmetic"), _expression(std::move(expression))
    {
    }
    ~ArithmeticNode() override = default;

    [[nodiscard]] const std::unique_ptr<expression::Operation> &expression() const
    {
        return _expression;
    }

    const Schema &check_and_emit_schema(TableMap &tables) override
    {
        _schema.clear();

        const auto &child_schema = child()->check_and_emit_schema(tables);

        // TODO: Check
        tables.check_and_replace_table(_expression);

        std::copy(child_schema.begin(), child_schema.end(), std::back_inserter(_schema));
        if (_expression->result().has_value())
        {
            _schema.push_back(_expression->result().value());
        }
        return _schema;
    }

    [[nodiscard]] const Schema &schema() const override
    {
        return _schema;
    }

  private:
    std::unique_ptr<expression::Operation> _expression{};
    Schema _schema;
};
} // namespace beedb::plan::logical