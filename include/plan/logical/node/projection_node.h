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
#include <sstream>

namespace beedb::plan::logical
{
class ProjectionNode final : public UnaryNode
{
  public:
    explicit ProjectionNode(Schema &&schema) : UnaryNode("Projection"), _schema(std::move(schema))
    {
    }
    ~ProjectionNode() override = default;

    const Schema &check_and_emit_schema(TableMap &tables) override
    {
        const auto &child_schema = child()->check_and_emit_schema(tables);

        // TODO: Check
        for (auto &term : _schema)
        {
            if (term.is_attribute() && term.is_generated() == false)
            {
                tables.check_and_replace_table(term.get<expression::Attribute>());
            }
        }

        // TODO: Rebuild schema or use own schema just for order?
        Schema real_schema;
        for (auto &term : _schema)
        {
            if (term.is_attribute())
            {
                auto &attribute = term.get<expression::Attribute>();
                const auto asterisk_has_table = attribute.table_name().has_value();
                if (attribute.is_asterisk())
                {
                    for (const auto &child_term : child_schema)
                    {
                        if (asterisk_has_table == false)
                        {
                            real_schema.push_back(child_term);
                        }
                        else if (child_term.is_attribute() &&
                                 child_term.get<expression::Attribute>().table_name() == attribute.table_name())
                        {
                            real_schema.emplace_back(expression::Attribute{child_term.get<expression::Attribute>(),
                                                                           attribute.is_print_table_name()});
                        }
                    }
                }
                else
                {
                    real_schema.emplace_back(std::move(term));
                }
            }
            else
            {
                real_schema.emplace_back(std::move(term));
            }
        }

        _schema = std::move(real_schema);

        return _schema;
    }

    [[nodiscard]] const Schema &schema() const override
    {
        return _schema;
    }

  private:
    Schema _schema;
};
} // namespace beedb::plan::logical