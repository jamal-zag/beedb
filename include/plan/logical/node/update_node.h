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
class UpdateNode final : public UnaryNode
{
  public:
    explicit UpdateNode(std::vector<std::pair<expression::Attribute, std::unique_ptr<expression::Operation>>> &&updates)
        : UnaryNode("Update"), _updates(std::move(updates))
    {
    }
    ~UpdateNode() override = default;

    [[nodiscard]] const std::string &table_name() const
    {
        return _table_name;
    }

    [[nodiscard]] const std::vector<std::pair<expression::Attribute, std::unique_ptr<expression::Operation>>> &updates()
        const
    {
        return _updates;
    }

    const Schema &check_and_emit_schema(TableMap &tables) override
    {
        _schema.clear();

        const auto &child = UnaryNode::child()->check_and_emit_schema(tables);

        for (auto &update : _updates)
        {
            tables.check_and_replace_table(std::get<0>(update));
            tables.check_and_replace_table(std::get<1>(update));
        }

        const auto all_tables = tables.tables();
        _table_name = all_tables.front();

        return _schema;
    }

    [[nodiscard]] const Schema &schema() const override
    {
        return _schema;
    }

  private:
    Schema _schema;
    std::string _table_name;
    std::vector<std::pair<expression::Attribute, std::unique_ptr<expression::Operation>>> _updates;
};
} // namespace beedb::plan::logical