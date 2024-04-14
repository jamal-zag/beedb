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
class InsertNode final : public NotSchematizedNode
{
  public:
    InsertNode(Database &database, std::string &&table_name, std::vector<std::string> &&column_names,
               std::vector<std::vector<table::Value>> &&value_lists)
        : NotSchematizedNode("Insert"), _database(database), _table_name(std::move(table_name)),
          _column_names(std::move(column_names)), _value_lists(std::move(value_lists))
    {
    }
    ~InsertNode() override = default;

    [[nodiscard]] const std::string &table_name() const
    {
        return _table_name;
    }
    [[nodiscard]] const std::vector<std::string> &column_names() const
    {
        return _column_names;
    }
    [[nodiscard]] const std::vector<std::vector<table::Value>> &value_lists() const
    {
        return _value_lists;
    }
    [[nodiscard]] std::vector<std::vector<table::Value>> &value_lists()
    {
        return _value_lists;
    }

    const Schema &check_and_emit_schema(TableMap &tables) override
    {
        if (_database.table_exists(_table_name) == false)
        {
            throw exception::TableNotFoundException(_table_name);
        }

        const auto &table_schema = _database.table(_table_name)->schema();
        for (const auto &column_name : _column_names)
        {
            if (table_schema.contains(column_name) == false)
            {
                throw exception::ColumnNotFoundException(_table_name, column_name);
            }
        }

        for (const auto &term : table_schema.terms())
        {
            const auto &column_name = term.get<expression::Attribute>().column_name();
            const auto column = std::find(_column_names.cbegin(), _column_names.cend(), column_name);
            if (column == _column_names.cend())
            {
                throw exception::ColumnCanNotBeNull(_table_name, column_name);
            }
        }

        return NotSchematizedNode::check_and_emit_schema(tables);
    }

  private:
    Database &_database;
    std::string _table_name;
    std::vector<std::string> _column_names;
    std::vector<std::vector<table::Value>> _value_lists;
};
} // namespace beedb::plan::logical